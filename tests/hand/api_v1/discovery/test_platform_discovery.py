"""Cross-backend identity, reconstruction, deadline and cleanup integration."""

from dataclasses import replace

import can
import pytest

from linkerbot_hand import (
    DiscoveryChannel,
    HandSide,
    MiniCANFDDiscoverySource,
    PCANDiscoverySource,
    SLCANDiscoverySource,
    TransportKind,
    discover_hands,
)
from linkerbot_hand.discovery import sources
from linkerbot_transport import TransportState
from linkerbot_transport.can import CANTransport
from linkerbot_transport.canfd import CANFDTransport, PythonCANFDTransport
from linkerbot_transport.testing import FakeDispatcher
from tests.hand.api_v1.runtime.classic_can_fake import make_script
from tests.hand.api_v1.runtime.l30_canfd_fake import L30DeviceScript
from tests.hand.api_v1.runtime.o20_canfd_fake import O20DeviceScript
from tests.hand.api_v1.runtime.o30_canfd_fake import O30DeviceScript

from .test_discovery import _FAST, _empty_transport, _fd_channel, _Source
from .test_platform_sources import enumerators  # noqa: F401 - shared pytest fixture


class PeerDispatcher(FakeDispatcher):
    def __init__(self, on_error, script):
        super().__init__(on_error)
        self.script = script

    def send_now(self, message):
        super().send_now(message)
        request = message
        if isinstance(message, can.Message) and message.arbitration_id == 0xFF:
            request = can.Message(
                arbitration_id=self.script.arbitration_id,
                data=message.data,
                is_extended_id=False,
            )
        for response in self.script.respond(request):
            self.emit(response)


def attach_peer(monkeypatch, script):
    dispatchers = []

    def dispatcher_factory(on_error):
        dispatcher = PeerDispatcher(on_error, script)
        dispatchers.append(dispatcher)
        return dispatcher

    for name, cls in (
        ("CANTransport", CANTransport),
        ("PythonCANFDTransport", PythonCANFDTransport),
        ("CANFDTransport", CANFDTransport),
    ):

        def constructor(*args, cls=cls, **kwargs):
            return cls(*args, **kwargs, dispatcher_factory=dispatcher_factory)

        monkeypatch.setattr(sources, name, constructor)
    return dispatchers


@pytest.mark.parametrize("model", ["L6", "O6", "L20Lite", "L20"])
@pytest.mark.usefixtures("enumerators")
def test_all_classic_models_discover_and_reconstruct_through_pcan(monkeypatch, model):
    script = make_script(model, HandSide.LEFT)
    dispatchers = attach_peer(monkeypatch, script)
    report = discover_hands(_FAST, source=PCANDiscoverySource())
    candidate = report.require_one()
    assert candidate.model == model
    assert candidate.transport.connection_key.interface_type == "pcan"
    assert all(d.stop_count == 1 for d in dispatchers)
    count = len(dispatchers)
    hand = candidate.create_hand_device()
    assert len(dispatchers) == count
    hand.open()
    try:
        assert len(dispatchers) == count + 1
    finally:
        hand.close()


@pytest.mark.parametrize("model", ["O20", "O30", "L30"])
@pytest.mark.parametrize(
    "source_type", [SLCANDiscoverySource, MiniCANFDDiscoverySource]
)
@pytest.mark.usefixtures("enumerators")
def test_all_fd_models_discover_and_reconstruct_through_usb_sources(
    monkeypatch, model, source_type
):
    script = {
        "O20": lambda: O20DeviceScript(HandSide.LEFT),
        "O30": O30DeviceScript,
        "L30": L30DeviceScript,
    }[model]()
    dispatchers = attach_peer(monkeypatch, script)
    report = discover_hands(_FAST, source=source_type())
    candidate = report.require_one()
    assert candidate.model == model
    assert candidate.wire_protocol is TransportKind.CAN_FD
    assert all(d.stop_count == 1 for d in dispatchers)
    key = candidate.transport.connection_key
    assert candidate.transport.create_transport().pool_key == key
    count = len(dispatchers)
    hand = candidate.create_hand_device()
    assert len(dispatchers) == count
    hand.open()
    try:
        assert len(dispatchers) == count + 1
    finally:
        hand.close()


@pytest.mark.parametrize("discovery_clock", ("logical", "real"), indirect=True)
def test_total_budget_stops_more_sends_and_closes_transport(discovery_clock):
    transport = _empty_transport()
    channel = DiscoveryChannel("fake", "test", TransportKind.CAN)
    source = _Source(channel, {TransportKind.CAN: lambda: transport})
    config = replace(_FAST, response_timeout_s=1.0, total_timeout_s=0.06)
    report = discover_hands(config, source=source)
    assert len(transport.sent) <= 1
    assert any(f.code == "scan_timeout" for f in report.failures)
    assert transport.get_health().state is TransportState.CLOSED
    assert report.elapsed_s < 0.5


def test_close_failure_keeps_cleanup_reachable_and_prevents_fd_open():
    transport = _empty_transport()
    original_open = transport.open

    def open_failing_close():
        original_open()
        transport.current_dispatcher.stop_error = OSError("USB close failed")

    transport.open = open_failing_close
    source = _Source(_fd_channel(), {TransportKind.CAN: lambda: transport})
    report = discover_hands(_FAST, source=source)
    (failure,) = report.failures
    assert failure.operation == "close_classic_probe_transport"
    assert [wire for wire, _ in source.created] == [TransportKind.CAN]
    assert transport.get_health().state is TransportState.FAULTED
    failure.cleanup()
    assert transport.get_health().state is TransportState.CLOSED


@pytest.mark.usefixtures("enumerators")
def test_default_entry_keeps_one_backend_failure_and_probes_another(monkeypatch):
    monkeypatch.setattr(sources.platform, "system", lambda: "Windows")

    def broken(self, config):
        raise OSError("PCAN driver missing")

    monkeypatch.setattr(PCANDiscoverySource, "enumerate_channels", broken)
    attach_peer(monkeypatch, O30DeviceScript())
    report = discover_hands(replace(_FAST, backends=("pcan", "slcan")))
    assert report.require_one().model == "O30"
    assert report.failures[0].backend == "pcan"


def test_exhausted_enumeration_budget_never_opens_transport():
    from linkerbot_hand.discovery import _budget

    channel = DiscoveryChannel("fake", "test", TransportKind.CAN)

    class SlowSource:
        def list_channels(self, config):
            _budget.deadline.set(0.0)
            return (channel,)

        def create_transport(self, channel, wire):
            pytest.fail("deadline expired during enumeration")

    report = discover_hands(_FAST, source=SlowSource())
    assert report.channels == (channel,)
    assert not report.candidates
    assert [f.code for f in report.failures] == ["scan_timeout"]
    assert _budget.deadline.get() is None
