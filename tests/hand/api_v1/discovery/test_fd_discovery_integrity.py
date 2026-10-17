"""CAN FD discovery attribution and receive-overflow regressions."""

from __future__ import annotations

from dataclasses import replace

import pytest

from linkerbot_hand import HandSide, TransportKind, discover_hands
from linkerbot_transport import SendReceipt, TransportState
from linkerbot_transport.testing import FakeTransport
from tests.hand.api_v1.runtime.o20_canfd_fake import O20DeviceScript
from tests.hand.api_v1.runtime.o30_canfd_fake import O30DeviceScript

from .test_discovery import (
    _FAST,
    _fd_channel,
    _O20CollisionTransport,
    _route_key,
    _Source,
)


def test_fd_overflow_does_not_verify_a_conflicting_identity() -> None:
    transport = _O20CollisionTransport(
        O20DeviceScript(HandSide.LEFT, serial_number="SN-OLD"),
        O20DeviceScript(HandSide.LEFT, serial_number="SN-NEW"),
    )
    source = _Source(
        replace(_fd_channel(), probe_protocols=(TransportKind.CAN_FD,)),
        {TransportKind.CAN_FD: lambda: transport},
    )

    report = discover_hands(
        replace(_FAST, route_queue_capacity=1),
        source=source,
    )

    assert transport.get_health().state is TransportState.CLOSED
    assert transport.get_health().rx_overflows == 1
    assert not report.verified_candidates
    assert any(f.operation == "probe_o20_identity" for f in report.failures)


class _PreclaimO20Transport(FakeTransport):
    """Emit an old O20 identity before the send receipt is claimed."""

    def __init__(self) -> None:
        super().__init__(route_key_of=_route_key)
        self.old = O20DeviceScript(HandSide.LEFT, serial_number="SN-OLD")
        self.actual = O30DeviceScript()

    def send(self, message):
        receipt = SendReceipt(generation=self.get_health().generation)
        for response in self.old.respond(message):
            self.emit(response)
        receipt._claim()
        FakeTransport.send(self, message).result()
        for response in self.actual.respond(message):
            self.emit(response)
        receipt._dispatch()
        return receipt


@pytest.mark.parametrize("equal_timestamps", (False, True))
def test_fd_preclaim_identity_is_ignored_and_actual_model_is_probed(
    equal_timestamps, monkeypatch
) -> None:
    if equal_timestamps:
        clock = lambda: 10.0
        monkeypatch.setitem(SendReceipt.__init__.__kwdefaults__, "monotonic", clock)
        monkeypatch.setitem(FakeTransport.__init__.__kwdefaults__, "monotonic", clock)
    transport = _PreclaimO20Transport()
    source = _Source(
        replace(_fd_channel(), probe_protocols=(TransportKind.CAN_FD,)),
        {TransportKind.CAN_FD: lambda: transport},
    )

    report = discover_hands(_FAST, source=source)

    assert transport.get_health().state is TransportState.CLOSED
    assert [candidate.model for candidate in report.verified_candidates] == ["O30"]
