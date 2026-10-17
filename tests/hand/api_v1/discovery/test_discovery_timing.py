"""Separate identity semantics from simulated processing and scan timeouts."""

import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from linkerbot_hand import DiscoveryStatus, HandSide, TransportKind, discover_hands
from linkerbot_hand.discovery import _budget, _engine
from linkerbot_transport import RouteSubscription, TransportState
from linkerbot_transport.errors import TimeoutError as RxTimeout
from linkerbot_transport.testing import FakeTransport
from tests.hand.api_v1.runtime.classic_can_fake import (
    ScriptedClassicCANTransport,
    make_script,
)

from ._clock import DiscoveryClock
from .test_discovery import _FAST, _classic_channel, _Source


@pytest.mark.parametrize("model", ("L6", "O6", "L20Lite", "L20"))
def test_fragment_processing_pause_cannot_truncate_synchronous_identity(
    model, monkeypatch, discovery_clock
):
    # A controlled 4 ms processing pause represents descheduling between already
    # queued fragments. It must not turn an identity-semantic test into a race
    # against _FAST's 3 ms receive window.
    wall_clock = DiscoveryClock()
    receive = RouteSubscription.receive
    fragments = []

    def receive_after_processing_pause(subscription, *, timeout_s=None):
        routed = receive(subscription, timeout_s=timeout_s)
        if bytes(routed.message.data)[:1] == b"\xc0":
            fragments.append(routed.message)
            wall_clock.sleep(0.004)
        return routed

    monkeypatch.setattr(RouteSubscription, "receive", receive_after_processing_pause)

    def scan():
        source = _Source(
            _classic_channel(),
            {
                TransportKind.CAN: lambda: ScriptedClassicCANTransport(
                    make_script(model, HandSide.LEFT)
                )
            },
        )
        report = discover_hands(_FAST, source=source)
        assert not report.failures
        assert all(
            transport.get_health().state is TransportState.CLOSED
            and transport.get_health().route_count == 0
            for _, transport in source.created
        )
        return report

    # Negative control: the previous wall-clock setup loses queued fragments
    # deterministically, before reaching the behavior the original test checks.
    with monkeypatch.context() as old_timing:
        old_timing.setattr(_engine, "time", wall_clock)
        old_timing.setattr(_budget, "time", wall_clock)
        partial = scan().require_one(allow_partial=True)
    assert partial.status is DiscoveryStatus.PARTIAL
    assert partial.problems[0].code == "product_code_incomplete"
    assert fragments

    fragments.clear()
    wall_before = wall_clock.monotonic()
    candidate = scan().require_one()
    assert candidate.status is DiscoveryStatus.VERIFIED
    assert candidate.model == model
    assert len(fragments) > 1
    assert wall_clock.monotonic() - wall_before > _FAST.response_timeout_s


@pytest.mark.parametrize("runtime_thread", (False, True))
def test_runtime_receives_and_transport_metadata_keep_real_time(
    monkeypatch, discovery_clock, runtime_thread
):
    transport = FakeTransport()
    transport.open()
    try:
        with transport.subscribe_route("reply") as subscription:
            state_receive = subscription._state.receive
            received_timeouts = []

            def record_wait(timeout_s):
                received_timeouts.append((timeout_s, _budget.deadline.get()))
                return state_receive(timeout_s)

            monkeypatch.setattr(subscription._state, "receive", record_wait)
            logical_before = discovery_clock.monotonic()
            assert _budget.deadline.get() is None
            if runtime_thread:
                # A background runtime thread does not inherit the active
                # discovery budget in its caller's context.
                token = _budget.deadline.set(logical_before + 1)
                try:
                    with ThreadPoolExecutor(max_workers=1) as worker:
                        pending = worker.submit(subscription.receive, timeout_s=0.01)
                        with pytest.raises(RxTimeout):
                            pending.result(timeout=1)
                finally:
                    _budget.deadline.reset(token)
            else:
                with pytest.raises(RxTimeout):
                    subscription.receive(timeout_s=0.01)
            assert received_timeouts == [(0.01, None)]
            assert discovery_clock.monotonic() == logical_before

            before = time.monotonic()
            result = transport.send("request").result()
            transport.emit("reply")
            routed = subscription.receive(timeout_s=1)
            after = time.monotonic()
            assert before <= result.accepted_at_s <= result.claimed_at_s <= after
            assert before <= routed.received_at_s <= after
            assert result.claimed_order < routed.received_order
    finally:
        transport.close()
