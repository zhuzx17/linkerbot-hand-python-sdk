"""Receive timing must bound transactions without discarding timely backlog."""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import replace
from types import SimpleNamespace

import pytest

from linkerbot_hand import ClosedError, CommandSemantic, get_declared_capabilities
from linkerbot_hand.protocols.base import ProtocolTransaction, ResponseProgress
from linkerbot_hand.protocols.common.classic_can import (
    build_classic_message,
    classic_route,
)
from linkerbot_hand.protocols.l6.transactions import L6ProtocolBackend
from linkerbot_hand.runtime._errors import ResponseTimeoutError
from linkerbot_hand.runtime._route_mailbox import InboundMessage
from linkerbot_hand.runtime._transaction_runner import (
    PendingTransaction,
    TransactionRunner,
)
from linkerbot_transport import SendReceipt
from linkerbot_transport.testing import FakeTransport


class RunnerRig:
    def __init__(self):
        self.now = 1.0
        self.incoming = deque()
        self.published = []
        self.reports = []
        self.errors = []
        self.stop = threading.Event()
        self.receive_hook = None
        self.wait_hook = None
        self.capabilities = get_declared_capabilities("L6", side="left")
        self.clock = SimpleNamespace(
            monotonic=lambda: self.now, time=lambda: self.now, wait=self.wait
        )
        self.runner = TransactionRunner(
            binding=None,
            backend=SimpleNamespace(decode_report=self.decode_report),
            endpoint_id="response-window-test",
            clock=self.clock,
            wake=threading.Event(),
            stop=self.stop,
            recovery_requested=lambda: False,
            next_message=self.receive,
            receiver_error=lambda: None,
            report_error=self.errors.append,
            snapshot_store=SimpleNamespace(publish=self.publish),
            metrics=SimpleNamespace(
                received=lambda **_: None,
                sent=lambda **_: None,
                transaction_completed=lambda **_: None,
            ),
        )

    def receive(self):
        if self.receive_hook is not None:
            return self.receive_hook()
        if not self.incoming:
            return None
        inbound = self.incoming.popleft()
        self.now = max(self.now, inbound.received_at_s)
        return inbound

    def wait(self, _event, timeout):
        if self.wait_hook is not None:
            self.wait_hook()
        else:
            assert timeout is not None and timeout > 0
            self.now += timeout

    def decode_report(self, message, _capabilities):
        self.reports.append(message)
        return ()

    def publish(self, payload, **_kwargs):
        self.published.append(payload)
        return payload

    def complete(self, transaction, *, receipt=None, response_timeout_s=None):
        if receipt is None:
            receipt = SendReceipt.completed(monotonic=lambda: 1.0)
        return self.runner.complete(
            PendingTransaction(
                transaction=transaction,
                receipt=receipt,
                capabilities=self.capabilities,
                publish_generation=1,
                started_at_s=1.0,
                deadline_at_s=None,
                priority=100,
                response_timeout_s=response_timeout_s,
            )
        )


def read_transaction():
    def consume(message):
        if message == "malformed":
            raise ValueError("invalid response")
        return ResponseProgress(
            matched=message in ("partial", "complete"), complete=message == "complete"
        )

    return ProtocolTransaction(
        key="read",
        correlation_key="read",
        request_messages=("request",),
        response_timeout_s=0.02,
        estimated_duration_s=0.001,
        response_consumer=consume,
        late_response_guard_s=0.01,
    )


@pytest.mark.parametrize("traffic", ("unrelated", "partial", "quarantined"))
def test_continuous_receive_traffic_does_not_extend_response_window(traffic):
    rig = RunnerRig()
    if traffic == "quarantined":
        # Establish another timed-out transaction with an incomplete collector.
        old = ProtocolTransaction(
            key="old",
            correlation_key="old",
            request_messages=("old",),
            response_timeout_s=0.001,
            estimated_duration_s=0.001,
            late_response_guard_s=0.2,
            response_consumer=lambda message: ResponseProgress(
                matched=message == traffic
            ),
        )
        with pytest.raises(ResponseTimeoutError):
            rig.complete(old)

    def receive():
        rig.now += 0.005
        assert rig.now < 1.2, "traffic kept the actor past its response deadline"
        return InboundMessage(traffic, rig.now)

    rig.receive_hook = receive
    with pytest.raises(ResponseTimeoutError, match="protocol response"):
        rig.complete(read_transaction())
    assert rig.now < 1.03
    assert rig.published == []


@pytest.mark.parametrize("delay", (0.02, 0.03))
def test_complete_reply_at_or_after_deadline_cannot_succeed(delay):
    rig = RunnerRig()
    rig.incoming.append(InboundMessage("complete", 1.0 + delay))
    with pytest.raises(ResponseTimeoutError):
        rig.complete(read_transaction())
    assert rig.published == []


def test_timely_queued_response_can_complete_after_actor_resumes_late():
    rig = RunnerRig()
    rig.now = 1.2
    rig.incoming.extend(
        [
            InboundMessage("unrelated", 1.005),
            InboundMessage("partial", 1.01),
            InboundMessage("complete", 1.019),
        ]
    )
    rig.complete(read_transaction())
    assert rig.reports == ["unrelated"]


def test_incomplete_timely_backlog_still_times_out():
    rig = RunnerRig()
    rig.now = 1.2
    rig.incoming.extend(
        [InboundMessage("unrelated", 1.005), InboundMessage("partial", 1.01)]
    )
    with pytest.raises(ResponseTimeoutError):
        rig.complete(read_transaction())
    assert rig.now == 1.2


def test_late_unrelated_report_is_preserved_when_read_times_out():
    rig = RunnerRig()
    rig.incoming.append(InboundMessage("unrelated", 1.03))
    with pytest.raises(ResponseTimeoutError):
        rig.complete(read_transaction())
    assert rig.reports == ["unrelated"]


def test_malformed_late_reply_cannot_replace_timeout_with_parser_error():
    rig = RunnerRig()
    rig.incoming.append(InboundMessage("malformed", 1.03))
    with pytest.raises(ResponseTimeoutError):
        rig.complete(read_transaction())
    assert len(rig.errors) == 1
    assert isinstance(rig.errors[0], ValueError)


def test_timeout_override_applies_to_receive_timestamp_cutoff():
    rig = RunnerRig()
    rig.incoming.append(InboundMessage("complete", 1.03))
    rig.complete(read_transaction(), response_timeout_s=0.04)


def test_close_interrupts_even_timely_receive_backlog():
    rig = RunnerRig()

    def receive():
        rig.stop.set()
        return InboundMessage("partial", 1.001)

    rig.receive_hook = receive
    with pytest.raises(ClosedError):
        rig.complete(read_transaction())


def test_late_l6_write_reply_does_not_publish_configuration():
    rig = RunnerRig()
    transaction = L6ProtocolBackend(rig.capabilities.side).command_transaction(
        CommandSemantic.CURRENT_LIMITS, (20,) * 6, rig.capabilities
    )
    reply = build_classic_message(
        classic_route(rig.capabilities.side), bytes((2,) + (9,) * 6)
    )
    rig.incoming.append(InboundMessage(reply, 1.2))
    with pytest.raises(ResponseTimeoutError):
        rig.complete(transaction)
    assert rig.published == []


def l6_limit_write(rig):
    transaction = L6ProtocolBackend(rig.capabilities.side).command_transaction(
        CommandSemantic.CURRENT_LIMITS, (20,) * 6, rig.capabilities
    )
    reply = build_classic_message(
        classic_route(rig.capabilities.side), bytes((2,) + (9,) * 6)
    )
    return transaction, reply


@pytest.mark.parametrize(
    "dispatch_state", ("unclaimed", "claimed", "dispatched", "claim_during_pop")
)
def test_presend_l6_reply_cannot_acknowledge_or_publish_a_new_write(dispatch_state):
    rig = RunnerRig()
    transaction, reply = l6_limit_write(rig)
    receipt = SendReceipt(monotonic=rig.clock.monotonic)
    received_at = 1.05 if dispatch_state in ("unclaimed", "claim_during_pop") else 0.9
    rig.incoming.append(InboundMessage(reply, received_at))
    if dispatch_state in ("claimed", "dispatched"):
        receipt._claim()
    if dispatch_state == "dispatched":
        receipt._dispatch()
    if dispatch_state == "claim_during_pop":
        # The batch is claimed after this frame arrived, while the actor pops it.
        def receive():
            rig.receive_hook = None
            rig.now = 1.1
            receipt._claim()
            return rig.incoming.popleft()

        rig.receive_hook = receive

    def dispatch():
        rig.wait_hook = None
        rig.now = 1.1
        receipt._claim()
        receipt._dispatch()

    rig.wait_hook = dispatch
    with pytest.raises(ResponseTimeoutError):
        rig.complete(transaction, receipt=receipt)
    assert rig.published == []


def test_presend_malformed_frame_does_not_poison_the_new_collector():
    rig = RunnerRig()
    receipt = SendReceipt(monotonic=rig.clock.monotonic)
    rig.incoming.append(InboundMessage("malformed", 0.9))

    def dispatch():
        rig.wait_hook = None
        rig.now = 1.1
        receipt._claim()
        receipt._dispatch()
        rig.incoming.append(InboundMessage("complete", 1.11))

    rig.wait_hook = dispatch
    rig.complete(read_transaction(), receipt=receipt)
    assert rig.reports == ["malformed"]
    assert rig.errors == []


def test_presend_fragment_cannot_complete_a_new_multipart_response():
    rig = RunnerRig()
    parts = set()

    def consume(message):
        parts.add(message)
        return ResponseProgress(matched=True, complete=parts == {"first", "second"})

    transaction = replace(read_transaction(), response_consumer=consume)
    rig.incoming.extend([InboundMessage("first", 0.9), InboundMessage("second", 1.01)])
    with pytest.raises(ResponseTimeoutError):
        rig.complete(transaction)
    assert parts == {"second"}


@pytest.mark.parametrize("buffered", (False, True))
@pytest.mark.parametrize("fresh_reply", (False, True))
def test_equal_timestamp_stale_reply_cannot_acknowledge_a_new_write(
    buffered, fresh_reply
):
    rig = RunnerRig()
    transaction, reply = l6_limit_write(rig)
    transport = FakeTransport(route_key_of=lambda _: "reply", monotonic=lambda: 1.0)
    transport.open()
    route = transport.subscribe_route("reply")
    receipt = SendReceipt(monotonic=lambda: 1.0)
    try:
        transport.emit(reply)
        stale = route.receive(timeout_s=0)
        receipt._claim()
        rig.incoming.append(
            InboundMessage(reply, stale.received_at_s, stale.received_order)
        )
        if fresh_reply:
            transport.emit(reply)
            fresh = route.receive(timeout_s=0)
            assert stale.received_at_s == fresh.received_at_s == receipt.claimed_at_s
            assert stale.received_order < receipt.claimed_order < fresh.received_order
            rig.incoming.append(
                InboundMessage(reply, fresh.received_at_s, fresh.received_order)
            )
        if buffered:

            def dispatch():
                rig.wait_hook = None
                receipt._dispatch()

            rig.wait_hook = dispatch
        else:
            receipt._dispatch()
        if fresh_reply:
            outcome = rig.complete(transaction, receipt=receipt)
            assert outcome.acknowledged
            assert len(rig.published) == 1
        else:
            with pytest.raises(ResponseTimeoutError):
                rig.complete(transaction, receipt=receipt)
            assert rig.published == []
    finally:
        transport.close()


@pytest.mark.parametrize("received_at", (1.0, 1.01))
def test_reply_during_batch_is_valid_and_preserves_the_returned_value(received_at):
    rig = RunnerRig()
    transaction, reply = l6_limit_write(rig)
    receipt = SendReceipt(monotonic=rig.clock.monotonic)
    receipt._claim()
    rig.incoming.append(InboundMessage(reply, received_at))

    def dispatch():
        # Buffered replies remain private until every request has been sent.
        assert rig.published == []
        rig.wait_hook = None
        rig.now = 1.1
        receipt._dispatch()

    rig.wait_hook = dispatch
    outcome = rig.complete(transaction, receipt=receipt)
    assert outcome.acknowledged
    assert len(rig.published) == 1
    assert rig.published[0].raw == (9,) * 6


def test_send_failure_discards_even_a_valid_buffered_reply():
    rig = RunnerRig()
    transaction, reply = l6_limit_write(rig)
    receipt = SendReceipt(monotonic=rig.clock.monotonic)
    receipt._claim()
    rig.incoming.append(InboundMessage(reply, 1.01))
    error = OSError("later frame in batch failed")
    rig.wait_hook = lambda: receipt._fail(error)
    with pytest.raises(OSError) as captured:
        rig.complete(transaction, receipt=receipt)
    assert captured.value is error
    assert rig.published == []


@pytest.mark.parametrize("message", ("complete", "malformed"))
def test_dispatch_completing_during_receive_cannot_bypass_response_deadline(message):
    rig = RunnerRig()
    receipt = SendReceipt(monotonic=rig.clock.monotonic)
    receipt._claim()

    def receive():
        rig.receive_hook = None
        rig.now = 1.005
        receipt._dispatch()
        # The actor resumes after receipt completion and the response cutoff.
        rig.now = 1.04
        return InboundMessage(message, 1.04)

    rig.receive_hook = receive
    with pytest.raises(ResponseTimeoutError):
        rig.complete(read_transaction(), receipt=receipt)
    assert rig.published == []


def test_dispatch_completing_during_receive_preserves_a_timely_reply():
    rig = RunnerRig()
    receipt = SendReceipt(monotonic=rig.clock.monotonic)
    receipt._claim()

    def receive():
        rig.receive_hook = None
        rig.now = 1.005
        receipt._dispatch()
        rig.now = 1.1
        return InboundMessage("complete", 1.015)

    rig.receive_hook = receive
    rig.complete(read_transaction(), receipt=receipt)


def test_each_stage_uses_its_own_claim_as_the_receive_lower_bound():
    rig = RunnerRig()
    first = replace(
        read_transaction(),
        response_consumer=lambda message: ResponseProgress(
            matched=message == "first", complete=message == "first"
        ),
        next_transaction=read_transaction(),
    )
    rig.incoming.extend(
        [InboundMessage("first", 1.01), InboundMessage("complete", 1.02)]
    )

    def submit(*_args, **_kwargs):
        rig.now = 1.1
        return SendReceipt.completed(monotonic=rig.clock.monotonic)

    rig.runner._binding = SimpleNamespace(submit=submit)
    with pytest.raises(ResponseTimeoutError) as captured:
        rig.complete(first)
    assert captured.value.transaction_sent_frames == 2
