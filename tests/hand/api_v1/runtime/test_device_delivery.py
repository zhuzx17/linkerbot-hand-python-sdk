"""Command delivery, cache, and read-coalescing runtime contracts."""

from __future__ import annotations

import threading
import time
from dataclasses import replace

import pytest

from linkerbot_hand.api import (
    AckSupport,
    AngleFeedbackPolicy,
    AngleSample,
    CommandStage,
    DeviceConfig,
    DeviceNotReadyError,
    ErrorCode,
    HandError,
    PollingPlan,
    ProtocolError,
    TimeoutError,
)
from linkerbot_hand.api.enums import DataOrigin
from linkerbot_hand.capabilities.registry import get_declared_capabilities
from linkerbot_hand.capabilities.types import (
    CommandSemantic,
    DeviceCapabilities,
    TelemetrySemantic,
)
from linkerbot_hand.protocols import (
    ProtocolTransaction,
    ResponseProgress,
    SamplePayload,
)

from .factories import (
    hold_next_idle_deadline_check,
    make_hand,
    make_shared_hands,
    opened_runtime,
)
from .fakes import WireMessage, wait_until


def _staged_operation_transaction(
    endpoint_id: str,
    stages: tuple[str, ...],
    *,
    response_timeout_s: float = 0.1,
) -> ProtocolTransaction:
    next_transaction = None
    for stage in reversed(stages):

        def consume(message: object, expected: str = stage) -> ResponseProgress:
            if (
                not isinstance(message, WireMessage)
                or message.kind != "staged_ack"
                or message.semantic != expected
            ):
                return ResponseProgress(matched=False)
            return ResponseProgress(
                matched=True,
                complete=True,
                acknowledged=True,
            )

        next_transaction = ProtocolTransaction(
            key=("staged_operation", stage),
            correlation_key=("staged_operation", stage),
            request_messages=(WireMessage(endpoint_id, "staged_operation", stage),),
            response_timeout_s=response_timeout_s,
            estimated_duration_s=response_timeout_s,
            response_consumer=consume,
            response_acknowledges_command=True,
            next_transaction=next_transaction,
        )
    assert next_transaction is not None
    return next_transaction


def test_acknowledged_command_reports_every_real_delivery_stage() -> None:
    declared = get_declared_capabilities("l6", side="left")
    capabilities = replace(
        declared,
        commands=tuple(
            replace(command, ack_support=AckSupport.SUPPORTED)
            if command.semantic is CommandSemantic.ANGLES
            else command
            for command in declared.commands
        ),
    )
    hand, state, _transport = make_hand(capabilities=capabilities)
    hand.open()
    try:
        handle = hand.set_angles((10,) * 6)

        assert handle.wait(timeout=1) is CommandStage.ACKNOWLEDGED
        assert tuple(item.stage for item in handle.history) == (
            CommandStage.ACCEPTED,
            CommandStage.DISPATCHED,
            CommandStage.ACKNOWLEDGED,
        )
        with state.lock:
            assert state.command_log == [(CommandSemantic.ANGLES, (10,) * 6)]
    finally:
        hand.close()


def test_command_dispatch_does_not_build_transport_health_snapshots() -> None:
    hand, _state, transport = make_hand()
    health_calls = 0
    original_get_health = transport.get_health

    def counted_get_health():
        nonlocal health_calls
        health_calls += 1
        return original_get_health()

    transport.get_health = counted_get_health  # ty: ignore[invalid-assignment]
    hand.open()
    try:
        calls_after_open = health_calls

        handle = hand.set_angles((10,) * 6)

        assert handle.wait(timeout=1) is CommandStage.DISPATCHED
        assert health_calls == calls_after_open
    finally:
        hand.close()


def test_staged_operation_dispatches_only_after_the_final_stage_send() -> None:
    hand, state, transport = make_hand(capabilities=get_declared_capabilities("l30"))
    backend = transport.backends["fake-hand"]
    backend.operation_transaction = (  # ty: ignore[invalid-assignment]
        lambda operation, _capabilities: _staged_operation_transaction(
            backend.endpoint_id,
            ("disable", "unlock", operation),
        )
    )
    original_responses_for = backend.responses_for

    def responses_for(message: WireMessage) -> tuple[WireMessage, ...]:
        if message.kind != "staged_operation":
            return original_responses_for(message)
        with state.lock:
            state.operation_log.append(message.semantic or "")
        return (
            WireMessage(
                backend.route_key,
                "staged_ack",
                message.semantic,
            ),
        )

    backend.responses_for = responses_for  # ty: ignore[invalid-assignment]
    hand.open()
    final_send_started = threading.Event()
    allow_final_send = threading.Event()
    original_send = transport.send

    def send(message: WireMessage):
        if message.kind == "staged_operation" and message.semantic == "calibrate":
            final_send_started.set()
            if not allow_final_send.wait(timeout=1):
                raise RuntimeError("test did not release the final stage")
        return original_send(message)

    transport.send = send  # ty: ignore[invalid-assignment]
    try:
        handle = hand.calibrate()

        assert final_send_started.wait(timeout=1)
        assert handle.stage is CommandStage.ACCEPTED
        with state.lock:
            assert state.operation_log == ["disable", "unlock"]

        allow_final_send.set()
        assert handle.wait(timeout=1) is CommandStage.ACKNOWLEDGED
        assert handle.sent_frames == 3
        assert tuple(item.stage for item in handle.history) == (
            CommandStage.ACCEPTED,
            CommandStage.DISPATCHED,
            CommandStage.ACKNOWLEDGED,
        )
        with state.lock:
            assert state.operation_log == ["disable", "unlock", "calibrate"]
    finally:
        allow_final_send.set()
        transport.send = original_send  # ty: ignore[invalid-assignment]
        hand.close()


def test_staged_operation_timeout_stops_before_the_next_stage() -> None:
    hand, state, transport = make_hand(capabilities=get_declared_capabilities("l30"))
    backend = transport.backends["fake-hand"]
    backend.operation_transaction = (  # ty: ignore[invalid-assignment]
        lambda operation, _capabilities: _staged_operation_transaction(
            backend.endpoint_id,
            ("disable", "unlock", operation),
            response_timeout_s=0.01,
        )
    )
    original_responses_for = backend.responses_for

    def responses_for(message: WireMessage) -> tuple[WireMessage, ...]:
        if message.kind != "staged_operation":
            return original_responses_for(message)
        with state.lock:
            state.operation_log.append(message.semantic or "")
        return ()

    backend.responses_for = responses_for  # ty: ignore[invalid-assignment]
    hand.open()
    try:
        handle = hand.calibrate()

        assert handle.wait(timeout=1) is CommandStage.FAILED
        assert handle.sent_frames == 1
        assert handle.error is not None
        assert handle.error.code is ErrorCode.TIMEOUT
        with state.lock:
            assert state.operation_log == ["disable"]
    finally:
        hand.close()


def test_staged_operation_final_ack_timeout_keeps_cumulative_delivery() -> None:
    hand, state, transport = make_hand(capabilities=get_declared_capabilities("l30"))
    backend = transport.backends["fake-hand"]
    backend.operation_transaction = (  # ty: ignore[invalid-assignment]
        lambda operation, _capabilities: _staged_operation_transaction(
            backend.endpoint_id,
            ("disable", "unlock", operation),
            response_timeout_s=0.01,
        )
    )
    original_responses_for = backend.responses_for

    def responses_for(message: WireMessage) -> tuple[WireMessage, ...]:
        if message.kind != "staged_operation":
            return original_responses_for(message)
        with state.lock:
            state.operation_log.append(message.semantic or "")
        if message.semantic == "calibrate":
            return ()
        return (
            WireMessage(
                backend.route_key,
                "staged_ack",
                message.semantic,
            ),
        )

    backend.responses_for = responses_for  # ty: ignore[invalid-assignment]
    hand.open()
    try:
        handle = hand.calibrate()

        assert handle.wait(timeout=1) is CommandStage.FAILED
        assert handle.sent_frames == 3
        assert handle.error is not None
        assert handle.error.code is ErrorCode.TIMEOUT
        assert tuple(item.stage for item in handle.history) == (
            CommandStage.ACCEPTED,
            CommandStage.DISPATCHED,
            CommandStage.FAILED,
        )
        with state.lock:
            assert state.operation_log == ["disable", "unlock", "calibrate"]
    finally:
        hand.close()


def test_response_before_transport_receipt_preserves_delivery_order() -> None:
    declared = get_declared_capabilities("l6", side="left")
    capabilities = replace(
        declared,
        commands=tuple(
            replace(command, ack_support=AckSupport.SUPPORTED)
            if command.semantic is CommandSemantic.ANGLES
            else command
            for command in declared.commands
        ),
    )
    hand, _state, transport = make_hand(capabilities=capabilities)
    backend = transport.backends["fake-hand"]
    original = backend.command_transaction
    response_consumed = threading.Event()

    def command_transaction(*args, **kwargs):
        transaction = original(*args, **kwargs)
        consumer = transaction.response_consumer
        assert consumer is not None

        def consume(message):
            progress = consumer(message)
            if progress.matched:
                response_consumed.set()
            return progress

        return replace(transaction, response_consumer=consume)

    backend.command_transaction = command_transaction  # ty: ignore[invalid-assignment]

    def wait_for_response() -> None:
        response_consumed.wait(timeout=0.2)

    transport.command_sent_hook = wait_for_response
    hand.open()
    try:
        handle = hand.set_angles((10,) * 6)

        assert handle.wait(timeout=1) is CommandStage.ACKNOWLEDGED
        assert response_consumed.is_set()
        assert tuple(item.stage for item in handle.history) == (
            CommandStage.ACCEPTED,
            CommandStage.DISPATCHED,
            CommandStage.ACKNOWLEDGED,
        )
    finally:
        transport.command_sent_hook = None
        hand.close()


def test_transport_failure_never_reports_dispatched() -> None:
    hand, state, transport = make_hand()
    hand.open()
    try:
        with state.lock:
            state.command_log.clear()
        transport.current_dispatcher.send_error = OSError("simulated bus write failure")

        handle = hand.set_angles((10,) * 6)

        assert handle.wait(timeout=1) is CommandStage.FAILED
        assert tuple(item.stage for item in handle.history) == (
            CommandStage.ACCEPTED,
            CommandStage.FAILED,
        )
        assert handle.sent_frames == 0
        assert handle.error is not None
        assert handle.error.code is ErrorCode.TRANSPORT
        with state.lock:
            assert state.command_log == []
        wait_until(lambda: not hand.get_health().connected)
    finally:
        hand.close()


def test_command_cache_returns_configured_speed_without_device_query() -> None:
    hand, state, transport = make_hand(capabilities=get_declared_capabilities("l30"))
    values = (7,) * 17
    hand.open()
    try:
        handle = hand.set_speeds(values)
        assert handle.wait(timeout=1) is CommandStage.DISPATCHED
        sent_before = len(transport.sent)

        sample = hand.get_configured_speeds(timeout=0.1)

        assert sample.raw == values
        assert sample.meta.origin is DataOrigin.COMMAND_CACHE
        assert len(transport.sent) == sent_before
        with state.lock:
            assert TelemetrySemantic.CONFIGURED_SPEEDS not in state.query_log
    finally:
        hand.close()


def test_active_read_satisfies_equivalent_poll_without_second_query() -> None:
    hand, state, transport = make_hand()
    hand.open()
    result: list[AngleSample] = []
    try:
        with state.lock:
            state.query_log.clear()
            state.readings[TelemetrySemantic.ANGLES] = (42,) * 6
        transport.read_send_started.clear()
        transport.read_gate.clear()

        reader = threading.Thread(
            target=lambda: result.append(hand.get_angles(timeout=1))
        )
        reader.start()
        assert transport.read_send_started.wait(timeout=1)
        hand.start_polling(PollingPlan(angles_hz=1))
        transport.read_gate.set()
        reader.join(timeout=1)
        hand.stop_polling()

        assert not reader.is_alive()
        assert len(result) == 1 and result[0].raw == (42,) * 6
        with state.lock:
            assert state.query_log == [TelemetrySemantic.ANGLES]
    finally:
        transport.read_gate.set()
        hand.close()


def test_adaptive_angle_feedback_avoids_queries_at_the_requested_rate() -> None:
    hand, state, _transport = make_hand(
        config=DeviceConfig(command_rate_hz=500),
    )
    hand.open()
    try:
        with state.lock:
            state.query_log.clear()
        hand.start_polling(PollingPlan(angles_hz=20))

        deadline = time.monotonic() + 0.25
        submitted = 0
        while time.monotonic() < deadline:
            submitted += 1
            hand.set_angles((submitted % 255,) * 6)
            time.sleep(0.025)

        wait_until(
            lambda: (
                next(
                    item
                    for item in hand.get_health().channels
                    if item.channel == "angles"
                ).command_feedback_samples
                >= submitted
            ),
        )
        with state.lock:
            assert state.query_log == []
        metrics = next(
            item for item in hand.get_health().channels if item.channel == "angles"
        )
        assert metrics.completed_reads == 0
        assert metrics.command_feedback_samples >= submitted
        assert metrics.feedback_samples >= submitted
        assert metrics.feedback_policy is AngleFeedbackPolicy.ADAPTIVE
    finally:
        hand.close()


def test_adaptive_feedback_uses_queries_only_for_command_gaps() -> None:
    hand, state, _transport = make_hand(
        config=DeviceConfig(command_rate_hz=500),
    )
    hand.open()
    try:
        with state.lock:
            state.query_log.clear()
        hand.start_polling(PollingPlan(angles_hz=40))

        submitted = 0
        deadline = time.monotonic() + 0.3
        while time.monotonic() < deadline:
            submitted += 1
            hand.set_angles((submitted,) * 6)
            time.sleep(0.1)

        wait_until(
            lambda: (
                next(
                    item
                    for item in hand.get_health().channels
                    if item.channel == "angles"
                ).command_feedback_samples
                >= submitted
            ),
        )
        time.sleep(0.05)
        wait_until(
            lambda: (
                next(
                    item
                    for item in hand.get_health().channels
                    if item.channel == "angles"
                ).completed_reads
                == state.query_log.count(TelemetrySemantic.ANGLES)
            ),
        )
        with state.lock:
            query_count = state.query_log.count(TelemetrySemantic.ANGLES)
        assert query_count > 0
        assert query_count < 20
        metrics = next(
            item for item in hand.get_health().channels if item.channel == "angles"
        )
        assert metrics.completed_reads == query_count
        assert metrics.command_feedback_samples >= submitted
        assert metrics.feedback_samples == (
            metrics.completed_reads + metrics.command_feedback_samples
        )
    finally:
        hand.close()


def test_adaptive_no_feedback_profile_keeps_querying_during_commands() -> None:
    hand, state, _transport = make_hand(
        capabilities=get_declared_capabilities("o20", side="left"),
        config=DeviceConfig(command_rate_hz=500),
    )
    hand.open()
    try:
        with state.lock:
            state.query_log.clear()
        hand.start_polling(PollingPlan(angles_hz=40))

        deadline = time.monotonic() + 0.2
        while time.monotonic() < deadline:
            hand.set_angles((0,) * 16)
            time.sleep(0.01)

        wait_until(
            lambda: (
                next(
                    item
                    for item in hand.get_health().channels
                    if item.channel == "angles"
                ).completed_reads
                >= 4
            ),
        )
        metrics = next(
            item for item in hand.get_health().channels if item.channel == "angles"
        )
        with state.lock:
            query_count = state.query_log.count(TelemetrySemantic.ANGLES)
        assert query_count >= 4
        assert metrics.completed_reads == query_count
        assert metrics.command_feedback_samples == 0
        assert metrics.feedback_samples == metrics.completed_reads
    finally:
        hand.close()


def test_missing_feedback_keeps_dispatch_truth_and_counts_timeout() -> None:
    hand, state, _transport = make_hand()
    hand.open()
    try:
        hand.start_polling(
            PollingPlan(
                angles_hz=20,
                angle_feedback=AngleFeedbackPolicy.COMMAND_FEEDBACK_ONLY,
            )
        )
        state.drop_angle_command_feedback = True

        handle = hand.set_angles((10,) * 6)

        assert handle.wait(timeout=1) is CommandStage.DISPATCHED
        assert handle.sent_frames == 1
        assert handle.error is None
        wait_until(
            lambda: (
                next(
                    item
                    for item in hand.get_health().channels
                    if item.channel == "angles"
                ).timeouts
                == 1
            )
        )
        metrics = next(
            item for item in hand.get_health().channels if item.channel == "angles"
        )
        assert metrics.command_feedback_samples == 0
        assert metrics.feedback_samples == 0
        assert metrics.timeouts == 1
    finally:
        hand.close()


def test_query_only_does_not_let_command_feedback_suppress_queries() -> None:
    hand, state, _transport = make_hand(
        config=DeviceConfig(command_rate_hz=500),
    )
    hand.open()
    try:
        with state.lock:
            state.query_log.clear()
        hand.start_polling(
            PollingPlan(
                angles_hz=20,
                angle_feedback=AngleFeedbackPolicy.QUERY_ONLY,
            )
        )
        for value in range(5):
            hand.set_angles((value,) * 6).wait(timeout=1)
            time.sleep(0.02)
        wait_until(
            lambda: (
                next(
                    item
                    for item in hand.get_health().channels
                    if item.channel == "angles"
                ).completed_reads
                >= 2
            ),
        )

        with state.lock:
            before_explicit = state.query_log.count(TelemetrySemantic.ANGLES)
        result = hand.get_angles(timeout=0.2)
        assert result.meta.origin is DataOrigin.DEVICE_RESPONSE
        with state.lock:
            after_explicit = state.query_log.count(TelemetrySemantic.ANGLES)
        assert after_explicit == before_explicit + 1

        metrics = next(
            item for item in hand.get_health().channels if item.channel == "angles"
        )
        assert metrics.command_feedback_samples >= 5
        assert metrics.feedback_samples == metrics.completed_reads
    finally:
        hand.close()


def test_command_feedback_only_never_creates_periodic_angle_queries() -> None:
    hand, state, _transport = make_hand(
        config=DeviceConfig(command_rate_hz=500),
    )
    hand.open()
    try:
        with state.lock:
            state.query_log.clear()
        hand.start_polling(
            PollingPlan(
                angles_hz=50,
                angle_feedback=AngleFeedbackPolicy.COMMAND_FEEDBACK_ONLY,
            )
        )
        time.sleep(0.08)
        hand.set_angles((42,) * 6).wait(timeout=1)
        time.sleep(0.04)

        with state.lock:
            assert state.query_log == []
        snapshot = hand.get_snapshot()
        assert snapshot.angles is not None
        assert snapshot.angles.raw == (42,) * 6
        assert snapshot.angles.meta.origin is DataOrigin.DEVICE_COMMAND_FEEDBACK
        metrics = next(
            item for item in hand.get_health().channels if item.channel == "angles"
        )
        assert metrics.completed_reads == 0
        assert metrics.command_feedback_samples == 1
        assert metrics.feedback_samples == 1
        assert metrics.missed_deadlines > 0

        explicit = hand.get_angles(timeout=0.2)
        assert explicit.meta.origin is DataOrigin.DEVICE_RESPONSE
        with state.lock:
            assert state.query_log == [TelemetrySemantic.ANGLES]
    finally:
        hand.close()


def test_pending_command_total_does_not_double_count_discrete_fifo() -> None:
    hand, _state, transport = make_hand()
    hand.open()
    try:
        transport.command_send_started.clear()
        transport.command_gate.clear()
        motion = hand.set_angles((10,) * 6)
        assert transport.command_send_started.wait(timeout=1)
        discrete = hand.clear_faults()

        wait_until(lambda: hand.get_health().discrete_queue_depth == 1)
        health = hand.get_health()
        assert health.pending_commands == 1
        assert health.discrete_queue_depth == 1

        transport.command_gate.set()
        assert motion.wait(timeout=1) is CommandStage.DISPATCHED
        assert discrete.wait(timeout=1) is CommandStage.DISPATCHED
    finally:
        transport.command_gate.set()
        hand.close()


def test_new_target_replaces_an_old_target_waiting_on_a_shared_transport() -> None:
    l6, l6_state, o6, _o6_state, transport = make_shared_hands()
    l6.open()
    o6.open()
    try:
        with l6_state.lock:
            l6_state.command_log.clear()
        transport.command_send_started.clear()
        transport.command_gate.clear()
        blocker = o6.set_angles((30,) * 6)
        assert transport.command_send_started.wait(timeout=1)
        old = l6.set_angles((10,) * 6)
        wait_until(lambda: l6.get_health().transport.pending_tx >= 2)
        before = l6.get_snapshot().angles
        assert before is not None

        latest = l6.set_angles((20,) * 6)
        transport.emit(
            WireMessage(
                "l6-left-rx",
                "report",
                TelemetrySemantic.ANGLES.value,
                (55,) * 6,
            )
        )

        wait_until(lambda: old.stage is CommandStage.SUPERSEDED)

        def latest_report_was_published() -> bool:
            angles = l6.get_snapshot().angles
            return angles is not None and angles.raw == (55,) * 6

        wait_until(latest_report_was_published)
        assert old.wait(timeout=0.1) is CommandStage.SUPERSEDED
        assert latest.stage is CommandStage.ACCEPTED
        assert l6.get_health().transport.pending_tx == 2

        transport.command_gate.set()
        assert blocker.wait(timeout=1) is CommandStage.DISPATCHED
        assert latest.wait(timeout=1) is CommandStage.DISPATCHED
        with l6_state.lock:
            assert l6_state.command_log == [(CommandSemantic.ANGLES, (20,) * 6)]
    finally:
        transport.command_gate.set()
        l6.close()
        o6.close()


def test_fifo_receipt_wait_keeps_processing_unsolicited_reports() -> None:
    l6, _l6_state, o6, _o6_state, transport = make_shared_hands()
    l6.open()
    o6.open()
    try:
        transport.command_send_started.clear()
        transport.command_gate.clear()
        blocker = o6.set_angles((30,) * 6)
        assert transport.command_send_started.wait(timeout=1)
        discrete = l6.clear_faults()
        wait_until(lambda: l6.get_health().transport.pending_tx >= 2)

        transport.emit(
            WireMessage(
                "l6-left-rx",
                "report",
                TelemetrySemantic.ANGLES.value,
                (66,) * 6,
            )
        )

        def fifo_report_was_published() -> bool:
            angles = l6.get_snapshot().angles
            return angles is not None and angles.raw == (66,) * 6

        wait_until(fifo_report_was_published)
        assert discrete.stage is CommandStage.ACCEPTED

        transport.command_gate.set()
        assert blocker.wait(timeout=1) is CommandStage.DISPATCHED
        assert discrete.wait(timeout=1) is CommandStage.DISPATCHED
    finally:
        transport.command_gate.set()
        l6.close()
        o6.close()


@pytest.mark.parametrize("fresh_ack", (False, True))
def test_shared_channel_presend_ack_cannot_confirm_a_queued_command(fresh_ack) -> None:
    l6, l6_state, o6, _o6_state, transport = make_shared_hands()
    backend = transport.backends["l6-left"]
    original_responses_for = backend.responses_for

    def responses_for(message):
        responses = original_responses_for(message)
        if (
            not fresh_ack
            and message.kind == "command"
            and message.semantic == CommandSemantic.CURRENT_LIMITS.value
        ):
            return ()
        return responses

    backend.responses_for = responses_for  # ty: ignore[invalid-assignment]
    l6.open()
    o6.open()
    try:
        transport.command_send_started.clear()
        transport.command_gate.clear()
        blocker = o6.set_angles((30,) * 6)
        assert transport.command_send_started.wait(timeout=1)
        queued = l6.set_current_limits((20,) * 6)
        wait_until(lambda: l6.get_health().transport.pending_tx >= 2)

        transport.emit(
            WireMessage(
                "l6-left-rx", "command_ack", CommandSemantic.CURRENT_LIMITS.value
            )
        )
        # Observing the following report proves the older ACK was drained while
        # the other hand still held the writer. No timing sleeps are needed.
        transport.emit(
            WireMessage(
                "l6-left-rx", "report", TelemetrySemantic.ANGLES.value, (66,) * 6
            )
        )

        def report_was_published():
            angles = l6.get_snapshot().angles
            return angles is not None and angles.raw == (66,) * 6

        wait_until(report_was_published)
        assert queued.stage is CommandStage.ACCEPTED
        transport.command_gate.set()
        assert blocker.wait(timeout=1) is CommandStage.DISPATCHED
        expected = CommandStage.ACKNOWLEDGED if fresh_ack else CommandStage.FAILED
        assert queued.wait(timeout=1) is expected
        assert queued.sent_frames == 1
        if not fresh_ack:
            assert queued.error is not None and queued.error.code is ErrorCode.TIMEOUT
        with l6_state.lock:
            assert l6_state.command_log == [(CommandSemantic.CURRENT_LIMITS, (20,) * 6)]
    finally:
        transport.command_gate.set()
        l6.close()
        o6.close()


def test_new_target_waits_in_device_slot_after_prior_target_is_claimed() -> None:
    declared = get_declared_capabilities("l6", side="left")
    capabilities = replace(
        declared,
        commands=tuple(
            replace(command, ack_support=AckSupport.SUPPORTED)
            if command.semantic is CommandSemantic.ANGLES
            else command
            for command in declared.commands
        ),
    )
    hand, state, transport = make_hand(capabilities=capabilities)
    hand.open()
    try:
        with state.lock:
            state.command_log.clear()
        transport.command_send_started.clear()
        transport.command_gate.clear()
        claimed = hand.set_angles((10,) * 6)
        assert transport.command_send_started.wait(timeout=1)

        latest = hand.set_angles((20,) * 6)

        wait_until(lambda: hand.get_health().pending_commands == 1)
        assert claimed.stage is CommandStage.ACCEPTED
        assert latest.stage is CommandStage.ACCEPTED
        assert hand.get_health().transport.pending_tx == 1

        transport.command_gate.set()
        assert claimed.wait(timeout=1) is CommandStage.ACKNOWLEDGED
        assert latest.wait(timeout=1) is CommandStage.ACKNOWLEDGED
        with state.lock:
            assert state.command_log == [
                (CommandSemantic.ANGLES, (10,) * 6),
                (CommandSemantic.ANGLES, (20,) * 6),
            ]
    finally:
        transport.command_gate.set()
        hand.close()


def test_close_reports_a_claimed_successful_send_as_dispatched() -> None:
    hand, state, transport = make_hand()
    hand.open()
    transport.command_send_started.clear()
    transport.command_gate.clear()
    handle = hand.set_angles((10,) * 6)
    assert transport.command_send_started.wait(timeout=1)
    errors: list[BaseException] = []

    def close() -> None:
        try:
            hand.close()
        except BaseException as error:  # noqa: BLE001 - test thread boundary
            errors.append(error)

    closer = threading.Thread(target=close)
    closer.start()
    wait_until(lambda: hand.get_health().lifecycle_state.value == "closing")
    try:
        assert closer.is_alive()
        assert handle.stage is CommandStage.ACCEPTED
    finally:
        transport.command_gate.set()
    closer.join(timeout=1)

    assert not closer.is_alive()
    assert errors == []
    assert handle.wait(timeout=1) is CommandStage.DISPATCHED
    assert tuple(item.stage for item in handle.history) == (
        CommandStage.ACCEPTED,
        CommandStage.DISPATCHED,
    )
    with state.lock:
        assert state.command_log == [(CommandSemantic.ANGLES, (10,) * 6)]


def test_unsolicited_report_wakes_an_idle_actor_and_updates_snapshot() -> None:
    hand, _state, transport = make_hand()
    hand.open()
    try:
        before = hand.get_snapshot().angles
        assert before is not None

        transport.emit(
            WireMessage(
                "fake-hand-rx",
                "report",
                TelemetrySemantic.ANGLES.value,
                (42,) * 6,
            )
        )

        def report_was_published() -> bool:
            angles = hand.get_snapshot().angles
            return angles is not None and angles.meta.sequence > before.meta.sequence

        wait_until(report_was_published)
        angles = hand.get_snapshot().angles
        assert angles is not None and angles.raw == (42,) * 6
    finally:
        hand.close()


def test_timed_out_reply_is_quarantined_instead_of_published_as_a_report() -> None:
    hand, state, transport = make_hand()
    hand.open()
    try:
        with state.lock:
            state.drop_reads.add(TelemetrySemantic.ANGLES)
            state.late_response_guard_s = 0.1
        before = hand.get_snapshot().angles
        assert before is not None

        with pytest.raises(TimeoutError, match="response"):
            hand.get_angles(timeout=1)

        transport.emit(
            WireMessage(
                "fake-hand-rx",
                "read_reply",
                TelemetrySemantic.ANGLES.value,
                (99,) * 6,
            )
        )
        time.sleep(0.02)

        quarantined = hand.get_snapshot().angles
        assert quarantined is not None
        assert quarantined.meta.sequence == before.meta.sequence
        assert quarantined.raw == before.raw
    finally:
        hand.close()


def test_consecutive_response_timeouts_accumulate_until_a_real_success() -> None:
    hand, state, _transport = make_hand()
    hand.open()
    try:
        assert hand.get_health().consecutive_errors == 0

        with state.lock:
            state.drop_reads.add(TelemetrySemantic.ANGLES)

        for expected in (1, 2, 3):
            with pytest.raises(TimeoutError, match="response"):
                hand.get_angles(timeout=1)
            assert hand.get_health().consecutive_errors == expected

        with state.lock:
            state.drop_reads.clear()

        hand.get_angles(timeout=1)
        assert hand.get_health().consecutive_errors == 0
    finally:
        hand.close()


def test_read_without_target_publication_accumulates_like_a_timeout() -> None:
    hand, _state, transport = make_hand()
    backend = transport.backends["fake-hand"]
    hand.open()

    def consume_without_result(message: object) -> ResponseProgress:
        if (
            not isinstance(message, WireMessage)
            or message.kind != "read_reply"
            or message.semantic != TelemetrySemantic.ANGLES.value
        ):
            return ResponseProgress(matched=False)
        return ResponseProgress(matched=True, complete=True, result=None)

    original_read_transaction = backend.read_transaction

    def read_transaction(
        semantic: TelemetrySemantic,
        capabilities: object,
    ) -> ProtocolTransaction:
        if semantic is not TelemetrySemantic.ANGLES:
            return original_read_transaction(semantic, capabilities)
        return ProtocolTransaction(
            key=("read", semantic),
            correlation_key=("read", semantic),
            request_messages=(
                WireMessage(backend.endpoint_id, "read", semantic.value),
            ),
            response_timeout_s=0.1,
            estimated_duration_s=0.001,
            response_consumer=consume_without_result,
        )

    backend.read_transaction = read_transaction  # ty: ignore[invalid-assignment]
    try:
        for expected in (1, 2, 3):
            with pytest.raises(ProtocolError, match="did not publish angles"):
                hand.get_angles(timeout=1)
            assert hand.get_health().consecutive_errors == expected
    finally:
        backend.read_transaction = (  # ty: ignore[invalid-assignment]
            original_read_transaction
        )
        hand.close()


@pytest.mark.parametrize("failure", ("raise", "none", "false"))
def test_quarantined_parse_failure_is_not_attributed_to_the_next_transaction(
    failure,
) -> None:
    hand, state, transport = make_hand()
    backend = transport.backends["fake-hand"]
    original_read_transaction = backend.read_transaction
    armed: list[bool] = []

    def read_transaction(
        semantic: TelemetrySemantic,
        capabilities: DeviceCapabilities,
    ) -> ProtocolTransaction:
        transaction = original_read_transaction(semantic, capabilities)
        if semantic is not TelemetrySemantic.ANGLES or not armed:
            return transaction

        def consume(message: object) -> ResponseProgress:
            if isinstance(message, WireMessage) and message.kind == "poison":
                raise ProtocolError("quarantined assembler rejected a late frame")
            return transaction.consume(message)

        if failure == "raise":
            return replace(transaction, response_consumer=consume)
        # An optional drain is still a codec boundary. Its invalid return must
        # be reported on the quarantined operation, not on the following read.
        return replace(
            transaction,
            late_response_consumer=lambda _: None if failure == "none" else False,
        )

    backend.read_transaction = read_transaction  # ty: ignore[invalid-assignment]
    hand.open()
    try:
        with state.lock:
            state.drop_reads.add(TelemetrySemantic.ANGLES)
            state.late_response_guard_s = 5.0

        # Strand a transaction whose consumer rejects the poison frame. Only
        # this one is armed — the baseline read during open() must stay clean,
        # and so must every read after it.
        armed.append(True)
        with pytest.raises(TimeoutError, match="response"):
            hand.get_angles(timeout=1)
        armed.clear()
        with state.lock:
            state.drop_reads.clear()

        # The next read drains the mailbox on behalf of that quarantine entry.
        transport.emit(WireMessage(backend.route_key, "poison"))
        assert hand.get_angles(timeout=1) is not None

        # The entry must be gone, not re-raising for the rest of the window.
        transport.emit(WireMessage(backend.route_key, "poison"))
        assert hand.get_angles(timeout=1) is not None
    finally:
        backend.read_transaction = (  # ty: ignore[invalid-assignment]
            original_read_transaction
        )
        hand.close()


def test_interstage_disconnect_does_not_reach_the_ended_generation() -> None:
    hand, _state, transport = make_hand()
    backend = transport.backends["fake-hand"]
    hand.open()
    try:
        runtime = opened_runtime(hand)
        actor = runtime._actor
        original_read_transaction = backend.read_transaction

        # Rebuild the ANGLES read as two response-gated stages so runner.complete
        # reaches a stage boundary and runs the interstage angle dispatch.
        # Distinct request payloads (1,) / (2,) let the test tell which stages
        # were actually sent on the wire.
        def two_stage_angles(
            semantic: TelemetrySemantic,
            capabilities: DeviceCapabilities,
        ) -> ProtocolTransaction:
            if semantic is not TelemetrySemantic.ANGLES:
                return original_read_transaction(semantic, capabilities)

            def consume(message: object) -> ResponseProgress:
                if (
                    isinstance(message, WireMessage)
                    and message.kind == "read_reply"
                    and message.semantic == TelemetrySemantic.ANGLES.value
                ):
                    return ResponseProgress(
                        matched=True,
                        complete=True,
                        result=SamplePayload(
                            semantic=TelemetrySemantic.ANGLES,
                            raw=message.values,
                        ),
                    )
                return ResponseProgress(matched=False)

            second_stage = ProtocolTransaction(
                key=("read", "angles", "second"),
                correlation_key=("read", "angles"),
                request_messages=(
                    WireMessage(
                        backend.endpoint_id,
                        "read",
                        TelemetrySemantic.ANGLES.value,
                        (2,),
                    ),
                ),
                response_timeout_s=0.1,
                estimated_duration_s=0.001,
                response_consumer=consume,
            )
            return ProtocolTransaction(
                key=("read", "angles", "first"),
                correlation_key=("read", "angles"),
                request_messages=(
                    WireMessage(
                        backend.endpoint_id,
                        "read",
                        TelemetrySemantic.ANGLES.value,
                        (1,),
                    ),
                ),
                response_timeout_s=0.1,
                estimated_duration_s=0.001,
                response_consumer=consume,
                next_transaction=second_stage,
                yield_after_response=True,
            )

        backend.read_transaction = two_stage_angles  # ty: ignore[invalid-assignment]

        # Stand in for a nested command/read that hits a connection failure and
        # ends the generation in place: disconnect exactly once at the first
        # interstage boundary, then let the real loop and waiter boundary react.
        disconnects: list[HandError] = []
        original_dispatch_one = actor._dispatch_one_interstage_angle_transaction
        original_disconnect = actor._disconnect

        def counting_disconnect(error: HandError) -> None:
            disconnects.append(error)
            original_disconnect(error)

        def disconnect_at_boundary(runner: object) -> bool:
            if actor._state.ready():
                actor._disconnect(
                    DeviceNotReadyError("nested interstage command lost the link")
                )
            return True

        actor._disconnect = counting_disconnect  # ty: ignore[invalid-assignment]
        actor._dispatch_one_interstage_angle_transaction = (  # ty: ignore[invalid-assignment]
            disconnect_at_boundary
        )

        sent_before = len(transport.sent)
        with pytest.raises(DeviceNotReadyError):
            hand.get_angles(timeout=1)

        angle_reads = [
            message
            for message in transport.sent[sent_before:]
            if isinstance(message, WireMessage)
            and message.kind == "read"
            and message.semantic == TelemetrySemantic.ANGLES.value
        ]
        # Only the first stage may reach the wire; the second stage would be a
        # frame delivered to a generation that has already ended.
        assert angle_reads == [
            WireMessage(
                backend.endpoint_id, "read", TelemetrySemantic.ANGLES.value, (1,)
            )
        ]
        # The in-place disconnect must not be repeated by the outer waiter
        # boundary — a second _disconnect would overwrite the saved polling plan.
        assert len(disconnects) == 1
    finally:
        actor._dispatch_one_interstage_angle_transaction = (  # ty: ignore[invalid-assignment]
            original_dispatch_one
        )
        actor._disconnect = original_disconnect  # ty: ignore[invalid-assignment]
        backend.read_transaction = (  # ty: ignore[invalid-assignment]
            original_read_transaction
        )
        hand.close()


def test_unsolicited_report_is_not_starved_by_sustained_commands(monkeypatch) -> None:
    hand, _state, transport = make_hand()
    hand.open()
    keep_refilling = threading.Event()
    keep_refilling.set()
    submitted = 0
    report_published = threading.Event()
    store = opened_runtime(hand)._snapshot_store
    publish = store.publish

    def observe_publish(payload, **kwargs):
        sample = publish(payload, **kwargs)
        if (
            payload.semantic is TelemetrySemantic.ANGLES
            and sample.raw == (77,) * 6
            and sample.meta.origin is DataOrigin.DEVICE_RESPONSE
        ):
            report_published.set()
        return sample

    monkeypatch.setattr(store, "publish", observe_publish)

    def refill() -> None:
        nonlocal submitted
        if not keep_refilling.is_set():
            return
        submitted += 1
        hand.set_angles((submitted % 256,) * 6)

    transport.command_sent_hook = refill
    try:
        before = hand.get_snapshot().angles
        assert before is not None
        hand.set_angles((1,) * 6)
        assert transport.command_send_started.wait(timeout=1)
        transport.emit(
            WireMessage(
                "fake-hand-rx",
                "report",
                TelemetrySemantic.ANGLES.value,
                (77,) * 6,
            )
        )

        # The next command may immediately replace the cached angles. Record
        # publication durably so a slow observer cannot miss a processed report.
        assert report_published.wait(timeout=0.5)
        assert submitted > 0
    finally:
        keep_refilling.clear()
        transport.command_sent_hook = None
        hand.close()


def test_idle_polling_start_cannot_be_lost_before_actor_wait() -> None:
    hand, state, _transport = make_hand()
    hand.open()
    entered, release, restore = hold_next_idle_deadline_check(hand)
    try:
        assert entered.wait(timeout=1)
        with state.lock:
            state.query_log.clear()

        hand.start_polling(PollingPlan(angles_hz=10))
        restore()
        release.set()

        wait_until(lambda: bool(state.query_log))
    finally:
        restore()
        release.set()
        hand.stop_polling()
        hand.close()
