"""Actor-thread execution of one protocol transaction at a time."""

from __future__ import annotations

import threading
from collections.abc import Callable, Hashable
from dataclasses import dataclass

from linkerbot_transport import SendOutcome, SendReceipt

from ..api.errors import (
    ClosedError,
    DeviceNotReadyError,
    ProtocolError,
    TimeoutError,
    ValidationError,
)
from ..capabilities.types import DeviceCapabilities, TelemetrySemantic
from ..protocols.base import (
    IdentityResult,
    ProtocolBackend,
    ProtocolTransaction,
    ResponseProgress,
    SamplePayload,
)
from ._clock import RuntimeClock
from ._errors import ResponseTimeoutError
from ._route_mailbox import InboundMessage
from ._transport_binding import TransportBinding
from .metrics import RuntimeMetrics
from .snapshot_store import SnapshotStore


class TransactionSuperseded(Exception):
    """Internal signal that no frame in a replaceable batch was sent."""


@dataclass(slots=True)
class DispatchGuard:
    """Device settling deadline retained when its connection runner is replaced."""

    not_before_s: float = 0.0


@dataclass(frozen=True, slots=True)
class TransactionOutcome:
    """Decoded result and publications from one complete transaction."""

    result: IdentityResult | SamplePayload | None
    acknowledged: bool
    publications: tuple[tuple[SamplePayload, object], ...]
    sent_frames: int


@dataclass(frozen=True, slots=True)
class _StageOutcome:
    """Internal result of one dispatched stage in a transaction chain."""

    result: IdentityResult | SamplePayload | None
    acknowledged: bool
    publications: tuple[tuple[SamplePayload, object], ...]
    sent_frames: int


@dataclass(frozen=True, slots=True)
class PendingTransaction:
    """A protocol request accepted by transport but not yet completed."""

    transaction: ProtocolTransaction
    receipt: SendReceipt
    capabilities: DeviceCapabilities
    publish_generation: int | None
    started_at_s: float
    deadline_at_s: float | None
    priority: int
    response_timeout_s: float | None = None


@dataclass(frozen=True, slots=True)
class _QuarantinedTransaction:
    transaction: ProtocolTransaction
    deadline_at_s: float


@dataclass(slots=True)
class _BufferedResponse:
    """Response progress received before transport delivery is published."""

    progress: ResponseProgress | None = None
    publication_batches: tuple[tuple[SamplePayload, ...], ...] = ()
    error: BaseException | None = None
    deferred_inbound: InboundMessage | None = None


class TransactionRunner:
    """Dispatch request batches and feed routed responses to model codecs."""

    def __init__(
        self,
        *,
        binding: TransportBinding,
        backend: ProtocolBackend,
        endpoint_id: Hashable,
        clock: RuntimeClock,
        wake: threading.Event,
        stop: threading.Event,
        recovery_requested: Callable[[], bool],
        next_message: Callable[[], InboundMessage | None],
        receiver_error: Callable[[], BaseException | None],
        report_error: Callable[[BaseException], None],
        snapshot_store: SnapshotStore,
        metrics: RuntimeMetrics,
        dispatch_guard: DispatchGuard | None = None,
        on_stage_start: Callable[[float], None] | None = None,
        default_response_timeout_s: float | None = None,
        execution_deadline_at_s: float | None = None,
    ) -> None:
        self._binding = binding
        self._backend = backend
        self._endpoint_id = endpoint_id
        self._clock = clock
        self._wake = wake
        self._stop = stop
        self._recovery_requested = recovery_requested
        self._next_message = next_message
        self._receiver_error = receiver_error
        self._report_error = report_error
        self._snapshot_store = snapshot_store
        self._metrics = metrics
        self._quarantined: dict[Hashable, _QuarantinedTransaction] = {}
        self._dispatch_guard = dispatch_guard or DispatchGuard()
        self._on_stage_start = on_stage_start
        self._default_response_timeout_s = default_response_timeout_s
        self._execution_deadline_at_s = execution_deadline_at_s

    def execute(
        self,
        transaction: ProtocolTransaction,
        *,
        capabilities: DeviceCapabilities,
        publish_generation: int | None,
        latest_family: Hashable | None,
        priority: int,
        deadline_at_s: float | None = None,
        on_dispatched: Callable[[float, int], None] | None = None,
        on_stage_boundary: Callable[[float], None] | None = None,
        require_published_semantic: TelemetrySemantic | None = None,
        response_timeout_s: float | None = None,
    ) -> TransactionOutcome:
        """Execute one transaction without allowing another correlation conflict."""

        pending = self.begin(
            transaction,
            capabilities=capabilities,
            publish_generation=publish_generation,
            latest_family=latest_family,
            priority=priority,
            deadline_at_s=deadline_at_s,
            response_timeout_s=response_timeout_s,
        )
        return self.complete(
            pending,
            on_dispatched=on_dispatched,
            on_stage_boundary=on_stage_boundary,
            require_published_semantic=require_published_semantic,
        )

    def begin(
        self,
        transaction: ProtocolTransaction,
        *,
        capabilities: DeviceCapabilities,
        publish_generation: int | None,
        latest_family: Hashable | None,
        priority: int,
        deadline_at_s: float | None = None,
        response_timeout_s: float | None = None,
    ) -> PendingTransaction:
        """Submit a transaction and return before its transport receipt completes."""

        if response_timeout_s is None:
            response_timeout_s = self._default_response_timeout_s
        if self._execution_deadline_at_s is not None:
            deadline_at_s = (
                self._execution_deadline_at_s
                if deadline_at_s is None
                else min(deadline_at_s, self._execution_deadline_at_s)
            )
        self.raise_if_interrupted()
        if self._on_stage_start is not None:
            self._on_stage_start(_stage_wait_budget(transaction, response_timeout_s))
        self._wait_for_quarantine(
            transaction.correlation_key,
            capabilities=capabilities,
            generation=publish_generation,
        )
        self._wait_for_dispatch(
            max(
                self._dispatch_guard.not_before_s,
                self._clock.monotonic() + transaction.pre_dispatch_delay_s,
            ),
            capabilities=capabilities,
            generation=publish_generation,
            deadline_at_s=deadline_at_s,
        )
        started = self._clock.monotonic()
        receipt = self._binding.submit(
            transaction.request_messages,
            request_interval_s=transaction.request_interval_s,
            endpoint_id=self._endpoint_id,
            latest_family=latest_family,
            priority=priority,
            deadline_at_s=deadline_at_s,
        )
        receipt.add_done_callback(lambda _receipt: self._wake.set())
        return PendingTransaction(
            transaction=transaction,
            receipt=receipt,
            capabilities=capabilities,
            publish_generation=publish_generation,
            started_at_s=started,
            deadline_at_s=deadline_at_s,
            priority=priority,
            response_timeout_s=response_timeout_s,
        )

    def replace_pending(
        self,
        transaction: ProtocolTransaction,
        *,
        capabilities: DeviceCapabilities,
        publish_generation: int | None,
        latest_family: Hashable,
        priority: int,
        deadline_at_s: float | None = None,
    ) -> PendingTransaction | None:
        """Replace a still-unclaimed keyed request without adding a new job."""

        started = self._clock.monotonic()
        receipt = self._binding.replace_latest_if_pending(
            transaction.request_messages,
            request_interval_s=transaction.request_interval_s,
            endpoint_id=self._endpoint_id,
            latest_family=latest_family,
            priority=priority,
            deadline_at_s=deadline_at_s,
        )
        if receipt is None:
            return None
        receipt.add_done_callback(lambda _receipt: self._wake.set())
        return PendingTransaction(
            transaction=transaction,
            receipt=receipt,
            capabilities=capabilities,
            publish_generation=publish_generation,
            started_at_s=started,
            deadline_at_s=deadline_at_s,
            priority=priority,
            response_timeout_s=self._default_response_timeout_s,
        )

    def complete(
        self,
        pending: PendingTransaction,
        *,
        on_dispatched: Callable[[float, int], None] | None = None,
        on_stage_boundary: Callable[[float], None] | None = None,
        require_published_semantic: TelemetrySemantic | None = None,
    ) -> TransactionOutcome:
        """Finish every response-gated stage and publish canonical samples.

        ``require_published_semantic`` makes "the caller actually got its data"
        part of the transaction's success condition. It has to be checked here
        rather than by the caller: ``transaction_completed`` below clears the
        consecutive-error counter, so a check made after this returns would be
        counted only after the counter had already been reset.
        """

        started_at_s = pending.started_at_s
        sent_frames = 0
        publications: tuple[tuple[SamplePayload, object], ...] = ()
        result: IdentityResult | SamplePayload | None = None
        acknowledged = False
        current = pending

        while True:
            transaction = current.transaction
            final_stage = (
                transaction.next_transaction is None
                and transaction.next_transaction_factory is None
            )

            def mark_final_dispatched(
                timestamp: float,
                stage_sent_frames: int,
                prior_sent_frames: int = sent_frames,
            ) -> None:
                if on_dispatched is not None:
                    on_dispatched(
                        timestamp,
                        prior_sent_frames + stage_sent_frames,
                    )

            try:
                outcome = self._complete_stage(
                    current,
                    on_dispatched=(mark_final_dispatched if final_stage else None),
                )
            except BaseException as error:
                _record_sent_frames(error, sent_frames)
                raise

            sent_frames += outcome.sent_frames
            publications += outcome.publications
            if outcome.result is not None:
                result = outcome.result
            acknowledged = outcome.acknowledged
            try:
                next_transaction = transaction.build_next_transaction()
            except BaseException as error:
                _record_sent_frames(error, sent_frames)
                raise
            if next_transaction is None:
                if require_published_semantic is not None and not any(
                    payload.semantic is require_published_semantic
                    for payload, _sample in publications
                ):
                    error = ProtocolError(
                        "read transaction did not publish "
                        f"{require_published_semantic.value}"
                    )
                    _record_sent_frames(error, sent_frames)
                    raise error
                completed_at = self._clock.monotonic()
                self._metrics.transaction_completed(
                    timestamp=completed_at,
                    duration_s=max(0.0, completed_at - started_at_s),
                )
                return TransactionOutcome(
                    result,
                    acknowledged,
                    publications,
                    sent_frames,
                )

            if transaction.yield_after_response and on_stage_boundary is not None:
                try:
                    on_stage_boundary(next_transaction.estimated_duration_s)
                except BaseException as error:
                    _record_sent_frames(error, sent_frames)
                    raise

            try:
                current = self.begin(
                    next_transaction,
                    capabilities=current.capabilities,
                    publish_generation=current.publish_generation,
                    latest_family=None,
                    priority=current.priority,
                    deadline_at_s=current.deadline_at_s,
                    response_timeout_s=current.response_timeout_s,
                )
            except BaseException as error:
                _record_sent_frames(error, sent_frames)
                raise

    def _complete_stage(
        self,
        pending: PendingTransaction,
        *,
        on_dispatched: Callable[[float, int], None] | None,
    ) -> _StageOutcome:
        """Finish one stage without exposing a whole-command dispatch early."""

        transaction = pending.transaction
        receipt = pending.receipt
        sent_frames = 0
        try:
            buffered = self._wait_for_receipt(pending)
            result = receipt.result()
            if result.outcome is SendOutcome.SUPERSEDED:
                raise TransactionSuperseded
            if result.outcome is not SendOutcome.DISPATCHED:
                raise ProtocolError(f"send ended as {result.outcome.value}")
            if result.claimed_at_s is None:
                raise ProtocolError("dispatched batch has no claim timestamp")
            dispatched_at = result.completed_at_s
            # Reserve the endpoint even if the ACK later times out or fails to
            # parse. The device may already have applied a mode-changing write.
            self._dispatch_guard.not_before_s = max(
                self._dispatch_guard.not_before_s,
                dispatched_at + transaction.post_dispatch_delay_s,
            )
            sent_frames = len(transaction.request_messages)
            self._metrics.sent(timestamp=dispatched_at)
            if on_dispatched is not None:
                on_dispatched(dispatched_at, sent_frames)

            publications = self._publish(
                transaction.dispatch_publications,
                generation=pending.publish_generation,
            )
            if not transaction.expects_response:
                return _StageOutcome(
                    None,
                    False,
                    publications,
                    sent_frames,
                )
            if buffered.error is not None:
                raise buffered.error
            for payloads in buffered.publication_batches:
                publications += self._publish(
                    _deduplicate_payloads(payloads),
                    generation=pending.publish_generation,
                )

            response_timeout_s = (
                transaction.response_timeout_s
                if pending.response_timeout_s is None
                else pending.response_timeout_s
            )
            response_deadline = dispatched_at + response_timeout_s
            if pending.deadline_at_s is not None:
                response_deadline = min(response_deadline, pending.deadline_at_s)
            progress = buffered.progress
            if progress is None or not progress.complete:
                progress, response_publications = self._wait_for_response(
                    transaction,
                    capabilities=pending.capabilities,
                    generation=pending.publish_generation,
                    not_before_s=result.claimed_at_s,
                    not_before_order=getattr(result, "claimed_order", None),
                    deadline_s=response_deadline,
                    response_timeout_s=response_timeout_s,
                    first_inbound=buffered.deferred_inbound,
                )
                publications += response_publications
            if progress.acknowledged != transaction.response_acknowledges_command:
                raise ProtocolError(
                    "response acknowledgement disagrees with transaction contract"
                )
            return _StageOutcome(
                progress.result,
                progress.acknowledged,
                publications,
                sent_frames,
            )
        except BaseException as error:
            if (
                sent_frames
                and transaction.late_response_consumer is not None
                and isinstance(error, ProtocolError)
            ):
                # Some write echoes share opcodes with reads. A parse failure
                # does not mean the device has stopped sending the other groups.
                self._quarantine_failed_response(transaction)
            _record_sent_frames(error, sent_frames)
            raise

    def process_unsolicited(
        self,
        inbound: InboundMessage,
        *,
        capabilities: DeviceCapabilities,
        generation: int | None,
    ) -> None:
        """Decode report traffic only when no request transaction owns it."""

        self._metrics.received(timestamp=inbound.received_at_s)
        if self._consume_quarantined(inbound.message):
            return
        self._publish_report(inbound, capabilities=capabilities, generation=generation)

    def _publish_report(
        self,
        inbound: InboundMessage,
        *,
        capabilities: DeviceCapabilities,
        generation: int | None,
    ) -> None:
        """Decode and publish report traffic behind a backend error boundary.

        ``ProtocolBackend.decode_report`` must return an empty tuple when a
        message is unmatched, but a backend that raises instead must never fail
        whichever transaction happens to be in flight.
        """

        try:
            payloads = self._backend.decode_report(inbound.message, capabilities)
        except Exception as error:  # noqa: BLE001 - report callback boundary
            self._report_error(error)
            return
        self._publish(payloads, generation=generation)

    def _wait_for_receipt(
        self,
        pending: PendingTransaction,
    ) -> _BufferedResponse:
        receipt = pending.receipt
        deadline_at_s = pending.deadline_at_s
        buffered = _BufferedResponse()
        while not receipt.done():
            self.raise_if_interrupted()
            now = self._clock.monotonic()
            if deadline_at_s is not None and now >= deadline_at_s:
                raise TimeoutError("transport queue deadline expired")
            remaining = None if deadline_at_s is None else max(0.0, deadline_at_s - now)
            self._wake.clear()
            if receipt.done():
                break
            if self._process_before_receipt(
                pending,
                buffered,
            ):
                continue
            self.raise_if_interrupted()
            self._clock.wait(self._wake, remaining)
        return buffered

    def _process_before_receipt(
        self,
        pending: PendingTransaction,
        buffered: _BufferedResponse,
    ) -> bool:
        inbound = self._next_message()
        if inbound is None:
            return False
        transaction = pending.transaction
        waiting_for_response = (
            transaction.expects_response
            and buffered.error is None
            and (buffered.progress is None or not buffered.progress.complete)
        )
        if waiting_for_response and pending.receipt.done():
            # Receipt completion may race the pop above. Defer this frame until
            # send success and the final response deadline have been checked.
            buffered.deferred_inbound = inbound
            return True
        self._metrics.received(timestamp=inbound.received_at_s)
        if self._consume_quarantined(inbound.message):
            return True
        # Read the claim after popping: a claim racing receive cannot make an
        # earlier frame eligible. Completion is too late as a lower bound, since
        # valid replies may arrive while the rest of a batch is still sending.
        claimed_at_s = pending.receipt.claimed_at_s
        if waiting_for_response and _received_after_claim(
            inbound, claimed_at_s, getattr(pending.receipt, "claimed_order", None)
        ):
            try:
                progress = transaction.consume(inbound.message)
            except Exception as error:  # noqa: BLE001 - defer until DISPATCHED
                buffered.error = error
                return True
            if progress.matched:
                payloads = progress.publications
                if isinstance(progress.result, SamplePayload):
                    payloads += (progress.result,)
                buffered.progress = progress
                buffered.publication_batches += (payloads,)
                return True
        try:
            self._publish(
                self._backend.decode_report(inbound.message, pending.capabilities),
                generation=pending.publish_generation,
            )
        except Exception as error:  # noqa: BLE001 - report callback boundary
            self._report_error(error)
        return True

    def _wait_for_response(
        self,
        transaction: ProtocolTransaction,
        *,
        capabilities: DeviceCapabilities,
        generation: int | None,
        not_before_s: float,
        deadline_s: float,
        response_timeout_s: float,
        not_before_order: int | None = None,
        first_inbound: InboundMessage | None = None,
    ) -> tuple[ResponseProgress, tuple[tuple[SamplePayload, object], ...]]:
        """Accept only frames received before the response deadline.

        A paused actor may still drain timely frames already in the bounded,
        timestamp-ordered route queue. Newly received traffic cannot extend
        that finite backlog past the cutoff. Lifecycle/execution deadlines
        remain checked on every iteration, even while draining timely frames.
        """

        publications: tuple[tuple[SamplePayload, object], ...] = ()
        expired_inbound: InboundMessage | None = None
        try:
            while True:
                self.raise_if_interrupted()
                self._wake.clear()
                inbound = first_inbound
                first_inbound = None
                if inbound is None:
                    inbound = self._next_message()
                remaining = deadline_s - self._clock.monotonic()
                if (inbound is not None and inbound.received_at_s >= deadline_s) or (
                    inbound is None and remaining <= 0
                ):
                    expired_inbound = inbound
                    self.raise_if_interrupted()
                    raise ResponseTimeoutError(
                        transaction.key,
                        response_timeout_s=response_timeout_s,
                        late_response_guard_s=transaction.late_response_guard_s,
                    )
                if inbound is not None:
                    self._metrics.received(timestamp=inbound.received_at_s)
                    if self._consume_quarantined(inbound.message):
                        continue
                    if not _received_after_claim(
                        inbound, not_before_s, not_before_order
                    ):
                        self._publish_report(
                            inbound, capabilities=capabilities, generation=generation
                        )
                        continue
                    progress = transaction.consume(inbound.message)
                    if not progress.matched:
                        if generation is not None:
                            self._publish_report(
                                inbound,
                                capabilities=capabilities,
                                generation=generation,
                            )
                        continue
                    payloads = progress.publications
                    if isinstance(progress.result, SamplePayload):
                        payloads += (progress.result,)
                    publications += self._publish(
                        _deduplicate_payloads(payloads),
                        generation=generation,
                    )
                    if progress.complete:
                        return progress, publications
                    continue
                self.raise_if_interrupted()
                self._clock.wait(self._wake, remaining)
        except TimeoutError:
            self._quarantine_failed_response(transaction)
            if expired_inbound is not None:
                # The cutoff frame was already popped. Give it to quarantine
                # first, preserving unrelated reports without completing this
                # expired transaction or hiding its timeout behind a parser error.
                try:
                    self.process_unsolicited(
                        expired_inbound,
                        capabilities=capabilities,
                        generation=generation,
                    )
                except Exception as error:  # noqa: BLE001 - expired report boundary
                    self._report_error(error)
            raise

    def _quarantine_failed_response(self, transaction: ProtocolTransaction) -> None:
        if transaction.late_response_guard_s > 0:
            self._quarantined[transaction.correlation_key] = _QuarantinedTransaction(
                transaction=transaction,
                deadline_at_s=(
                    self._clock.monotonic() + transaction.late_response_guard_s
                ),
            )

    def _publish(
        self,
        payloads: tuple[SamplePayload, ...],
        *,
        generation: int | None,
    ) -> tuple[tuple[SamplePayload, object], ...]:
        if generation is None:
            return ()
        published: list[tuple[SamplePayload, object]] = []
        for payload in payloads:
            now = self._clock.monotonic()
            sample = self._snapshot_store.publish(
                payload,
                generation=generation,
                collected_at=self._clock.time(),
                collected_monotonic=now,
            )
            published.append((payload, sample))
        return tuple(published)

    def _wait_for_dispatch(
        self,
        not_before_s: float,
        *,
        capabilities: DeviceCapabilities,
        generation: int | None,
        deadline_at_s: float | None,
    ) -> None:
        """Wait for device settling without blocking close/recovery or RX."""

        while True:
            self.raise_if_interrupted()
            self._wake.clear()
            now = self._clock.monotonic()
            if deadline_at_s is not None and now >= deadline_at_s:
                raise TimeoutError("transaction deadline expired before dispatch")
            remaining = not_before_s - now
            if remaining <= 0:
                return
            inbound = self._next_message()
            if inbound is not None:
                self.process_unsolicited(
                    inbound,
                    capabilities=capabilities,
                    generation=generation,
                )
                continue
            if deadline_at_s is not None:
                remaining = min(remaining, deadline_at_s - now)
            self.raise_if_interrupted()
            self._clock.wait(self._wake, remaining)

    def _wait_for_quarantine(
        self,
        correlation_key: Hashable,
        *,
        capabilities: DeviceCapabilities,
        generation: int | None,
    ) -> None:
        while (quarantined := self._active_quarantine(correlation_key)) is not None:
            self.raise_if_interrupted()
            self._wake.clear()
            while (inbound := self._next_message()) is not None:
                self.process_unsolicited(
                    inbound,
                    capabilities=capabilities,
                    generation=generation,
                )
                if correlation_key not in self._quarantined:
                    return
            remaining = max(
                0.0,
                quarantined.deadline_at_s - self._clock.monotonic(),
            )
            if self._execution_deadline_at_s is not None:
                remaining = min(
                    remaining,
                    max(0.0, self._execution_deadline_at_s - self._clock.monotonic()),
                )
            self.raise_if_interrupted()
            self._clock.wait(self._wake, remaining)

    def _active_quarantine(
        self, correlation_key: Hashable
    ) -> _QuarantinedTransaction | None:
        quarantined = self._quarantined.get(correlation_key)
        if (
            quarantined is not None
            and self._clock.monotonic() >= quarantined.deadline_at_s
        ):
            del self._quarantined[correlation_key]
            return None
        return quarantined

    def _consume_quarantined(self, message: object) -> bool:
        now = self._clock.monotonic()
        for correlation_key, quarantined in tuple(self._quarantined.items()):
            if now >= quarantined.deadline_at_s:
                del self._quarantined[correlation_key]
                continue
            try:
                consumer = quarantined.transaction.late_response_consumer
                progress = (
                    quarantined.transaction.consume(message)
                    if consumer is None
                    else consumer(message)
                )
                if not isinstance(progress, ResponseProgress):
                    raise ValidationError(
                        "late_response_consumer must return ResponseProgress"
                    )
            except Exception as error:  # noqa: BLE001 - quarantine parse boundary
                # A quarantined transaction's parse failure must never be
                # attributed to whichever transaction is in flight now — the
                # same invariant _publish_report holds for decode_report. Drop
                # the entry as well: keeping it would re-raise on every later
                # frame for the rest of the guard window, and would block the
                # next transaction that shares this correlation key.
                del self._quarantined[correlation_key]
                self._report_error(error)
                return True
            if not progress.matched:
                continue
            if progress.complete:
                del self._quarantined[correlation_key]
            return True
        return False

    def wait_for_recovery_quiet(self, quiet_s: float) -> None:
        """Discard this endpoint's old replies before constructing a new round.

        Every received frame restarts the quiet interval. The recovery execution
        deadline and close still bound the wait, including continuous traffic.
        Protocols without a request sequence cannot distinguish a reply delayed
        beyond this interval from a reply to a later identical request.
        """

        quiet_until = self._clock.monotonic() + quiet_s
        while True:
            self.raise_if_interrupted()
            self._wake.clear()
            inbound = self._next_message()
            if inbound is not None:
                self._metrics.received(timestamp=inbound.received_at_s)
                quiet_until = self._clock.monotonic() + quiet_s
                continue
            self.raise_if_interrupted()
            now = self._clock.monotonic()
            if now >= quiet_until:
                self._quarantined.clear()
                return
            deadline = quiet_until
            if self._execution_deadline_at_s is not None:
                deadline = min(deadline, self._execution_deadline_at_s)
            self._clock.wait(self._wake, max(0.0, deadline - now))

    def raise_if_interrupted(self) -> None:
        """Raise when lifecycle control invalidates the active transaction."""

        if self._stop.is_set():
            raise ClosedError("device runtime is stopping")
        if self._recovery_requested():
            raise DeviceNotReadyError("device recovery was requested")
        if (
            self._execution_deadline_at_s is not None
            and self._clock.monotonic() >= self._execution_deadline_at_s
        ):
            raise TimeoutError("recovery execution budget expired")
        receiver_error = self._receiver_error()
        if receiver_error is not None:
            raise receiver_error


def _received_after_claim(
    inbound: InboundMessage,
    claimed_at_s: float | None,
    claimed_order: int | None,
) -> bool:
    if inbound.received_order is not None and claimed_order is not None:
        return inbound.received_order > claimed_order
    return claimed_at_s is not None and inbound.received_at_s >= claimed_at_s


def _stage_wait_budget(
    transaction: ProtocolTransaction, response_timeout_s: float | None
) -> float:
    """Budget one actual startup stage, including its protocol-imposed waits."""

    response_wait = (
        transaction.response_timeout_s
        if response_timeout_s is None
        else response_timeout_s
    )
    return (
        (response_wait if transaction.expects_response else 0.0)
        + (len(transaction.request_messages) - 1) * transaction.request_interval_s
        + transaction.pre_dispatch_delay_s
        + transaction.post_dispatch_delay_s
        + transaction.late_response_guard_s
    )


def _deduplicate_payloads(
    payloads: tuple[SamplePayload, ...],
) -> tuple[SamplePayload, ...]:
    result: list[SamplePayload] = []
    for payload in payloads:
        if payload not in result:
            result.append(payload)
    return tuple(result)


def _record_sent_frames(error: BaseException, prior_frames: int) -> None:
    """Attach a cumulative count without obscuring the original error type."""

    current_frames = getattr(error, "transaction_sent_frames", None)
    if current_frames is None:
        current_frames = getattr(error, "sent_frames", 0)
    try:
        error.transaction_sent_frames = (  # ty: ignore[unresolved-attribute]
            prior_frames + current_frames
        )
    except (AttributeError, TypeError):
        pass


__all__ = [
    "DispatchGuard",
    "PendingTransaction",
    "TransactionOutcome",
    "TransactionRunner",
    "TransactionSuperseded",
]
