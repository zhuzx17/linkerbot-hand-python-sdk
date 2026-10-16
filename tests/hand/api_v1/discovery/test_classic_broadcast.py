"""Broadcast discovery must preserve identity, address and attempt boundaries."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace

import can
import pytest

from linkerbot_hand import (
    DiscoveryStatus,
    HandSide,
    IncompleteIdentityError,
    TransportKind,
    discover_hands,
)
from linkerbot_hand.discovery import classic_discovery_route_key
from linkerbot_transport import SendReceipt, TransportState
from linkerbot_transport.testing import FakeTransport
from tests.hand.api_v1.discovery.test_discovery import _FAST, _classic_channel, _Source
from tests.hand.api_v1.runtime.classic_can_fake import make_script

_PRODUCTS = {
    "O6": "LHO6-04-453-{side}-B-1-D",
    "L6": "LHL6-03.3-595-{side}-B-1-D",
    "L20Lite": "LHT10-07-0-{side}-B-1-A",
    "L20": "LHT20-010-499-{side}-B-1-D",
}
_Replies = tuple[can.Message, ...]
_Filter = Callable[[can.Message, _Replies], _Replies]


@pytest.fixture(params=(False, True), ids=("system-clock", "equal-timestamps"))
def claim_clock(request, monkeypatch):
    if request.param:
        clock = lambda: 10.0
        monkeypatch.setitem(SendReceipt.__init__.__kwdefaults__, "monotonic", clock)
        monkeypatch.setitem(FakeTransport.__init__.__kwdefaults__, "monotonic", clock)


class _BroadcastTransport(FakeTransport[can.Message]):
    def __init__(
        self,
        model="L6",
        side=HandSide.LEFT,
        *,
        offsets=False,
        broadcast=True,
        response_id=None,
        transform: _Filter | None = None,
    ):
        super().__init__(route_key_of=classic_discovery_route_key)
        self.script = make_script(model, side)
        self.script.product_code = _PRODUCTS[model].format(
            side="L" if side is HandSide.LEFT else "R"
        )
        self.offsets = offsets
        self.broadcast = broadcast
        self.response_id = (
            self.script.arbitration_id if response_id is None else response_id
        )
        self.transform = transform
        self.received = []
        self.reply_receipts = {}

    def subscribe_routes(self, routes, **kwargs):
        subscription = super().subscribe_routes(routes, **kwargs)
        receive = subscription.receive

        def record(**receive_kwargs):
            routed = receive(**receive_kwargs)
            self.received.append(routed)
            return routed

        subscription.receive = record
        return subscription

    def send(self, message: can.Message) -> SendReceipt:
        receipt = super().send(message)
        if receipt.exception() is not None:
            return receipt
        for reply in self._responses(message):
            self.emit(reply)
        return receipt

    def _responses(self, message: can.Message) -> _Replies:
        request = message
        if message.arbitration_id == 0xFF:
            if not self.broadcast:
                return ()
            assert bytes(message.data) == b"\xc0"
            request = can.Message(
                arbitration_id=self.script.arbitration_id,
                data=message.data,
                is_extended_id=False,
            )
        replies = self.script.respond(request)
        for reply in replies:
            reply.arbitration_id = self.response_id
            if self.offsets and reply.data[0] == 0xC0:
                reply.data[1] *= 6
        if self.transform is not None:
            replies = self.transform(message, replies)
        return replies


class _ClaimBoundaryTransport(_BroadcastTransport):
    """Inject replies on either side of a real transport receipt's claim."""

    def __init__(self, model, *, stale_stage=None):
        super().__init__(model)
        self.stale_stage = stale_stage
        self.reply_states: dict[int, tuple[int, int, bool, bool]] = {}

    def send(self, message: can.Message) -> SendReceipt:
        receipt = SendReceipt(generation=self.get_health().generation)
        replies = self._responses(message)
        opcode = message.data[0]
        verification = message.arbitration_id != 0xFF
        stale = self.stale_stage == "product"
        if verification and self.stale_stage == "identity":
            # Replay the entire old identity before the first request is claimed.
            replies = ()
            if opcode == 0xC0:
                pcb = b"\xc1\x01" if self.script.model == "L6" else b"\xc1"
                replies = tuple(
                    reply
                    for payload in (b"\xc0", pcb, b"\xc2", b"\xc4")
                    for reply in self._responses(
                        can.Message(
                            arbitration_id=self.script.arbitration_id,
                            is_extended_id=False,
                            data=payload,
                        )
                    )
                )
            stale = True
        elif verification and self.stale_stage == "later_query":
            # Each version reply predates its own claim, but follows C0's claim.
            stale = opcode != 0xC0
        if not stale:
            receipt._claim()
        for reply in replies:
            self.emit(reply)
            self.reply_receipts[id(reply)] = receipt
            self.reply_states[id(reply)] = (
                message.arbitration_id,
                reply.data[0],
                receipt.claimed_at_s is not None,
                receipt.done(),
            )
        if stale:
            receipt._claim()
        FakeTransport.send(self, message).result()
        receipt._dispatch()
        return receipt


class _SynchronousReplyTransport(_BroadcastTransport):
    """Deliver replies from the dispatcher before synchronous send returns."""

    def __init__(self, model):
        super().__init__(model)
        self.emitted = []

    def open(self):
        super().open()
        dispatch = self.current_dispatcher.send_now

        def send_with_reply(message):
            dispatch(message)
            for reply in self._responses(message):
                self.emit(reply)
                self.emitted.append(reply)

        self.current_dispatcher.send_now = send_with_reply

    def send(self, message):
        start = len(self.emitted)
        receipt = FakeTransport.send(self, message)
        assert len(self.emitted) > start
        for reply in self.emitted[start:]:
            self.reply_receipts[id(reply)] = receipt
        return receipt


def _scan(transport: _BroadcastTransport, config=_FAST):
    source = _Source(_classic_channel(), {TransportKind.CAN: lambda: transport})
    report = discover_hands(config, source=source)
    assert transport.get_health().state is TransportState.CLOSED
    assert transport.get_health().route_count == 0
    return report


def _c0_ids(transport):
    return [m.arbitration_id for m in transport.sent if bytes(m.data) == b"\xc0"]


@pytest.mark.parametrize("model", _PRODUCTS)
@pytest.mark.parametrize("side", (HandSide.LEFT, HandSide.RIGHT))
@pytest.mark.parametrize("offsets", (False, True))
def test_all_classic_models_accept_both_markers_from_broadcast(model, side, offsets):
    def reverse(request, replies):
        return tuple(reversed(replies))

    transport = _BroadcastTransport(model, side, offsets=offsets, transform=reverse)
    report = _scan(transport)
    candidate = report.require_one()
    assert report.failures == ()
    assert candidate.model == model
    assert candidate.side is side
    assert candidate.identity.product_code == transport.script.product_code
    assert candidate.address.request_id == transport.script.arbitration_id
    assert candidate.address.response_id == transport.script.arbitration_id
    assert _c0_ids(transport) == [0xFF, transport.script.arbitration_id]
    assert {bytes(m.data) for m in transport.sent} <= {
        b"\xc0",
        b"\xc1",
        b"\xc1\x01",
        b"\xc2",
        b"\xc4",
    }


@pytest.mark.parametrize(
    ("side", "ids"),
    [(HandSide.RIGHT, [0xFF, 0x27, 0x27]), (HandSide.LEFT, [0xFF, 0x27, 0x28, 0x28])],
)
def test_unanswered_broadcast_uses_bounded_ordered_fallback(side, ids):
    transport = _BroadcastTransport(side=side, broadcast=False)
    assert _scan(transport).require_one().side is side
    assert _c0_ids(transport) == ids


def test_incomplete_broadcast_is_replaced_by_fresh_complete_direct_reply():
    def drop_last(request, replies):
        return replies[:-1] if request.arbitration_id == 0xFF else replies

    transport = _BroadcastTransport(transform=drop_last)
    assert _scan(transport).require_one().model == "L6"
    assert _c0_ids(transport) == [0xFF, 0x27, 0x28, 0x28]


def test_fragments_from_different_attempts_cannot_complete_each_other():
    def split(request, replies):
        return replies[:2] if request.arbitration_id == 0xFF else replies[2:]

    transport = _BroadcastTransport(transform=split)
    candidate = _scan(transport).require_one(allow_partial=True)
    assert candidate.problems[0].code == "product_code_incomplete"
    assert _c0_ids(transport) == [0xFF, 0x27, 0x28]
    with pytest.raises(IncompleteIdentityError):
        candidate.create_hand_device()


def test_identical_duplicate_fragments_are_accepted():
    transport = _BroadcastTransport(transform=lambda request, replies: replies * 2)
    assert _scan(transport).require_one().status is DiscoveryStatus.VERIFIED


@pytest.mark.parametrize("offset_marker", (1, 6))
def test_conflicting_fragments_are_diagnostic_without_fallback(offset_marker):
    def conflict(request, replies):
        duplicate = can.Message(
            arbitration_id=0x28,
            is_extended_id=False,
            data=bytes((0xC0, offset_marker)) + b"xxxxxx",
        )
        return (*replies, duplicate)

    transport = _BroadcastTransport(transform=conflict)
    candidate = _scan(transport).require_one(allow_partial=True)
    assert candidate.problems[0].code == "address_collision"
    assert _c0_ids(transport) == [0xFF]


@pytest.mark.parametrize("address", (0, 0x35, 0xFF, 0x7FF))
def test_nondefault_reply_address_is_preserved_but_cannot_create_a_device(address):
    transport = _BroadcastTransport(response_id=address)
    candidate = _scan(transport).require_one(allow_partial=True)
    assert candidate.address.request_id == candidate.address.response_id == address
    assert candidate.model == "L6"
    assert candidate.side is HandSide.LEFT
    assert candidate.problems[0].code == "unsupported_address"
    assert _c0_ids(transport) == [0xFF]
    with pytest.raises(IncompleteIdentityError):
        candidate.create_hand_device()


def test_side_comes_from_identity_and_disagreement_with_default_id_is_rejected():
    transport = _BroadcastTransport(side=HandSide.RIGHT, response_id=0x28)
    candidate = _scan(transport).require_one(allow_partial=True)
    assert candidate.side is HandSide.RIGHT
    assert candidate.problems[0].code == "identity_mismatch"
    assert _c0_ids(transport) == [0xFF]


def test_unknown_product_remains_a_partial_candidate():
    transport = _BroadcastTransport()
    transport.script.product_code = "OTHER-01-123-L-A-1-C"
    candidate = _scan(transport).require_one(allow_partial=True)
    assert candidate.model is None
    assert candidate.identity.product_code == transport.script.product_code
    assert candidate.problems[0].code == "unknown_model"


@pytest.mark.parametrize("markers", (False, True))
def test_legacy_l6_three_fragment_product_remains_supported(markers):
    transport = _BroadcastTransport(offsets=markers)
    transport.script.product_code = "LHL6 -04-077-L-B-1"
    assert (
        _scan(transport).require_one().identity.product_code
        == transport.script.product_code
    )


def test_receive_overflow_cannot_produce_a_verified_candidate():
    transport = _BroadcastTransport()
    report = _scan(transport, replace(_FAST, route_queue_capacity=1))
    assert report.verified_candidates == ()
    assert len(report.failures) == 1
    assert "overflowed" in report.failures[0].message


def test_no_response_sends_only_three_product_probes_and_closes():
    transport = _BroadcastTransport(transform=lambda request, replies: ())
    report = _scan(transport)
    assert report.candidates == ()
    assert report.failures == ()
    assert _c0_ids(transport) == [0xFF, 0x27, 0x28]


def test_product_code_must_stay_the_same_during_full_verification():
    transport = _BroadcastTransport()

    def change_product(request, replies):
        if request.arbitration_id == 0xFF:
            transport.script.product_code = "LHL6-03.3-999-L-B-1-D"
        return replies

    transport.transform = change_product
    candidate = _scan(transport).require_one(allow_partial=True)
    assert candidate.problems[0].code == "identity_mismatch"


@pytest.mark.parametrize("conflicting", (False, True))
def test_final_version_duplicate_is_checked_before_identity_is_accepted(conflicting):
    def repeat_final(request, replies):
        if bytes(request.data) != b"\xc4":
            return replies
        duplicate = can.Message(
            arbitration_id=0x28,
            is_extended_id=False,
            data=b"\xc4\x02\x00\x02" if conflicting else b"\xc4\x01\x00\x02",
        )
        return (*replies, duplicate)

    candidate = _scan(_BroadcastTransport(transform=repeat_final)).require_one(
        allow_partial=conflicting
    )
    assert candidate.status is (
        DiscoveryStatus.PARTIAL if conflicting else DiscoveryStatus.VERIFIED
    )
    if conflicting:
        assert candidate.problems[0].code == "malformed_identity"


@pytest.mark.parametrize("offset_marker", (2, 12))
def test_trailing_product_fragment_cannot_preserve_an_outdated_complete_identity(
    offset_marker,
):
    def extra_fragment(request, replies):
        if bytes(request.data) != b"\xc4":
            return replies
        return (
            *replies,
            can.Message(
                arbitration_id=0x27,
                is_extended_id=False,
                data=bytes((0xC0, offset_marker)) + b"ABCDEF",
            ),
        )

    transport = _BroadcastTransport("O6", HandSide.RIGHT, transform=extra_fragment)
    transport.script.product_code = "LHO6-1-2-R"
    candidate = _scan(transport).require_one(allow_partial=True)
    assert candidate.status is DiscoveryStatus.PARTIAL
    assert candidate.problems[0].code == "malformed_identity"


def test_queued_old_identity_is_discarded_before_verification():
    class StaleIdentityTransport(_BroadcastTransport):
        def subscribe_routes(self, routes, **kwargs):
            subscription = super().subscribe_routes(routes, **kwargs)
            if self.sent:
                for payload in (b"\xc0", b"\xc1\x01", b"\xc2", b"\xc4"):
                    request = can.Message(
                        arbitration_id=0x28, is_extended_id=False, data=payload
                    )
                    for reply in self.script.respond(request):
                        self.emit(reply)
            return subscription

    transport = StaleIdentityTransport(
        transform=lambda request, replies: (
            replies if request.arbitration_id == 0xFF else ()
        )
    )
    candidate = _scan(transport).require_one(allow_partial=True)
    assert candidate.problems[0].code == "identity_timeout"


@pytest.mark.parametrize("model", _PRODUCTS)
@pytest.mark.parametrize("stale_stage", ("product", "identity", "later_query"))
def test_replies_before_their_query_claim_cannot_verify_identity(
    model, stale_stage, claim_clock
):
    transport = _ClaimBoundaryTransport(model, stale_stage=stale_stage)
    report = _scan(transport)

    assert report.failures == ()
    assert report.verified_candidates == ()
    if stale_stage == "product":
        assert report.candidates == ()
        assert _c0_ids(transport) == [0xFF, 0x27, 0x28]
    else:
        candidate = report.require_one(allow_partial=True)
        assert candidate.problems[0].code == "identity_timeout"
    old_replies = [
        routed
        for routed in transport.received
        for request_id, opcode, _, _ in (transport.reply_states[id(routed.message)],)
        if stale_stage == "product"
        or (request_id != 0xFF and (stale_stage == "identity" or opcode != 0xC0))
    ]
    assert old_replies
    for routed in old_replies:
        receipt = transport.reply_receipts[id(routed.message)]
        assert routed.received_order < receipt.claimed_order
        assert transport.reply_states[id(routed.message)][2:] == (False, False)


@pytest.mark.parametrize("model", _PRODUCTS)
def test_fast_replies_after_claim_and_before_batch_completion_are_preserved(
    model, claim_clock
):
    transport = _ClaimBoundaryTransport(model)
    assert _scan(transport).require_one().model == model

    assert transport.received
    for routed in transport.received:
        receipt = transport.reply_receipts[id(routed.message)]
        result = receipt.result()
        assert result.claimed_order < routed.received_order
        assert transport.reply_states[id(routed.message)][2:] == (True, False)


@pytest.mark.parametrize("model", _PRODUCTS)
def test_synchronous_dispatcher_replies_before_send_returns_are_preserved(
    model, claim_clock
):
    transport = _SynchronousReplyTransport(model)
    assert _scan(transport).require_one().model == model
    assert transport.received
    for routed in transport.received:
        receipt = transport.reply_receipts[id(routed.message)]
        result = receipt.result()
        assert result.claimed_order < routed.received_order


def test_broadcast_candidate_can_reopen_with_the_same_default_receive_routes():
    transport = _BroadcastTransport("O6", HandSide.RIGHT)
    candidate = _scan(transport).require_one()
    hand = candidate.create_hand_device()
    try:
        hand.open()
        assert (
            hand.get_device_info(timeout=1).product_code
            == transport.script.product_code
        )
    finally:
        hand.close()
