"""Multi-frame angle work can run between complete L20-family finger reads."""

from __future__ import annotations

import time

import can
import pytest

from linkerbot_hand.api import (
    AngleFeedbackPolicy,
    CommandStage,
    HandDevice,
    HandSide,
    PollingPlan,
    TimeoutError,
)

from .classic_can_fake import (
    ClassicCANDeviceScript,
    ScriptedClassicCANTransport,
    make_script,
)


@pytest.fixture(params=("L20", "L20Lite"))
def device(request):
    transport = ScriptedClassicCANTransport(make_script(request.param, HandSide.RIGHT))
    hand = HandDevice(model=request.param, side="right", transport=transport)
    hand.open()
    try:
        yield hand, transport
    finally:
        hand.close()


@pytest.mark.parametrize("query_delay_s", (0.0, 0.02))
def test_tactile_yields_to_complete_angle_write_and_query(
    device, monkeypatch, query_delay_s
):
    hand, transport = device
    original_send = transport.send
    handle = None
    snapshots = []
    angle_opcodes = (
        tuple(range(0x41, 0x46)) if transport.script.model == "L20" else (0x01, 0x04)
    )
    delayed = False

    def send(message):
        nonlocal handle, delayed
        if bytes(message.data) == b"\xb1\xc6":
            hand.start_polling(
                PollingPlan(
                    angles_hz=100,
                    angle_feedback=AngleFeedbackPolicy.QUERY_ONLY,
                )
            )
            handle = hand.set_angles((42,) * len(hand.get_capabilities().joints))
        if (
            query_delay_s
            and not delayed
            and bytes(message.data) == bytes((angle_opcodes[-1],))
        ):
            # A slow query makes another 100 Hz poll due at the same safe
            # boundary, as can happen on a loaded Windows runner.
            delayed = True
            time.sleep(query_delay_s)
        if bytes(message.data) in tuple(bytes((i, 0xC6)) for i in range(0xB1, 0xB6)):
            snapshots.append(hand.get_snapshot().force)
        return original_send(message)

    monkeypatch.setattr(transport, "send", send)
    force = hand.get_force(timeout=2)
    assert handle is not None
    assert handle.wait(timeout=1) is CommandStage.DISPATCHED
    payloads = transport.script.sent_payloads
    first_finger = payloads.index(b"\xb1\xc6")
    second_finger = payloads.index(b"\xb2\xc6", first_finger + 1)
    interstage = payloads[first_finger + 1 : second_finger]
    # All fragments must fit within one boundary, never split across fingers.
    assert tuple(p[0] for p in interstage if len(p) > 1) == angle_opcodes
    queries = tuple(p[0] for p in interstage if len(p) == 1)
    assert queries
    assert len(queries) % len(angle_opcodes) == 0
    query_count = len(queries) // len(angle_opcodes)
    # The boundary permits up to four transactions: this one command and
    # complete query groups. Repeated polling must still let tactile advance.
    assert 1 <= query_count <= 3
    assert queries == angle_opcodes * query_count
    if query_delay_s:
        assert delayed
        assert query_count >= 2
    assert len(snapshots) == 5
    assert all(sample is not None and not sample.meta.valid for sample in snapshots)
    assert len(force.tactile) == 5
    assert all((m.rows, m.columns) == (12, 6) for m in force.tactile)
    assert force.meta.sequence == 1
    hand.stop_polling()


def test_missing_row_stops_next_finger_and_keeps_previous_atomic_sample(
    device,
    monkeypatch,
):
    hand, transport = device
    first = hand.get_force(timeout=1)
    original_frames = ClassicCANDeviceScript._tactile_frames

    def drop_one_row(script, opcode):
        frames = original_frames(script, opcode)
        return tuple(f for f in frames if opcode != 0xB3 or f.data[1] != 4)

    monkeypatch.setattr(ClassicCANDeviceScript, "_tactile_frames", drop_one_row)
    start = len(transport.script.sent_payloads)
    with pytest.raises(TimeoutError):
        hand.get_force(timeout=1)
    assert transport.script.sent_payloads[start:] == [
        b"\xb1\xc6",
        b"\xb2\xc6",
        b"\xb3\xc6",
    ]
    cached = hand.get_snapshot().force
    assert cached is not None
    assert cached.meta.sequence == first.meta.sequence
    assert cached.tactile == first.tactile
    assert not hand.get_capabilities().firmware_unsupported_telemetry
    assert hand.get_angles(timeout=1).meta.valid


def test_l20_hardware_trailers_do_not_break_tactile_or_next_angle_read(monkeypatch):
    original_frames = ClassicCANDeviceScript._tactile_frames

    def with_trailer(script, opcode):
        frames = original_frames(script, opcode)
        # LHT20 firmware 1.0.18 sends this four-byte frame after each finger's
        # twelve rows. The old five-finger burst misparsed it as a short row.
        trailer = can.Message(
            arbitration_id=frames[0].arbitration_id,
            is_extended_id=False,
            data=bytes((opcode, 0xFF, 0, 0)),
        )
        return (*frames, trailer)

    monkeypatch.setattr(ClassicCANDeviceScript, "_tactile_frames", with_trailer)
    transport = ScriptedClassicCANTransport(make_script("L20", HandSide.LEFT))
    hand = HandDevice(model="L20", side="left", transport=transport)
    hand.open()
    try:
        for sequence in (1, 2):
            force = hand.get_force(timeout=1)
            assert force.meta.sequence == sequence
            assert len(force.tactile) == 5
            assert all((m.rows, m.columns) == (12, 6) for m in force.tactile)
            assert hand.get_angles(timeout=1).meta.valid
    finally:
        hand.close()
