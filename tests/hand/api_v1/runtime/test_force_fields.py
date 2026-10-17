"""Public tactile fields preserve native semantics and physical query sharing."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from threading import Barrier

import pytest

from linkerbot_hand import (
    HandDevice,
    TelemetrySemantic,
    UnsupportedOperationError,
    ValidationError,
    get_declared_capabilities,
)
from linkerbot_hand.api._initial_state import make_initial_snapshot
from linkerbot_hand.api.readings import TactileMatrix
from linkerbot_hand.protocols.base import SamplePayload
from linkerbot_hand.runtime.snapshot_store import SnapshotStore

from .o30_canfd_fake import O30DeviceScript, ScriptedO30Transport
from .test_o30_tactile_availability import MixedSensors


@pytest.mark.parametrize("model", ["L6", "O6", "L20", "L20Lite", "L30", "O20"])
def test_matrix_models_expose_normal_points_with_existing_shape(model):
    caps = get_declared_capabilities(model, side=None if model == "L30" else "left")
    expected = ("fn", "fns", "fts", "fds", "px") if model == "L20Lite" else ("fn",)
    assert (
        tuple(field.name for field in caps.force_fields if field.supported) == expected
    )
    force = caps.telemetry_channel(TelemetrySemantic.FORCE)
    assert force is not None
    rows, columns = force.shape[1:]
    matrices = tuple(
        TactileMatrix(
            name=name,
            rows=rows,
            columns=columns,
            raw=tuple(
                tuple(row * columns + col for col in range(columns))
                for row in range(rows)
            ),
            online=False if model == "O20" and index == 1 else None,
        )
        for index, name in enumerate(force.channels)
    )
    store = SnapshotStore(caps, make_initial_snapshot(caps))
    sample = store.publish(
        SamplePayload(TelemetrySemantic.FORCE, tactile=matrices),
        generation=0,
        collected_at=100,
        collected_monotonic=10,
    )
    assert sample.fn.raw[0] == tuple(range(rows * columns))
    assert sample.fn.units == (force.raw_unit,) * 5
    assert sample.fn.sensor_names == force.channels
    assert sample.tactile is matrices
    assert sample.status("fns") == ("not_read" if model == "L20Lite" else "unsupported")
    assert sample.px is None
    if model == "O20":
        assert not any(sample.fn.valid_values[1])
        assert sample.fn.raw[1] == sample.fn.raw[0]
    old = store.view(captured_at=105, monotonic_now=15)
    assert old.force.fn.meta.age_s == 5
    store.invalidate(generation=0, captured_at=106)
    invalid = store.view(captured_at=107, monotonic_now=17)
    assert invalid.force.status("fn") == "invalidated"
    assert old.force.fn.meta.valid


def test_o30_queries_both_normal_fields_and_learns_descriptors():
    script = O30DeviceScript(sensor_variant="TSSP_JZG")
    hand = HandDevice(model="O30", side="right", transport=ScriptedO30Transport(script))
    assert hand.get_capabilities().force_field("fn").supported is None
    hand.open()
    try:
        first = hand.get_force("fn", timeout=1)
        assert first.fn.raw[0] == tuple(range(10, 80))
        assert first.fns.raw[0] == (1001,)
        assert first.fn.meta is first.fns.meta
        assert first.fn.units == ("g",) * 5
        assert first.fns.encodings == ("U16",) * 5
        assert hand.get_capabilities().force_field("fn").supported is True
        assert hand.get_capabilities().force_field("ft").supported is False
        count = len(script.sent_messages)
        second = hand.get_force("fns", timeout=1)
        requests = script.sent_messages[count:]
        assert second.fn is not None and second.fns is not None
        assert sum(message.data[:3] == b"\x31\x6e\x01" for message in requests) == 5
        count = len(script.sent_messages)
        for _ in range(10):
            assert second.fn.raw
            assert hand.get_snapshot().force.fns is not None
            assert hand.get_capabilities().force_fields
        assert len(script.sent_messages) == count
        with pytest.raises(UnsupportedOperationError, match="ft"):
            hand.get_force("ft", timeout=1)
        assert len(script.sent_messages) == count
    finally:
        hand.close()


def test_o30_partial_sensors_multiple_resultants_and_repeated_fields():
    script = MixedSensors(sensor_variant="TSSP_JZG")
    script.descriptor = "FnS_U16_2;Fn_U8_2;Fn_U8_1;Px_U8_1"
    hand = HandDevice(model="O30", side="right", transport=ScriptedO30Transport(script))
    with closing(hand.open()):
        sample = hand.get_force("fns", "px")
        assert sample.fn.raw[0] == (4, 5, 6)
        assert sample.fns.raw[0] == (256, 770)
        assert sample.px.raw[0] == (7,)
        assert sample.fn.raw[1] is None
        assert sample.fn.valid_values[1] is None
        assert sample.px.units == (None,) * 5
        assert hand.get_capabilities().force_field("px").sensor_names == (
            "thumb",
            "middle",
            "pinky",
        )


@pytest.mark.parametrize("selector", ["FN", "normal", "", 1, True, None])
def test_invalid_selector_is_rejected_before_io(selector):
    hand = HandDevice(model="L6", side="left", transport=object())
    with pytest.raises(ValidationError):
        hand.get_force(selector)


def test_o30_concurrent_selectors_never_corrupt_sensor_selection():
    script = O30DeviceScript(sensor_variant="TSSP_JZG")
    hand = HandDevice(model="O30", side="right", transport=ScriptedO30Transport(script))
    with closing(hand.open()):
        initial = hand.get_force()
        # Immediate replies can finish before another caller joins. Overlapping
        # request coalescing is checked deterministically in test_transactions.
        barrier = Barrier(8)

        def read(index):
            barrier.wait()
            return hand.get_force("fn" if index % 2 else "fns")

        with ThreadPoolExecutor(max_workers=8) as pool:
            samples = list(pool.map(read, range(8)))
        for sample in samples:
            assert sample.fn.raw == tuple(
                tuple(range(finger * 10, finger * 10 + 70)) for finger in range(1, 6)
            )
            assert sample.fns.raw == tuple((1000 + index,) for index in range(1, 6))
            assert sample.fn.meta is sample.fns.meta is sample.meta
            assert sample.meta.valid
            assert sample.meta.sequence > initial.meta.sequence
            assert (
                sample.meta.connection_generation == initial.meta.connection_generation
            )
