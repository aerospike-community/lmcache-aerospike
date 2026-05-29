"""Native L2 payload size and multi-segment sharding tests."""

from __future__ import annotations

import pytest
import torch

from tests.integration.native_l2_support import (
    KIB,
    NATIVE_IT_NAMESPACE,
    NATIVE_IT_SET,
    fake_obj,
    fake_obj_pattern,
    load,
    lookup,
    memory_obj,
    meta_bins_for_key,
    multi_segment_payload_size,
    native_l2_adapter,
    object_key,
    requires_native_integration,
    round_trip_payload_sizes,
    store,
)

pytestmark = requires_native_integration


@pytest.mark.parametrize("payload_bytes", round_trip_payload_sizes())
def test_round_trip_payload_size_matrix(native_l2_adapter, payload_bytes: int):
    adapter = native_l2_adapter
    key = object_key(
        5000 + payload_bytes,
        model_name=f"native-payload-{payload_bytes}",
    )
    stored = fake_obj_pattern(payload_bytes)
    loaded = fake_obj(payload_bytes, fill=0)

    store(adapter, [key], [stored])
    assert lookup(adapter, [key]).test(0)
    assert load(adapter, [key], [loaded]).test(0)
    assert bytes(loaded.byte_array[:payload_bytes]) == bytes(
        stored.byte_array[:payload_bytes]
    )
    adapter.submit_unlock([key])


def test_multi_segment_meta_record(native_l2_adapter):
    adapter = native_l2_adapter
    size = multi_segment_payload_size()
    key = object_key(5100, model_name="native-payload-sharded")
    stored = fake_obj_pattern(size)
    loaded = fake_obj(size, fill=0)

    store(adapter, [key], [stored])
    bins = meta_bins_for_key(
        key, namespace=NATIVE_IT_NAMESPACE, set_name=NATIVE_IT_SET
    )
    assert bins is not None
    assert bins.get("state") == "ready"
    nseg = int(bins.get("nseg", 1))
    assert nseg > 1

    assert lookup(adapter, [key]).test(0)
    assert load(adapter, [key], [loaded]).test(0)
    assert bytes(loaded.byte_array[:size]) == bytes(stored.byte_array[:size])
    adapter.submit_unlock([key])


def test_tensor_payload_4k(native_l2_adapter):
    adapter = native_l2_adapter
    key = object_key(5200, model_name="native-payload-4k-tensor")
    store_obj = memory_obj(size=4 * KIB, fill_value=42.0)
    load_obj = memory_obj(size=4 * KIB, fill_value=0.0)

    store(adapter, [key], [store_obj])
    assert lookup(adapter, [key]).test(0)
    load_bm = load(adapter, [key], [load_obj])
    assert load_bm.test(0)
    assert torch.all(load_obj.tensor == 42.0)
    adapter.submit_unlock([key])


def test_mixed_sizes_in_one_batch(native_l2_adapter):
    adapter = native_l2_adapter
    sizes = [512, 2 * KIB, 8 * KIB]
    keys = [
        object_key(5300 + i, model_name="native-payload-mixed") for i in range(len(sizes))
    ]
    store_objs = [fake_obj(n, fill=0x10 + i) for i, n in enumerate(sizes)]
    load_objs = [fake_obj(n, fill=0) for n in sizes]

    store(adapter, keys, store_objs)
    lookup_bm = lookup(adapter, keys)
    for i in range(len(sizes)):
        assert lookup_bm.test(i)

    load_bm = load(adapter, keys, load_objs)
    for i, n in enumerate(sizes):
        assert load_bm.test(i)
        assert bytes(load_objs[i].byte_array[:n]) == bytes(store_objs[i].byte_array[:n])

    adapter.submit_unlock(keys)
