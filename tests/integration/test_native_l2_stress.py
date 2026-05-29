"""Higher fan-out native L2 scenarios (still CI-sized)."""

from __future__ import annotations

import pytest

from tests.integration.native_l2_support import (
    KIB,
    fake_obj,
    load,
    lookup,
    native_l2_adapter,
    object_key,
    requires_native_integration,
    store,
)

pytestmark = requires_native_integration


@pytest.mark.parametrize("batch_size", [16, 32])
def test_large_batch_store_lookup_load(native_l2_adapter, batch_size: int):
    adapter = native_l2_adapter
    keys = [
        object_key(8000 + i, model_name=f"native-stress-{batch_size}")
        for i in range(batch_size)
    ]
    payload_len = 1024
    store_objs = [fake_obj(payload_len, fill=(0x20 + (i % 200))) for i in range(batch_size)]
    load_objs = [fake_obj(payload_len, fill=0) for _ in range(batch_size)]

    store(adapter, keys, store_objs)
    lookup_bm = lookup(adapter, keys)
    for i in range(batch_size):
        assert lookup_bm.test(i)

    load_bm = load(adapter, keys, load_objs)
    for i in range(batch_size):
        assert load_bm.test(i)
        assert bytes(load_objs[i].byte_array[:payload_len]) == bytes(
            store_objs[i].byte_array[:payload_len]
        )

    adapter.submit_unlock(keys)


def test_interleaved_missing_keys_in_large_lookup(native_l2_adapter):
    adapter = native_l2_adapter
    stored_keys = [
        object_key(8100 + i, model_name="native-stress-interleave") for i in range(4)
    ]
    missing_keys = [
        object_key(8200 + i, model_name="native-stress-interleave") for i in range(4)
    ]
    all_keys = []
    for i in range(4):
        all_keys.append(stored_keys[i])
        all_keys.append(missing_keys[i])

    store(
        adapter,
        stored_keys,
        [fake_obj(512, fill=0x33 + i) for i in range(4)],
    )
    lookup_bm = lookup(adapter, all_keys)
    for i in range(4):
        assert lookup_bm.test(2 * i) is True
        assert lookup_bm.test(2 * i + 1) is False
    adapter.submit_unlock(stored_keys)


def test_repeated_store_same_key_idempotent_payload(native_l2_adapter):
    adapter = native_l2_adapter
    key = object_key(8300, model_name="native-stress-repeat")
    for fill in (0x01, 0x02, 0x03):
        store(adapter, [key], [fake_obj(2 * KIB, fill=fill)])
    loaded = fake_obj(2 * KIB, fill=0)
    assert lookup(adapter, [key]).test(0)
    assert load(adapter, [key], [loaded]).test(0)
    assert bytes(loaded.byte_array[: 2 * KIB]) == bytes([0x03]) * (2 * KIB)
    adapter.submit_unlock([key])
