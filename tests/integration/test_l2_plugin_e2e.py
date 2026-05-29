"""Smoke: single store / lookup / load via PluginL2AdapterConfig (FakeMemoryObj)."""

from __future__ import annotations

import pytest

from tests.integration.l2_plugin_support import (
    RUN_INTEGRATION,
    aerospike_l2_adapter,
    object_key,
    wait_for_event_fd,
)
from tests.unit.fakes import FakeMemoryObj

pytestmark = pytest.mark.skipif(
    not RUN_INTEGRATION,
    reason="Set RUN_INTEGRATION=1 (or source .aerospike-ci.env)",
)


def test_aerospike_l2_plugin_store_lookup_load_roundtrip(aerospike_l2_adapter):
    adapter = aerospike_l2_adapter
    key = object_key(42)
    missing = object_key(999)
    store_obj = FakeMemoryObj(bytearray(b"z" * 256))

    store_tid = adapter.submit_store_task([key], [store_obj])
    assert wait_for_event_fd(adapter.get_store_event_fd())
    store_done = adapter.pop_completed_store_tasks()
    assert store_tid in store_done
    assert store_done[store_tid].is_successful()

    lookup_tid = adapter.submit_lookup_and_lock_task([key, missing])
    assert wait_for_event_fd(adapter.get_lookup_and_lock_event_fd())
    bitmap = adapter.query_lookup_and_lock_result(lookup_tid)
    assert bitmap is not None
    assert bitmap.test(0) is True
    assert bitmap.test(1) is False

    loaded = FakeMemoryObj(bytearray(b"\x00" * 256))
    load_tid = adapter.submit_load_task([key], [loaded])
    assert wait_for_event_fd(adapter.get_load_event_fd())
    load_bitmap = adapter.query_load_result(load_tid)
    assert load_bitmap is not None
    assert load_bitmap.test(0) is True
    assert bytes(loaded.byte_array[:256]) == bytes(store_obj.byte_array[:256])

    adapter.submit_unlock([key])
