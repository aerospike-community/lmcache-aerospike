"""Live Aerospike L2 plugin round-trip (LMCache dev + PluginL2AdapterConfig)."""

from __future__ import annotations

import importlib
import os
import select

import pytest

from tests.integration.helpers import (
    aerospike_hosts,
    ensure_native_storage_ops_for_l2_tests,
)

ensure_native_storage_ops_for_l2_tests()

l2_mod = importlib.import_module("lmcache_aerospike.l2_plugin")
if not l2_mod.L2_MP_AVAILABLE:
    pytest.skip(
        "LMCache multiprocess L2 APIs (L2StoreResult) not in this lmcache build",
        allow_module_level=True,
    )

from lmcache.v1.distributed.api import ObjectKey  # noqa: E402
from lmcache.v1.protocol import init_remote_metadata_info  # noqa: E402

init_remote_metadata_info(1)
from lmcache.v1.distributed.l2_adapters.factory import (  # noqa: E402
    create_l2_adapter_from_registry,
)
from lmcache.v1.distributed.l2_adapters.plugin_l2_adapter import (  # noqa: E402
    PluginL2AdapterConfig,
)
from lmcache.v1.platform import consume_fd  # noqa: E402
from tests.unit.fakes import FakeMemoryObj  # noqa: E402

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_INTEGRATION") != "1",
    reason="Set RUN_INTEGRATION=1 (or source .aerospike-ci.env)",
)


def _wait_for_event_fd(event_fd: int, timeout: float = 30.0) -> bool:
    poll = select.poll()
    poll.register(event_fd, select.POLLIN)
    if not poll.poll(timeout * 1000):
        return False
    try:
        consume_fd(event_fd)
    except BlockingIOError:
        pass
    return True


def _object_key(chunk_id: int) -> ObjectKey:
    return ObjectKey(
        chunk_hash=ObjectKey.IntHash2Bytes(chunk_id),
        model_name="lmcache_aerospike_it",
        kv_rank=0,
    )


@pytest.fixture
def aerospike_l2_adapter():
    host, port = aerospike_hosts()[0]
    cfg = PluginL2AdapterConfig(
        module_path="lmcache_aerospike.l2_plugin",
        class_name="AerospikeL2Plugin",
        adapter_params={
            "hosts": f"{host}:{port}",
            "namespace": os.environ.get("AEROSPIKE_TEST_NAMESPACE", "lmcache"),
            "set_name": "kv_chunks_l2_it",
        },
    )
    adapter = create_l2_adapter_from_registry(cfg)
    try:
        yield adapter
    finally:
        adapter.close()


def test_aerospike_l2_plugin_store_lookup_load_roundtrip(aerospike_l2_adapter):
    adapter = aerospike_l2_adapter
    key = _object_key(42)
    missing = _object_key(999)
    store_obj = FakeMemoryObj(bytearray(b"z" * 256))

    store_tid = adapter.submit_store_task([key], [store_obj])
    assert _wait_for_event_fd(adapter.get_store_event_fd())
    store_done = adapter.pop_completed_store_tasks()
    assert store_tid in store_done
    assert store_done[store_tid].is_successful()

    lookup_tid = adapter.submit_lookup_and_lock_task([key, missing])
    assert _wait_for_event_fd(adapter.get_lookup_and_lock_event_fd())
    bitmap = adapter.query_lookup_and_lock_result(lookup_tid)
    assert bitmap is not None
    assert bitmap.test(0) is True
    assert bitmap.test(1) is False

    loaded = FakeMemoryObj(bytearray(b"\x00" * 256))
    load_tid = adapter.submit_load_task([key], [loaded])
    assert _wait_for_event_fd(adapter.get_load_event_fd())
    load_bitmap = adapter.query_load_result(load_tid)
    assert load_bitmap is not None
    assert load_bitmap.test(0) is True
    assert bytes(loaded.byte_array[:256]) == bytes(store_obj.byte_array[:256])

    adapter.submit_unlock([key])
