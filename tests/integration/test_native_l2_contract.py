"""Native L2 adapter contract tests (mirrors ``test_l2_plugin_contract.py``)."""

from __future__ import annotations

import torch

import pytest

from lmcache.v1.distributed.l2_adapters.config import get_type_name_for_config
from lmcache.v1.distributed.l2_adapters.factory import create_l2_adapter_from_registry
from lmcache.v1.distributed.l2_adapters.native_plugin_l2_adapter import (
    NativePluginL2AdapterConfig,
)

from tests.integration.helpers import aerospike_hosts
from tests.integration.native_l2_support import (
    fake_obj,
    load,
    lookup,
    memory_obj,
    native_config,
    native_l2_adapter,
    object_key,
    requires_native_integration,
    store,
    wait_for_event_fd,
)

pytestmark = requires_native_integration


def test_native_plugin_config_from_dict():
    host, port = aerospike_hosts()[0]
    raw = {
        "type": "native_plugin",
        "module_path": "lmcache_aerospike.native_connector",
        "class_name": "AerospikeNativeConnector",
        "adapter_params": {
            "hosts": f"{host}:{port}",
            "namespace": "lmcache",
            "set_name": "kv_chunks_native_it",
            "num_workers": 2,
        },
    }
    cfg = NativePluginL2AdapterConfig.from_dict(raw)
    assert cfg.module_path == "lmcache_aerospike.native_connector"
    assert cfg.class_name == "AerospikeNativeConnector"
    assert get_type_name_for_config(cfg) == "native_plugin"


def test_factory_creates_native_adapter():
    adapter = create_l2_adapter_from_registry(native_config())
    try:
        assert adapter is not None
        assert hasattr(adapter, "submit_store_task")
    finally:
        adapter.close()


def test_event_fds_are_distinct(native_l2_adapter):
    fds = {
        native_l2_adapter.get_store_event_fd(),
        native_l2_adapter.get_lookup_and_lock_event_fd(),
        native_l2_adapter.get_load_event_fd(),
    }
    assert len(fds) == 3


def test_store_lookup_load_workflow_tensor(native_l2_adapter):
    adapter = native_l2_adapter
    key = object_key(4101, model_name="native-contract-tensor")
    store_obj = memory_obj(size=256, fill_value=123.0)
    load_obj = memory_obj(size=256, fill_value=0.0)

    store(adapter, [key], [store_obj])
    lookup_bm = lookup(adapter, [key])
    assert lookup_bm.test(0)

    load_bm = load(adapter, [key], [load_obj])
    assert load_bm.test(0)
    assert torch.all(load_obj.tensor == 123.0)

    adapter.submit_unlock([key])


def test_store_lookup_load_workflow_bytes(native_l2_adapter):
    adapter = native_l2_adapter
    key = object_key(4102, model_name="native-contract-bytes")
    missing = object_key(4199, model_name="native-contract-bytes")
    stored = fake_obj(256, fill=0x7A)
    loaded = fake_obj(256, fill=0)

    store(adapter, [key], [stored])
    lookup_bm = lookup(adapter, [key, missing])
    assert lookup_bm.test(0) is True
    assert lookup_bm.test(1) is False

    load_bm = load(adapter, [key], [loaded])
    assert load_bm.test(0) is True
    assert bytes(loaded.byte_array[:256]) == bytes(stored.byte_array[:256])

    adapter.submit_unlock([key])


def test_multiple_objects_workflow(native_l2_adapter):
    adapter = native_l2_adapter
    n = 8
    keys = [object_key(4200 + i, model_name="native-contract-multi") for i in range(n)]
    store_objs = [memory_obj(size=64, fill_value=float(i * 10)) for i in range(n)]
    load_objs = [memory_obj(size=64, fill_value=0.0) for _ in range(n)]

    store(adapter, keys, store_objs)
    lookup_bm = lookup(adapter, keys)
    for i in range(n):
        assert lookup_bm.test(i)

    load_bm = load(adapter, keys, load_objs)
    for i in range(n):
        assert load_bm.test(i)
        assert torch.all(load_objs[i].tensor == float(i * 10))

    adapter.submit_unlock(keys)


def test_lookup_prefix_miss_bitmap(native_l2_adapter):
    adapter = native_l2_adapter
    hit = object_key(4301, model_name="native-contract-miss")
    miss = object_key(4399, model_name="native-contract-miss")
    obj = memory_obj(size=128)

    store(adapter, [hit], [obj])
    lookup_bm = lookup(adapter, [hit, miss])
    assert lookup_bm.test(0) is True
    assert lookup_bm.test(1) is False
    adapter.submit_unlock([hit])


def test_load_miss_after_lookup_hit(native_l2_adapter):
    """Load buffer smaller than stored payload should fail the slot."""
    adapter = native_l2_adapter
    key = object_key(4302, model_name="native-contract-load-miss")
    store(adapter, [key], [fake_obj(512, fill=0x11)])
    assert lookup(adapter, [key]).test(0)

    short_buf = fake_obj(128, fill=0)
    load_bm = load(adapter, [key], [short_buf])
    assert load_bm.test(0) is False
    adapter.submit_unlock([key])


def test_overwrite_store_replaces_payload(native_l2_adapter):
    adapter = native_l2_adapter
    key = object_key(4303, model_name="native-contract-overwrite")
    first = fake_obj(256, fill=0x01)
    second = fake_obj(256, fill=0x02)
    loaded = fake_obj(256, fill=0)

    store(adapter, [key], [first])
    store(adapter, [key], [second])
    assert lookup(adapter, [key]).test(0)
    assert load(adapter, [key], [loaded]).test(0)
    assert bytes(loaded.byte_array[:256]) == bytes(second.byte_array[:256])
    adapter.submit_unlock([key])


def test_delete_then_lookup_miss(native_l2_adapter):
    adapter = native_l2_adapter
    key = object_key(4304, model_name="native-contract-delete")
    store(adapter, [key], [fake_obj(512, fill=0xEE)])
    assert lookup(adapter, [key]).test(0)
    adapter.delete([key])
    assert lookup(adapter, [key]).test(0) is False


def test_pop_completed_store_tasks_drains_queue(native_l2_adapter):
    adapter = native_l2_adapter
    keys = [
        object_key(4400 + i, model_name="native-contract-pop") for i in range(3)
    ]
    objs = [fake_obj(64, fill=i) for i in range(3)]
    tid = adapter.submit_store_task(keys, objs)
    assert wait_for_event_fd(adapter.get_store_event_fd())
    done = adapter.pop_completed_store_tasks()
    assert tid in done
    assert done[tid].is_successful()
    second = adapter.pop_completed_store_tasks()
    assert second == {} or tid not in second
