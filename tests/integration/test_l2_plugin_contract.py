"""Live Aerospike L2 tests aligned with meaningful LMCache upstream patterns.

Ports the high-signal cases from:
- ``tests/v1/distributed/test_mock_l2_adapter.py`` (L2AdapterInterface contract)
- ``tests/v1/distributed/test_resp_l2_adapter_integration.py`` (real backend)
- ``examples/lmc_external_l2_adapter/tests/test_plugin.py`` (plugin loader)

Does **not** run LMCache's full suite (mostly mocks / other backends).
"""

from __future__ import annotations

import pytest
import torch

from lmcache.v1.distributed.l2_adapters.config import get_type_name_for_config
from lmcache.v1.distributed.l2_adapters.factory import create_l2_adapter_from_registry
from lmcache.v1.distributed.l2_adapters.plugin_l2_adapter import PluginL2AdapterConfig
from lmcache_aerospike.l2_plugin import AerospikeL2Plugin

from tests.integration.helpers import aerospike_hosts
from tests.integration.l2_plugin_support import (
    RUN_INTEGRATION,
    aerospike_l2_adapter,
    memory_obj,
    object_key,
    plugin_config,
    wait_for_event_fd,
)

pytestmark = pytest.mark.skipif(
    not RUN_INTEGRATION,
    reason="Set RUN_INTEGRATION=1 (or source .aerospike-ci.env)",
)


def test_plugin_config_from_dict():
    """Same surface as lmc_external_l2_adapter config parsing tests."""
    host, port = aerospike_hosts()[0]
    raw = {
        "type": "plugin",
        "module_path": "lmcache_aerospike.l2_plugin",
        "class_name": "AerospikeL2Plugin",
        "adapter_params": {
            "hosts": f"{host}:{port}",
            "namespace": "lmcache",
            "set_name": "kv_chunks",
        },
    }
    cfg = PluginL2AdapterConfig.from_dict(raw)
    assert cfg.module_path == "lmcache_aerospike.l2_plugin"
    assert cfg.class_name == "AerospikeL2Plugin"
    assert get_type_name_for_config(cfg) == "plugin"


def test_factory_creates_aerospike_plugin():
    """Mirrors RESP integration ``test_factory_creates_adapter``."""
    adapter = create_l2_adapter_from_registry(plugin_config())
    try:
        assert isinstance(adapter, AerospikeL2Plugin)
    finally:
        adapter.close()


def test_event_fds_are_distinct(aerospike_l2_adapter):
    fds = {
        aerospike_l2_adapter.get_store_event_fd(),
        aerospike_l2_adapter.get_lookup_and_lock_event_fd(),
        aerospike_l2_adapter.get_load_event_fd(),
    }
    assert len(fds) == 3


def test_store_lookup_load_workflow(aerospike_l2_adapter):
    """Contract from ``TestEndToEndWorkflow.test_store_lookup_load_workflow``."""
    adapter = aerospike_l2_adapter
    key = object_key(1001)
    store_obj = memory_obj(size=256, fill_value=123.0)
    load_obj = memory_obj(size=256, fill_value=0.0)

    store_tid = adapter.submit_store_task([key], [store_obj])
    assert wait_for_event_fd(adapter.get_store_event_fd())
    assert adapter.pop_completed_store_tasks()[store_tid].is_successful()

    lookup_tid = adapter.submit_lookup_and_lock_task([key])
    assert wait_for_event_fd(adapter.get_lookup_and_lock_event_fd())
    lookup_bm = adapter.query_lookup_and_lock_result(lookup_tid)
    assert lookup_bm is not None and lookup_bm.test(0)

    load_tid = adapter.submit_load_task([key], [load_obj])
    assert wait_for_event_fd(adapter.get_load_event_fd())
    load_bm = adapter.query_load_result(load_tid)
    assert load_bm is not None and load_bm.test(0)
    assert torch.all(load_obj.tensor == 123.0)

    adapter.submit_unlock([key])


def test_multiple_objects_workflow(aerospike_l2_adapter):
    """Contract from ``TestEndToEndWorkflow.test_multiple_objects_workflow``."""
    adapter = aerospike_l2_adapter
    n = 5
    keys = [object_key(2000 + i) for i in range(n)]
    store_objs = [memory_obj(size=64, fill_value=float(i * 10)) for i in range(n)]
    load_objs = [memory_obj(size=64, fill_value=0.0) for _ in range(n)]

    store_tid = adapter.submit_store_task(keys, store_objs)
    assert wait_for_event_fd(adapter.get_store_event_fd())
    assert adapter.pop_completed_store_tasks()[store_tid].is_successful()

    lookup_tid = adapter.submit_lookup_and_lock_task(keys)
    assert wait_for_event_fd(adapter.get_lookup_and_lock_event_fd())
    lookup_bm = adapter.query_lookup_and_lock_result(lookup_tid)
    assert lookup_bm is not None
    for i in range(n):
        assert lookup_bm.test(i)

    load_tid = adapter.submit_load_task(keys, load_objs)
    assert wait_for_event_fd(adapter.get_load_event_fd())
    load_bm = adapter.query_load_result(load_tid)
    assert load_bm is not None
    for i in range(n):
        assert load_bm.test(i)
        assert torch.all(load_objs[i].tensor == float(i * 10))

    adapter.submit_unlock(keys)


def test_lookup_prefix_miss_bitmap(aerospike_l2_adapter):
    """Hit + miss in one lookup (our e2e case; RESP-style existence check)."""
    adapter = aerospike_l2_adapter
    hit = object_key(3001)
    miss = object_key(3999)
    obj = memory_obj(size=128)

    store_tid = adapter.submit_store_task([hit], [obj])
    assert wait_for_event_fd(adapter.get_store_event_fd())
    assert adapter.pop_completed_store_tasks()[store_tid].is_successful()

    lookup_tid = adapter.submit_lookup_and_lock_task([hit, miss])
    assert wait_for_event_fd(adapter.get_lookup_and_lock_event_fd())
    bm = adapter.query_lookup_and_lock_result(lookup_tid)
    assert bm is not None
    assert bm.test(0) is True
    assert bm.test(1) is False
    adapter.submit_unlock([hit])
