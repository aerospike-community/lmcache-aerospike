from __future__ import annotations

import importlib
from unittest.mock import MagicMock, patch

import pytest

l2_mod = importlib.import_module("lmcache_aerospike.l2_plugin")
if not l2_mod.L2_MP_AVAILABLE:
    pytest.skip(
        "LMCache multiprocess L2 APIs (L2StoreResult) not in this lmcache build",
        allow_module_level=True,
    )

pytest.importorskip("lmcache.native_storage_ops")

from lmcache.v1.distributed.api import ObjectKey

from lmcache_aerospike import limits
from lmcache_aerospike.client import AerospikeClientHolder
from lmcache_aerospike.l2_plugin import AerospikeL2Plugin, AerospikeL2PluginConfig
from tests.unit.fakes import FakeClient, FakeMemoryObj
from tests.unit.test_connector import small_resolved


@pytest.fixture
def l2_plugin(fake_client: FakeClient, small_resolved):
    holder = MagicMock(spec=AerospikeClientHolder)
    holder.client = fake_client
    holder.release = MagicMock()
    cfg = AerospikeL2PluginConfig(hosts="127.0.0.1:3000")
    with patch.object(
        AerospikeClientHolder,
        "get_or_create",
        return_value=holder,
    ), patch(
        "lmcache_aerospike.engine.limits.discover_limits",
        return_value=limits.ServerLimits(8388608, "max-record-size", 120),
    ), patch(
        "lmcache_aerospike.engine.limits.resolve_segment_limits",
        return_value=small_resolved,
    ):
        plugin = AerospikeL2Plugin(cfg)
    yield plugin
    plugin.close()


def test_l2_store_and_lookup(l2_plugin: AerospikeL2Plugin):
    kv_rank = ObjectKey.ComputeKVRank(1, 0, 1, 0)
    ok = ObjectKey(
        chunk_hash=b"\xaa\xbb",
        model_name="m",
        kv_rank=kv_rank,
    )
    obj = FakeMemoryObj(bytearray(b"z" * 256))
    tid = l2_plugin.submit_store_task([ok], [obj])
    for _ in range(50):
        done = l2_plugin.pop_completed_store_tasks()
        if tid in done:
            assert done[tid].is_successful()
            break
    else:
        pytest.fail("store task did not complete")

    ltid = l2_plugin.submit_lookup_and_lock_task([ok])
    for _ in range(50):
        bm = l2_plugin.query_lookup_and_lock_result(ltid)
        if bm is not None:
            assert bm.test(0)
            break
    else:
        pytest.fail("lookup did not complete")
