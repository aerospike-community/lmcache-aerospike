from __future__ import annotations

import importlib
import importlib.util
import select
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

l2_mod = importlib.import_module("lmcache_aerospike.l2_plugin")
if not l2_mod.L2_MP_AVAILABLE:
    pytest.skip(
        "LMCache multiprocess L2 APIs (L2StoreResult) not in this lmcache build",
        allow_module_level=True,
    )

_lmcache_root = Path(__file__).resolve().parents[3] / "LMCache"
_raw_utils = _lmcache_root / "tests/v1/storage_backend/raw_block_test_utils.py"
if _raw_utils.is_file():
    _spec = importlib.util.spec_from_file_location(
        "lmcache_raw_block_test_utils", _raw_utils
    )
    _raw_mod = importlib.util.module_from_spec(_spec)
    assert _spec.loader is not None
    _spec.loader.exec_module(_raw_mod)
    _raw_mod.install_native_storage_ops_fallback()
else:
    pytest.importorskip("lmcache.native_storage_ops")

from lmcache.v1.protocol import init_remote_metadata_info

init_remote_metadata_info(1)

from lmcache.v1.distributed.api import ObjectKey
from lmcache.v1.platform import consume_fd

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


def _wait_fd(fd: int, timeout: float = 5.0) -> bool:
    poll = select.poll()
    poll.register(fd, select.POLLIN)
    if not poll.poll(timeout * 1000):
        return False
    try:
        consume_fd(fd)
    except BlockingIOError:
        pass
    return True


def test_l2_store_and_lookup(l2_plugin: AerospikeL2Plugin):
    kv_rank = ObjectKey.ComputeKVRank(1, 0, 1, 0)
    ok = ObjectKey(
        chunk_hash=b"\xaa\xbb",
        model_name="m",
        kv_rank=kv_rank,
    )
    obj = FakeMemoryObj(bytearray(b"z" * 256))
    tid = l2_plugin.submit_store_task([ok], [obj])
    assert _wait_fd(l2_plugin.get_store_event_fd())
    done = l2_plugin.pop_completed_store_tasks()
    assert tid in done
    assert done[tid].is_successful()

    ltid = l2_plugin.submit_lookup_and_lock_task([ok])
    assert _wait_fd(l2_plugin.get_lookup_and_lock_event_fd())
    bm = l2_plugin.query_lookup_and_lock_result(ltid)
    assert bm is not None
    assert bm.test(0)
