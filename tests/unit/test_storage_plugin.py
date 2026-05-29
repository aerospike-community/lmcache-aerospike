from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from lmcache_aerospike import limits
from lmcache_aerospike.client import AerospikeClientHolder
from lmcache_aerospike.storage_plugin import AerospikeStoragePlugin
from tests.unit.conftest import make_config_mock, make_metadata_mock
from tests.unit.fakes import FakeClient, FakeMemoryObj
from tests.unit.test_connector import small_resolved


@pytest.fixture
def plugin(fake_client: FakeClient, as_config, event_loop, small_resolved):
    holder = MagicMock(spec=AerospikeClientHolder)
    holder.client = fake_client
    holder.release = MagicMock()
    config = make_config_mock(
        {
            "storage_plugin.aerospike.hosts": "127.0.0.1:3000",
        }
    )
    metadata = make_metadata_mock()
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
        p = AerospikeStoragePlugin(
            config=config,
            metadata=metadata,
            loop=event_loop,
            plugin_name="aerospike",
        )
    yield p
    p.close()


def test_contains_and_put_get(plugin, fake_client):
    from lmcache.utils import CacheEngineKey

    key = CacheEngineKey(
        model_name="m",
        world_size=1,
        worker_id=0,
        chunk_hash=42,
        dtype=__import__("torch").bfloat16,
    )
    assert plugin.contains(key) is False
    obj = FakeMemoryObj(bytearray(b"x" * 128))
    futures = plugin.batched_submit_put_task([key], [obj])
    assert futures is not None
    futures[0].result(timeout=5)
    assert plugin.contains(key) is True
    got = plugin.get_blocking(key)
    assert got is not None
    if got is not None:
        got.ref_count_down()
