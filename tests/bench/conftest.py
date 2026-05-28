"""Fixtures for in-process connector benchmarks (FakeClient, no Aerospike)."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest
import torch

from lmcache.utils import CacheEngineKey
from lmcache_aerospike import limits
from lmcache_aerospike.client import AerospikeClientHolder
from lmcache_aerospike.config import AerospikeConfig
from lmcache_aerospike.connector import AerospikeRemoteConnector
from lmcache_aerospike.limits import ResolvedLimits
from tests.unit.conftest import make_config_mock, make_metadata_mock
from tests.unit.fakes import FakeClient, FakeLocalCPUBackend, FakeMemoryObj


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def fake_client() -> FakeClient:
    return FakeClient()


def _resolved(target_segment_bytes: int) -> ResolvedLimits:
    effective = 8 * 1024 * 1024 - 65536
    target = min(target_segment_bytes, effective)
    return ResolvedLimits(
        server_max_record_bytes=8 * 1024 * 1024,
        effective_max_segment_bytes=effective,
        max_segment_bytes=effective,
        target_segment_bytes=target,
        single_record_threshold_bytes=target,
        min_segment_bytes=65536,
    )


@pytest.fixture
def bench_connector(fake_client: FakeClient, request):
    """Connector with configurable target_segment_bytes via indirect param."""
    target = getattr(request, "param", 4 * 1024 * 1024)
    resolved = _resolved(target)
    backend = FakeLocalCPUBackend(alloc_size=8 * 1024 * 1024)
    holder = MagicMock(spec=AerospikeClientHolder)
    holder.client = fake_client
    holder.release = MagicMock()
    loop = asyncio.new_event_loop()
    with patch(
        "lmcache_aerospike.connector.limits.discover_limits",
        return_value=limits.ServerLimits(8 * 1024 * 1024, "max-record-size", 120),
    ), patch(
        "lmcache_aerospike.connector.limits.resolve_segment_limits",
        return_value=resolved,
    ):
        conn = AerospikeRemoteConnector(
            config=make_config_mock(),
            metadata=make_metadata_mock(),
            local_cpu_backend=backend,
            loop=loop,
            aerospike_config=AerospikeConfig.from_extra_config(
                {"remote_storage_plugin.aerospike.hosts": "127.0.0.1:3000"},
                "aerospike",
            ),
            client_holder=holder,
        )
    payload_size = min(4 * 1024 * 1024, backend.alloc_size)
    mo = FakeMemoryObj(bytearray(payload_size))
    key = CacheEngineKey(
        model_name="bench",
        world_size=1,
        worker_id=0,
        chunk_hash=42,
        dtype=torch.float16,
    )
    yield conn, key, mo, payload_size, loop
    loop.run_until_complete(conn.close())


@pytest.fixture(params=[1 << 20, 2 << 20, 4 << 20, 8 << 20], ids=["1MiB", "2MiB", "4MiB", "8MiB"])
def segment_sweep_connector(fake_client: FakeClient, request):
    target = request.param
    resolved = _resolved(target)
    backend = FakeLocalCPUBackend(alloc_size=max(target, 8 * 1024 * 1024))
    holder = MagicMock(spec=AerospikeClientHolder)
    holder.client = fake_client
    holder.release = MagicMock()
    loop = asyncio.new_event_loop()
    with patch(
        "lmcache_aerospike.connector.limits.discover_limits",
        return_value=limits.ServerLimits(8 * 1024 * 1024, "max-record-size", 120),
    ), patch(
        "lmcache_aerospike.connector.limits.resolve_segment_limits",
        return_value=resolved,
    ):
        conn = AerospikeRemoteConnector(
            config=make_config_mock(),
            metadata=make_metadata_mock(),
            local_cpu_backend=backend,
            loop=loop,
            aerospike_config=AerospikeConfig.from_extra_config(
                {"remote_storage_plugin.aerospike.hosts": "127.0.0.1:3000"},
                "aerospike",
            ),
            client_holder=holder,
        )
    payload_size = min(4 * 1024 * 1024, backend.alloc_size)
    mo = FakeMemoryObj(bytearray(payload_size))
    key = CacheEngineKey(
        model_name="bench",
        world_size=1,
        worker_id=0,
        chunk_hash=99,
        dtype=torch.float16,
    )
    yield conn, key, mo, target, payload_size, loop
    loop.run_until_complete(conn.close())
