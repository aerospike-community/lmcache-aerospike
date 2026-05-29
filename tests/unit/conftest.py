"""Pytest fixtures for unit tests."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest
import torch

from lmcache_aerospike.config import AerospikeConfig
from lmcache_aerospike.limits import ResolvedLimits
from tests.unit.fakes import FakeClient, FakeLocalCPUBackend


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def as_config() -> AerospikeConfig:
    return AerospikeConfig.from_extra_config(
        {"remote_storage_plugin.aerospike.hosts": "127.0.0.1:3000"},
        "aerospike",
    )


@pytest.fixture
def resolved_limits() -> ResolvedLimits:
    effective = 8388608 - 65536
    return ResolvedLimits(
        server_max_record_bytes=8388608,
        effective_max_segment_bytes=effective,
        max_segment_bytes=effective,
        target_segment_bytes=4194304,
        single_record_threshold_bytes=4194304,
        min_segment_bytes=65536,
    )


@pytest.fixture
def fake_client() -> FakeClient:
    return FakeClient()


@pytest.fixture
def fake_backend() -> FakeLocalCPUBackend:
    return FakeLocalCPUBackend(alloc_size=64 * 1024 * 1024)


def make_metadata_mock():
    metadata = MagicMock()
    metadata.get_shapes.return_value = [torch.Size([2, 1, 8, 128])]
    metadata.get_dtypes.return_value = [torch.float16]
    metadata.use_mla = False
    metadata.chunk_size = 8
    metadata.get_num_groups.return_value = 1
    metadata.world_size = 1
    return metadata


def make_config_mock(extra_config=None):
    config = MagicMock()
    config.extra_config = extra_config
    config.use_layerwise = False
    return config
