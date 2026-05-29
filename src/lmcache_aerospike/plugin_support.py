"""Shared helpers for Phase 2 storage plugin and L2 adapter construction."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import torch

from lmcache.utils import CacheEngineKey
from lmcache.v1.config import LMCacheEngineConfig
from lmcache.v1.memory_management import MemoryObj
from lmcache.v1.metadata import LMCacheMetadata
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector

from lmcache_aerospike import serde
from lmcache_aerospike.client import AerospikeClientHolder
from lmcache_aerospike.config import AerospikeConfig
from lmcache_aerospike.engine import AerospikeStorageEngine, EngineMetadata


class _MetadataBridge(RemoteConnector):
    """Concrete RemoteConnector used only for metadata + reshape helpers."""

    async def exists(self, key: CacheEngineKey) -> bool:
        del key
        return False

    def exists_sync(self, key: CacheEngineKey) -> bool:
        del key
        return False

    async def get(self, key: CacheEngineKey) -> Optional[MemoryObj]:
        del key
        return None

    async def put(self, key: CacheEngineKey, memory_obj: MemoryObj) -> None:
        del key, memory_obj

    async def list(self) -> List[str]:
        return []

    async def close(self) -> None:
        return None


def build_engine(
    *,
    config: LMCacheEngineConfig,
    metadata: LMCacheMetadata,
    as_cfg: AerospikeConfig,
    holder: AerospikeClientHolder,
    allocator,
) -> tuple[AerospikeStorageEngine, ThreadPoolExecutor, _MetadataBridge]:
    bridge = _MetadataBridge(config, metadata)
    resolved = AerospikeStorageEngine.discover_and_resolve(holder.client, as_cfg)
    engine = AerospikeStorageEngine(
        cfg=as_cfg,
        client=holder.client,
        metadata=EngineMetadata.from_remote_connector(bridge),
        resolved=resolved,
        reshape_partial_chunk=bridge.reshape_partial_chunk,
        allocate_for_read=lambda bins: serde.allocate_for_read(allocator, bridge, bins),
    )
    executor = ThreadPoolExecutor(
        max_workers=as_cfg.executor_threads,
        thread_name_prefix="as-plugin",
    )
    return engine, executor, bridge


def build_engine_l2(
    *,
    as_cfg: AerospikeConfig,
    holder: AerospikeClientHolder,
    dtype: torch.dtype,
) -> tuple[AerospikeStorageEngine, ThreadPoolExecutor]:
    """Lightweight engine for L2 (load uses caller-provided buffers)."""
    from lmcache.v1.memory_management import MemoryFormat

    resolved = AerospikeStorageEngine.discover_and_resolve(holder.client, as_cfg)
    # Bench / MP L2 callers pass opaque byte buffers (e.g. bench l2 TensorMemoryObj)
    # without per-chunk RemoteMetadata shapes; use fixed connector metadata on read.
    meta = EngineMetadata(
        save_chunk_meta=False,
        meta_shapes=[torch.Size([2, 1, 8, 128])],
        meta_dtypes=[dtype],
        meta_fmt=MemoryFormat.KV_2LTD,
    )

    def _reshape(memory_obj, _bytes_read: int):
        return memory_obj

    def _allocate(_bins: dict):
        return None, False

    engine = AerospikeStorageEngine(
        cfg=as_cfg,
        client=holder.client,
        metadata=meta,
        resolved=resolved,
        reshape_partial_chunk=_reshape,
        allocate_for_read=_allocate,
    )
    executor = ThreadPoolExecutor(
        max_workers=as_cfg.executor_threads,
        thread_name_prefix="as-l2",
    )
    return engine, executor
