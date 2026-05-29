"""Phase 2 StoragePluginInterface backend for Aerospike."""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future
from typing import Any, Callable, List, Optional, Sequence, Union

import torch

from lmcache import torch_device_type
from lmcache.logging import init_logger
from lmcache.utils import CacheEngineKey
from lmcache.v1.config import LMCacheEngineConfig
from lmcache.v1.memory_management import (
    AdHocMemoryAllocator,
    MemoryAllocatorInterface,
    MemoryFormat,
    MemoryObj,
)
from lmcache.v1.metadata import LMCacheMetadata
from lmcache.v1.storage_backend.abstract_backend import (
    AllocatorBackendInterface,
    StoragePluginInterface,
)

from lmcache_aerospike.client import AerospikeClientHolder
from lmcache_aerospike.config import AerospikeConfig
from lmcache_aerospike.engine import AerospikeStorageEngine
from lmcache_aerospike.plugin_support import build_engine

logger = init_logger(__name__)


class AerospikeStoragePlugin(StoragePluginInterface, AllocatorBackendInterface):
    """LMCache storage plugin backed by Aerospike (Phase 1 data model)."""

    def __init__(
        self,
        config: Optional[LMCacheEngineConfig] = None,
        metadata: Optional[LMCacheMetadata] = None,
        local_cpu_backend=None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        dst_device: str = torch_device_type,
        plugin_name: str = "aerospike",
    ) -> None:
        super().__init__(
            dst_device=dst_device,
            config=config,
            metadata=metadata,
            local_cpu_backend=local_cpu_backend,
            loop=loop,
        )
        if self.config is None or self.metadata is None:
            raise ValueError("AerospikeStoragePlugin requires config and metadata")

        self._plugin_name = plugin_name
        self.as_cfg = AerospikeConfig.from_storage_plugin_config(
            self.config.extra_config,
            plugin_name,
        )
        self._holder = AerospikeClientHolder.get_or_create(self.as_cfg)
        self._allocator: MemoryAllocatorInterface = AdHocMemoryAllocator(device="cpu")
        self._engine, self._executor, self._bridge = build_engine(
            config=self.config,
            metadata=self.metadata,
            as_cfg=self.as_cfg,
            holder=self._holder,
            allocator=self._allocator,
        )
        self.loop = loop
        self._put_lock = threading.Lock()
        self._put_tasks: set[CacheEngineKey] = set()
        self._closed = False
        logger.info(
            "AerospikeStoragePlugin ready plugin=%s hosts=%s namespace=%s",
            plugin_name,
            self.as_cfg.hosts,
            self.as_cfg.namespace,
        )

    def __str__(self) -> str:
        return "AerospikeStoragePlugin"

    # --- AllocatorBackendInterface ---

    def initialize_allocator(
        self,
        config: LMCacheEngineConfig,
        metadata: LMCacheMetadata,
    ) -> MemoryAllocatorInterface:
        del config, metadata
        return self._allocator

    def get_memory_allocator(self) -> MemoryAllocatorInterface:
        return self._allocator

    def get_allocator_backend(self) -> AllocatorBackendInterface:
        if self.local_cpu_backend is not None:
            return self.local_cpu_backend
        return self

    def allocate(
        self,
        shapes: Union[torch.Size, list[torch.Size]],
        dtypes: Union[torch.dtype, list[torch.dtype]],
        fmt: MemoryFormat = MemoryFormat.KV_2LTD,
        eviction: bool = True,
        busy_loop: bool = True,
    ) -> Optional[MemoryObj]:
        del eviction, busy_loop
        return self._allocator.allocate(shapes, dtypes, fmt)

    def batched_allocate(
        self,
        shapes: Union[torch.Size, list[torch.Size]],
        dtypes: Union[torch.dtype, list[torch.dtype]],
        batch_size: int,
        fmt: MemoryFormat = MemoryFormat.KV_2LTD,
        eviction: bool = True,
        busy_loop: bool = True,
    ) -> Optional[list[MemoryObj]]:
        del eviction, busy_loop
        return self._allocator.batched_allocate(shapes, dtypes, batch_size, fmt)

    def calculate_chunk_budget(self) -> int:
        return 0

    # --- StorageBackendInterface ---

    def contains(self, key: CacheEngineKey, pin: bool = False) -> bool:
        if pin and self._engine.exists(key):
            self._engine.pin(key)
            return True
        return self._engine.exists(key)

    def exists_in_put_tasks(self, key: CacheEngineKey) -> bool:
        with self._put_lock:
            return key in self._put_tasks

    def batched_submit_put_task(
        self,
        keys: Sequence[CacheEngineKey],
        objs: List[MemoryObj],
        transfer_spec: Any = None,
        on_complete_callback: Optional[Callable[[CacheEngineKey], None]] = None,
    ) -> Union[List[Future], None]:
        del transfer_spec
        keys_list = list(keys)

        def _put_all() -> None:
            for key, obj in zip(keys_list, objs, strict=True):
                try:
                    self._engine.put(key, obj)
                    if on_complete_callback is not None:
                        on_complete_callback(key)
                finally:
                    with self._put_lock:
                        self._put_tasks.discard(key)

        with self._put_lock:
            self._put_tasks.update(keys_list)

        fut = self._executor.submit(_put_all)
        return [fut]

    async def async_batched_submit_put_task(
        self,
        keys: Sequence[CacheEngineKey],
        objs: List[MemoryObj],
        transfer_spec: Any = None,
        on_complete_callback: Optional[Callable[[CacheEngineKey], None]] = None,
    ) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self._executor,
            lambda: self.batched_submit_put_task(
                keys, objs, transfer_spec, on_complete_callback
            ),
        )

    def get_blocking(self, key: CacheEngineKey) -> Optional[MemoryObj]:
        return self._engine.get(key)

    async def batched_async_contains(
        self,
        lookup_id: str,
        keys: List[CacheEngineKey],
        pin: bool = False,
    ) -> int:
        del lookup_id
        loop = asyncio.get_running_loop()

        def _run() -> int:
            count = self._engine.batched_contains(keys)
            if pin and count > 0:
                self._engine.pin_keys(keys[:count])
            return count

        return await loop.run_in_executor(self._executor, _run)

    async def batched_get_non_blocking(
        self,
        lookup_id: str,
        keys: list[CacheEngineKey],
        transfer_spec: Any = None,
    ) -> list[MemoryObj]:
        del lookup_id, transfer_spec
        loop = asyncio.get_running_loop()
        memory_objs: list[MemoryObj] = []
        for key in keys:
            mo = await loop.run_in_executor(
                self._executor, self._engine.get, key
            )
            if mo is None:
                break
            memory_objs.append(mo)
        return memory_objs

    def pin(self, key: CacheEngineKey) -> bool:
        return self._engine.pin(key)

    def unpin(self, key: CacheEngineKey) -> bool:
        return self._engine.unpin(key)

    def remove(self, key: CacheEngineKey, force: bool = True) -> bool:
        del force
        return self._engine.remove(key)

    def touch_cache(self) -> None:
        return None

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._executor.shutdown(wait=True)
        self._holder.release()
