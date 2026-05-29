"""Phase 2 L2AdapterInterface (Python plugin) for Aerospike."""

from __future__ import annotations

import asyncio
import threading
from collections import defaultdict
from typing import Any, Union

import torch

from lmcache.logging import init_logger
from lmcache.utils import CacheEngineKey
from lmcache.v1.distributed.api import ObjectKey
from lmcache.v1.distributed.internal_api import L2StoreResult
from lmcache.v1.distributed.l2_adapters.base import (
    L2AdapterInterface,
    L2TaskId,
)
from lmcache.v1.distributed.l2_adapters.config import L2AdapterConfigBase
from lmcache.v1.memory_management import MemoryObj
from lmcache.v1.platform import create_event_notifier

from lmcache_aerospike.client import AerospikeClientHolder
from lmcache_aerospike.config import AerospikeConfig
from lmcache_aerospike.object_keys import object_key_to_cache_engine_key
from lmcache_aerospike.plugin_support import build_engine_l2

logger = init_logger(__name__)


def _bitmap(size: int):
    from lmcache.native_storage_ops import Bitmap

    return Bitmap(size)


class AerospikeL2PluginConfig(L2AdapterConfigBase):
    """Typed config for :class:`AerospikeL2Plugin`."""

    def __init__(
        self,
        hosts: str = "127.0.0.1:3000",
        namespace: str = "lmcache",
        set_name: str = "kv_chunks",
        default_ttl_seconds: int = 86400,
        max_capacity_gb: float = 0,
        dtype: str = "bfloat16",
        plugin_name: str = "aerospike",
    ) -> None:
        self.hosts = hosts
        self.namespace = namespace
        self.set_name = set_name
        self.default_ttl_seconds = default_ttl_seconds
        self.max_capacity_gb = max_capacity_gb
        self.dtype = dtype
        self.plugin_name = plugin_name

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> AerospikeL2PluginConfig:
        return cls(
            hosts=str(d.get("hosts", "127.0.0.1:3000")),
            namespace=str(d.get("namespace", "lmcache")),
            set_name=str(d.get("set", d.get("set_name", "kv_chunks"))),
            default_ttl_seconds=int(d.get("default_ttl_seconds", 86400)),
            max_capacity_gb=float(d.get("max_capacity_gb", 0)),
            dtype=str(d.get("dtype", "bfloat16")),
            plugin_name=str(d.get("plugin_name", "aerospike")),
        )

    @classmethod
    def help(cls) -> str:
        return (
            "AerospikeL2Plugin adapter_params:\n"
            "- hosts (str): Aerospike seed hosts host:port[,...]\n"
            "- namespace (str): Aerospike namespace\n"
            "- set / set_name (str): Aerospike set name\n"
            "- default_ttl_seconds (int): record TTL\n"
            "- max_capacity_gb (float): 0 = unknown capacity (no global eviction)\n"
            "- dtype (str): torch dtype name for ObjectKey mapping\n"
        )


_DTYPE_MAP = {
    "float16": torch.float16,
    "bfloat16": torch.bfloat16,
    "float32": torch.float32,
}


class AerospikeL2Plugin(L2AdapterInterface):
    """Multiprocess L2 adapter storing KV chunks in Aerospike."""

    config_class_name = "AerospikeL2PluginConfig"

    def __init__(
        self,
        config: Union[AerospikeL2PluginConfig, dict[str, Any]],
        l1_memory_desc: Any = None,
        **_kwargs: object,
    ) -> None:
        del l1_memory_desc, _kwargs
        if isinstance(config, dict):
            config = AerospikeL2PluginConfig.from_dict(config)

        cap_bytes = int(config.max_capacity_gb * (1024**3))
        super().__init__(max_capacity_bytes=cap_bytes)

        dtype = _DTYPE_MAP.get(config.dtype.lower(), torch.bfloat16)
        self._dtype = dtype
        as_cfg = AerospikeConfig.from_adapter_params(
            {
                "hosts": config.hosts,
                "namespace": config.namespace,
                "set": config.set_name,
                "default_ttl_seconds": config.default_ttl_seconds,
            },
            config.plugin_name,
        )
        self._holder = AerospikeClientHolder.get_or_create(as_cfg)

        self._engine, self._executor = build_engine_l2(
            as_cfg=as_cfg,
            holder=self._holder,
            dtype=dtype,
        )

        self._store_efd = create_event_notifier()
        self._lookup_efd = create_event_notifier()
        self._load_efd = create_event_notifier()

        self._locked: dict[ObjectKey, int] = defaultdict(int)
        self._next_id: L2TaskId = 0
        self._done_store: dict[L2TaskId, L2StoreResult] = {}
        self._done_lookup: dict[L2TaskId, Any] = {}
        self._done_load: dict[L2TaskId, Any] = {}
        self._lock = threading.Lock()

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

        logger.info(
            "AerospikeL2Plugin ready hosts=%s namespace=%s",
            config.hosts,
            config.namespace,
        )

    def _to_ck(self, key: ObjectKey) -> CacheEngineKey:
        return object_key_to_cache_engine_key(key, dtype=self._dtype)

    def get_store_event_fd(self) -> int:
        return self._store_efd.fileno()

    def get_lookup_and_lock_event_fd(self) -> int:
        return self._lookup_efd.fileno()

    def get_load_event_fd(self) -> int:
        return self._load_efd.fileno()

    def submit_store_task(
        self,
        keys: list[ObjectKey],
        objects: list[MemoryObj],
    ) -> L2TaskId:
        with self._lock:
            tid = self._alloc_id()
        asyncio.run_coroutine_threadsafe(
            self._do_store(keys, objects, tid),
            self._loop,
        )
        return tid

    def pop_completed_store_tasks(self) -> dict[L2TaskId, L2StoreResult]:
        with self._lock:
            done = self._done_store
            self._done_store = {}
        return done

    def submit_lookup_and_lock_task(self, keys: list[ObjectKey]) -> L2TaskId:
        with self._lock:
            tid = self._alloc_id()
        self._loop.call_soon_threadsafe(self._do_lookup, keys, tid)
        return tid

    def query_lookup_and_lock_result(self, task_id: L2TaskId) -> Any | None:
        with self._lock:
            return self._done_lookup.pop(task_id, None)

    def submit_unlock(self, keys: list[ObjectKey]) -> None:
        def _unlock(ks: list[ObjectKey]) -> None:
            for ok in ks:
                ck = self._to_ck(ok)
                if ok not in self._locked:
                    continue
                if self._locked[ok] <= 1:
                    del self._locked[ok]
                else:
                    self._locked[ok] -= 1
                self._engine.unpin(ck)

        self._loop.call_soon_threadsafe(_unlock, keys)

    def submit_load_task(
        self,
        keys: list[ObjectKey],
        objects: list[MemoryObj],
    ) -> L2TaskId:
        with self._lock:
            tid = self._alloc_id()
        asyncio.run_coroutine_threadsafe(
            self._do_load(keys, objects, tid),
            self._loop,
        )
        return tid

    def query_load_result(self, task_id: L2TaskId) -> Any | None:
        with self._lock:
            return self._done_load.pop(task_id, None)

    def delete(self, keys: list[ObjectKey]) -> None:
        sizes: list[int] = []
        for ok in keys:
            ck = self._to_ck(ok)
            sizes.append(0)
            self._engine.remove(ck)
        self._notify_keys_deleted(keys, sizes)

    def close(self) -> None:
        if self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)
        self._executor.shutdown(wait=True)
        self._holder.release()
        self._store_efd.close()
        self._lookup_efd.close()
        self._load_efd.close()

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _alloc_id(self) -> L2TaskId:
        tid = self._next_id
        self._next_id += 1
        return tid

    async def _do_store(
        self,
        keys: list[ObjectKey],
        objects: list[MemoryObj],
        tid: L2TaskId,
    ) -> None:
        total = 0
        ok = True
        stored_keys: list[ObjectKey] = []
        stored_sizes: list[int] = []
        try:
            for ok_key, obj in zip(keys, objects, strict=False):
                ck = self._to_ck(ok_key)
                sz = obj.get_size()
                await asyncio.to_thread(self._engine.put, ck, obj)
                stored_keys.append(ok_key)
                stored_sizes.append(sz)
                total += sz
        except Exception:
            logger.exception("Aerospike L2 store task failed")
            ok = False

        with self._lock:
            self._done_store[tid] = L2StoreResult(ok, total)
        if ok and stored_keys:
            self._notify_keys_stored(stored_keys, stored_sizes)
        self._store_efd.notify()

    def _do_lookup(self, keys: list[ObjectKey], tid: L2TaskId) -> None:
        bm = _bitmap(len(keys))
        hit_keys: list[CacheEngineKey] = []
        for i, ok in enumerate(keys):
            ck = self._to_ck(ok)
            if not self._engine.exists(ck):
                continue
            bm.set(i)
            self._locked[ok] += 1
            hit_keys.append(ck)
        if hit_keys:
            self._engine.pin_keys(hit_keys)
            hit_keys = [k for i, k in enumerate(keys) if bm.test(i)]
            self._notify_keys_accessed(hit_keys)
        with self._lock:
            self._done_lookup[tid] = bm
        self._lookup_efd.notify()

    async def _do_load(
        self,
        keys: list[ObjectKey],
        objects: list[MemoryObj],
        tid: L2TaskId,
    ) -> None:
        bm = _bitmap(len(keys))
        for i, (ok, obj) in enumerate(zip(keys, objects, strict=False)):
            ck = self._to_ck(ok)

            def _read() -> bool:
                got = self._engine.get(ck, preallocated=obj)
                return got is not None

            if await asyncio.to_thread(_read):
                bm.set(i)
        with self._lock:
            self._done_load[tid] = bm
        self._load_efd.notify()
