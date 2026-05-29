"""Phase 2 L2AdapterInterface (Python plugin) for Aerospike."""

from __future__ import annotations

import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, TypeVar, Union

_T = TypeVar("_T")

import torch

from lmcache.logging import init_logger
from lmcache.utils import CacheEngineKey
from lmcache.v1.distributed.api import ObjectKey
from lmcache.v1.memory_management import MemoryObj

from lmcache_aerospike.client import AerospikeClientHolder
from lmcache_aerospike.config import AerospikeConfig
from lmcache_aerospike.object_keys import object_key_to_cache_engine_key
from lmcache_aerospike.plugin_support import build_engine_l2

logger = init_logger(__name__)

# Multiprocess L2 APIs landed after the PyPI 0.4.5 line; require L2StoreResult.
L2_MP_AVAILABLE = False
try:
    from lmcache.v1.distributed.internal_api import L2StoreResult
    from lmcache.v1.distributed.l2_adapters.base import (
        L2AdapterInterface,
        L2TaskId,
    )
    from lmcache.v1.distributed.l2_adapters.config import L2AdapterConfigBase
    from lmcache.v1.platform import create_event_notifier

    L2_MP_AVAILABLE = True
except ImportError:
    L2StoreResult = None  # type: ignore[assignment,misc]
    L2AdapterInterface = object  # type: ignore[assignment,misc]
    L2AdapterConfigBase = object  # type: ignore[assignment,misc]
    L2TaskId = int  # type: ignore[assignment,misc]

    def create_event_notifier():  # type: ignore[misc]
        raise ImportError(_L2_IMPORT_ERROR)


_L2_IMPORT_ERROR = (
    "AerospikeL2Plugin requires LMCache multiprocess L2 APIs (L2StoreResult). "
    "PyPI lmcache 0.4.x does not include them yet; use an LMCache build from "
    "the dev branch (or a future release) that ships lmcache.v1.distributed "
    "L2 adapters."
)


def _require_l2_mp() -> None:
    if not L2_MP_AVAILABLE:
        raise ImportError(_L2_IMPORT_ERROR)


def _bitmap(size: int):
    from lmcache.native_storage_ops import Bitmap

    return Bitmap(size)


if L2_MP_AVAILABLE:

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
            # Overlap chunked Aerospike batch I/O (client is thread-safe).
            self._batch_executor = ThreadPoolExecutor(
                max_workers=min(4, max(1, as_cfg.executor_threads // 4)),
                thread_name_prefix="as-l2-batch",
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
            self._executor.submit(self._store_sync, keys, objects, tid)
            return tid

        def pop_completed_store_tasks(self) -> dict[L2TaskId, L2StoreResult]:
            with self._lock:
                done = self._done_store
                self._done_store = {}
            return done

        def submit_lookup_and_lock_task(self, keys: list[ObjectKey]) -> L2TaskId:
            with self._lock:
                tid = self._alloc_id()
            self._executor.submit(self._lookup_sync, keys, tid)
            return tid

        def query_lookup_and_lock_result(self, task_id: L2TaskId) -> Any | None:
            with self._lock:
                return self._done_lookup.pop(task_id, None)

        def submit_unlock(self, keys: list[ObjectKey]) -> None:
            self._executor.submit(self._unlock_sync, keys)

        def submit_load_task(
            self,
            keys: list[ObjectKey],
            objects: list[MemoryObj],
        ) -> L2TaskId:
            with self._lock:
                tid = self._alloc_id()
            self._executor.submit(self._load_sync, keys, objects, tid)
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
            self._batch_executor.shutdown(wait=True)
            self._executor.shutdown(wait=True)
            self._holder.release()
            self._store_efd.close()
            self._lookup_efd.close()
            self._load_efd.close()

        def _alloc_id(self) -> L2TaskId:
            tid = self._next_id
            self._next_id += 1
            return tid

        def _parallel_chunks(
            self,
            fn: Callable[[list[_T]], list[bool]],
            items: list[_T],
            *,
            chunk_size: int = 32,
        ) -> list[bool]:
            if len(items) <= chunk_size:
                return fn(items)
            chunks = [
                items[i : i + chunk_size] for i in range(0, len(items), chunk_size)
            ]
            if len(chunks) == 1:
                return fn(items)
            ordered: list[list[bool] | None] = [None] * len(chunks)
            futures = {
                self._batch_executor.submit(fn, chunk): idx
                for idx, chunk in enumerate(chunks)
            }
            for fut in as_completed(futures):
                idx = futures[fut]
                ordered[idx] = fut.result()
            out: list[bool] = []
            for part in ordered:
                assert part is not None
                out.extend(part)
            return out

        def _store_sync(
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
                items = [
                    (self._to_ck(ok_key), obj)
                    for ok_key, obj in zip(keys, objects, strict=False)
                ]
                results = self._parallel_chunks(
                    lambda chunk: self._engine.batched_put(chunk, pinned=False),
                    items,
                )
                for ok_key, obj, success in zip(keys, objects, results, strict=False):
                    if not success:
                        ok = False
                        continue
                    stored_keys.append(ok_key)
                    stored_sizes.append(obj.get_size())
                    total += obj.get_size()
            except Exception:
                logger.exception("Aerospike L2 store task failed")
                ok = False

            with self._lock:
                self._done_store[tid] = L2StoreResult(ok, total)
            if ok and stored_keys:
                self._notify_keys_stored(stored_keys, stored_sizes)
            self._store_efd.notify()

        def _lookup_sync(self, keys: list[ObjectKey], tid: L2TaskId) -> None:
            bm = _bitmap(len(keys))
            cks = [self._to_ck(ok) for ok in keys]
            try:
                hits = self._engine.batched_exists_mask(cks)
            except Exception:
                logger.exception("Aerospike L2 lookup task failed")
                hits = [False] * len(keys)

            hit_cks: list[CacheEngineKey] = []
            for i, (ok, ck, hit) in enumerate(
                zip(keys, cks, hits, strict=False)
            ):
                if not hit:
                    continue
                bm.set(i)
                self._locked[ok] += 1
                hit_cks.append(ck)

            if hit_cks:
                self._engine.pin_keys(hit_cks)
                accessed = [k for i, k in enumerate(keys) if bm.test(i)]
                self._notify_keys_accessed(accessed)

            with self._lock:
                self._done_lookup[tid] = bm
            self._lookup_efd.notify()

        def _load_sync(
            self,
            keys: list[ObjectKey],
            objects: list[MemoryObj],
            tid: L2TaskId,
        ) -> None:
            bm = _bitmap(len(keys))
            try:
                items = [
                    (self._to_ck(ok), obj)
                    for ok, obj in zip(keys, objects, strict=False)
                ]
                hits = self._parallel_chunks(
                    lambda chunk: self._engine.batched_get_preallocated(chunk),
                    items,
                )
                for i, hit in enumerate(hits):
                    if hit:
                        bm.set(i)
            except Exception:
                logger.exception("Aerospike L2 load task failed")

            with self._lock:
                self._done_load[tid] = bm
            self._load_efd.notify()

        def _unlock_sync(self, keys: list[ObjectKey]) -> None:
            for ok in keys:
                ck = self._to_ck(ok)
                if ok not in self._locked:
                    continue
                if self._locked[ok] <= 1:
                    del self._locked[ok]
                else:
                    self._locked[ok] -= 1
                self._engine.unpin(ck)

else:

    class AerospikeL2PluginConfig:  # type: ignore[no-redef]
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            _require_l2_mp()

    class AerospikeL2Plugin:  # type: ignore[no-redef]
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            _require_l2_mp()
