"""Aerospike RemoteConnector implementation."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

from lmcache.logging import init_logger
from lmcache.utils import CacheEngineKey
from lmcache.v1.memory_management import MemoryObj
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector

from lmcache_aerospike import metrics, serde
from lmcache_aerospike.client import AerospikeClientHolder
from lmcache_aerospike.config import AerospikeConfig
from lmcache_aerospike.engine import AerospikeStorageEngine, EngineMetadata

logger = init_logger(__name__)


class AerospikeRemoteConnector(RemoteConnector):
    def __init__(
        self,
        *,
        config,
        metadata,
        local_cpu_backend,
        loop,
        aerospike_config: AerospikeConfig,
        client_holder: AerospikeClientHolder,
    ) -> None:
        super().__init__(config, metadata)
        self.cfg = aerospike_config
        self.local_cpu_backend = local_cpu_backend
        self.loop = loop
        self._holder = client_holder
        self._client = client_holder.client
        self._executor = ThreadPoolExecutor(
            max_workers=aerospike_config.executor_threads,
            thread_name_prefix="as-conn",
        )
        self._batch_sem = asyncio.Semaphore(aerospike_config.batch_max_in_flight)
        self._closed = False
        self._list_disabled_logged = False
        resolved = AerospikeStorageEngine.discover_and_resolve(
            self._client, self.cfg
        )
        self._engine = AerospikeStorageEngine(
            cfg=self.cfg,
            client=self._client,
            metadata=EngineMetadata.from_remote_connector(self),
            resolved=resolved,
            reshape_partial_chunk=self.reshape_partial_chunk,
            allocate_for_read=lambda bins: serde.allocate_for_read(
                self.local_cpu_backend, self, bins
            ),
        )

    async def _run(self, fn, *args):
        return await self.loop.run_in_executor(self._executor, fn, *args)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._executor.shutdown(wait=True)
        self._holder.release()

    def exists_sync(self, key: CacheEngineKey) -> bool:
        return self._engine.exists(key)

    async def exists(self, key: CacheEngineKey) -> bool:
        return await self._run(self._engine.exists, key)

    async def get(self, key: CacheEngineKey) -> Optional[MemoryObj]:
        return await self._run(self._engine.get, key)

    async def put(self, key: CacheEngineKey, memory_obj: MemoryObj) -> None:
        await self._run(self._engine.put, key, memory_obj)

    def support_batched_get(self) -> bool:
        return True

    async def batched_get(self, keys: List[CacheEngineKey]) -> List[Optional[MemoryObj]]:
        async with self._batch_sem:
            with metrics.track_in_flight():
                order = list(keys)
                unique = list(dict.fromkeys(order))
                fetched = {k: await self.get(k) for k in unique}
                return [fetched[k] for k in order]

    def support_batched_put(self) -> bool:
        return True

    async def batched_put(
        self, keys: List[CacheEngineKey], memory_objs: List[MemoryObj]
    ) -> None:
        async with self._batch_sem:
            with metrics.track_in_flight():
                await asyncio.gather(
                    *(self.put(k, mo) for k, mo in zip(keys, memory_objs))
                )

    def support_batched_contains(self) -> bool:
        return True

    def batched_contains(self, keys: List[CacheEngineKey]) -> int:
        return self._engine.batched_contains(keys)

    async def batched_async_contains(
        self,
        lookup_id: str,
        keys: List[CacheEngineKey],
        pin: bool = False,
    ) -> int:
        del lookup_id

        def _contains_and_pin() -> int:
            count = self._engine.batched_contains(keys)
            if pin and count > 0:
                self._engine.pin_keys(keys[:count])
            return count

        return await self._run(_contains_and_pin)

    async def _sem_get(self, key: CacheEngineKey) -> Optional[MemoryObj]:
        async with self._batch_sem:
            return await self.get(key)

    async def batched_get_non_blocking(
        self,
        lookup_id: str,
        keys: List[CacheEngineKey],
    ) -> List[MemoryObj]:
        del lookup_id
        results = await asyncio.gather(
            *(self._sem_get(k) for k in keys), return_exceptions=True
        )
        memory_objs: List[MemoryObj] = []
        found_failure = False
        for result in results:
            if found_failure:
                if isinstance(result, MemoryObj):
                    result.ref_count_down()
            elif isinstance(result, MemoryObj):
                memory_objs.append(result)
            else:
                if isinstance(result, Exception):
                    logger.warning("Exception during batched get: %s", result)
                found_failure = True
        return memory_objs

    def remove_sync(self, key: CacheEngineKey) -> bool:
        return self._engine.remove(key)

    async def list(self) -> List[str]:
        if not self.cfg.enable_list:
            if not self._list_disabled_logged:
                logger.info("list() disabled; set enable_list=true to scan")
                self._list_disabled_logged = True
            return []

        def _scan() -> List[str]:
            keys_out: List[str] = []
            scan = self._client.scan(self.cfg.namespace, self.cfg.set_name)
            for _key, _meta, _bins in scan.results():
                user_key = _key[2]
                if isinstance(user_key, (bytes, bytearray)):
                    user_key = user_key.decode("utf-8", errors="replace")
                if str(user_key).endswith("|m"):
                    keys_out.append(str(user_key)[:-2])
            return keys_out

        return await self._run(_scan)

    def support_ping(self) -> bool:
        return True

    async def ping(self) -> int:
        def _ping() -> int:
            try:
                if self._client.is_connected() and self._client.get_node_names():
                    return 0
            except Exception:
                pass
            return 1

        return await self._run(_ping)
