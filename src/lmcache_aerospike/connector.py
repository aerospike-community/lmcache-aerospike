"""Aerospike RemoteConnector implementation."""

from __future__ import annotations

import asyncio
import zlib
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import aerospike
from aerospike import exception as ax
from aerospike_helpers.batch import records as br
from aerospike_helpers.operations import operations as op

from lmcache.logging import init_logger
from lmcache.utils import CacheEngineKey
from lmcache.v1.memory_management import MemoryObj
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector

from lmcache_aerospike import keys as K
from lmcache_aerospike import limits, policies, serde
from lmcache_aerospike.client import AerospikeClientHolder
from lmcache_aerospike.config import AerospikeConfig
from lmcache_aerospike.errors import (
    AerospikeBusyError,
    AerospikeConnectionError,
    AerospikeRecordTooBigError,
    AerospikeTTLConfigError,
    classify,
    map_aerospike_error,
)
from lmcache_aerospike.sharding import plan as shard_plan

logger = init_logger(__name__)

AEROSPIKE_OK = 0
AEROSPIKE_ERR_RECORD_NOT_FOUND = 2
AEROSPIKE_ERR_KEY_BUSY = 14


class _BatchResultError(Exception):
    def __init__(self, code: int) -> None:
        self.code = code
        super().__init__(f"batch result code {code}")


def _result_to_exc(result_code: int) -> BaseException:
    return _BatchResultError(result_code)


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
        self._limits_ready = False
        self._resolved: limits.ResolvedLimits | None = None
        self._list_disabled_logged = False
        self._ensure_limits()

    def _ensure_limits(self) -> None:
        if self._limits_ready:
            return
        server = limits.discover_limits(self._client, self.cfg.namespace)
        self._resolved = limits.resolve_segment_limits(self.cfg, server)
        self._limits_ready = True

    def post_init(self) -> None:
        self._ensure_limits()

    def _ttl_value(self, pinned: bool) -> int:
        if pinned:
            return -1
        return self.cfg.default_ttl_seconds

    def _put_meta(self, ttl: int) -> dict:
        # aerospike client <19: meta={"ttl": N}
        return {"ttl": ttl}

    async def _run(self, fn, *args):
        return await self.loop.run_in_executor(self._executor, fn, *args)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._executor.shutdown(wait=True)
        self._holder.release()

    def _exists_sync_impl(self, ck: CacheEngineKey) -> bool:
        mk = K.meta_key(self.cfg.namespace, self.cfg.set_name, ck)
        try:
            _key, _meta, _bins = self._client.select(mk, ["state"])
        except ax.RecordNotFound:
            return False
        except ax.AerospikeError as exc:
            if classify(exc) == "timeout":
                logger.warning("exists timeout for %s", ck)
                return False
            raise map_aerospike_error("exists", exc) from exc
        return True

    def exists_sync(self, key: CacheEngineKey) -> bool:
        return self._exists_sync_impl(key)

    async def exists(self, key: CacheEngineKey) -> bool:
        return await self._run(self._exists_sync_impl, key)

    def _get_sync_impl(self, ck: CacheEngineKey) -> Optional[MemoryObj]:
        mk = K.meta_key(self.cfg.namespace, self.cfg.set_name, ck)
        try:
            _key, _meta, bins = self._client.get(mk)
        except ax.RecordNotFound:
            return None
        except ax.AerospikeError as exc:
            if classify(exc) == "timeout":
                logger.warning("get timeout for %s", ck)
                return None
            if classify(exc) == "connection":
                raise AerospikeConnectionError(str(exc)) from exc
            raise map_aerospike_error("get", exc) from exc

        if bins.get("state") != "ready":
            return None

        nseg = int(bins["nseg"])
        mo, expect_reshape = serde.allocate_for_read(
            self.local_cpu_backend, self, bins
        )
        if mo is None:
            return None

        total_written = 0
        try:
            if nseg == 1:
                payload = bins.get("b")
                if payload is None:
                    return None
                total_written = serde.write_payload_into(mo, payload)
            else:
                seg_keys = K.segment_keys(
                    self.cfg.namespace, self.cfg.set_name, ck, nseg
                )
                brs = self._client.batch_read(seg_keys, ["b"])
                offset = 0
                for rec in brs.batch_records:
                    if rec.result != AEROSPIKE_OK or rec.record is None:
                        logger.warning("missing/in-flight segment for %s", ck)
                        mo.ref_count_down()
                        return None
                    chunk = rec.record[2]["b"]
                    total_written += serde.write_payload_into(mo, chunk, offset)
                    offset += len(chunk)

            if self.cfg.enable_crc32:
                expected = bins.get("crc32")
                if expected is not None:
                    payload_view = memoryview(mo.byte_array)[:total_written]
                    actual = zlib.crc32(payload_view) & 0xFFFFFFFF
                    if actual != expected:
                        logger.error("crc32 mismatch for %s", ck)
                        mo.ref_count_down()
                        return None

            if expect_reshape:
                mo = self.reshape_partial_chunk(mo, total_written)
            return mo
        except Exception:
            mo.ref_count_down()
            raise

    async def get(self, key: CacheEngineKey) -> Optional[MemoryObj]:
        return await self._run(self._get_sync_impl, key)

    def _put_sync_impl(self, ck: CacheEngineKey, memory_obj: MemoryObj) -> None:
        assert self._resolved is not None
        view = memory_obj.byte_array
        total = len(view)
        r = self._resolved
        p = shard_plan(
            total,
            target_segment_bytes=r.target_segment_bytes,
            max_segment_bytes=r.max_segment_bytes,
            min_segment_bytes=r.min_segment_bytes,
            single_record_threshold_bytes=r.single_record_threshold_bytes,
        )
        ttl = self._ttl_value(pinned=False)
        wmeta = self._put_meta(ttl)
        wp = policies.write_policy(self.cfg)
        mk = K.meta_key(self.cfg.namespace, self.cfg.set_name, ck)

        mbins = serde.meta_bins(
            plan=p,
            memory_obj=memory_obj,
            save_chunk_meta=self.save_chunk_meta,
            enable_crc32=self.cfg.enable_crc32,
            default_ttl=ttl,
            pinned=False,
        )

        try:
            if p.nseg == 1:
                self._client.put(mk, mbins, meta=wmeta, policy=wp)
                return

            seg_b = p.seg_b
            writes = []
            for i in range(p.nseg):
                start = i * seg_b
                chunk = bytes(view[start : start + seg_b])
                seg_ops = [op.write("b", chunk)]
                if self.cfg.enable_crc32:
                    seg_ops.append(
                        op.write("crc32", zlib.crc32(chunk) & 0xFFFFFFFF)
                    )
                writes.append(
                    br.Write(
                        key=K.segment_key(
                            self.cfg.namespace, self.cfg.set_name, ck, i
                        ),
                        ops=seg_ops,
                        meta=wmeta,
                        policy=wp,
                    )
                )
            batch = br.BatchRecords(writes)
            self._client.batch_write(batch)
            for rec in batch.batch_records:
                if rec.result != AEROSPIKE_OK:
                    raise map_aerospike_error(
                        "put-segment", _result_to_exc(rec.result)
                    )

            self._client.put(mk, mbins, meta=wmeta, policy=wp)
        except ax.RecordTooBig as exc:
            raise AerospikeRecordTooBigError(
                f"put payload {total} bytes exceeds server limits "
                f"(max_segment={r.max_segment_bytes})"
            ) from exc
        except ax.AerospikeError as exc:
            bucket = classify(exc)
            if bucket == "forbidden_ttl":
                raise AerospikeTTLConfigError(str(exc)) from exc
            if bucket == "busy":
                raise AerospikeBusyError(str(exc)) from exc
            raise map_aerospike_error("put", exc) from exc

    async def put(self, key: CacheEngineKey, memory_obj: MemoryObj) -> None:
        await self._run(self._put_sync_impl, key, memory_obj)

    def support_batched_get(self) -> bool:
        return True

    async def batched_get(self, keys: List[CacheEngineKey]) -> List[Optional[MemoryObj]]:
        async with self._batch_sem:
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
            await asyncio.gather(
                *(self.put(k, mo) for k, mo in zip(keys, memory_objs))
            )

    def support_batched_contains(self) -> bool:
        return True

    def _batched_contains_sync(self, keys: List[CacheEngineKey]) -> int:
        if not keys:
            return 0
        meta_keys = [
            K.meta_key(self.cfg.namespace, self.cfg.set_name, k) for k in keys
        ]
        try:
            brs = self._client.batch_read(meta_keys, [])
        except ax.AerospikeError:
            return 0
        count = 0
        for rec in brs.batch_records:
            if rec.result != AEROSPIKE_OK:
                return count
            count += 1
        return count

    def batched_contains(self, keys: List[CacheEngineKey]) -> int:
        return self._batched_contains_sync(keys)

    async def batched_async_contains(
        self,
        lookup_id: str,
        keys: List[CacheEngineKey],
        pin: bool = False,
    ) -> int:
        del lookup_id, pin
        return await self._run(self._batched_contains_sync, keys)

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
        mk = K.meta_key(self.cfg.namespace, self.cfg.set_name, key)
        nseg = 1
        try:
            _k, _meta, bins = self._client.select(mk, ["nseg"])
            if bins and "nseg" in bins:
                nseg = int(bins["nseg"])
        except ax.RecordNotFound:
            return False
        except ax.AerospikeError as exc:
            logger.warning("remove select failed for %s: %s", key, exc)

        try:
            self._client.remove(mk)
        except ax.RecordNotFound:
            return False
        except ax.AerospikeError as exc:
            raise map_aerospike_error("remove", exc) from exc

        if nseg > 1:
            removes = [
                br.Remove(
                    key=K.segment_key(
                        self.cfg.namespace, self.cfg.set_name, key, i
                    )
                )
                for i in range(nseg)
            ]
            try:
                batch = br.BatchRecords(removes)
                self._client.batch_write(batch)
            except ax.AerospikeError as exc:
                logger.warning("segment remove failed for %s: %s", key, exc)
        return True

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
