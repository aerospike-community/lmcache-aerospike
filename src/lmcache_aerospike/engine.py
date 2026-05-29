"""Shared Aerospike meta/segment I/O used by Phase 1 connector and Phase 2 plugins."""

from __future__ import annotations

import zlib
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Protocol

import aerospike
from aerospike import exception as ax
from aerospike_helpers.batch import records as br
from aerospike_helpers.operations import operations as op

from lmcache.logging import init_logger
from lmcache.utils import CacheEngineKey
from lmcache.v1.memory_management import MemoryFormat, MemoryObj

from lmcache_aerospike import keys as K
from lmcache_aerospike import limits, metrics, policies, serde
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


class _BatchResultError(Exception):
    def __init__(self, code: int) -> None:
        self.code = code
        super().__init__(f"batch result code {code}")


def _result_to_exc(result_code: int) -> BaseException:
    return _BatchResultError(result_code)


class _AllocateForRead(Protocol):
    save_chunk_meta: bool
    meta_shapes: list
    meta_dtypes: list
    meta_fmt: MemoryFormat

    def reshape_partial_chunk(self, memory_obj: MemoryObj, num_tokens: int) -> MemoryObj:
        ...


@dataclass
class EngineMetadata:
    """LMCache metadata fields required for serde on read/write."""

    save_chunk_meta: bool
    meta_shapes: list
    meta_dtypes: list
    meta_fmt: MemoryFormat

    @classmethod
    def from_remote_connector(cls, base: _AllocateForRead) -> EngineMetadata:
        return cls(
            save_chunk_meta=base.save_chunk_meta,
            meta_shapes=base.meta_shapes,
            meta_dtypes=base.meta_dtypes,
            meta_fmt=base.meta_fmt,
        )


class AerospikeStorageEngine:
    """Synchronous Aerospike KV engine (meta + segment records)."""

    def __init__(
        self,
        *,
        cfg: AerospikeConfig,
        client: aerospike.Client,
        metadata: EngineMetadata,
        resolved: limits.ResolvedLimits,
        reshape_partial_chunk: Callable[[MemoryObj, int], MemoryObj],
        allocate_for_read: Callable[[dict], tuple[MemoryObj | None, bool]],
        pin_ttl_seconds: int | None = None,
    ) -> None:
        self.cfg = cfg
        self._client = client
        self._meta = metadata
        self._resolved = resolved
        self._reshape = reshape_partial_chunk
        self._allocate_for_read = allocate_for_read
        self._pin_ttl_seconds = pin_ttl_seconds or max(
            cfg.default_ttl_seconds * 2, cfg.default_ttl_seconds
        )

    @property
    def resolved(self) -> limits.ResolvedLimits:
        return self._resolved

    def _ttl_value(self, pinned: bool) -> int:
        if pinned:
            return -1
        return self.cfg.default_ttl_seconds

    def _put_meta(self, ttl: int) -> dict:
        return {"ttl": ttl}

    def exists(self, ck: CacheEngineKey) -> bool:
        timer = metrics.OpTimer("exists")
        try:
            mk = K.meta_key(self.cfg.namespace, self.cfg.set_name, ck)
            try:
                _key, _meta, _bins = self._client.select(mk, ["state"])
            except ax.RecordNotFound:
                timer.result = "miss"
                return False
            except ax.AerospikeError as exc:
                if classify(exc) == "timeout":
                    logger.warning("exists timeout for %s", ck)
                    timer.result = "timeout"
                    return False
                timer.map_exception(exc)
                raise map_aerospike_error("exists", exc) from exc
            timer.result = "hit"
            return True
        finally:
            timer.finish()

    def get(
        self,
        ck: CacheEngineKey,
        *,
        preallocated: MemoryObj | None = None,
    ) -> Optional[MemoryObj]:
        """Read chunk into a new or preallocated MemoryObj."""
        timer = metrics.OpTimer("get")
        try:
            mk = K.meta_key(self.cfg.namespace, self.cfg.set_name, ck)
            try:
                _key, _meta, bins = self._client.get(mk)
            except ax.RecordNotFound:
                timer.result = "miss"
                return None
            except ax.AerospikeError as exc:
                if classify(exc) == "timeout":
                    logger.warning("get timeout for %s", ck)
                    timer.result = "timeout"
                    return None
                if classify(exc) == "connection":
                    timer.result = "error"
                    raise AerospikeConnectionError(str(exc)) from exc
                timer.map_exception(exc)
                raise map_aerospike_error("get", exc) from exc

            if bins.get("state") != "ready":
                timer.result = "miss"
                return None

            nseg = int(bins["nseg"])
            if preallocated is not None:
                mo = preallocated
                expect_reshape = False
            else:
                mo, expect_reshape = self._allocate_for_read(bins)
            if mo is None:
                timer.result = "miss"
                return None

            total_written = 0
            try:
                if nseg == 1:
                    payload = bins.get("b")
                    if payload is None:
                        timer.result = "miss"
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
                            timer.result = "miss"
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
                            timer.result = "miss"
                            return None

                if expect_reshape:
                    mo = self._reshape(mo, total_written)
                timer.result = "hit"
                return mo
            except Exception:
                mo.ref_count_down()
                timer.result = "error"
                raise
        finally:
            timer.finish()

    def put(self, ck: CacheEngineKey, memory_obj: MemoryObj, *, pinned: bool = False) -> None:
        timer = metrics.OpTimer("put")
        r = self._resolved
        view = memory_obj.byte_array
        total = len(view)
        p = shard_plan(
            total,
            target_segment_bytes=r.target_segment_bytes,
            max_segment_bytes=r.max_segment_bytes,
            min_segment_bytes=r.min_segment_bytes,
            single_record_threshold_bytes=r.single_record_threshold_bytes,
        )
        metrics.observe_segments(p.nseg, total)
        ttl = self._ttl_value(pinned=pinned)
        wmeta = self._put_meta(ttl)
        wp = policies.write_policy(self.cfg)
        mk = K.meta_key(self.cfg.namespace, self.cfg.set_name, ck)

        mbins = serde.meta_bins(
            plan=p,
            memory_obj=memory_obj,
            save_chunk_meta=self._meta.save_chunk_meta,
            enable_crc32=self.cfg.enable_crc32,
            default_ttl=ttl,
            pinned=pinned,
        )

        try:
            if p.nseg == 1:
                self._client.put(mk, mbins, meta=wmeta, policy=wp)
                timer.result = "ok"
                return

            seg_b = p.seg_b
            writes = []
            for i in range(p.nseg):
                start = i * seg_b
                chunk = bytes(view[start : start + seg_b])
                seg_ops = [op.write("b", chunk)]
                if self.cfg.enable_crc32:
                    seg_ops.append(op.write("crc32", zlib.crc32(chunk) & 0xFFFFFFFF))
                writes.append(
                    br.Write(
                        key=K.segment_key(self.cfg.namespace, self.cfg.set_name, ck, i),
                        ops=seg_ops,
                        meta=wmeta,
                        policy=wp,
                    )
                )
            batch = br.BatchRecords(writes)
            self._client.batch_write(batch)
            for rec in batch.batch_records:
                if rec.result != AEROSPIKE_OK:
                    raise map_aerospike_error("put-segment", _result_to_exc(rec.result))

            self._client.put(mk, mbins, meta=wmeta, policy=wp)
            timer.result = "ok"
        except ax.RecordTooBig as exc:
            timer.result = "record_too_big"
            raise AerospikeRecordTooBigError(
                f"put payload {total} bytes exceeds server limits "
                f"(max_segment={r.max_segment_bytes})"
            ) from exc
        except ax.AerospikeError as exc:
            bucket = classify(exc)
            if bucket == "forbidden_ttl":
                timer.result = "error"
                raise AerospikeTTLConfigError(str(exc)) from exc
            if bucket == "busy":
                timer.result = "busy"
                raise AerospikeBusyError(str(exc)) from exc
            timer.map_exception(exc)
            raise map_aerospike_error("put", exc) from exc
        finally:
            timer.finish()

    def batched_contains(self, keys: List[CacheEngineKey]) -> int:
        timer = metrics.OpTimer("batched_contains")
        try:
            if not keys:
                timer.result = "ok"
                return 0
            meta_keys = [
                K.meta_key(self.cfg.namespace, self.cfg.set_name, k) for k in keys
            ]
            try:
                brs = self._client.batch_read(meta_keys, [])
            except ax.AerospikeError:
                timer.result = "error"
                return 0
            count = 0
            for rec in brs.batch_records:
                if rec.result != AEROSPIKE_OK:
                    timer.result = "ok"
                    return count
                count += 1
            timer.result = "ok"
            return count
        finally:
            timer.finish()

    def pin_keys(self, keys: List[CacheEngineKey]) -> None:
        """Refresh TTL and set pin bin on prefix hits."""
        ttl_meta = self._put_meta(self._pin_ttl_seconds)
        wp = policies.write_policy(self.cfg)
        for ck in keys:
            mk = K.meta_key(self.cfg.namespace, self.cfg.set_name, ck)
            try:
                self._client.touch(mk, meta=ttl_meta, policy=wp)
                self._client.operate(
                    mk,
                    [op.write("pin", True)],
                    meta=ttl_meta,
                    policy=wp,
                )
            except ax.RecordNotFound:
                continue
            except ax.AerospikeError as exc:
                logger.warning("pin failed for %s: %s", ck, exc)

    def unpin(self, ck: CacheEngineKey) -> bool:
        mk = K.meta_key(self.cfg.namespace, self.cfg.set_name, ck)
        ttl_meta = self._put_meta(self.cfg.default_ttl_seconds)
        wp = policies.write_policy(self.cfg)
        try:
            self._client.touch(mk, meta=ttl_meta, policy=wp)
            self._client.operate(
                mk,
                [op.write("pin", False)],
                meta=ttl_meta,
                policy=wp,
            )
            return True
        except ax.RecordNotFound:
            return False
        except ax.AerospikeError as exc:
            logger.warning("unpin failed for %s: %s", ck, exc)
            return False

    def pin(self, ck: CacheEngineKey) -> bool:
        if not self.exists(ck):
            return False
        self.pin_keys([ck])
        return True

    def remove(self, key: CacheEngineKey) -> bool:
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
                    key=K.segment_key(self.cfg.namespace, self.cfg.set_name, key, i)
                )
                for i in range(nseg)
            ]
            try:
                batch = br.BatchRecords(removes)
                self._client.batch_write(batch)
            except ax.AerospikeError as exc:
                logger.warning("segment remove failed for %s: %s", key, exc)
        return True

    @staticmethod
    def discover_and_resolve(
        client: aerospike.Client, cfg: AerospikeConfig
    ) -> limits.ResolvedLimits:
        server = limits.discover_limits(client, cfg.namespace)
        return limits.resolve_segment_limits(cfg, server)
