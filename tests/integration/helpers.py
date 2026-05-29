"""Helpers to build LMCache + Aerospike connector fixtures for integration tests."""

from __future__ import annotations

import asyncio
import os
from typing import Any

import torch

from lmcache.utils import CacheEngineKey
from lmcache.v1.config import LMCacheEngineConfig
from lmcache.v1.memory_management import MemoryFormat
from lmcache.v1.metadata import LMCacheMetadata
from lmcache.v1.storage_backend.local_cpu_backend import LocalCPUBackend

from lmcache_aerospike.client import AerospikeClientHolder
from lmcache_aerospike.config import AerospikeConfig
from lmcache_aerospike.connector import AerospikeRemoteConnector
from lmcache_aerospike import keys as K
from lmcache_aerospike import limits, serde
from lmcache_aerospike.sharding import plan as shard_plan


def on_github_actions() -> bool:
    return os.environ.get("GITHUB_ACTIONS", "").lower() == "true"


def aerospike_hosts() -> tuple[tuple[str, int], ...]:
    host = os.environ.get("AEROSPIKE_TEST_HOST", "127.0.0.1")
    port = int(os.environ.get("AEROSPIKE_TEST_PORT", "3000"))
    return ((host, port),)


def make_engine_config(
    *,
    plugin_name: str = "aerospike",
    save_chunk_meta: bool = True,
    default_ttl_seconds: int = 86400,
    extra: dict[str, Any] | None = None,
) -> LMCacheEngineConfig:
    prefix = f"remote_storage_plugin.{plugin_name}."
    plugin_extra = {
        f"{prefix}hosts": ",".join(f"{h}:{p}" for h, p in aerospike_hosts()),
        f"{prefix}namespace": os.environ.get("AEROSPIKE_TEST_NAMESPACE", "lmcache"),
        f"{prefix}set": "it_chunks",
        f"{prefix}default_ttl_seconds": str(default_ttl_seconds),
    }
    if extra:
        plugin_extra.update(extra)
    ec: dict[str, Any] = {
        "chunk_size": 256,
        "local_cpu": True,
        "max_local_cpu_size": 1.0 if on_github_actions() else 4.0,
        "extra_config": {
            "save_chunk_meta": save_chunk_meta,
            **plugin_extra,
        },
    }
    return LMCacheEngineConfig.from_dict(ec)


def make_metadata(*, num_tokens: int = 128) -> LMCacheMetadata:
    """Default 64 KiB chunk (2 x num_tokens x 128 x fp16)."""
    return LMCacheMetadata(
        model_name="integration-test",
        world_size=1,
        local_world_size=1,
        worker_id=0,
        local_worker_id=0,
        kv_dtype=torch.float16,
        kv_shape=(2, 1, num_tokens, 1, 128),
        chunk_size=num_tokens,
        use_mla=False,
    )


def make_cache_key(chunk_hash: int = 1) -> CacheEngineKey:
    return CacheEngineKey(
        model_name="integration-test",
        world_size=1,
        worker_id=0,
        chunk_hash=chunk_hash,
        dtype=torch.float16,
    )


def chunk_byte_size(metadata: LMCacheMetadata) -> int:
    from lmcache.integration.vllm.utils import get_size_bytes

    return get_size_bytes(metadata.get_shapes(), metadata.get_dtypes())


def num_tokens_for_payload_bytes(target_bytes: int) -> int:
    """Smallest num_tokens so the LMCache chunk is at least *target_bytes*."""
    for nt in range(1, 200_000):
        if chunk_byte_size(make_metadata(num_tokens=nt)) >= target_bytes:
            return nt
    raise ValueError(f"cannot size chunk for {target_bytes} bytes")


def memory_obj_payload_bytes(memory_obj, length: int) -> bytes:
    """First *length* bytes of a tensor-backed MemoryObj."""
    raw = memory_obj.raw_data
    return bytes(raw.view(torch.uint8).flatten()[:length].clone().numpy())


def make_memory_obj(backend: LocalCPUBackend, payload: bytes):
    """Allocate a full-chunk MemoryObj; *payload* must match chunk byte size."""
    meta = backend.metadata
    fmt = MemoryFormat.KV_2LTD
    mo = backend.allocate(meta.get_shapes(), meta.get_dtypes(), fmt)
    assert mo is not None
    chunk_len = chunk_byte_size(meta)
    assert len(payload) == chunk_len, f"payload {len(payload)} != chunk {chunk_len}"
    serde.write_payload_into(mo, payload)
    return mo


def payload_pattern(length: int) -> bytes:
    return bytes((i % 251) for i in range(length))


def build_connector(
    *,
    plugin_name: str = "aerospike",
    set_name: str = "it_chunks",
    namespace: str | None = None,
    default_ttl_seconds: int = 86400,
    save_chunk_meta: bool = True,
    extra_plugin: dict[str, str] | None = None,
    num_tokens: int = 128,
) -> tuple[AerospikeRemoteConnector, LocalCPUBackend, LMCacheEngineConfig, LMCacheMetadata]:
    ns = namespace or os.environ.get("AEROSPIKE_TEST_NAMESPACE", "lmcache")
    extra = {
        f"remote_storage_plugin.{plugin_name}.set": set_name,
        f"remote_storage_plugin.{plugin_name}.namespace": ns,
    }
    if extra_plugin:
        extra.update(extra_plugin)
    config = make_engine_config(
        plugin_name=plugin_name,
        save_chunk_meta=save_chunk_meta,
        default_ttl_seconds=default_ttl_seconds,
        extra=extra,
    )
    metadata = make_metadata(num_tokens=num_tokens)
    backend = LocalCPUBackend(config, metadata=metadata)
    as_cfg = AerospikeConfig.from_extra_config(config.extra_config, plugin_name)
    holder = AerospikeClientHolder.get_or_create(as_cfg)
    loop = asyncio.new_event_loop()
    connector = AerospikeRemoteConnector(
        config=config,
        metadata=metadata,
        local_cpu_backend=backend,
        loop=loop,
        aerospike_config=as_cfg,
        client_holder=holder,
    )
    return connector, backend, config, metadata


async def close_connector(connector: AerospikeRemoteConnector) -> None:
    await connector.close()
    connector.loop.close()


def sync_put(connector: AerospikeRemoteConnector, key: CacheEngineKey, mo) -> None:
    connector._engine.put(key, mo)


def sync_get(connector: AerospikeRemoteConnector, key: CacheEngineKey):
    return connector._engine.get(key)


def meta_record_bins(connector: AerospikeRemoteConnector, key: CacheEngineKey) -> dict | None:
    mk = K.meta_key(connector.cfg.namespace, connector.cfg.set_name, key)
    try:
        _k, _meta, bins = connector._client.get(mk)
    except Exception:
        return None
    return bins


def expected_nseg(payload_len: int, connector: AerospikeRemoteConnector) -> int:
    assert connector._resolved is not None
    r = connector._resolved
    p = shard_plan(
        payload_len,
        target_segment_bytes=r.target_segment_bytes,
        max_segment_bytes=r.max_segment_bytes,
        min_segment_bytes=r.min_segment_bytes,
        single_record_threshold_bytes=r.single_record_threshold_bytes,
    )
    return p.nseg


def put_pinned(connector: AerospikeRemoteConnector, key: CacheEngineKey, mo) -> None:
    """Write a pinned record (TTL -1) using the same path as production puts."""
    from lmcache_aerospike import policies

    assert connector._resolved is not None
    total = mo.get_size()
    r = connector._resolved
    p = shard_plan(
        total,
        target_segment_bytes=r.target_segment_bytes,
        max_segment_bytes=r.max_segment_bytes,
        min_segment_bytes=r.min_segment_bytes,
        single_record_threshold_bytes=r.single_record_threshold_bytes,
    )
    ttl = connector._ttl_value(pinned=True)
    wmeta = connector._put_meta(ttl)
    wp = policies.write_policy(connector.cfg)
    mk = K.meta_key(connector.cfg.namespace, connector.cfg.set_name, key)
    mbins = serde.meta_bins(
        plan=p,
        memory_obj=mo,
        save_chunk_meta=connector.save_chunk_meta,
        enable_crc32=connector.cfg.enable_crc32,
        default_ttl=ttl,
        pinned=True,
    )
    connector._client.put(mk, mbins, meta=wmeta, policy=wp)
