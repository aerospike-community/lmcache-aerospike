"""Map LMCache distributed ObjectKey to CacheEngineKey for Aerospike records."""

from __future__ import annotations

import torch

from lmcache.utils import CacheEngineKey
from lmcache.v1.distributed.api import ObjectKey


def object_key_to_cache_engine_key(
    key: ObjectKey,
    *,
    dtype: torch.dtype = torch.bfloat16,
) -> CacheEngineKey:
    """Convert an L2 ObjectKey to CacheEngineKey (Phase 1 wire format).

    ``cache_salt`` is encoded as an LMCache tag so Phase 1 and Phase 2 keys
    stay consistent with ``CacheEngineKey.to_string()`` tagging rules.
    """
    world_size = (key.kv_rank >> 24) & 0xFF
    worker_id = (key.kv_rank >> 16) & 0xFF
    if world_size <= 0:
        world_size = 1
    chunk_hash = int.from_bytes(key.chunk_hash, byteorder="big") & ((1 << 64) - 1)
    request_configs: dict[str, str] = {}
    if key.cache_salt:
        request_configs["lmcache.tag.cache_salt"] = key.cache_salt
    return CacheEngineKey(
        model_name=key.model_name,
        world_size=world_size,
        worker_id=worker_id,
        chunk_hash=chunk_hash,
        dtype=dtype,
        request_configs=request_configs or None,
    )
