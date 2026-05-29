from __future__ import annotations

import torch

from lmcache.utils import CacheEngineKey
from lmcache.v1.distributed.api import ObjectKey

from lmcache_aerospike.object_keys import object_key_to_cache_engine_key


def test_object_key_round_trip_fields():
    kv_rank = ObjectKey.ComputeKVRank(
        world_size=2,
        global_rank=1,
        local_world_size=2,
        local_rank=1,
    )
    ok = ObjectKey(
        chunk_hash=b"\x01\x02\x03\x04",
        model_name="llama",
        kv_rank=kv_rank,
        cache_salt="user-a",
    )
    ck = object_key_to_cache_engine_key(ok, dtype=torch.bfloat16)
    assert ck.model_name == "llama"
    assert ck.world_size == 2
    assert ck.worker_id == 1
    assert ck.dtype == torch.bfloat16
    assert ck.request_configs is not None
    assert ck.request_configs.get("lmcache.tag.cache_salt") == "user-a"
    assert isinstance(ck, CacheEngineKey)
