"""Large-chunk put/get (multi-segment path at default 4 MiB target)."""

from __future__ import annotations

import asyncio
from typing import Any

import torch
from ai_ecosystem_benchmark import BaseBenchmarkWorkload
from lmcache.utils import CacheEngineKey
from lmcache.v1.protocol import init_remote_metadata_info

from tests.integration.helpers import (
    build_connector,
    chunk_byte_size,
    close_connector,
    make_memory_obj,
    num_tokens_for_payload_bytes,
    payload_pattern,
    sync_get,
    sync_put,
)

from ._fixtures import DEFAULT_URI, apply_connection_env, parse_connection_string

MIB = 1024 * 1024


class KvChunkWorkload(BaseBenchmarkWorkload):
    """Stress multi-segment sharding with LMCache-sized payloads."""

    def __init__(
        self,
        aerospike_connection_string: str | None = None,
        **params: Any,
    ) -> None:
        super().__init__(aerospike_connection_string=aerospike_connection_string)
        self._target_bytes = int(params.get("target_payload_bytes", 4 * MIB))
        self._conn = None
        self._backend = None
        self._loop = None
        self._key: CacheEngineKey | None = None
        self._payload: bytes = b""

    def setup(self) -> None:
        uri = self.aerospike_connection_string or DEFAULT_URI
        p = parse_connection_string(uri)
        apply_connection_env(p)
        init_remote_metadata_info(1)
        num_tokens = num_tokens_for_payload_bytes(self._target_bytes)
        set_name = p["set_name"] + "_lg"
        extra = {
            f"remote_storage_plugin.aerospike.set": set_name,
            f"remote_storage_plugin.aerospike.namespace": p["namespace"],
            f"remote_storage_plugin.aerospike.target_segment_bytes": str(
                p["target_segment_bytes"]
            ),
        }
        self._loop = asyncio.new_event_loop()
        self._conn, self._backend, _, _ = build_connector(
            set_name=set_name,
            namespace=p["namespace"],
            num_tokens=num_tokens,
            extra_plugin=extra,
        )
        chunk_len = chunk_byte_size(self._backend.metadata)
        self._payload = payload_pattern(chunk_len)
        self._key = CacheEngineKey(
            model_name="bench-eco-lg",
            world_size=1,
            worker_id=0,
            chunk_hash=88_001,
            dtype=torch.float16,
        )
        sync_put(self._conn, self._key, make_memory_obj(self._backend, self._payload))

    def between_benchmarks(self) -> None:
        return None

    def teardown(self) -> None:
        if self._conn is not None and self._loop is not None:
            self._loop.run_until_complete(close_connector(self._conn))
        self._conn = None
        self._backend = None
        self._loop = None

    def aerospike_kv_put_large(self) -> None:
        assert self._conn is not None and self._backend is not None and self._key
        sync_put(
            self._conn,
            self._key,
            make_memory_obj(self._backend, self._payload),
        )

    def aerospike_kv_get_large(self) -> None:
        assert self._conn is not None and self._key
        mo = sync_get(self._conn, self._key)
        if mo is not None:
            mo.ref_count_down()
