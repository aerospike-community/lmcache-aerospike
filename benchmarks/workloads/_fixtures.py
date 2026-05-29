"""Connector bootstrap for ecosystem benchmarks (repo-local, not published)."""

from __future__ import annotations

import asyncio
import itertools
import os
from typing import Any
from urllib.parse import parse_qs, urlparse

import torch

from lmcache.utils import CacheEngineKey
from lmcache.v1.protocol import init_remote_metadata_info

from tests.integration.helpers import (
    build_connector,
    chunk_byte_size,
    close_connector,
    make_memory_obj,
    payload_pattern,
    sync_get,
    sync_put,
)

DEFAULT_URI = "aerospike://127.0.0.1:3000/lmcache?set=bench_eco_kv&num_tokens=128"


def parse_connection_string(uri: str) -> dict[str, Any]:
    """Parse ``aerospike://host:port/namespace?set=...&num_tokens=...``."""
    parsed = urlparse(uri)
    if parsed.scheme not in ("aerospike", ""):
        raise ValueError(f"expected aerospike:// URI, got {uri!r}")
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 3000
    namespace = (parsed.path or "/lmcache").lstrip("/") or "lmcache"
    query = parse_qs(parsed.query)

    def _one(key: str, default: str) -> str:
        vals = query.get(key)
        return vals[0] if vals else default

    return {
        "host": host,
        "port": int(port),
        "namespace": namespace,
        "set_name": _one("set", "bench_eco_kv"),
        "num_tokens": int(_one("num_tokens", "128")),
        "target_segment_bytes": int(_one("target_segment_bytes", "4194304")),
        "save_chunk_meta": _one("save_chunk_meta", "true").lower() in ("1", "true", "yes"),
    }


def apply_connection_env(params: dict[str, Any]) -> None:
    os.environ["AEROSPIKE_TEST_HOST"] = params["host"]
    os.environ["AEROSPIKE_TEST_PORT"] = str(params["port"])
    os.environ["AEROSPIKE_TEST_NAMESPACE"] = params["namespace"]


class ConnectorBenchContext:
    """Live Aerospike connector + pre-seeded keys for hot-path benchmarks."""

    def __init__(self, uri: str, *, key_count: int = 32) -> None:
        self.uri = uri
        self.params = parse_connection_string(uri)
        self.key_count = key_count
        self._conn = None
        self._backend = None
        self._loop = None
        self._keys: list[CacheEngineKey] = []
        self._payload: bytes = b""
        self._rr = itertools.count()

    def setup(self) -> None:
        apply_connection_env(self.params)
        init_remote_metadata_info(1)
        extra = {
            f"remote_storage_plugin.aerospike.set": self.params["set_name"],
            f"remote_storage_plugin.aerospike.namespace": self.params["namespace"],
            f"remote_storage_plugin.aerospike.target_segment_bytes": str(
                self.params["target_segment_bytes"]
            ),
        }
        self._loop = asyncio.new_event_loop()
        self._conn, self._backend, _, _ = build_connector(
            set_name=self.params["set_name"],
            namespace=self.params["namespace"],
            save_chunk_meta=self.params["save_chunk_meta"],
            num_tokens=self.params["num_tokens"],
            extra_plugin=extra,
        )
        chunk_len = chunk_byte_size(self._backend.metadata)
        self._payload = payload_pattern(chunk_len)
        for i in range(self.key_count):
            key = CacheEngineKey(
                model_name="bench-eco",
                world_size=1,
                worker_id=0,
                chunk_hash=10_000 + i,
                dtype=torch.float16,
            )
            mo = make_memory_obj(self._backend, self._payload)
            sync_put(self._conn, key, mo)
            self._keys.append(key)

    def teardown(self) -> None:
        if self._conn is not None and self._loop is not None:
            self._loop.run_until_complete(close_connector(self._conn))
        self._conn = None
        self._backend = None
        self._loop = None

    @property
    def conn(self):
        assert self._conn is not None
        return self._conn

    def next_key(self) -> CacheEngineKey:
        assert self._keys
        idx = next(self._rr) % len(self._keys)
        return self._keys[idx]

    def miss_key(self) -> CacheEngineKey:
        n = next(self._rr)
        return CacheEngineKey(
            model_name="bench-eco",
            world_size=1,
            worker_id=0,
            chunk_hash=90_000_000 + n,
            dtype=torch.float16,
        )

    def fresh_put_key(self) -> CacheEngineKey:
        n = next(self._rr)
        return CacheEngineKey(
            model_name="bench-eco",
            world_size=1,
            worker_id=0,
            chunk_hash=20_000 + n,
            dtype=torch.float16,
        )

    def make_obj(self):
        assert self._backend is not None
        return make_memory_obj(self._backend, self._payload)
