"""LMCache KV remote connector hot path (put / get hit / get miss / exists)."""

from __future__ import annotations

from typing import Any

from ai_ecosystem_benchmark import BaseBenchmarkWorkload

from tests.integration.helpers import sync_get, sync_put

from ._fixtures import ConnectorBenchContext, DEFAULT_URI


class KvHotpathWorkload(BaseBenchmarkWorkload):
    """Models steady remote-cache traffic: write chunks and read them back.

    ``aerospike_kv_put`` — single ``put`` (may shard at 4 MiB default).
    ``aerospike_kv_get_hit`` — ``get`` on a pre-seeded key.
    ``aerospike_kv_get_miss`` — ``get`` on a key that was never written.
    ``aerospike_kv_exists`` — ``exists_sync`` on a seeded key.
    """

    def __init__(
        self,
        aerospike_connection_string: str | None = None,
        **params: Any,
    ) -> None:
        super().__init__(aerospike_connection_string=aerospike_connection_string)
        self._key_count = int(params.get("key_count", 32))
        self._ctx: ConnectorBenchContext | None = None

    def setup(self) -> None:
        uri = self.aerospike_connection_string or DEFAULT_URI
        self._ctx = ConnectorBenchContext(uri, key_count=self._key_count)
        self._ctx.setup()

    def between_benchmarks(self) -> None:
        return None

    def teardown(self) -> None:
        if self._ctx is not None:
            self._ctx.teardown()
        self._ctx = None

    def aerospike_kv_put(self) -> None:
        assert self._ctx is not None
        key = self._ctx.fresh_put_key()
        sync_put(self._ctx.conn, key, self._ctx.make_obj())

    def aerospike_kv_get_hit(self) -> None:
        assert self._ctx is not None
        key = self._ctx.next_key()
        mo = sync_get(self._ctx.conn, key)
        if mo is not None:
            mo.ref_count_down()

    def aerospike_kv_get_miss(self) -> None:
        assert self._ctx is not None
        key = self._ctx.miss_key()
        mo = sync_get(self._ctx.conn, key)
        if mo is not None:
            mo.ref_count_down()

    def aerospike_kv_exists(self) -> None:
        assert self._ctx is not None
        self._ctx.conn.exists_sync(self._ctx.next_key())
