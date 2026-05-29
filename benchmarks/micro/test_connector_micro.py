"""pytest-benchmark micro harness (no Aerospike). See benchmarks/micro/README.md."""

from __future__ import annotations

import os

import pytest
from lmcache.utils import CacheEngineKey

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_BENCH") != "1",
    reason="Set RUN_BENCH=1 to run micro benchmarks",
)

LIVE = os.environ.get("RUN_BENCH_LIVE") == "1"


def test_put_roundtrip(benchmark, bench_connector):
    conn, key, mo, _payload_size, _loop = bench_connector
    conn._put_sync_impl(key, mo)

    def run():
        conn._get_sync_impl(key)

    benchmark(run)


def test_put_write(benchmark, bench_connector):
    conn, key, mo, _payload_size, _loop = bench_connector

    def run():
        conn._put_sync_impl(key, mo)

    benchmark(run)


def test_get_miss(benchmark, bench_connector):
    conn, key, _mo, _payload_size, _loop = bench_connector
    missing = CacheEngineKey(
        model_name=key.model_name,
        world_size=key.world_size,
        worker_id=key.worker_id,
        chunk_hash=key.chunk_hash + 1,
        dtype=key.dtype,
    )

    def run():
        conn._get_sync_impl(missing)

    benchmark(run)


def test_segment_size_sweep(benchmark, segment_sweep_connector):
    conn, key, mo, target, payload_size, _loop = segment_sweep_connector
    conn._put_sync_impl(key, mo)

    def run():
        conn._get_sync_impl(key)

    benchmark(run)
    mean_s = benchmark.stats.stats.mean
    mib_per_s = (payload_size / mean_s) / (1024 * 1024) if mean_s > 0 else 0.0
    print(
        f"\nsegment_sweep target_segment={target // (1024 * 1024)}MiB "
        f"payload={payload_size} mean_get_s={mean_s:.6f} approx_MiB/s={mib_per_s:.2f}"
    )


@pytest.mark.skipif(not LIVE, reason="Set RUN_BENCH_LIVE=1 with Aerospike CE up")
def test_live_put_get(benchmark):
    import asyncio

    from tests.integration.helpers import (
        build_connector,
        chunk_byte_size,
        close_connector,
        make_cache_key,
        make_memory_obj,
        payload_pattern,
    )

    conn, backend, _, _ = build_connector(num_tokens=128)
    try:
        chunk_len = chunk_byte_size(backend.metadata)
        payload = payload_pattern(chunk_len)
        key = make_cache_key(1)
        mo = make_memory_obj(backend, payload)
        conn._put_sync_impl(key, mo)

        def run():
            got = conn._get_sync_impl(key)
            if got is not None:
                got.ref_count_down()

        benchmark(run)
    finally:
        asyncio.run(close_connector(conn))
