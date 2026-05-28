"""End-to-end connector tests against live Aerospike CE (S14)."""

from __future__ import annotations

import os
import time

import pytest
from aerospike_helpers.batch import records as br
from aerospike_helpers.operations import operations as op

from lmcache_aerospike import keys as K
from lmcache_aerospike import limits
from lmcache_aerospike.errors import AerospikeTTLConfigError
from lmcache_aerospike.sharding import plan as shard_plan
from tests.integration.helpers import (
    build_connector,
    chunk_byte_size,
    close_connector,
    expected_nseg,
    make_cache_key,
    make_memory_obj,
    memory_obj_payload_bytes,
    meta_record_bins,
    num_tokens_for_payload_bytes,
    on_github_actions,
    payload_pattern,
    put_pinned,
    sync_get,
    sync_put,
)

MIB = 1024 * 1024
KIB = 1024


def round_trip_payload_sizes() -> list[int]:
    """Sizes exercised in CI; larger payloads need local RAM (see RUN_LARGE_INTEGRATION)."""
    sizes = [512, 64 * KIB]
    if not on_github_actions():
        sizes.extend([1 * MIB, 4 * MIB])
    return sizes


def crash_test_target_bytes() -> int:
    """Payload large enough to shard; keep small on GitHub-hosted runners (RAM)."""
    if on_github_actions():
        return 512 * KIB + 8192
    return 2 * MIB

requires_large = pytest.mark.skipif(
    os.environ.get("RUN_LARGE_INTEGRATION") != "1",
    reason="Set RUN_LARGE_INTEGRATION=1 for 16MiB+ payloads (high RAM)",
)


def test_discovery_clamps_segment_limits(connector):
    conn, _backend = connector
    assert conn._resolved is not None
    server = limits.discover_limits(conn._client, conn.cfg.namespace)
    assert server.server_max_record_bytes == 1048576
    expected_max = server.server_max_record_bytes - limits.SAFETY_MARGIN_BYTES
    assert conn._resolved.max_segment_bytes == expected_max


@pytest.mark.parametrize("target_bytes", round_trip_payload_sizes())
def test_round_trip_size_matrix(chunk_id_counter, target_bytes):
    import asyncio

    num_tokens = num_tokens_for_payload_bytes(target_bytes)
    conn, backend, _, _ = build_connector(num_tokens=num_tokens)
    try:
        chunk_len = chunk_byte_size(backend.metadata)
        assert chunk_len >= target_bytes
        payload = payload_pattern(chunk_len)
        key = make_cache_key(chunk_id_counter())
        sync_put(conn, key, make_memory_obj(backend, payload))
        bins = meta_record_bins(conn, key)
        assert bins is not None
        assert bins["state"] == "ready"
        assert bins["nseg"] == expected_nseg(chunk_len, conn)
        got = sync_get(conn, key)
        assert got is not None
        assert memory_obj_payload_bytes(got, chunk_len) == payload
        got.ref_count_down()
    finally:
        asyncio.run(close_connector(conn))


@requires_large
@pytest.mark.parametrize("target_bytes", [16 * MIB, 64 * MIB])
def test_round_trip_large_payloads(chunk_id_counter, target_bytes):
    import asyncio

    num_tokens = num_tokens_for_payload_bytes(target_bytes)
    conn, backend, _, _ = build_connector(num_tokens=num_tokens)
    try:
        chunk_len = chunk_byte_size(backend.metadata)
        payload = payload_pattern(chunk_len)
        key = make_cache_key(chunk_id_counter())
        sync_put(conn, key, make_memory_obj(backend, payload))
        got = sync_get(conn, key)
        assert got is not None
        assert memory_obj_payload_bytes(got, chunk_len) == payload
        got.ref_count_down()
    finally:
        asyncio.run(close_connector(conn))


def test_ttl_expiry(chunk_id_counter):
    import asyncio

    conn, backend, _, _ = build_connector(
        default_ttl_seconds=2, num_tokens=num_tokens_for_payload_bytes(512)
    )
    try:
        key = make_cache_key(chunk_id_counter())
        mo = make_memory_obj(
            backend, payload_pattern(chunk_byte_size(backend.metadata))
        )
        sync_put(conn, key, mo)
        assert sync_get(conn, key) is not None
        time.sleep(5)
        assert sync_get(conn, key) is None
        assert conn.exists_sync(key) is False
    finally:
        asyncio.run(close_connector(conn))


def test_pinned_survives_ttl_window(chunk_id_counter):
    import asyncio

    conn, backend, _, _ = build_connector(
        default_ttl_seconds=2, num_tokens=num_tokens_for_payload_bytes(512)
    )
    try:
        key = make_cache_key(chunk_id_counter())
        mo = make_memory_obj(
            backend, payload_pattern(chunk_byte_size(backend.metadata))
        )
        put_pinned(conn, key, mo)
        time.sleep(5)
        assert sync_get(conn, key) is not None
    finally:
        asyncio.run(close_connector(conn))


def test_crash_before_meta_returns_none(chunk_id_counter):
    import asyncio

    target = crash_test_target_bytes()
    conn, backend, _, _ = build_connector(
        num_tokens=num_tokens_for_payload_bytes(target)
    )
    try:
        key = make_cache_key(chunk_id_counter())
        conn.remove_sync(key)  # clear any prior meta/segments for this key
        chunk_len = chunk_byte_size(backend.metadata)
        payload = payload_pattern(chunk_len)
        mo = make_memory_obj(backend, payload)
        assert conn._resolved is not None
        p = shard_plan(
            chunk_len,
            target_segment_bytes=conn._resolved.target_segment_bytes,
            max_segment_bytes=conn._resolved.max_segment_bytes,
            min_segment_bytes=conn._resolved.min_segment_bytes,
            single_record_threshold_bytes=conn._resolved.single_record_threshold_bytes,
        )
        assert p.nseg > 1
        from lmcache_aerospike import policies

        wp = policies.write_policy(conn.cfg)
        wmeta = conn._put_meta(conn._ttl_value(pinned=False))
        writes = []
        for i in range(p.nseg):
            start = i * p.seg_b
            end = min(start + p.seg_b, chunk_len)
            chunk = payload[start:end]
            writes.append(
                br.Write(
                    key=K.segment_key(
                        conn.cfg.namespace, conn.cfg.set_name, key, i
                    ),
                    ops=[op.write("b", chunk)],
                    meta=wmeta,
                    policy=wp,
                )
            )
        conn._client.batch_write(br.BatchRecords(writes))
        assert sync_get(conn, key) is None
    finally:
        asyncio.run(close_connector(conn))


def test_nsup_zero_namespace_fails_at_construction():
    with pytest.raises(AerospikeTTLConfigError):
        build_connector(namespace="lmcache_no_nsup", default_ttl_seconds=3600)


def test_multi_set_isolation(chunk_id_counter):
    import asyncio

    conn_a, backend_a, _, _ = build_connector(
        plugin_name="aerospike.primary", set_name="it_primary"
    )
    conn_b, _backend_b, _, _ = build_connector(
        plugin_name="aerospike.dr", set_name="it_dr"
    )
    try:
        key = make_cache_key(chunk_id_counter())
        mo = make_memory_obj(
            backend_a,
            payload_pattern(chunk_byte_size(backend_a.metadata)),
        )
        sync_put(conn_a, key, mo)
        got_a = sync_get(conn_a, key)
        assert got_a is not None
        got_a.ref_count_down()
        assert sync_get(conn_b, key) is None
    finally:
        asyncio.run(close_connector(conn_a))
        asyncio.run(close_connector(conn_b))
