from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest
from aerospike import exception as ax

from lmcache_aerospike import keys as K
from lmcache_aerospike import limits
from lmcache_aerospike.client import AerospikeClientHolder
from lmcache_aerospike.connector import AerospikeRemoteConnector
from lmcache_aerospike.errors import AerospikeBusyError, AerospikeRecordTooBigError
from lmcache_aerospike.limits import ResolvedLimits
from tests.unit.conftest import make_config_mock, make_metadata_mock
from tests.unit.fakes import (
    FakeBatchRecord,
    FakeBatchRecords,
    FakeClient,
    FakeKey,
    FakeMemoryObj,
)


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def small_resolved() -> ResolvedLimits:
    return ResolvedLimits(
        server_max_record_bytes=8388608,
        effective_max_segment_bytes=8000000,
        max_segment_bytes=8000000,
        target_segment_bytes=1024,
        single_record_threshold_bytes=1024,
        min_segment_bytes=64,
    )


@pytest.fixture
def holder(fake_client: FakeClient):
    h = MagicMock(spec=AerospikeClientHolder)
    h.client = fake_client
    h.release = MagicMock()
    return h


@pytest.fixture
def connector(holder, as_config, fake_backend, small_resolved, event_loop):
    with patch(
        "lmcache_aerospike.engine.limits.discover_limits",
        return_value=limits.ServerLimits(8388608, "max-record-size", 120),
    ), patch(
        "lmcache_aerospike.engine.limits.resolve_segment_limits",
        return_value=small_resolved,
    ):
        conn = AerospikeRemoteConnector(
            config=make_config_mock(),
            metadata=make_metadata_mock(),
            local_cpu_backend=fake_backend,
            loop=event_loop,
            aerospike_config=as_config,
            client_holder=holder,
        )
    return conn


def test_constructor_sets_limits(connector):
    assert connector._engine.resolved is not None


def test_close_idempotent(connector, holder, event_loop):
    event_loop.run_until_complete(connector.close())
    event_loop.run_until_complete(connector.close())
    holder.release.assert_called_once()


def test_put_single_record(connector, fake_client):
    key = FakeKey("k1")
    mo = FakeMemoryObj(bytearray(b"a" * 256))
    connector._engine.put(key, mo)
    assert len(fake_client.put_calls) == 1
    assert fake_client.batch_write_batches == []


def test_put_multi_segment_then_meta(connector, fake_client):
    key = FakeKey("big")
    payload = b"x" * 3000
    mo = FakeMemoryObj(bytearray(payload))
    connector._engine.put(key, mo)
    assert len(fake_client.batch_write_batches) == 1
    assert len(fake_client.put_calls) == 1
    meta_key, bins, *_ = fake_client.put_calls[0]
    assert meta_key[2].endswith("|m")
    assert "b" not in bins


def test_get_single_record_roundtrip(connector, fake_client, fake_backend):
    key = FakeKey("r1")
    payload = b"payload-bytes"
    mo = FakeMemoryObj(bytearray(payload))
    connector._engine.put(key, mo)
    fake_backend.alloc_size = len(payload)
    got = connector._engine.get(key)
    assert got is not None
    assert bytes(got.byte_array[: len(payload)]) == payload


def test_get_missing_segment_returns_none(connector, fake_client):
    key = FakeKey("segmiss")
    mk = K.meta_key(connector.cfg.namespace, connector.cfg.set_name, key)
    fake_client.get_store[mk] = {
        "state": "ready",
        "nseg": 2,
        "seg_b": 10,
        "tot_b": 20,
    }
    fake_client.batch_read_results = FakeBatchRecords(
        [FakeBatchRecord(0, None), FakeBatchRecord(2, None)]
    )
    got = connector._engine.get(key)
    assert got is None


def test_batched_contains_prefix(connector, fake_client):
    keys = [FakeKey("a"), FakeKey("b"), FakeKey("c"), FakeKey("d")]
    for k in keys[:2]:
        mk = K.meta_key(connector.cfg.namespace, connector.cfg.set_name, k)
        fake_client.get_store[mk] = {"state": "ready", "nseg": 1}
    fake_client.batch_read_results = FakeBatchRecords(
        [
            FakeBatchRecord(0, None),
            FakeBatchRecord(0, None),
            FakeBatchRecord(2, None),
            FakeBatchRecord(0, None),
        ]
    )
    assert connector.batched_contains(keys) == 2


def test_put_record_too_big(connector, fake_client):
    key = FakeKey("bigerr")
    mo = FakeMemoryObj(bytearray(b"z" * 100))

    def failing_put(*_a, **_k):
        raise ax.RecordTooBig()

    fake_client.put = failing_put
    with pytest.raises(AerospikeRecordTooBigError):
        connector._engine.put(key, mo)


def test_put_batch_busy(connector, fake_client):
    key = FakeKey("busy")
    mo = FakeMemoryObj(bytearray(b"y" * 3000))

    class BusyBatch:
        batch_records = [FakeBatchRecord(14, None)]

    def busy_batch_write(batch):
        batch.batch_records = BusyBatch.batch_records

    fake_client.batch_write = busy_batch_write
    with pytest.raises(AerospikeBusyError):
        connector._engine.put(key, mo)


def test_remove_sync(connector, fake_client):
    key = FakeKey("rm")
    mo = FakeMemoryObj(bytearray(b"rm"))
    connector._engine.put(key, mo)
    assert connector.remove_sync(key) is True
    mk = K.meta_key(connector.cfg.namespace, connector.cfg.set_name, key)
    assert mk not in fake_client.get_store
