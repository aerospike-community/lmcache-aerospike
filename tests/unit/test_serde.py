from unittest.mock import MagicMock

from lmcache.v1.protocol import RemoteMetadata, init_remote_metadata_info

from lmcache_aerospike.sharding import ShardPlan
from lmcache_aerospike import serde
from tests.unit.fakes import FakeMemoryObj


def test_meta_bins_save_chunk_meta_true():
    init_remote_metadata_info(1)
    mo = FakeMemoryObj(bytearray(b"hello"))
    bins = serde.meta_bins(
        plan=ShardPlan(1, 5),
        memory_obj=mo,
        save_chunk_meta=True,
        enable_crc32=False,
        default_ttl=3600,
        pinned=False,
    )
    assert "md" in bins
    assert "b" in bins
    assert bins["nseg"] == 1


def test_meta_bins_save_chunk_meta_false_no_md():
    mo = FakeMemoryObj(bytearray(b"x" * 8))
    bins = serde.meta_bins(
        plan=ShardPlan(2, 4),
        memory_obj=mo,
        save_chunk_meta=False,
        enable_crc32=False,
        default_ttl=0,
        pinned=False,
    )
    assert "md" not in bins
    assert "b" not in bins
    assert bins["nseg"] == 2


def test_allocate_for_read_paths():
    init_remote_metadata_info(1)
    mo = FakeMemoryObj(bytearray(16))
    md = RemoteMetadata(
        16, mo.get_shapes(), mo.get_dtypes(), mo.get_memory_format()
    ).serialize()
    backend = MagicMock()
    backend.allocate.return_value = FakeMemoryObj(bytearray(32))

    base = MagicMock()
    base.save_chunk_meta = True
    out, reshape = serde.allocate_for_read(
        backend, base, {"md": md, "state": "ready"}
    )
    assert out is not None and reshape is False

    base.save_chunk_meta = False
    out2, reshape2 = serde.allocate_for_read(backend, base, {"state": "ready"})
    assert out2 is not None and reshape2 is True
