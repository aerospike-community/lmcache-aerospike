from lmcache_aerospike import keys as K
from tests.unit.fakes import FakeKey


def test_meta_and_segment_keys():
    ck = FakeKey("abc|hash")
    assert K.meta_key("lmcache", "kv_chunks", ck) == (
        "lmcache",
        "kv_chunks",
        "abc|hash|m",
    )
    assert K.segment_key("lmcache", "kv_chunks", ck, 3) == (
        "lmcache",
        "kv_chunks",
        "abc|hash|s|3",
    )


def test_segment_keys_list():
    ck = FakeKey("k")
    keys = K.segment_keys("ns", "set", ck, 4)
    assert len(keys) == 4
    assert [k[2] for k in keys] == ["k|s|0", "k|s|1", "k|s|2", "k|s|3"]
