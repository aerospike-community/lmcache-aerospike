import pytest

from lmcache_aerospike.errors import AerospikeConfigError
from lmcache_aerospike.sharding import plan, slice_lengths

MIB = 1024 * 1024
MAX = 8 * MIB
TARGET = 4 * MIB
MIN = 64 * 1024
THRESH = 4 * MIB


def _plan(payload: int):
    return plan(
        payload,
        target_segment_bytes=TARGET,
        max_segment_bytes=MAX,
        min_segment_bytes=MIN,
        single_record_threshold_bytes=THRESH,
    )


def test_small_payloads_single_record():
    p256 = _plan(256)
    assert p256.nseg == 1 and p256.seg_b == 256
    p4 = _plan(4 * MIB)
    assert p4.nseg == 1 and p4.seg_b == 4 * MIB


def test_over_threshold_multi_segment():
    p = _plan(4 * MIB + 1)
    assert p.nseg == 2
    assert p.seg_b <= TARGET
    lengths = slice_lengths(4 * MIB + 1, p)
    assert sum(lengths) == 4 * MIB + 1


def test_16_and_64_mib():
    p16 = _plan(16 * MIB)
    assert p16 == plan(
        16 * MIB,
        target_segment_bytes=TARGET,
        max_segment_bytes=MAX,
        min_segment_bytes=MIN,
        single_record_threshold_bytes=THRESH,
    )
    assert p16.nseg == 4 and p16.seg_b == TARGET
    p64 = _plan(64 * MIB)
    assert p64.nseg == 16 and p64.seg_b == TARGET
    assert sum(slice_lengths(64 * MIB, p64)) == 64 * MIB


def test_target_exceeds_max_raises():
    with pytest.raises(AerospikeConfigError):
        plan(
            1024,
            target_segment_bytes=10 * MIB,
            max_segment_bytes=MAX,
            min_segment_bytes=MIN,
            single_record_threshold_bytes=THRESH,
        )
