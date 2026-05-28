import pytest

from lmcache_aerospike.config import AerospikeConfig
from lmcache_aerospike.errors import (
    AerospikeNamespaceProbeError,
    AerospikeServerLimitError,
    AerospikeTTLConfigError,
)
from lmcache_aerospike import limits


def test_parse_namespace_info_strips_prefix():
    parsed = limits.parse_namespace_info(
        "ns\tmax-record-size=1048576;nsup-period=120;write-block-size=512"
    )
    assert parsed["max-record-size"] == "1048576"
    assert parsed["nsup-period"] == "120"


def test_discover_limits_71_max_record_size():
    class Client:
        def info_random_node(self, _q):
            return "max-record-size=1048576;nsup-period=120"

    sl = limits.discover_limits(Client(), "lmcache")
    assert sl.server_max_record_bytes == 1048576
    assert sl.source == "max-record-size"


def test_discover_limits_70_write_block_size():
    class Client:
        def info_random_node(self, _q):
            return "write-block-size=1048576;nsup-period=120"

    sl = limits.discover_limits(Client(), "lmcache")
    assert sl.server_max_record_bytes == 1048576
    assert sl.source == "write-block-size"


def test_discover_limits_bad_cap_raises():
    class Client:
        def info_random_node(self, _q):
            return "max-record-size=0"

    with pytest.raises(AerospikeServerLimitError):
        limits.discover_limits(Client(), "lmcache")


def test_resolve_nsup_zero_with_ttl_raises():
    cfg = AerospikeConfig.from_extra_config(
        {"remote_storage_plugin.aerospike.hosts": "h:3000"},
        "aerospike",
    )
    server = limits.ServerLimits(8388608, "max-record-size", nsup_period=0)
    with pytest.raises(AerospikeTTLConfigError):
        limits.resolve_segment_limits(cfg, server)


def test_empty_info_raises_probe():
    class Client:
        def info_random_node(self, _q):
            return ""

    with pytest.raises(AerospikeNamespaceProbeError):
        limits.discover_limits(Client(), "lmcache")
