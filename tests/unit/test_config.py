import pytest

from lmcache_aerospike.config import AerospikeConfig
from lmcache_aerospike.errors import AerospikeConfigError


def test_from_extra_config_valid():
    extra = {
        "remote_storage_plugin.aerospike.hosts": "h1:3000,h2:3001",
        "remote_storage_plugin.aerospike.namespace": "testns",
        "remote_storage_plugin.aerospike.set": "chunks",
        "remote_storage_plugin.aerospike.target_segment_bytes": "8388608",
        "remote_storage_plugin.aerospike.enable_list": "true",
    }
    cfg = AerospikeConfig.from_extra_config(extra, "aerospike")
    assert cfg.hosts == (("h1", 3000), ("h2", 3001))
    assert cfg.namespace == "testns"
    assert cfg.set_name == "chunks"
    assert cfg.target_segment_bytes == 8388608
    assert cfg.enable_list is True
    assert cfg.key("hosts") == "remote_storage_plugin.aerospike.hosts"


def test_missing_hosts_raises():
    with pytest.raises(AerospikeConfigError, match="hosts"):
        AerospikeConfig.from_extra_config({}, "aerospike")


def test_invalid_commit_level_raises():
    extra = {
        "remote_storage_plugin.aerospike.hosts": "localhost:3000",
        "remote_storage_plugin.aerospike.commit_level": "bogus",
    }
    with pytest.raises(AerospikeConfigError, match="commit_level"):
        AerospikeConfig.from_extra_config(extra, "aerospike")


def test_instance_scoped_plugin_name():
    extra = {
        "remote_storage_plugin.aerospike.primary.hosts": "primary:3000",
        "remote_storage_plugin.aerospike.primary.namespace": "lmcache",
    }
    cfg = AerospikeConfig.from_extra_config(extra, "aerospike.primary")
    assert cfg.hosts == (("primary", 3000),)
    assert cfg.plugin_name == "aerospike.primary"
    assert cfg.namespace == "lmcache"
