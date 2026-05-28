import aerospike
import pytest

from lmcache_aerospike.config import AerospikeConfig
from lmcache_aerospike.errors import AerospikeConfigError
from lmcache_aerospike import policies


def test_read_write_batch_policies_map_constants():
    cfg = AerospikeConfig.from_extra_config(
        {
            "remote_storage_plugin.aerospike.hosts": "h:3000",
            "remote_storage_plugin.aerospike.commit_level": "master",
            "remote_storage_plugin.aerospike.replica": "any",
        },
        "aerospike",
    )
    rp = policies.read_policy(cfg)
    wp = policies.write_policy(cfg)
    bp = policies.batch_policy(cfg)
    assert rp["replica"] == aerospike.POLICY_REPLICA_ANY
    assert wp["commit_level"] == aerospike.POLICY_COMMIT_LEVEL_MASTER
    assert wp["exists"] == aerospike.POLICY_EXISTS_IGNORE
    assert bp["total_timeout"] == max(cfg.read_timeout_ms, cfg.write_timeout_ms)


def test_unknown_replica_raises():
    from dataclasses import replace

    cfg = AerospikeConfig.from_extra_config(
        {"remote_storage_plugin.aerospike.hosts": "h:3000"},
        "aerospike",
    )
    cfg = replace(cfg, replica="bogus")
    with pytest.raises(AerospikeConfigError, match="replica"):
        policies.read_policy(cfg)
