"""Aerospike read/write/batch policy dict factories."""

from __future__ import annotations

import aerospike

from lmcache_aerospike.config import AerospikeConfig
from lmcache_aerospike.errors import AerospikeConfigError

_COMMIT = {
    "all": "POLICY_COMMIT_LEVEL_ALL",
    "master": "POLICY_COMMIT_LEVEL_MASTER",
}
_REPLICA = {
    "master": "POLICY_REPLICA_MASTER",
    "any": "POLICY_REPLICA_ANY",
    "sequence": "POLICY_REPLICA_SEQUENCE",
    "prefer_rack": "POLICY_REPLICA_PREFER_RACK",
}


def _policy_const(name: str) -> int:
    if not hasattr(aerospike, name):
        raise AerospikeConfigError(f"aerospike client missing policy constant {name}")
    return int(getattr(aerospike, name))


def _commit_level(value: str) -> int:
    const = _COMMIT.get(value)
    if const is None:
        raise AerospikeConfigError(f"unknown commit_level {value!r}")
    return _policy_const(const)


def _replica(value: str) -> int:
    const = _REPLICA.get(value)
    if const is None:
        raise AerospikeConfigError(f"unknown replica {value!r}")
    return _policy_const(const)


def _key_policy(send_key: bool) -> int:
    name = "POLICY_KEY_SEND" if send_key else "POLICY_KEY_DIGEST"
    return _policy_const(name)


def read_policy(cfg: AerospikeConfig) -> dict:
    return {
        "total_timeout": cfg.read_timeout_ms,
        "socket_timeout": cfg.read_timeout_ms,
        "replica": _replica(cfg.replica),
        "key": _key_policy(cfg.send_key),
        "max_retries": 2,
    }


def write_policy(cfg: AerospikeConfig) -> dict:
    return {
        "total_timeout": cfg.write_timeout_ms,
        "socket_timeout": cfg.write_timeout_ms,
        "commit_level": _commit_level(cfg.commit_level),
        "key": _key_policy(cfg.send_key),
        "exists": _policy_const("POLICY_EXISTS_IGNORE"),
        "gen": _policy_const("POLICY_GEN_IGNORE"),
        "max_retries": 0,
    }


def batch_policy(cfg: AerospikeConfig) -> dict:
    timeout = max(cfg.read_timeout_ms, cfg.write_timeout_ms)
    return {"total_timeout": timeout, "socket_timeout": timeout}
