"""Ref-counted singleton Aerospike client holder."""

from __future__ import annotations

import logging
import threading
from typing import ClassVar

import aerospike

from lmcache_aerospike.config import AerospikeConfig
from lmcache_aerospike import policies

logger = logging.getLogger(__name__)

_HOLDERS: dict[tuple, AerospikeClientHolder] = {}
_LOCK = threading.Lock()


class AerospikeClientHolder:
    """One connected client per (hosts, namespace, tls_name)."""

    _registry: ClassVar[dict[tuple, AerospikeClientHolder]] = _HOLDERS
    _lock: ClassVar[threading.Lock] = _LOCK

    def __init__(self, cfg: AerospikeConfig) -> None:
        self._cfg = cfg
        self._refcount = 0
        config = {
            "hosts": list(cfg.hosts),
            "policies": {
                "read": policies.read_policy(cfg),
                "write": policies.write_policy(cfg),
                "batch": policies.batch_policy(cfg),
            },
        }
        if cfg.username:
            config["user"] = cfg.username
        if cfg.password:
            config["password"] = cfg.password
        if cfg.tls_name:
            config["tls"] = {"name": cfg.tls_name}

        self._client = aerospike.client(config)
        self._client.connect()

    @property
    def client(self):
        return self._client

    @classmethod
    def get_or_create(cls, cfg: AerospikeConfig) -> AerospikeClientHolder:
        key = (cfg.hosts, cfg.namespace, cfg.tls_name)
        with cls._lock:
            holder = cls._registry.get(key)
            if holder is None:
                holder = cls(cfg)
                cls._registry[key] = holder
            holder._refcount += 1
            return holder

    def release(self) -> None:
        with self._lock:
            if self._refcount <= 0:
                logger.warning("AerospikeClientHolder release called with refcount 0")
                return
            self._refcount -= 1
            if self._refcount > 0:
                return
            key = (self._cfg.hosts, self._cfg.namespace, self._cfg.tls_name)
            try:
                self._client.close()
            except Exception as exc:
                logger.warning("error closing aerospike client: %s", exc)
            self._registry.pop(key, None)
