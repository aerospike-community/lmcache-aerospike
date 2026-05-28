"""LMCache ConnectorAdapter entry point."""

from __future__ import annotations

from lmcache.logging import init_logger
from lmcache.v1.storage_backend.connector import (
    ConnectorAdapter,
    ConnectorContext,
    extract_plugin_type,
)

from lmcache_aerospike.client import AerospikeClientHolder
from lmcache_aerospike.config import AerospikeConfig
from lmcache_aerospike.connector import AerospikeRemoteConnector

logger = init_logger(__name__)


class AerospikeConnectorAdapter(ConnectorAdapter):
    def __init__(self) -> None:
        super().__init__("aerospike://")

    def can_parse(self, url: str) -> bool:
        if url.startswith("aerospike://"):
            return True
        if url.startswith("plugin://"):
            plugin_name = url[len("plugin://") :]
            return extract_plugin_type(plugin_name) == "aerospike"
        return False

    def create_connector(self, context: ConnectorContext):
        lcb = context.local_cpu_backend
        config = getattr(lcb, "config", None) or context.config
        metadata = getattr(lcb, "metadata", None) or context.metadata
        plugin_name = context.plugin_name or "aerospike"
        as_cfg = AerospikeConfig.from_extra_config(
            config.extra_config if config else None,
            plugin_name,
        )
        holder = AerospikeClientHolder.get_or_create(as_cfg)
        logger.info(
            "Creating Aerospike remote connector plugin=%s hosts=%s namespace=%s",
            plugin_name,
            as_cfg.hosts,
            as_cfg.namespace,
        )
        return AerospikeRemoteConnector(
            config=config,
            metadata=metadata,
            local_cpu_backend=lcb,
            loop=context.loop,
            aerospike_config=as_cfg,
            client_holder=holder,
        )
