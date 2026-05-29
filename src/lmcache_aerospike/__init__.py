"""Aerospike remote storage backend for LMCache."""

from lmcache_aerospike.adapter import AerospikeConnectorAdapter
from lmcache_aerospike.connector import AerospikeRemoteConnector

__all__ = [
    "AerospikeConnectorAdapter",
    "AerospikeRemoteConnector",
    "AerospikeStoragePlugin",
    "AerospikeL2Plugin",
    "AerospikeL2PluginConfig",
]
__version__ = "0.3.0"


def __getattr__(name: str):
    if name == "AerospikeStoragePlugin":
        from lmcache_aerospike.storage_plugin import AerospikeStoragePlugin

        return AerospikeStoragePlugin
    if name == "AerospikeL2Plugin":
        from lmcache_aerospike.l2_plugin import AerospikeL2Plugin

        return AerospikeL2Plugin
    if name == "AerospikeL2PluginConfig":
        from lmcache_aerospike.l2_plugin import AerospikeL2PluginConfig

        return AerospikeL2PluginConfig
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
