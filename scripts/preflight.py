# file: scripts/preflight.py
import inspect
import aerospike
from aerospike_helpers.batch import records as br
from aerospike_helpers.operations import operations as op
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector
from lmcache.v1.storage_backend.connector import (
    ConnectorAdapter, ConnectorContext, extract_plugin_type,
)
from lmcache.v1.protocol import (
    RemoteMetadata, get_remote_metadata_bytes, init_remote_metadata_info,
)

# 1) RemoteConnector abstract methods present
need = {"exists", "exists_sync", "get", "put", "list", "close"}
have = set(RemoteConnector.__abstractmethods__)
assert need <= have, f"missing abstract methods: {need - have}"

# 2) support_* defaults
assert RemoteConnector.support_batched_async_contains(RemoteConnector) is True
assert RemoteConnector.support_batched_get_non_blocking(RemoteConnector) is True
assert RemoteConnector.support_batched_contains(RemoteConnector) is False

# 3) ConnectorAdapter shape
assert "create_connector" in ConnectorAdapter.__abstractmethods__
assert extract_plugin_type("aerospike.primary") == "aerospike"

# 4) Aerospike client method names this plan uses
for name in ("put", "get", "select", "exists", "remove",
             "batch_read", "batch_write", "is_connected",
             "get_node_names", "info_random_node", "scan"):
    assert hasattr(aerospike.Client, name), f"client missing {name}"

# 5) Legacy methods we deliberately AVOID — warn if absent (expected)
for gone in ("exists_many", "get_many", "select_many"):
    print(f"legacy {gone} present:", hasattr(aerospike.Client, gone))

# 6) batch records constructors
assert all(hasattr(br, n) for n in ("BatchRecords", "Write", "Read", "Remove"))
assert hasattr(op, "write") and hasattr(op, "read")

# 7) TTL constants
for c in ("TTL_NEVER_EXPIRE", "TTL_NAMESPACE_DEFAULT", "TTL_DONT_UPDATE"):
    print(c, "=", getattr(aerospike, c, "MISSING"))

# 8) RemoteMetadata round-trip (num_groups=1)
init_remote_metadata_info(1)
print("remote_metadata_bytes =", get_remote_metadata_bytes())
print("OK: preflight passed")
