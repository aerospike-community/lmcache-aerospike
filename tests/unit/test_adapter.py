from unittest.mock import MagicMock, patch

from lmcache_aerospike.adapter import AerospikeConnectorAdapter


def test_can_parse_urls():
    adapter = AerospikeConnectorAdapter()
    assert adapter.can_parse("aerospike://cluster")
    assert adapter.can_parse("plugin://aerospike.primary")
    assert not adapter.can_parse("redis://host")


def test_create_connector_uses_local_cpu_backend_config():
    adapter = AerospikeConnectorAdapter()
    lcb = MagicMock()
    lcb.config.extra_config = {
        "remote_storage_plugin.aerospike.hosts": "127.0.0.1:3000"
    }
    lcb.metadata = MagicMock()
    lcb.metadata.get_shapes.return_value = []
    lcb.metadata.get_dtypes.return_value = []
    lcb.metadata.use_mla = False
    lcb.metadata.chunk_size = 8
    lcb.metadata.get_num_groups.return_value = 1

    context = MagicMock()
    context.local_cpu_backend = lcb
    context.config = None
    context.metadata = None
    context.plugin_name = "aerospike"
    context.loop = MagicMock()

    with patch(
        "lmcache_aerospike.adapter.AerospikeClientHolder.get_or_create"
    ) as get_holder, patch(
        "lmcache_aerospike.adapter.AerospikeRemoteConnector"
    ) as conn_cls:
        holder = MagicMock()
        get_holder.return_value = holder
        adapter.create_connector(context)
        get_holder.assert_called_once()
        conn_cls.assert_called_once()
        kwargs = conn_cls.call_args.kwargs
        assert kwargs["local_cpu_backend"] is lcb
        assert kwargs["client_holder"] is holder
