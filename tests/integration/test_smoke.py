"""Minimal live-cluster smoke checks (RUN_INTEGRATION=1 only)."""


def test_lmcache_namespace_nsup_period():
    import aerospike

    client = aerospike.client({"hosts": [("127.0.0.1", 3000)]})
    client.connect()
    try:
        info = client.info_random_node("namespace/lmcache")
    finally:
        client.close()
    assert "nsup-period=120" in info
