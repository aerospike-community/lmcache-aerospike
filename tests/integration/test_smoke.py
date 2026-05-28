"""Minimal live-cluster smoke checks (RUN_INTEGRATION=1 only)."""

import os


def test_lmcache_namespace_nsup_period():
    import aerospike

    host = os.environ.get("AEROSPIKE_TEST_HOST", "127.0.0.1")
    port = int(os.environ.get("AEROSPIKE_TEST_PORT", "3000"))
    client = aerospike.client({"hosts": [(host, port)]})
    client.connect()
    try:
        info = client.info_random_node("namespace/lmcache")
    finally:
        client.close()
    assert "nsup-period=120" in info
