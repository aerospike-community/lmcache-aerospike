from __future__ import annotations

import pytest

import lmcache_aerospike.native_connector as native_connector


def test_native_connector_raises_clear_error_when_extension_unavailable():
    if native_connector.NATIVE_AVAILABLE:
        pytest.skip("native extension is available in this environment")

    with pytest.raises(RuntimeError, match="Aerospike native connector is not available"):
        native_connector.AerospikeNativeConnector(
            hosts="127.0.0.1:3000",
            namespace="lmcache",
            set_name="kv_chunks",
        )


def test_native_connector_forwards_validated_params(monkeypatch):
    captured = {}

    class FakeNativeClient:
        def __init__(self, *args):
            captured["args"] = args

    class FakeNativeModule:
        AerospikeNativeClient = FakeNativeClient

    monkeypatch.setattr(native_connector, "_native", FakeNativeModule)
    monkeypatch.setattr(native_connector, "NATIVE_AVAILABLE", True)
    monkeypatch.setattr(native_connector, "_NATIVE_IMPORT_ERROR", None)

    client = native_connector.AerospikeNativeConnector(
        hosts="127.0.0.1:3000",
        namespace="lmcache",
        set_name="kv_chunks",
        num_workers=4,
        read_timeout_ms=123,
        write_timeout_ms=456,
        default_ttl_seconds=789,
        dtype="float16",
        target_segment_bytes=1024,
        max_record_bytes=2048,
        username="u",
        password="p",
    )

    assert isinstance(client, FakeNativeClient)
    assert captured["args"] == (
        "127.0.0.1:3000",
        "lmcache",
        "kv_chunks",
        4,
        123,
        456,
        789,
        "float16",
        1024,
        2048,
        "u",
        "p",
    )


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"hosts": ""}, "hosts must be a non-empty string"),
        ({"namespace": ""}, "namespace must be a non-empty string"),
        ({"set_name": ""}, "set_name must be a non-empty string"),
        ({"num_workers": 0}, "num_workers must be a positive integer"),
        ({"target_segment_bytes": -1}, "target_segment_bytes must be a non-negative integer"),
        ({"tls_name": "tls"}, "tls_name is not supported"),
        ({"unexpected": True}, "unknown Aerospike native connector params"),
    ],
)
def test_native_connector_validates_params(monkeypatch, kwargs, match):
    class FakeNativeModule:
        class AerospikeNativeClient:
            pass

    monkeypatch.setattr(native_connector, "_native", FakeNativeModule)
    monkeypatch.setattr(native_connector, "NATIVE_AVAILABLE", True)

    params = {
        "hosts": "127.0.0.1:3000",
        "namespace": "lmcache",
        "set_name": "kv_chunks",
    }
    params.update(kwargs)
    with pytest.raises((RuntimeError, TypeError, ValueError), match=match):
        native_connector.AerospikeNativeConnector(**params)
