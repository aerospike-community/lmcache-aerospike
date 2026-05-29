from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_render_adapter():
    path = Path(__file__).resolve().parents[2] / "benchmarks" / "l2" / "render_adapter.py"
    spec = importlib.util.spec_from_file_location("l2_render_adapter", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_render_aerospike_native_adapter():
    render_adapter = _load_render_adapter()
    template = {
        "type": "native_plugin",
        "module_path": "lmcache_aerospike.native_connector",
        "class_name": "AerospikeNativeConnector",
        "adapter_params": {"num_workers": 1},
    }

    rendered = render_adapter.render_aerospike(
        template,
        host="10.0.0.1",
        port="3000",
        namespace="lmcache",
        set_name="kv_chunks_native",
        num_workers=6,
    )

    assert rendered["type"] == "native_plugin"
    assert rendered["adapter_params"] == {
        "hosts": "10.0.0.1:3000",
        "namespace": "lmcache",
        "set_name": "kv_chunks_native",
        "num_workers": 6,
    }
    assert "set" not in rendered["adapter_params"]


def test_render_aerospike_python_adapter_keeps_set_alias():
    render_adapter = _load_render_adapter()
    template = {
        "type": "plugin",
        "module_path": "lmcache_aerospike.l2_plugin",
        "class_name": "AerospikeL2Plugin",
        "adapter_params": {},
    }

    rendered = render_adapter.render_aerospike(
        template,
        host="127.0.0.1",
        port="3000",
        namespace="lmcache",
        set_name="kv_chunks",
        num_workers=6,
    )

    assert rendered["adapter_params"]["set"] == "kv_chunks"
    assert "set_name" not in rendered["adapter_params"]
    assert "num_workers" not in rendered["adapter_params"]
