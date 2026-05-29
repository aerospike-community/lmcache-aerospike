"""Gated native Aerospike connector integration tests."""

from __future__ import annotations

import os

import pytest

from tests.integration.helpers import aerospike_hosts
from tests.integration.l2_plugin_support import (
    RUN_INTEGRATION,
    object_key,
    wait_for_event_fd,
)
from tests.unit.fakes import FakeMemoryObj

from lmcache.v1.distributed.l2_adapters.factory import create_l2_adapter_from_registry
from lmcache.v1.distributed.l2_adapters.native_plugin_l2_adapter import (
    NativePluginL2AdapterConfig,
)
from lmcache.v1.distributed.l2_adapters.plugin_l2_adapter import PluginL2AdapterConfig
from lmcache_aerospike.native_connector import NATIVE_AVAILABLE

RUN_NATIVE = os.environ.get("RUN_NATIVE") == "1"

pytestmark = pytest.mark.skipif(
    not (RUN_INTEGRATION and RUN_NATIVE and NATIVE_AVAILABLE),
    reason=(
        "Set RUN_INTEGRATION=1 and RUN_NATIVE=1 with the native extension "
        "and Aerospike CE available"
    ),
)


def _native_config() -> NativePluginL2AdapterConfig:
    host, port = aerospike_hosts()[0]
    return NativePluginL2AdapterConfig(
        module_path="lmcache_aerospike.native_connector",
        class_name="AerospikeNativeConnector",
        adapter_params={
            "hosts": f"{host}:{port}",
            "namespace": os.environ.get("AEROSPIKE_TEST_NAMESPACE", "lmcache"),
            "set_name": os.environ.get("AEROSPIKE_NATIVE_TEST_SET", "kv_chunks_native_it"),
            "num_workers": int(os.environ.get("AEROSPIKE_NATIVE_WORKERS", "4")),
            "dtype": "bfloat16",
        },
    )


def _python_config() -> PluginL2AdapterConfig:
    host, port = aerospike_hosts()[0]
    return PluginL2AdapterConfig(
        module_path="lmcache_aerospike.l2_plugin",
        class_name="AerospikeL2Plugin",
        adapter_params={
            "hosts": f"{host}:{port}",
            "namespace": os.environ.get("AEROSPIKE_TEST_NAMESPACE", "lmcache"),
            "set_name": os.environ.get("AEROSPIKE_NATIVE_TEST_SET", "kv_chunks_native_it"),
            "dtype": "bfloat16",
        },
    )


def _store(adapter, key, obj) -> None:
    tid = adapter.submit_store_task([key], [obj])
    assert wait_for_event_fd(adapter.get_store_event_fd())
    done = adapter.pop_completed_store_tasks()
    assert tid in done
    assert done[tid].is_successful()


def _lookup(adapter, *keys):
    tid = adapter.submit_lookup_and_lock_task(list(keys))
    assert wait_for_event_fd(adapter.get_lookup_and_lock_event_fd())
    bitmap = adapter.query_lookup_and_lock_result(tid)
    assert bitmap is not None
    return bitmap


def _load(adapter, key, obj):
    tid = adapter.submit_load_task([key], [obj])
    assert wait_for_event_fd(adapter.get_load_event_fd())
    bitmap = adapter.query_load_result(tid)
    assert bitmap is not None
    return bitmap


def test_native_l2_roundtrip_and_delete():
    adapter = create_l2_adapter_from_registry(_native_config())
    try:
        key = object_key(3001, model_name="native-roundtrip")
        missing = object_key(9001, model_name="native-roundtrip")
        stored = FakeMemoryObj(bytearray(b"n" * 512))
        loaded = FakeMemoryObj(bytearray(b"\0" * 512))

        _store(adapter, key, stored)
        lookup = _lookup(adapter, key, missing)
        assert lookup.test(0) is True
        assert lookup.test(1) is False

        load = _load(adapter, key, loaded)
        assert load.test(0) is True
        assert bytes(loaded.byte_array) == bytes(stored.byte_array)

        adapter.delete([key])
        lookup_after_delete = _lookup(adapter, key)
        assert lookup_after_delete.test(0) is False
    finally:
        adapter.close()


def test_native_and_python_l2_share_phase12_schema():
    native = create_l2_adapter_from_registry(_native_config())
    python = create_l2_adapter_from_registry(_python_config())
    try:
        python_key = object_key(3002, model_name="native-compat")
        native_key = object_key(3003, model_name="native-compat")

        python_written = FakeMemoryObj(bytearray(b"p" * 512))
        native_loaded = FakeMemoryObj(bytearray(b"\0" * 512))
        _store(python, python_key, python_written)
        assert _load(native, python_key, native_loaded).test(0) is True
        assert bytes(native_loaded.byte_array) == bytes(python_written.byte_array)

        native_written = FakeMemoryObj(bytearray(b"c" * 512))
        python_loaded = FakeMemoryObj(bytearray(b"\0" * 512))
        _store(native, native_key, native_written)
        assert _load(python, native_key, python_loaded).test(0) is True
        assert bytes(python_loaded.byte_array) == bytes(native_written.byte_array)
    finally:
        native.close()
        python.close()
