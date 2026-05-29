"""Shared helpers and fixtures for native L2 integration tests."""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest

from lmcache.v1.distributed.l2_adapters.factory import create_l2_adapter_from_registry
from lmcache.v1.distributed.l2_adapters.native_plugin_l2_adapter import (
    NativePluginL2AdapterConfig,
)
from lmcache.v1.distributed.l2_adapters.plugin_l2_adapter import PluginL2AdapterConfig

from tests.integration.helpers import aerospike_hosts, on_github_actions
from tests.integration.l2_plugin_support import (
    RUN_INTEGRATION,
    memory_obj,
    object_key,
    wait_for_event_fd,
)
from tests.unit.fakes import FakeMemoryObj

from lmcache_aerospike.native_connector import NATIVE_AVAILABLE

if TYPE_CHECKING:
    from lmcache.v1.distributed.l2_adapters.l2_adapter_interface import L2AdapterInterface

RUN_NATIVE = os.environ.get("RUN_NATIVE") == "1"
NATIVE_IT_SET = os.environ.get("AEROSPIKE_NATIVE_TEST_SET", "kv_chunks_native_it")
NATIVE_IT_NAMESPACE = os.environ.get("AEROSPIKE_TEST_NAMESPACE", "lmcache")

KIB = 1024
MIB = 1024 * KIB

requires_native_integration = pytest.mark.skipif(
    not (RUN_INTEGRATION and RUN_NATIVE and NATIVE_AVAILABLE),
    reason=(
        "Set RUN_INTEGRATION=1 and RUN_NATIVE=1 with the native extension "
        "and Aerospike CE available"
    ),
)


def payload_pattern(size: int) -> bytes:
    return bytes((i * 17 + 3) % 256 for i in range(size))


def fake_obj(size: int, fill: int = 0xAB) -> FakeMemoryObj:
    return FakeMemoryObj(bytearray(bytes([fill & 0xFF]) * size))


def fake_obj_pattern(size: int) -> FakeMemoryObj:
    return FakeMemoryObj(bytearray(payload_pattern(size)))


def round_trip_payload_sizes() -> list[int]:
    """CI-safe sizes; local runs add 1 MiB."""
    sizes = [512, 64 * KIB]
    if not on_github_actions():
        sizes.append(1 * MIB)
    return sizes


def multi_segment_payload_size() -> int:
    """Payload large enough to exceed the native single-record threshold on CE."""
    if on_github_actions():
        return 1048576 - 65536 + 8192
    return 2 * MIB


def native_config(
    *,
    set_name: str | None = None,
    num_workers: int | None = None,
    default_ttl_seconds: int | None = None,
    target_segment_bytes: int | None = None,
) -> NativePluginL2AdapterConfig:
    host, port = aerospike_hosts()[0]
    params: dict = {
        "hosts": f"{host}:{port}",
        "namespace": NATIVE_IT_NAMESPACE,
        "set_name": set_name or NATIVE_IT_SET,
        "num_workers": num_workers or int(os.environ.get("AEROSPIKE_NATIVE_WORKERS", "4")),
        "dtype": "bfloat16",
    }
    if default_ttl_seconds is not None:
        params["default_ttl_seconds"] = default_ttl_seconds
    if target_segment_bytes is not None:
        params["target_segment_bytes"] = target_segment_bytes
    return NativePluginL2AdapterConfig(
        module_path="lmcache_aerospike.native_connector",
        class_name="AerospikeNativeConnector",
        adapter_params=params,
    )


def python_l2_config(*, set_name: str | None = None) -> PluginL2AdapterConfig:
    host, port = aerospike_hosts()[0]
    return PluginL2AdapterConfig(
        module_path="lmcache_aerospike.l2_plugin",
        class_name="AerospikeL2Plugin",
        adapter_params={
            "hosts": f"{host}:{port}",
            "namespace": NATIVE_IT_NAMESPACE,
            "set_name": set_name or NATIVE_IT_SET,
            "dtype": "bfloat16",
        },
    )


@pytest.fixture
def native_l2_adapter() -> Iterator[L2AdapterInterface]:
    adapter = create_l2_adapter_from_registry(native_config())
    try:
        yield adapter
    finally:
        adapter.close()


def store(adapter: L2AdapterInterface, keys, objs) -> int:
    tid = adapter.submit_store_task(list(keys), list(objs))
    assert wait_for_event_fd(adapter.get_store_event_fd())
    done = adapter.pop_completed_store_tasks()
    assert tid in done
    assert done[tid].is_successful()
    return tid


def lookup(adapter: L2AdapterInterface, keys):
    tid = adapter.submit_lookup_and_lock_task(list(keys))
    assert wait_for_event_fd(adapter.get_lookup_and_lock_event_fd())
    bitmap = adapter.query_lookup_and_lock_result(tid)
    assert bitmap is not None
    return bitmap


def load(adapter: L2AdapterInterface, keys, objs):
    tid = adapter.submit_load_task(list(keys), list(objs))
    assert wait_for_event_fd(adapter.get_load_event_fd())
    bitmap = adapter.query_load_result(tid)
    assert bitmap is not None
    return bitmap


def meta_bins_for_key(key, *, namespace: str, set_name: str, dtype: str = "bfloat16"):
    """Read Phase 1/2 meta record bins for an L2 ``ObjectKey``."""
    import aerospike
    import torch

    from lmcache_aerospike import keys as K
    from lmcache_aerospike.object_keys import object_key_to_cache_engine_key

    torch_dtype = getattr(torch, dtype, torch.bfloat16)
    ck = object_key_to_cache_engine_key(key, dtype=torch_dtype)
    host, port = aerospike_hosts()[0]
    client = aerospike.client({"hosts": [(host, port)]}).connect()
    try:
        _k, _meta, bins = client.get(K.meta_key(namespace, set_name, ck))
        return bins
    except aerospike.exception.RecordNotFound:
        return None
    finally:
        client.close()


__all__ = [
    "KIB",
    "MIB",
    "NATIVE_IT_NAMESPACE",
    "NATIVE_IT_SET",
    "RUN_NATIVE",
    "fake_obj",
    "fake_obj_pattern",
    "load",
    "lookup",
    "memory_obj",
    "meta_bins_for_key",
    "multi_segment_payload_size",
    "native_config",
    "native_l2_adapter",
    "object_key",
    "payload_pattern",
    "python_l2_config",
    "requires_native_integration",
    "round_trip_payload_sizes",
    "store",
    "wait_for_event_fd",
]
