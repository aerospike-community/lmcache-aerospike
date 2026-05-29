"""Shared helpers for live AerospikeL2Plugin integration tests (LMCache L2 contract)."""

from __future__ import annotations

import importlib
import os
import select
from collections.abc import Iterator

import pytest
import torch

from tests.integration.helpers import (
    aerospike_hosts,
    ensure_native_storage_ops_for_l2_tests,
)

ensure_native_storage_ops_for_l2_tests()

l2_mod = importlib.import_module("lmcache_aerospike.l2_plugin")
if not l2_mod.L2_MP_AVAILABLE:
    pytest.skip(
        "LMCache multiprocess L2 APIs (L2StoreResult) not in this lmcache build",
        allow_module_level=True,
    )

from lmcache.v1.distributed.api import ObjectKey  # noqa: E402
from lmcache.v1.distributed.l2_adapters.factory import (  # noqa: E402
    create_l2_adapter_from_registry,
)
from lmcache.v1.distributed.l2_adapters.plugin_l2_adapter import (  # noqa: E402
    PluginL2AdapterConfig,
)
from lmcache.v1.memory_management import (  # noqa: E402
    MemoryFormat,
    MemoryObjMetadata,
    TensorMemoryObj,
)
from lmcache.v1.platform import consume_fd  # noqa: E402
from lmcache.v1.protocol import init_remote_metadata_info  # noqa: E402

init_remote_metadata_info(1)

RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"
L2_IT_SET = os.environ.get("AEROSPIKE_L2_TEST_SET", "kv_chunks_l2_it")


def wait_for_event_fd(event_fd: int, timeout: float = 30.0) -> bool:
    poll = select.poll()
    poll.register(event_fd, select.POLLIN)
    if not poll.poll(timeout * 1000):
        return False
    try:
        consume_fd(event_fd)
    except BlockingIOError:
        pass
    return True


def object_key(chunk_id: int, model_name: str = "lmcache_aerospike_it") -> ObjectKey:
    return ObjectKey(
        chunk_hash=ObjectKey.IntHash2Bytes(chunk_id),
        model_name=model_name,
        kv_rank=0,
    )


def memory_obj(size: int = 256, fill_value: float = 1.0) -> TensorMemoryObj:
    """TensorMemoryObj with shapes/dtypes (LMCache dev metadata for serde)."""
    raw = torch.empty(size, dtype=torch.float32)
    raw.fill_(fill_value)
    meta = MemoryObjMetadata(
        shape=torch.Size([size]),
        dtype=torch.float32,
        address=0,
        phy_size=size * 4,
        fmt=MemoryFormat.KV_2LTD,
        ref_count=1,
        shapes=[torch.Size([size])],
        dtypes=[torch.float32],
    )
    return TensorMemoryObj(raw, meta, parent_allocator=None)


def plugin_config() -> PluginL2AdapterConfig:
    host, port = aerospike_hosts()[0]
    return PluginL2AdapterConfig(
        module_path="lmcache_aerospike.l2_plugin",
        class_name="AerospikeL2Plugin",
        adapter_params={
            "hosts": f"{host}:{port}",
            "namespace": os.environ.get("AEROSPIKE_TEST_NAMESPACE", "lmcache"),
            "set_name": L2_IT_SET,
        },
    )


@pytest.fixture
def aerospike_l2_adapter() -> Iterator:
    adapter = create_l2_adapter_from_registry(plugin_config())
    try:
        yield adapter
    finally:
        adapter.close()
