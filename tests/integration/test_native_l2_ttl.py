"""Native connector TTL behavior against live Aerospike CE."""

from __future__ import annotations

import time

import pytest

from lmcache.v1.distributed.l2_adapters.factory import create_l2_adapter_from_registry

from tests.integration.native_l2_support import (
    fake_obj,
    lookup,
    native_config,
    object_key,
    requires_native_integration,
    store,
)

pytestmark = requires_native_integration


def test_short_ttl_key_disappears():
    adapter = create_l2_adapter_from_registry(
        native_config(default_ttl_seconds=2)
    )
    try:
        key = object_key(7001, model_name="native-ttl-expire")
        store(adapter, [key], [fake_obj(512, fill=0xAA)])
        assert lookup(adapter, [key]).test(0)
        time.sleep(5)
        assert lookup(adapter, [key]).test(0) is False
    finally:
        adapter.close()
