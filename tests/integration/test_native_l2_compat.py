"""Cross-read compatibility between Python L2 plugin and native connector."""

from __future__ import annotations

from lmcache.v1.distributed.l2_adapters.factory import create_l2_adapter_from_registry

from tests.integration.native_l2_support import (
    fake_obj,
    load,
    native_config,
    object_key,
    python_l2_config,
    requires_native_integration,
    store,
)

pytestmark = requires_native_integration


def test_native_and_python_l2_share_phase12_schema_both_directions():
    native = create_l2_adapter_from_registry(native_config())
    python = create_l2_adapter_from_registry(python_l2_config())
    try:
        python_key = object_key(6001, model_name="native-compat-py-write")
        native_key = object_key(6002, model_name="native-compat-native-write")

        python_written = fake_obj(512, fill=0x70)
        native_loaded = fake_obj(512, fill=0)
        store(python, [python_key], [python_written])
        assert load(native, [python_key], [native_loaded]).test(0)
        assert bytes(native_loaded.byte_array[:512]) == bytes(
            python_written.byte_array[:512]
        )

        native_written = fake_obj(512, fill=0x63)
        python_loaded = fake_obj(512, fill=0)
        store(native, [native_key], [native_written])
        assert load(python, [native_key], [python_loaded]).test(0)
        assert bytes(python_loaded.byte_array[:512]) == bytes(
            native_written.byte_array[:512]
        )
    finally:
        native.close()
        python.close()


def test_python_sharded_payload_readable_by_native():
    """Python L2 write with payload above single-record threshold; native loads it."""
    native = create_l2_adapter_from_registry(native_config())
    python = create_l2_adapter_from_registry(python_l2_config())
    try:
        size = 128 * 1024
        key = object_key(6003, model_name="native-compat-sharded")
        written = fake_obj(size, fill=0x5A)
        loaded = fake_obj(size, fill=0)
        store(python, [key], [written])
        assert load(native, [key], [loaded]).test(0)
        assert bytes(loaded.byte_array[:size]) == bytes(written.byte_array[:size])
    finally:
        native.close()
        python.close()


def test_native_sharded_payload_readable_by_python():
    from tests.integration.native_l2_support import multi_segment_payload_size

    native = create_l2_adapter_from_registry(native_config())
    python = create_l2_adapter_from_registry(python_l2_config())
    try:
        size = multi_segment_payload_size()
        key = object_key(6004, model_name="native-compat-native-shard")
        written = fake_obj(size, fill=0xA5)
        loaded = fake_obj(size, fill=0)
        store(native, [key], [written])
        assert load(python, [key], [loaded]).test(0)
        assert bytes(loaded.byte_array[:size]) == bytes(written.byte_array[:size])
    finally:
        native.close()
        python.close()
