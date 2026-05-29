"""Python factory for the native Aerospike LMCache L2 connector."""

from __future__ import annotations

from typing import Any
import os

try:
    from lmcache_aerospike import _native

    NATIVE_AVAILABLE = True
    _NATIVE_IMPORT_ERROR: BaseException | None = None
except Exception as exc:  # pragma: no cover - covered by import-error test
    _native = None  # type: ignore[assignment]
    NATIVE_AVAILABLE = False
    _NATIVE_IMPORT_ERROR = exc


def _positive_int(value: Any, name: str) -> int:
    if not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _non_negative_int(value: Any, name: str) -> int:
    if not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


class AerospikeNativeConnector:
    """Factory class used by LMCache's ``native_plugin`` loader.

    The loader instantiates this class with ``adapter_params`` and expects the
    returned object to expose LMCache's native connector protocol. ``__new__``
    returns the pybind-wrapped C++ connector directly so the hot path does not
    pass through Python delegation.
    """

    def __new__(
        cls,
        hosts: str | None = None,
        namespace: str | None = None,
        set_name: str | None = None,
        num_workers: int = 8,
        read_timeout_ms: int = 1000,
        write_timeout_ms: int = 2000,
        default_ttl_seconds: int = 86400,
        dtype: str = "bfloat16",
        target_segment_bytes: int = 0,
        max_record_bytes: int = 0,
        username: str = "",
        password: str = "",
        tls_name: str = "",
        **extra: Any,
    ):
        if extra:
            unknown = ", ".join(sorted(extra))
            raise TypeError(f"unknown Aerospike native connector params: {unknown}")
        if not NATIVE_AVAILABLE:
            raise RuntimeError(
                "Aerospike native connector is not available. Install the "
                "package in an environment with pybind11, LMCache native "
                "headers, and libaerospike development files, or set "
                "LMCACHE_AEROSPIKE_FORCE_NATIVE=1 during build to fail fast."
            ) from _NATIVE_IMPORT_ERROR
        if tls_name:
            raise RuntimeError(
                "tls_name is not supported by the native Aerospike connector yet"
            )

        hosts = hosts or os.environ.get("LMCACHE_AEROSPIKE_HOSTS", "")
        namespace = namespace or os.environ.get("LMCACHE_AEROSPIKE_NAMESPACE", "")
        set_name = set_name or os.environ.get("LMCACHE_AEROSPIKE_SET", "")
        username = username or os.environ.get("LMCACHE_AEROSPIKE_USERNAME", "")
        password = password or os.environ.get("LMCACHE_AEROSPIKE_PASSWORD", "")

        if not hosts:
            raise ValueError("hosts must be a non-empty string")
        if not namespace:
            raise ValueError("namespace must be a non-empty string")
        if not set_name:
            raise ValueError("set_name must be a non-empty string")
        if not dtype:
            raise ValueError("dtype must be a non-empty string")

        return _native.AerospikeNativeClient(  # type: ignore[union-attr]
            hosts,
            namespace,
            set_name,
            _positive_int(num_workers, "num_workers"),
            _positive_int(read_timeout_ms, "read_timeout_ms"),
            _positive_int(write_timeout_ms, "write_timeout_ms"),
            _non_negative_int(default_ttl_seconds, "default_ttl_seconds"),
            dtype,
            _non_negative_int(target_segment_bytes, "target_segment_bytes"),
            _non_negative_int(max_record_bytes, "max_record_bytes"),
            username,
            password,
        )


__all__ = ["AerospikeNativeConnector", "NATIVE_AVAILABLE"]
