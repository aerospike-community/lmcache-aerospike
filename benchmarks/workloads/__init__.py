"""LMCache Aerospike workloads for ai-ecosystem-benchmark."""

from __future__ import annotations

from typing import Any, Type

from ai_ecosystem_benchmark import BaseBenchmarkWorkload

from .kv_chunk import KvChunkWorkload
from .kv_hotpath import KvHotpathWorkload

WORKLOAD_TYPES: dict[str, Type[BaseBenchmarkWorkload]] = {
    "kv_hotpath": KvHotpathWorkload,
    "kv_chunk": KvChunkWorkload,
}


def build_workload(
    name: str,
    connection_string: str,
    params: dict[str, Any],
) -> BaseBenchmarkWorkload:
    try:
        cls = WORKLOAD_TYPES[name]
    except KeyError as exc:
        known = ", ".join(sorted(WORKLOAD_TYPES))
        raise ValueError(f"unknown workload {name!r}; choose from: {known}") from exc
    return cls(aerospike_connection_string=connection_string, **params)
