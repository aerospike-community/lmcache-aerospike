"""Metadata bin encoding and payload assembly helpers."""

from __future__ import annotations

import time
import zlib
from typing import Any

from lmcache.v1.protocol import RemoteMetadata

from lmcache_aerospike.errors import AerospikeInternalError
from lmcache_aerospike.sharding import ShardPlan


def build_remote_metadata(memory_obj) -> RemoteMetadata:
    buf = memory_obj.byte_array
    return RemoteMetadata(
        len(buf),
        memory_obj.get_shapes(),
        memory_obj.get_dtypes(),
        memory_obj.get_memory_format(),
    )


def meta_bins(
    *,
    plan: ShardPlan,
    memory_obj,
    save_chunk_meta: bool,
    enable_crc32: bool,
    default_ttl: int,
    pinned: bool,
) -> dict:
    """Bins for the meta record (payload inline only when nseg == 1)."""
    del default_ttl  # TTL applied via write meta, not bins
    payload = bytes(memory_obj.byte_array)
    bins: dict = {
        "ver": 1,
        "state": "ready",
        "nseg": plan.nseg,
        "seg_b": plan.seg_b,
        "tot_b": len(payload),
        "created_at": int(time.time()),
        "pin": pinned,
    }
    if save_chunk_meta:
        bins["md"] = build_remote_metadata(memory_obj).serialize()
    if plan.nseg == 1:
        bins["b"] = payload
    if enable_crc32:
        bins["crc32"] = zlib.crc32(payload) & 0xFFFFFFFF
    return bins


def allocate_for_read(
    local_cpu_backend,
    base_self,
    meta_bins: dict,
) -> tuple[Any | None, bool]:
    """Allocate a MemoryObj for read; second value is expect_reshape."""
    if base_self.save_chunk_meta and "md" in meta_bins:
        rm = RemoteMetadata.deserialize(meta_bins["md"])
        mo = local_cpu_backend.allocate(rm.shapes, rm.dtypes, rm.fmt)
        return mo, False

    mo = local_cpu_backend.allocate(
        base_self.meta_shapes,
        base_self.meta_dtypes,
        base_self.meta_fmt,
    )
    return mo, True


def write_payload_into(memory_obj, payload: bytes | memoryview, offset: int = 0) -> int:
    """Copy *payload* into *memory_obj* at *offset*; return bytes written."""
    import torch

    n = len(payload)
    end = offset + n
    raw = getattr(memory_obj, "raw_data", None)
    if isinstance(raw, torch.Tensor):
        buf_bytes = raw.view(torch.uint8).flatten()
        if end > buf_bytes.numel():
            raise AerospikeInternalError(
                f"payload length {n} at offset {offset} "
                f"exceeds buffer {buf_bytes.numel()}"
            )
        src = torch.frombuffer(payload, dtype=torch.uint8)
        buf_bytes[offset:end].copy_(src)
        return n

    buf = memory_obj.byte_array
    if end > len(buf):
        raise AerospikeInternalError(
            f"payload length {n} at offset {offset} exceeds buffer {len(buf)}"
        )
    if isinstance(payload, (bytes, bytearray)):
        memoryview(buf)[offset:end] = payload
    else:
        memoryview(buf)[offset:end] = payload
    return n
