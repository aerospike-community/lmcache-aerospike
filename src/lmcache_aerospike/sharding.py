"""Adaptive segment planner (pure function, no I/O)."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from lmcache_aerospike.errors import AerospikeConfigError

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ShardPlan:
    nseg: int
    seg_b: int


def plan(
    payload_bytes: int,
    *,
    target_segment_bytes: int,
    max_segment_bytes: int,
    min_segment_bytes: int,
    single_record_threshold_bytes: int,
) -> ShardPlan:
    """Choose single-record vs multi-segment layout for *payload_bytes*."""
    if payload_bytes == 0:
        return ShardPlan(1, 0)

    if target_segment_bytes > max_segment_bytes:
        raise AerospikeConfigError(
            "target_segment_bytes exceeds max_segment_bytes after server clamping"
        )

    if (
        payload_bytes <= single_record_threshold_bytes
        and payload_bytes <= max_segment_bytes
    ):
        return ShardPlan(1, payload_bytes)

    nseg = -(-payload_bytes // target_segment_bytes)
    seg_b = -(-payload_bytes // nseg)

    if seg_b > max_segment_bytes:
        raise AerospikeConfigError(
            f"segment size {seg_b} exceeds max_segment_bytes {max_segment_bytes}"
        )

    if seg_b < min_segment_bytes and payload_bytes <= max_segment_bytes:
        logger.warning(
            "segment size %s below min_segment_bytes %s; using single-record path",
            seg_b,
            min_segment_bytes,
        )
        return ShardPlan(1, payload_bytes)

    return ShardPlan(nseg, seg_b)


def slice_lengths(payload_bytes: int, shard: ShardPlan) -> list[int]:
    """Per-segment byte lengths (last segment may be shorter)."""
    if shard.nseg == 1:
        return [payload_bytes]
    lengths: list[int] = []
    remaining = payload_bytes
    for i in range(shard.nseg):
        if i == shard.nseg - 1:
            lengths.append(remaining)
        else:
            lengths.append(shard.seg_b)
            remaining -= shard.seg_b
    return lengths
