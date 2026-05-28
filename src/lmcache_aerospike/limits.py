"""Server record-size discovery and resolved segment limits."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from aerospike import exception as ax

from lmcache_aerospike.config import AerospikeConfig
from lmcache_aerospike.errors import (
    AerospikeNamespaceProbeError,
    AerospikeServerLimitError,
    AerospikeTTLConfigError,
)

logger = logging.getLogger(__name__)

SAFETY_MARGIN_BYTES = 65536
_MIN_RECORD_BYTES = 131072
_MAX_RECORD_BYTES = 8388608


@dataclass(frozen=True, slots=True)
class ServerLimits:
    server_max_record_bytes: int
    source: str
    nsup_period: int


@dataclass(frozen=True, slots=True)
class ResolvedLimits:
    server_max_record_bytes: int
    effective_max_segment_bytes: int
    max_segment_bytes: int
    target_segment_bytes: int
    single_record_threshold_bytes: int
    min_segment_bytes: int


def parse_namespace_info(response: str) -> dict[str, str]:
    """Parse `info_random_node('namespace/...')` key=value; response."""
    text = response.strip()
    if "\t" in text:
        text = text.split("\t", 1)[1]
    pairs: dict[str, str] = {}
    for part in text.split(";"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        pairs[key.strip()] = value.strip()
    return pairs


def discover_limits(client, namespace: str) -> ServerLimits:
    """Query namespace config for record-size cap and nsup-period."""
    try:
        response = client.info_random_node(f"namespace/{namespace}")
    except ax.AerospikeError as exc:
        raise AerospikeNamespaceProbeError(
            f"info namespace/{namespace} failed: {exc}"
        ) from exc

    if not response:
        raise AerospikeNamespaceProbeError(
            f"empty info response for namespace/{namespace}"
        )

    cfg = parse_namespace_info(response)
    cap: int | None = None
    source = ""
    if "max-record-size" in cfg:
        try:
            value = int(cfg["max-record-size"])
        except ValueError as exc:
            raise AerospikeServerLimitError(
                f"invalid max-record-size: {cfg['max-record-size']!r}"
            ) from exc
        if value > 0:
            cap = value
            source = "max-record-size"
    if cap is None and "write-block-size" in cfg:
        try:
            value = int(cfg["write-block-size"])
        except ValueError as exc:
            raise AerospikeServerLimitError(
                f"invalid write-block-size: {cfg['write-block-size']!r}"
            ) from exc
        if value > 0:
            cap = value
            source = "write-block-size"

    if cap is None:
        raise AerospikeServerLimitError(
            "namespace info missing max-record-size and write-block-size"
        )
    if cap < _MIN_RECORD_BYTES or cap > _MAX_RECORD_BYTES:
        raise AerospikeServerLimitError(
            f"record size cap {cap} outside [{_MIN_RECORD_BYTES}, {_MAX_RECORD_BYTES}]"
        )

    try:
        nsup_period = int(cfg.get("nsup-period", "0"))
    except ValueError as exc:
        raise AerospikeServerLimitError(
            f"invalid nsup-period: {cfg.get('nsup-period')!r}"
        ) from exc

    return ServerLimits(
        server_max_record_bytes=cap,
        source=source,
        nsup_period=nsup_period,
    )


def resolve_segment_limits(
    cfg: AerospikeConfig, server: ServerLimits
) -> ResolvedLimits:
    """Derive clamped segment limits from config and server discovery."""
    if cfg.default_ttl_seconds > 0 and server.nsup_period <= 0:
        raise AerospikeTTLConfigError(
            f"namespace {cfg.namespace!r} has nsup-period=0 but "
            f"default_ttl_seconds={cfg.default_ttl_seconds}; set nsup-period > 0"
        )

    effective_max = server.server_max_record_bytes - SAFETY_MARGIN_BYTES
    max_segment = min(cfg.max_segment_bytes or effective_max, effective_max)
    if cfg.max_segment_bytes and cfg.max_segment_bytes > effective_max:
        logger.warning(
            "max_segment_bytes %s clamped to effective max %s",
            cfg.max_segment_bytes,
            effective_max,
        )

    target = min(cfg.target_segment_bytes, effective_max)
    if cfg.target_segment_bytes > effective_max:
        logger.warning(
            "target_segment_bytes %s clamped to effective max %s",
            cfg.target_segment_bytes,
            effective_max,
        )

    single_threshold = min(
        cfg.single_record_threshold_bytes or target,
        effective_max,
    )

    resolved = ResolvedLimits(
        server_max_record_bytes=server.server_max_record_bytes,
        effective_max_segment_bytes=effective_max,
        max_segment_bytes=max_segment,
        target_segment_bytes=target,
        single_record_threshold_bytes=single_threshold,
        min_segment_bytes=cfg.min_segment_bytes,
    )

    logger.info(
        "Aerospike limits: server_max=%s source=%s effective_max_segment=%s "
        "max_segment=%s target_segment=%s single_threshold=%s min_segment=%s",
        server.server_max_record_bytes,
        server.source,
        effective_max,
        max_segment,
        target,
        single_threshold,
        cfg.min_segment_bytes,
    )
    return resolved
