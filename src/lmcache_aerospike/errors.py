"""Typed connector exceptions and Aerospike error mapping."""

from __future__ import annotations

from aerospike import exception as ax

AEROSPIKE_ERR_FAIL_FORBIDDEN = 22
AEROSPIKE_ERR_KEY_BUSY = 14


class AerospikeConnectorError(Exception):
    """Base for all connector errors."""


class AerospikeConfigError(AerospikeConnectorError):
    """Invalid extra_config or operator override."""


class AerospikeConnectionError(AerospikeConnectorError):
    """Cluster unreachable or no nodes available."""


class AerospikeRecordTooBigError(AerospikeConnectorError):
    """Payload exceeds server or configured segment limits."""


class AerospikeTTLConfigError(AerospikeConnectorError):
    """TTL write rejected (typically nsup-period is 0)."""


class AerospikeBusyError(AerospikeConnectorError):
    """Transient overload (KEY_BUSY, device overload, queue full)."""


class AerospikeNamespaceProbeError(AerospikeConnectorError):
    """info probe failed at startup."""


class AerospikeServerLimitError(AerospikeConnectorError):
    """Parsed server record-size cap missing or invalid."""


class AerospikeInternalError(AerospikeConnectorError):
    """Invariant violation (e.g. key mismatch)."""


class AerospikeUnknownError(AerospikeConnectorError):
    """Unclassified Aerospike error."""


def classify(exc: BaseException) -> str:
    """Map an Aerospike exception to a behavior bucket."""
    code = getattr(exc, "code", None)

    if isinstance(exc, ax.RecordNotFound):
        return "not_found"
    if isinstance(exc, ax.RecordTooBig):
        return "too_big"
    if isinstance(exc, ax.TimeoutError):
        return "timeout"
    if isinstance(exc, (ax.ConnectionError, ax.ClientError)):
        return "connection"
    if code == AEROSPIKE_ERR_FAIL_FORBIDDEN or isinstance(exc, ax.ForbiddenError):
        return "forbidden_ttl"
    if isinstance(
        exc,
        (ax.DeviceOverload, ax.BatchQueueFullError, ax.RecordBusy),
    ) or code == AEROSPIKE_ERR_KEY_BUSY:
        return "busy"
    if isinstance(exc, ax.RecordKeyMismatch):
        return "key_mismatch"
    if isinstance(exc, ax.AerospikeError):
        return "unknown"
    return "unknown"


def map_aerospike_error(op_name: str, exc: BaseException) -> AerospikeConnectorError:
    """Convert an Aerospike exception to a typed connector error."""
    bucket = classify(exc)
    code = getattr(exc, "code", None)
    msg = getattr(exc, "msg", str(exc))
    detail = f"{op_name}: aerospike code={code} msg={msg!r}"

    if bucket == "not_found":
        return AerospikeConnectorError(detail)
    if bucket == "too_big":
        return AerospikeRecordTooBigError(detail)
    if bucket == "timeout":
        return AerospikeConnectorError(detail)
    if bucket == "connection":
        return AerospikeConnectionError(detail)
    if bucket == "forbidden_ttl":
        return AerospikeTTLConfigError(
            f"{detail}; ensure namespace nsup-period > 0 when using positive TTL"
        )
    if bucket == "busy":
        return AerospikeBusyError(detail)
    if bucket == "key_mismatch":
        return AerospikeInternalError(detail)
    if isinstance(exc, ax.AerospikeError):
        return AerospikeUnknownError(detail)
    return AerospikeUnknownError(detail)
