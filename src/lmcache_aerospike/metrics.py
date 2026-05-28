"""Optional Prometheus metrics hooks (S11). No hard dependency on prometheus_client."""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Iterator

try:
    from prometheus_client import Counter, Gauge, Histogram

    _PROM = True
except ImportError:
    _PROM = False

if _PROM:
    _OP_TOTAL = Counter(
        "aerospike_op_total",
        "Aerospike connector operations",
        ["op", "result"],
    )
    _OP_LATENCY = Histogram(
        "aerospike_op_latency_seconds",
        "Aerospike connector operation latency",
        ["op"],
        buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    )
    _SEGMENT_COUNT = Histogram(
        "aerospike_segment_count",
        "Segment count per put",
        buckets=(1, 2, 3, 4, 8, 16, 32, 64, 128),
    )
    _SEGMENT_BYTES = Histogram(
        "aerospike_segment_bytes",
        "Payload bytes per put",
        buckets=(
            256,
            1024,
            4096,
            16384,
            65536,
            262144,
            1048576,
            4194304,
            16777216,
            67108864,
        ),
    )
    _IN_FLIGHT = Gauge(
        "aerospike_concurrent_in_flight",
        "Concurrent batched connector operations",
    )


def observe_op(op: str, result: str, duration_s: float) -> None:
    try:
        if _PROM:
            _OP_TOTAL.labels(op=op, result=result).inc()
            _OP_LATENCY.labels(op=op).observe(duration_s)
    except Exception:
        pass


def observe_segments(nseg: int, payload_bytes: int) -> None:
    try:
        if _PROM:
            _SEGMENT_COUNT.observe(nseg)
            _SEGMENT_BYTES.observe(payload_bytes)
    except Exception:
        pass


def inc_in_flight(delta: int) -> None:
    try:
        if _PROM:
            if delta > 0:
                _IN_FLIGHT.inc(delta)
            else:
                _IN_FLIGHT.dec(-delta)
    except Exception:
        pass


@contextmanager
def track_in_flight() -> Iterator[None]:
    inc_in_flight(1)
    try:
        yield
    finally:
        inc_in_flight(-1)


class OpTimer:
    """Time one connector op and emit metrics (no-op without prometheus_client)."""

    __slots__ = ("op", "result", "_start", "_finished")

    def __init__(self, op: str, *, result: str = "ok") -> None:
        self.op = op
        self.result = result
        self._start = time.perf_counter()
        self._finished = False

    def finish(self) -> None:
        if self._finished:
            return
        self._finished = True
        observe_op(self.op, self.result, time.perf_counter() - self._start)

    def map_exception(self, exc: BaseException) -> None:
        from lmcache_aerospike.errors import classify

        bucket = classify(exc)
        self.result = {
            "timeout": "timeout",
            "too_big": "record_too_big",
            "busy": "busy",
            "connection": "error",
            "not_found": "miss",
            "forbidden_ttl": "error",
            "key_mismatch": "error",
            "unknown": "error",
        }.get(bucket, "error")
