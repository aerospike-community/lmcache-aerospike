"""Unit tests for optional Prometheus metrics (S11)."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest

from lmcache_aerospike import metrics


def test_observe_op_without_prometheus(monkeypatch):
    monkeypatch.setitem(sys.modules, "prometheus_client", None)
    # metrics module already imported; force no-op path
    monkeypatch.setattr(metrics, "_PROM", False)
    metrics.observe_op("get", "hit", 0.01)
    metrics.observe_segments(2, 4096)
    metrics.inc_in_flight(1)
    metrics.inc_in_flight(-1)


def test_observe_op_with_prometheus(monkeypatch):
    counter = MagicMock()
    hist = MagicMock()
    seg_count = MagicMock()
    seg_bytes = MagicMock()
    gauge = MagicMock()

    fake_prom = MagicMock()
    fake_prom.Counter.return_value = counter
    fake_prom.Histogram.side_effect = [hist, seg_count, seg_bytes]
    fake_prom.Gauge.return_value = gauge

    monkeypatch.setitem(sys.modules, "prometheus_client", fake_prom)
    monkeypatch.setattr(metrics, "_PROM", True)
    monkeypatch.setattr(metrics, "_OP_TOTAL", counter)
    monkeypatch.setattr(metrics, "_OP_LATENCY", hist)
    monkeypatch.setattr(metrics, "_SEGMENT_COUNT", seg_count)
    monkeypatch.setattr(metrics, "_SEGMENT_BYTES", seg_bytes)
    monkeypatch.setattr(metrics, "_IN_FLIGHT", gauge)

    metrics.observe_op("put", "ok", 0.5)
    counter.labels.assert_called_with(op="put", result="ok")
    counter.labels.return_value.inc.assert_called_once()
    hist.labels.assert_called_with(op="put")
    hist.labels.return_value.observe.assert_called_once_with(0.5)

    metrics.observe_segments(3, 1_000_000)
    seg_count.observe.assert_called_once_with(3)
    seg_bytes.observe.assert_called_once_with(1_000_000)


def test_op_timer_finish_once():
    timer = metrics.OpTimer("get", result="hit")
    timer.finish()
    timer.finish()  # idempotent
