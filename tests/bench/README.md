# Connector benchmarks (S15)

Synthetic benchmarks use `FakeClient` (no Aerospike). They measure connector
put/get overhead and segment-size sensitivity, not server-side latency.

```bash
pip install -e ".[dev]"
RUN_BENCH=1 pytest tests/bench -v --benchmark-only
```

Segment sweep prints a one-line summary per `target_segment_bytes` (1–8 MiB).
The shipped default remains **4 MiB** unless a deployment proves a different size
saturates the device (halve to 2 MiB, then 1 MiB; see `DESIGN.md` §4.6).

Optional live Aerospike CE (after `./scripts/start_aerospike_ce.sh`):

```bash
set -a && source .aerospike-ci.env && set +a
RUN_BENCH=1 RUN_BENCH_LIVE=1 pytest tests/bench -v -k live --benchmark-only
```

Prometheus metrics (`pip install -e ".[metrics]"`) are optional; benchmarks do
not require them.
