# Micro benchmarks (FakeClient)

In-process pytest-benchmark tests — no Aerospike server.

```bash
pip install -r benchmarks/requirements.txt
RUN_BENCH=1 pytest benchmarks/micro -v --benchmark-only
```

Optional live spot-check (Aerospike CE up):

```bash
RUN_BENCH=1 RUN_BENCH_LIVE=1 pytest benchmarks/micro -v -k live --benchmark-only
```
