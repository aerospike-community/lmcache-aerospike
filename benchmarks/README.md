# Benchmarks

Harnesses for **lmcache-aerospike** live under `benchmarks/` and are **not** shipped on PyPI
(only `src/lmcache_aerospike/` is packaged). Deps with VCS URLs live in
`benchmarks/requirements.txt` for the same reason as [adk-aerospike](https://github.com/aerospike-community/adk-aerospike).

| Harness | Runner | Use when |
|---|---|---|
| **Ecosystem** (`run.py`) | [ai-ecosystem-benchmark](https://github.com/aerospike-community/ai-ecosystem-benchmark) | Coordinated-omission-safe QPS/latency on a live Aerospike CE node |
| **L2** (`l2/`) | [`lmcache bench l2`](https://docs.lmcache.ai/cli/bench_l2.html) | Aerospike Python/native L2 vs Redis (`resp`); `compare.sh` runs same profile sequentially |
| **Micro** (`micro/`) | pytest-benchmark + FakeClient | Fast, no-server connector overhead / segment-size sweep |

## Ecosystem harness (recommended)

### Install

```bash
pip install -e .
pip install -r benchmarks/requirements.txt
./scripts/start_aerospike_ce.sh
set -a && source .aerospike-ci.env && set +a
```

### Run

```bash
python benchmarks/run.py --list-profiles
python benchmarks/run.py --list-workloads
python benchmarks/run.py --profile smoke
python benchmarks/run.py --profile kv_chunk_smoke --results-dir benchmarks/results
```

Connection URI format (override with `--uri`):

```text
aerospike://127.0.0.1:3000/lmcache?set=bench_eco_kv&num_tokens=128&target_segment_bytes=4194304
```

### Workloads

| Workload | Tests | Intent |
|---|---|---|
| `kv_hotpath` | `aerospike_kv_put`, `get_hit`, `get_miss`, `exists` | Steady KV remote-cache loop |
| `kv_chunk` | `aerospike_kv_put_large`, `get_large` | ~4 MiB payload (multi-segment) |

### Profiles

| Profile | Workload | Notes |
|---|---|---|
| `smoke` | `kv_hotpath` | Laptop sanity, ~5 s per test |
| `kv_chunk_smoke` | `kv_chunk` | 4 MiB target segment band |

## L2 harness (`lmcache bench l2`)

Benchmarks **Aerospike** (`AerospikeL2Plugin` / native `AerospikeNativeConnector`) and **Redis** (LMCache `resp` / `lmcache_redis`) via the official L2 bench CLI (LMCache `dev`).

```bash
./scripts/setup_l2_bench.sh
./scripts/start_aerospike_ce.sh
./scripts/start_redis_bench.sh
set -a && source .aerospike-ci.env && source .redis-bench.env && set +a
./benchmarks/l2/compare.sh           # same load, Aerospike then Redis (sequential)
./benchmarks/l2/run.sh               # single backend (--backend aerospike-native, resp, …)
```

See [`l2/README.md`](l2/README.md).

## Micro harness (no Aerospike)

Uses `FakeClient` from unit tests — not the ecosystem runner.

```bash
pip install -r benchmarks/requirements.txt
RUN_BENCH=1 pytest benchmarks/micro -v --benchmark-only
```

See [micro/README.md](micro/README.md).

## Data isolation

Workloads default to `bench_eco_*` sets under the `lmcache` namespace. They do not use
`it_chunks` integration-test data. Tear down by deleting the bench sets or the namespace prefix.
