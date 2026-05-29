# L2 adapter benchmark (`lmcache bench l2`)

End-to-end store / lookup / load benchmarks for [`AerospikeL2Plugin`](../../src/lmcache_aerospike/l2_plugin.py) using the same [`parse_args_to_l2_adapters_config`](https://docs.lmcache.ai/mp/configuration.html) + `create_l2_adapter` path as production MP mode.

Upstream docs: [lmcache bench l2](https://docs.lmcache.ai/cli/bench_l2.html).

## Requirements

| Piece | Why |
| ----- | --- |
| **LMCache `dev`** | PyPI `lmcache` 0.4.x does not ship `lmcache bench l2` or `L2StoreResult`. |
| **This package** (`pip install -e .`) | Provides `type: plugin` → `AerospikeL2Plugin`. |
| **torch + openai** | Bench CLI and `TensorMemoryObj` payloads (see `benchmarks/l2/requirements.txt`). |
| **Aerospike CE** (for real runs) | `./scripts/start_aerospike_ce.sh` → `.aerospike-ci.env`. |

`native_storage_ops` is optional for bench: `benchmarks/l2/bootstrap.py` installs the same Python fallback used in integration tests when the extension module is missing.

## One-time setup

```bash
# LMCache dev clone (default: ../LMCache next to this repo)
git clone https://github.com/LMCache/LMCache.git ../LMCache
cd ../LMCache && git checkout dev

cd /path/to/lmcache-aerospike
./scripts/setup_l2_bench.sh
```

Override the LMCache path:

```bash
LMCACHE_SRC=/path/to/LMCache ./scripts/setup_l2_bench.sh
```

Preflight only:

```bash
python3 scripts/preflight_l2_bench.py
```

## Running (when you are ready)

Start Aerospike and load connection env (dynamic port is written to `.aerospike-ci.env`):

```bash
./scripts/start_aerospike_ce.sh
set -a && source .aerospike-ci.env && set +a
```

Smoke (store → lookup → load, defaults from [bench_l2 docs](https://docs.lmcache.ai/cli/bench_l2.html)):

```bash
./benchmarks/l2/run.sh
```

Stress profile and single operation:

```bash
./benchmarks/l2/run.sh --profile stress
./benchmarks/l2/run.sh --only store
./benchmarks/l2/run.sh -- --no-skip-verify   # round-trip byte check on last round
```

Direct CLI (equivalent; `run.sh` sets `L2_ADAPTER_JSON` from `adapters/` + `.aerospike-ci.env`):

```bash
python3 benchmarks/l2/bootstrap.py
export L2_ADAPTER_JSON="$(cat benchmarks/l2/adapters/aerospike_smoke.json)"
lmcache bench l2 --l2-adapter "$L2_ADAPTER_JSON" --num-keys 32 --in-flight 1
```

## Layout

| Path | Purpose |
| ---- | ------- |
| `adapters/*.json` | Plugin specs (`type: plugin`, `AerospikeL2Plugin`). Hosts/port overridden by `run.sh`. |
| `profiles/*.env` | Default CLI flag bundles (`BENCH_L2_EXTRA_ARGS`). |
| `bootstrap.py` | `native_storage_ops` fallback + `init_remote_metadata_info(1)`. |
| `run.sh` | Sources Aerospike env, renders adapter JSON, runs `lmcache bench l2`. |
| `env.example` | Optional `benchmarks/l2/.env.local` knobs. |

Bench records use set **`kv_chunks_bench_l2`** by default (not `it_chunks` or `kv_chunks_l2_it`).

## Cold-cache / isolated operations

For adapters backed by the OS page cache, run operations separately and drop caches between runs (see upstream note in [bench_l2](https://docs.lmcache.ai/cli/bench_l2.html)):

```bash
./benchmarks/l2/run.sh --only store
sync && echo 3 | sudo tee /proc/sys/vm/drop_caches
./benchmarks/l2/run.sh --only lookup
```

Aerospike has no local page cache; the default combined `run.sh` pass is usually representative.
