# L2 adapter benchmark (`lmcache bench l2`)

End-to-end store / lookup / load benchmarks using LMCache’s [`lmcache bench l2`](https://docs.lmcache.ai/cli/bench_l2.html) CLI and the same `parse_args_to_l2_adapters_config` + `create_l2_adapter` path as production MP mode.

| Backend | Adapter | Local service |
| ------- | ------- | ------------- |
| **Aerospike Python** | `AerospikeL2Plugin` (`type: plugin`) | `./scripts/start_aerospike_ce.sh` |
| **Aerospike native** | `AerospikeNativeConnector` (`type: native_plugin`) | `./scripts/start_aerospike_ce.sh` |
| **Redis** | LMCache **RESP** L2 adapter (`type: resp`, C++ `lmcache_redis`) | `./scripts/start_redis_bench.sh` (port **6399**) |

Use **`compare.sh`** to run the **same profile** against Aerospike native and Redis **one after the other** (never in parallel). Redis is `FLUSHALL`’d immediately before its run. Add `--backend aerospike` to compare the older Python L2 path against Redis.

## Requirements

| Piece | Why |
| ----- | --- |
| **LMCache `dev`** | PyPI `lmcache` 0.4.x has no `bench l2` or `L2StoreResult`. |
| **LMCache built with `lmcache_redis`** | RESP/Redis L2 (`pip install -e "$LMCACHE_SRC" --no-build-isolation`). |
| **This package** (`pip install -e .`) | Aerospike `type: plugin` adapter. |
| **This package built with native deps** | Aerospike `type: native_plugin` adapter (`./scripts/build_libaerospike.sh` then `pip install -e .`). |
| **torch + openai** | Bench CLI payloads (`benchmarks/l2/requirements.txt`). |
| **Docker** | Aerospike CE + Redis bench containers. |

Native build (no sudo; downloads Aerospike C client + `libyaml` debs into `.deps/`):

```bash
./scripts/build_libaerospike.sh
source .deps/aerospike-client-c.env
LMCACHE_AEROSPIKE_FORCE_NATIVE=1 pip install -e . --no-build-isolation
```

`setup_l2_bench.sh` runs the steps above by default. Set `LMCACHE_AEROSPIKE_SKIP_NATIVE_DEPS=1`
to skip when you already have system `libaerospike-dev`. For non-standard installs, export
`AEROSPIKE_INCLUDE_DIR` and `AEROSPIKE_LIBRARY_DIR` before `pip install -e .`.

## One-time setup

```bash
git clone https://github.com/LMCache/LMCache.git ../LMCache
cd ../LMCache && git checkout dev

cd /path/to/lmcache-aerospike
./scripts/setup_l2_bench.sh
```

Preflight only:

```bash
python3 scripts/preflight_l2_bench.py --resp
```

## Running (when you are ready)

Start both backends and load env (dynamic Aerospike port is in `.aerospike-ci.env`):

```bash
./scripts/start_aerospike_ce.sh
./scripts/start_redis_bench.sh
set -a && source .aerospike-ci.env && source .redis-bench.env && set +a
```

### Compare Aerospike native vs Redis (same load, sequential)

```bash
./benchmarks/l2/compare.sh
./benchmarks/l2/compare.sh --profile stress
./benchmarks/l2/compare.sh --profile extended
./benchmarks/l2/compare.sh --backend aerospike  # Python L2 plugin vs Redis
./benchmarks/l2/summarize_results.sh benchmarks/l2/results/<timestamp>-stress
```

Logs land under `benchmarks/l2/results/<timestamp>-<profile>/` (`aerospike-native.log` or `aerospike.log`, plus `resp.log`). Each file contains the full `lmcache bench l2` summary blocks for store / lookup / load.

### Single backend

```bash
./benchmarks/l2/run.sh --backend aerospike
./benchmarks/l2/run.sh --backend aerospike-native
./benchmarks/l2/run.sh --backend resp
./benchmarks/l2/run.sh --backend aerospike --profile stress --only store
```

`--backend resp` accepts `redis` as an alias. `--backend aerospike-native` accepts `native` and `as-native` as aliases.

## Equivalent load

All backends use the **same** `profiles/<name>.env` → `BENCH_L2_EXTRA_ARGS` (`--num-keys`, `--in-flight`, `--data-size-kb`, `--rounds`, `--warmup-rounds`). Only the adapter JSON and backing service differ.

**Keys per round** = `--in-flight` × `--num-keys` (each in-flight slot submits a full batch of `num-keys`).

| Profile | Defaults |
| ------- | -------- |
| `smoke` | 32 keys/submit, in-flight 1, 256 KiB/key, 1 warmup + 1 measured round, **lookup hit rate 1.0** (same keys as store) |
| `stress` | 32 keys, in-flight 4, 512 KiB/key, 5 measured rounds |
| `extended` | Same batch as stress (128 keys/round), 10 measured rounds, 2 warmup |
| `miss_lookup` | Upstream default: `--lookup-max-hit-rate 0.0` (lookup only touches keys that were never stored) |

LMCache’s bench default is `lookup-max-hit-rate 0.0`: lookup keys start at index `total_run_keys`, disjoint from store/load. **`found=0/32` after a successful store is expected** with that default, not an adapter bug.

## Layout

| Path | Purpose |
| ---- | ------- |
| `compare.sh` | Aerospike native or Python run, then Redis run (with `FLUSHALL`), same profile |
| `run.sh` | Single-backend runner (`--backend aerospike\|aerospike-native\|resp`) |
| `adapters/` | JSON templates (`aerospike_*.json`, `aerospike_native_*.json`, `resp_*.json`) |
| `render_adapter.py` | Inject host/port (and Aerospike namespace/set) |
| `profiles/*.env` | Shared CLI flag bundles |
| `bootstrap.py` | `native_storage_ops` fallback + `init_remote_metadata_info(1)` |
| `results/` | Compare run logs (gitignored) |

Aerospike Python bench set: **`kv_chunks_bench_l2`**. Aerospike native bench set: **`kv_chunks_bench_l2_native`**. Redis uses DB 0 on **`127.0.0.1:6399`** by default (avoids a local `redis-server` on 6379).

## Notes

- **Schema compatibility first:** Aerospike native uses the same Phase 1/2 sharded KV layout by default, while Redis uses LMCache’s native raw RESP connector. The harness compares **the same bench driver and payload size** on two local tiers.
- **Cold cache:** For filesystem-like adapters, run `--only` per op and drop OS caches (see [bench_l2](https://docs.lmcache.ai/cli/bench_l2.html)). Aerospike/Redis are remote services; `compare.sh` only flushes Redis between backends.
- **Round-trip verify:** `./benchmarks/l2/compare.sh -- --no-skip-verify` (requires store + load in the same run).
