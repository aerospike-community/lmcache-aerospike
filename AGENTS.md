# Agent guide — lmcache-aerospike

Aerospike remote storage backend for [LMCache](https://github.com/LMCache/LMCache). LMCache caches LLM attention KV tensors; this package implements the durable shared tier via LMCache's `RemoteConnector` plugin contract.

**Status:** Phase 1 (`ConnectorAdapter` + `AerospikeRemoteConnector`) and Phase 2 (`AerospikeStoragePlugin`, `AerospikeL2Plugin`) are implemented on `main`. Phase 3 remains design-only in `DESIGN.md`.

## Read order

1. **This file** — workflow, pitfalls, and verification expectations.
2. `[DESIGN.md](DESIGN.md)` — authoritative contract: data model, config, error handling, phases.
3. `[IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md)` — step-by-step build guide (S0–S16).

For Aerospike client/modeling rules, use [aerospike/agent-skills](https://github.com/aerospike/agent-skills) (especially `skills/aerospike-development/`). Do not guess Aerospike APIs or namespace defaults.

## Phases (scope discipline)

| Phase | Surface | In this repo today |
| ----- | ------- | ------------------ |
| 1 | `ConnectorAdapter` + `RemoteConnector` (Python) | **Implemented** |
| 2 | `StoragePluginInterface`, `L2AdapterInterface` | **Implemented** (`storage_plugin.py`, `l2_plugin.py`) |
| 3 | C++ `ConnectorBase` / `libaerospike` | Architectural in `DESIGN.md` only |

Stay inside Phase 1 unless the user explicitly expands scope.

## Package layout

```text
src/lmcache_aerospike/     # published on PyPI
tests/unit/ tests/integration/
benchmarks/                # NOT published — ai-ecosystem-benchmark + micro harness
docker/ scripts/
```

## Verification

| Scope | Command | Notes |
| ----- | ------- | ----- |
| Preflight (S0) | `python scripts/preflight.py` | LMCache + Aerospike client symbols |
| Unit | `pytest tests/unit -q` | No network |
| Integration | `./scripts/start_aerospike_ce.sh` then `pytest tests/integration -q` | Live CE; CI also installs LMCache `dev` and runs `test_l2_plugin_e2e.py` |
| Ecosystem bench | `pip install -r benchmarks/requirements.txt` then `python benchmarks/run.py --profile smoke` | Not in CI by default |
| L2 bench | `./scripts/setup_l2_bench.sh` then `./benchmarks/l2/run.sh` (LMCache `dev` + live CE) | Not in CI by default |
| Micro bench | `RUN_BENCH=1 pytest benchmarks/micro --benchmark-only` | FakeClient only |

Pinned versions: `IMPLEMENTATION_PLAN.md` §0.2.

## Critical upstream facts (do not regress)

- **`post_init()` is never called** on remote connectors. Discovery runs in `_ensure_limits()` during construction.
- **Adapter has a no-arg `__init__`**. Config/metadata from `context.local_cpu_backend` (fallback `context.config` / `context.metadata`).
- **Connector is serde-agnostic.** MLA/layerwise key rewriting happens in `RemoteBackend` above the connector.
- **`save_chunk_meta`:** one `md` bin when true; `meta_*` + `reshape_partial_chunk` when false (mirror `FSConnector`).
- **Batch API:** `batch_read` / `batch_write(BatchRecords(...))` — not `exists_many` / `get_many`.
- **TTL:** `aerospike>=14,<19`, single `_put_meta` helper; `nsup-period > 0` for positive TTL.
- **Default `target_segment_bytes` = 4 MiB** (LMCache byte-throughput); clamp to server cap.

## What not to do

- Do not implement Phase 2/3 features in Phase 1 paths.
- Do not add benchmark-only deps to `pyproject.toml` `[project]` dependencies (use `benchmarks/requirements.txt`).
- Do not create a new `aerospike.Client` per request; use `AerospikeClientHolder`.
- Do not hardcode record-size caps; discover at construction.
- Do not commit unless the user asks.
