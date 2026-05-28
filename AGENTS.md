# Agent guide — lmcache-aerospike

Aerospike remote storage backend for [LMCache](https://github.com/LMCache/LMCache). LMCache caches LLM attention KV tensors; this package implements the durable shared tier via LMCache's `RemoteConnector` plugin contract.

**Status:** Pre-implementation (design + executable plan only; no `src/` yet). Do not invent APIs or skip the plan's verification gates.

## Read order

1. **This file** — workflow, pitfalls, and verification expectations.
2. `[DESIGN.md](DESIGN.md)` — authoritative contract: data model, config, error handling, phases.
3. `[IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md)` — step-by-step build guide (S0–S16). **When implementing, the plan wins over stale design text** until S16 reconciles `DESIGN.md`.

For Aerospike client/modeling rules, use the vetted skills in [aerospike/agent-skills](https://github.com/aerospike/agent-skills) (especially `[skills/aerospike-development/](https://github.com/aerospike/agent-skills/tree/main/skills/aerospike-development)`). Do not guess Aerospike APIs or namespace defaults.

## Phases (scope discipline)


| Phase | Surface                                         | In this repo today                |
| ----- | ----------------------------------------------- | --------------------------------- |
| 1     | `ConnectorAdapter` + `RemoteConnector` (Python) | **Only phase to implement now**   |
| 2     | `StoragePluginInterface`, `L2AdapterInterface`  | Architectural in `DESIGN.md` only |
| 3     | C++ `ConnectorBase` / `libaerospike`            | Architectural in `DESIGN.md` only |


Stay inside Phase 1 unless the user explicitly expands scope.

## Implementation workflow

1. Start at **S0** in `IMPLEMENTATION_PLAN.md` and proceed **in order** (S0 → S16).
2. **Stop if a step's verify gate fails** — fix before continuing; do not batch steps.
3. Mark **⚠ DESIGN-CORRECTION** items as non-negotiable (they were verified against upstream LMCache `dev` and the real Aerospike Python client).
4. After S16, reconcile `DESIGN.md` with the implementation (grep for `exists_many`, `post_init`, `shape0`, stale caps).

### Critical upstream facts (do not regress)

- `**post_init()` is never called** on remote connectors. Server record-size discovery runs in the connector constructor (`_ensure_limits`), not in `post_init`.
- **Adapter has a no-arg `__init__`**. LMCache instantiates `AerospikeConnectorAdapter()` then calls `create_connector(context)`.
- **Config/metadata** come from `context.local_cpu_backend` (`.config`, `.metadata`), with fallback to `context.config` / `context.metadata`.
- **Connector is serde-agnostic.** `naive` / `cachegen` / `kivi` and MLA/layerwise key rewriting happen in `RemoteBackend` above the connector.
- `**save_chunk_meta`:** when true, store one `md` bin (`RemoteMetadata.serialize()`); when false, use `self.meta_`* + `reshape_partial_chunk` on read (mirror `FSConnector`).
- **Batch API:** `batch_read(keys, bins=...)` and `batch_write(BatchRecords([Write|Read|Remove, ...]))`. Do **not** use removed `exists_many` / `get_many` / `select_many`.
- **TTL:** pin `aerospike>=14,<19` and centralize TTL in one helper (`meta={"ttl": N}`); namespace must have `nsup-period > 0` when using positive TTL.
- **Default `target_segment_bytes` = 4 MiB** (LMCache byte-throughput sweet spot); clamp to server-discovered cap, not Aerospike's 1–10 KiB ops sweet spot.

### Package layout (target)

```text
src/lmcache_aerospike/
  adapter.py connector.py client.py config.py keys.py
  sharding.py limits.py serde.py policies.py errors.py metrics.py
tests/unit/ tests/integration/ tests/bench/
docker/docker-compose.yml docker/aerospike.conf
```

## Verification (once code exists)


| Scope          | Command                                         | Notes                                                        |
| -------------- | ----------------------------------------------- | ------------------------------------------------------------ |
| Preflight (S0) | `python scripts/preflight.py`                   | Confirms LMCache + Aerospike client symbols                  |
| Unit           | `pytest tests/unit -q`                          | No network; mock `aerospike.Client`                          |
| Integration    | `RUN_INTEGRATION=1 pytest tests/integration -q` | Requires `docker compose -f docker/docker-compose.yml up -d` |
| Bench / vLLM   | per S15                                         | Optional; gated by env vars                                  |


Pinned versions are in `IMPLEMENTATION_PLAN.md` §0.2 — do not change without re-running S0.

## Aerospike skills (external)

Before changing client usage, policies, TTL, batching, or record sizing, read the relevant reference from [agent-skills](https://github.com/aerospike/agent-skills/tree/main/skills/aerospike-development/references/). `DESIGN.md` §8.3 lists the ones this design depends on.

For local Docker CE setup in integration tests, see [aerospike-getting-started/SKILL.md](https://github.com/aerospike/agent-skills/blob/main/skills/aerospike-getting-started/SKILL.md).

## LMCache upstream (re-verify when unsure)

- `[RemoteConnector](https://github.com/LMCache/LMCache/blob/dev/lmcache/v1/storage_backend/connector/base_connector.py)`
- `[ConnectorAdapter` / `ConnectorContext](https://github.com/LMCache/LMCache/blob/dev/lmcache/v1/storage_backend/connector/__init__.py)`
- `[RemoteBackend](https://github.com/LMCache/LMCache/blob/dev/lmcache/v1/storage_backend/remote_backend.py)` — serde, `init_connection`, no `post_init`
- `[FSConnector](https://github.com/LMCache/LMCache/blob/dev/lmcache/v1/storage_backend/connector/fs_connector.py)` — `save_chunk_meta` pattern
- `[redis_connector.py](https://github.com/LMCache/LMCache/blob/dev/lmcache/v1/storage_backend/connector/redis_connector.py)` — `batched_contains` consecutive-prefix semantics

## What not to do

- Do not implement Phase 2/3 features (pin/unpin L2, native C++, controller metadata, EE-only APIs) in Phase 1 code paths.
- Do not create a new `aerospike.Client` per request; use `AerospikeClientHolder` ref-counting.
- Do not use CDTs or secondary indexes for chunk payload storage.
- Do not hardcode 8 MiB record caps; discover `max-record-size` / `write-block-size` at construction.
- Do not commit unless the user asks.

## Commits and PRs

Follow repository commit style from `git log`. Summarize *why* in commit messages. For PRs, include unit-test results and note whether integration tests were run.
