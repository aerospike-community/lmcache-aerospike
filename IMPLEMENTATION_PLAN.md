# LMCache Aerospike Backend — Executable Implementation Plan (Phase 1)

**Companion to:** [`DESIGN.md`](DESIGN.md).
**Audience:** An implementer (human or AI) who will build the Phase 1 Python
`RemoteConnector` plugin step by step. This document is intentionally
prescriptive: do exactly what each step says, in order, and run the
verification gate at the end of each step before moving on.

**Golden rule:** If a step's verification gate fails, STOP and fix it before
continuing. Do not skip gates. Do not "batch" steps.

---

## 0. How to read this plan

- Steps are numbered `S0`, `S1`, … and **must be done in order**.
- Every step has three parts:
  - **Goal** — one sentence on what this step produces.
  - **Do** — the exact actions / file contents.
  - **Verify (gate)** — a command or check that must pass before the next step.
- Code blocks that are *new files to create* are shown as normal fenced blocks
  with a `# file: <path>` header comment on the first line. Create the file at
  that path with that content (minus the header comment if it is not valid in
  that language — for Python keep it, for `toml`/`yaml` keep it).
- Where this plan **deviates from `DESIGN.md`**, the deviation is marked
  **⚠ DESIGN-CORRECTION** with the reason. The plan is the source of truth for
  implementation; `DESIGN.md` should later be updated to match (tracked in
  Step S16).

---

## 0.1 Critical corrections to DESIGN.md (read before writing any code)

These were found by checking the design against the **actual upstream LMCache
`dev` source** and the **real Aerospike Python client API**. They are baked
into the steps below; this list is so you understand *why*.

1. **⚠ `post_init()` is never called for a remote connector.** Upstream
   `RemoteBackend.init_connection()` calls `CreateConnector()` and stores the
   result; nothing calls `connector.post_init()`. Therefore server-side
   record-size discovery MUST happen during connector construction (lazily,
   guarded), NOT in `post_init`. We still keep a `post_init` override for
   forward-compat, but we never rely on it being called.

2. **⚠ `batch_write` takes a `BatchRecords` object**, not a list of
   `(key, bins, meta)` tuples. Use
   `aerospike_helpers.batch.records.{BatchRecords, Write, Read, Remove}` and
   `aerospike_helpers.operations.operations as op`.

3. **⚠ `exists_many` / `get_many` / `select_many` are removed** in current
   Aerospike Python clients. Use `client.batch_read(keys, bins=[...])` for
   reads and `client.batch_read(keys, bins=[])` (empty bin list = metadata
   only) for existence checks.

4. **⚠ `meta={"ttl": N}` is deprecated** as of client v19.1.0. Pin a specific
   client version (Step S1) and set TTL through ONE helper so the API is in
   exactly one place.

5. **⚠ The connector is serde-agnostic.** `remote_serde`
   (`naive`/`cachegen`/`kivi`) is applied by `RemoteBackend` *above* the
   connector. The connector stores opaque bytes and reconstructs a `MemoryObj`
   that the deserializer can consume. Metadata handling MUST honor
   `self.save_chunk_meta` exactly like upstream `FSConnector`:
   - if `save_chunk_meta` is `True`: store a serialized `RemoteMetadata` blob
     and, on read, allocate from it (do NOT call `reshape_partial_chunk`);
   - if `save_chunk_meta` is `False`: store no metadata, allocate from
     `self.meta_shapes/meta_dtypes/meta_fmt`, and call
     `reshape_partial_chunk(memory_obj, bytes_read)` on read.

6. **⚠ Construction shape.** The adapter is instantiated by LMCache with **no
   arguments** (`AerospikeConnectorAdapter()`), then `create_connector(context)`
   is called. Read `config` and `metadata` from `context.local_cpu_backend`
   (`.config`, `.metadata`) — the canonical upstream pattern — falling back to
   `context.config`/`context.metadata`. The URL LMCache passes for a plugin is
   `plugin://{plugin_name}`.

## 0.2 Pinned versions (do not change without re-running S0 verification)

| Thing | Pin | Why |
|---|---|---|
| Python | `>=3.10,<3.14` | Tracks LMCache supported range |
| `aerospike` (client) | `>=14.0.0,<19.0.0` | Modern batch API present; pre-19 so `meta={"ttl"}` still valid. If you must use `>=19`, change ONLY the `_ttl_meta`/`_write_policy` helper (Step S8) per that version's docs |
| `lmcache` | pin a single known-good release, e.g. `>=0.4.5,<0.5` | Peer dependency; the connector contract is from the `dev`/0.4.x line |
| Aerospike server (tests) | CE `7.x` or `8.x` Docker image `aerospike/aerospike-server` | Matches design |

> If any pin is unavailable at implementation time, STOP and ask. Do not invent
> a version.

## 0.3 Glossary of upstream symbols you will use (verified against `dev`)

- `RemoteConnector` — abstract base, `lmcache.v1.storage_backend.connector.base_connector`.
  Abstract methods: `exists`, `exists_sync`, `get`, `put`, `list`, `close`.
  Non-abstract (override to enable): `ping`/`support_ping`,
  `batched_get`/`support_batched_get`, `batched_put`/`support_batched_put`,
  `batched_contains`/`support_batched_contains`, `remove_sync`.
  Defaults already `True` with base impls: `support_batched_async_contains`,
  `support_batched_get_non_blocking`.
- `ConnectorAdapter` — abstract base, `…connector.__init__`. `__init__(self, schema="")`,
  `can_parse(self, url)->bool`, abstract `create_connector(self, context)->RemoteConnector`.
- `ConnectorContext` — has `.url`, `.loop`, `.local_cpu_backend`, `.config`,
  `.metadata`, `.plugin_name`. `local_cpu_backend` may be a `SafeLocalCPUBackend`
  stub (scheduler role) — but for `RemoteBackend` it is always real.
- `extract_plugin_type(plugin_name)` — `"aerospike.primary" -> "aerospike"`.
- `RemoteMetadata` — `lmcache.v1.protocol`. Constructor:
  `RemoteMetadata(length:int, shapes:list[torch.Size], dtypes:list[torch.dtype], fmt:MemoryFormat)`.
  Methods: `.serialize()->bytes`, `.deserialize(buf)->RemoteMetadata` (static),
  `.serialize_into(buffer)`.
  Header size is fixed per process: `get_remote_metadata_bytes()` (the base
  class stores it on `self.remote_metadata_bytes`).
- `MemoryObj` — `lmcache.v1.memory_management`. You will use: `.byte_array`
  (a `memoryview`/buffer), `.get_size()`, `.get_shapes()`, `.get_dtypes()`,
  `.get_memory_format()`, `.ref_count_down()`, `.raw_data`, `.meta.shape`,
  `.meta.dtype`. Allocation: `local_cpu_backend.allocate(shapes, dtypes, fmt)`
  returns `MemoryObj` or `None`.
- `CacheEngineKey` / `LayerCacheEngineKey` — `lmcache.utils`. Use `.to_string()`
  (stable string) and `.chunk_hash`. Layerwise keys differ only in their string;
  no special code path is needed beyond using `to_string()` everywhere.
- Base-class attributes available after `super().__init__(config, metadata)`:
  `self.save_chunk_meta` (bool), `self.meta_shapes`, `self.meta_dtypes`,
  `self.meta_fmt`, `self.full_chunk_size_bytes`, `self.single_token_size`,
  `self.remote_metadata_bytes`.

---

## S0 — Preflight: prove the toolchain and the two external contracts

**Goal:** Before writing the package, confirm the LMCache contract symbols and
the Aerospike client API actually exist as this plan assumes, on the pinned
versions. This prevents building on wrong assumptions.

**Do:**

1. Create and activate a fresh virtualenv (Python in the pinned range).
2. Install the pinned `lmcache` and `aerospike` versions plus dev tooling:
   `pip install "aerospike>=14,<19" "lmcache>=0.4.5,<0.5" pytest pytest-asyncio`.
   (CPU-only `torch` is pulled in by `lmcache`.)
3. Run this probe script and read its output. Save it as `scripts/preflight.py`
   (temporary; it is not shipped):

```python
# file: scripts/preflight.py
import inspect
import aerospike
from aerospike_helpers.batch import records as br
from aerospike_helpers.operations import operations as op
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector
from lmcache.v1.storage_backend.connector import (
    ConnectorAdapter, ConnectorContext, extract_plugin_type,
)
from lmcache.v1.protocol import (
    RemoteMetadata, get_remote_metadata_bytes, init_remote_metadata_info,
)

# 1) RemoteConnector abstract methods present
need = {"exists", "exists_sync", "get", "put", "list", "close"}
have = set(RemoteConnector.__abstractmethods__)
assert need <= have, f"missing abstract methods: {need - have}"

# 2) support_* defaults
assert RemoteConnector.support_batched_async_contains(RemoteConnector) is True
assert RemoteConnector.support_batched_get_non_blocking(RemoteConnector) is True
assert RemoteConnector.support_batched_contains(RemoteConnector) is False

# 3) ConnectorAdapter shape
assert "create_connector" in ConnectorAdapter.__abstractmethods__
assert extract_plugin_type("aerospike.primary") == "aerospike"

# 4) Aerospike client method names this plan uses
for name in ("put", "get", "select", "exists", "remove",
             "batch_read", "batch_write", "is_connected",
             "get_node_names", "info_random_node", "scan"):
    assert hasattr(aerospike.Client, name), f"client missing {name}"

# 5) Legacy methods we deliberately AVOID — warn if absent (expected) 
for gone in ("exists_many", "get_many", "select_many"):
    print(f"legacy {gone} present:", hasattr(aerospike.Client, gone))

# 6) batch records constructors
assert all(hasattr(br, n) for n in ("BatchRecords", "Write", "Read", "Remove"))
assert hasattr(op, "write") and hasattr(op, "read")

# 7) TTL constants
for c in ("TTL_NEVER_EXPIRE", "TTL_NAMESPACE_DEFAULT", "TTL_DONT_UPDATE"):
    print(c, "=", getattr(aerospike, c, "MISSING"))

# 8) RemoteMetadata round-trip (num_groups=1)
init_remote_metadata_info(1)
print("remote_metadata_bytes =", get_remote_metadata_bytes())
print("OK: preflight passed")
```

4. Inspect the exact `put` TTL mechanism for your installed client version:
   `python -c "import aerospike, inspect; help(aerospike.Client.put)"` and note
   whether `meta={"ttl": N}` is accepted (it is for `<19`). Record the answer in
   a comment you will paste into Step S8's `_apply_ttl` helper.

**Verify (gate):**
- `python scripts/preflight.py` prints `OK: preflight passed` with **no
  AssertionError**.
- The TTL constants print integer values (not `MISSING`).
- If `batch_write`/`batch_read` are missing, your `aerospike` version is too
  old — STOP and fix the pin.

---

## S1 — Repository scaffolding and packaging

**Goal:** A pip-installable, import-clean package skeleton.

**Do:**

1. Create this exact layout (empty files where noted):

```text
lmcache-aerospike/
  pyproject.toml
  src/lmcache_aerospike/
    __init__.py
    adapter.py
    connector.py
    client.py
    config.py
    keys.py
    sharding.py
    serde.py
    limits.py
    policies.py
    errors.py
    metrics.py
  tests/
    unit/__init__.py
    integration/__init__.py
    bench/__init__.py
  docker/
    docker-compose.yml
    aerospike.conf
```

2. Write `pyproject.toml`:

```toml
# file: pyproject.toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "lmcache-aerospike"
version = "0.1.0"
description = "Aerospike remote storage backend for LMCache"
readme = "README.md"
requires-python = ">=3.10,<3.14"
license = { text = "Apache-2.0" }
dependencies = [
  "aerospike>=14.0.0,<19.0.0",
  "lmcache>=0.4.5,<0.5",
]

[project.optional-dependencies]
metrics = ["prometheus_client>=0.19"]
dev = ["pytest>=7", "pytest-asyncio>=0.23", "pytest-benchmark>=4"]

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

3. `src/lmcache_aerospike/__init__.py`:

```python
# file: src/lmcache_aerospike/__init__.py
"""Aerospike remote storage backend for LMCache."""
from lmcache_aerospike.adapter import AerospikeConnectorAdapter
from lmcache_aerospike.connector import AerospikeRemoteConnector

__all__ = ["AerospikeConnectorAdapter", "AerospikeRemoteConnector"]
__version__ = "0.1.0"
```

> NOTE: `__init__.py` importing `adapter`/`connector` means those modules must
> import cleanly even before all helpers exist. While building, it is fine for
> `__init__.py` to temporarily not import them; restore the imports at S9/S10.

**Verify (gate):**
- `pip install -e ".[dev]"` succeeds.
- `python -c "import lmcache_aerospike"` succeeds once S9/S10 are done (for now,
  with empty modules, just confirm `pip install -e .` works and the directory
  imports: temporarily comment the imports in `__init__.py`).

---

## S2 — `config.py`: parse and validate `extra_config`

**Goal:** A frozen dataclass `AerospikeConfig` built from LMCache's
`extra_config` dict, with strict validation and clear error messages.

**Do:**

1. Keys are read from `extra_config` under the prefix
   `remote_storage_plugin.{plugin_name}.` (e.g.
   `remote_storage_plugin.aerospike.hosts`). `plugin_name` may be `aerospike` or
   `aerospike.primary`.
2. Implement `AerospikeConfig` with these fields and defaults. The
   `target_segment_bytes` default is **4 MiB** — this is intentional and matches
   the LMCache authors' guidance that MB-scale chunks are the sweet spot between
   per-transfer network/round-trip overhead and transfer time (LMCache paper,
   "An Efficient KV Cache Layer for Enterprise-Scale LLM Inference",
   arXiv:2510.09665, §4.1 Batched Operations / §7 transfer-granularity
   evaluation: page-level KB transfers underutilize bandwidth; MB-scale chunk
   transfers are required to saturate PCIe/network links). Aerospike's own
   1–10 KiB record sweet spot does NOT apply here because LMCache is
   byte-throughput-dominated, not ops-dominated; we deliberately trade
   index-RAM efficiency for fewer round trips, and the value is still clamped to
   the server's discovered record-size cap at startup:

| Field | Type | Default | Notes |
|---|---|---|---|
| `hosts` | `tuple[tuple[str,int],...]` | required | parse `"h1:3000,h2:3000"` |
| `namespace` | str | `"lmcache"` | |
| `set_name` | str | `"kv_chunks"` | Aerospike `set` (avoid keyword `set`) |
| `target_segment_bytes` | int | `4194304` (4 MiB) | preferred segment size (LMCache authors' MB-scale sweet spot, arXiv:2510.09665); clamped down to the server cap at startup |
| `max_segment_bytes` | `int\|None` | `None` (discovered) | operator override; clamped to server cap |
| `min_segment_bytes` | int | `65536` (64 KiB) | |
| `single_record_threshold_bytes` | `int\|None` | `None` → `min(target,server_cap)` | |
| `default_ttl_seconds` | int | `86400` | `0`/`-1`/`-2` honored |
| `read_timeout_ms` | int | `1000` | |
| `write_timeout_ms` | int | `2000` | |
| `batch_max_in_flight` | int | `64` | |
| `executor_threads` | int | `16` | |
| `enable_list` | bool | `False` | |
| `enable_crc32` | bool | `False` | |
| `commit_level` | str | `"all"` | `all`\|`master` |
| `replica` | str | `"sequence"` | `master`\|`any`\|`sequence`\|`prefer_rack` |
| `send_key` | bool | `False` | |
| `username` | str | `""` | EE only; inert |
| `password` | str | `""` | EE only; inert |
| `tls_name` | str | `""` | EE only; inert |

3. Provide a classmethod:
   `AerospikeConfig.from_extra_config(extra_config: dict | None, plugin_name: str) -> AerospikeConfig`.
   - If `extra_config` is `None`, treat as empty.
   - Missing required key (`hosts`) → raise `AerospikeConfigError("…hosts is required")`.
   - Type/range errors → raise `AerospikeConfigError` naming the offending key.
   - `commit_level`/`replica` not in the allowed set → `AerospikeConfigError`.
   - Booleans parsed from `True/False/"true"/"false"/"1"/"0"` case-insensitively.
4. Add a small helper `key(self, name)` that returns the fully-qualified config
   key string for error messages, e.g.
   `remote_storage_plugin.aerospike.hosts`.

**Verify (gate):** Unit test (write now, run in S11):
- valid dict → all fields parsed, hosts tuple correct;
- missing `hosts` → `AerospikeConfigError` mentioning `hosts`;
- `commit_level="bogus"` → `AerospikeConfigError` mentioning `commit_level`;
- instance-scoped keys (`aerospike.primary`) resolved correctly.

---

## S3 — `errors.py`: typed exceptions + Aerospike→behavior mapping

**Goal:** One module that defines the connector's exception types and a single
`map_aerospike_error(exc)` function used everywhere.

**Do:**

1. Define exceptions (all subclass `AerospikeConnectorError(Exception)`):
   `AerospikeConfigError`, `AerospikeConnectionError`, `AerospikeRecordTooBigError`,
   `AerospikeTTLConfigError`, `AerospikeBusyError`, `AerospikeNamespaceProbeError`,
   `AerospikeServerLimitError`, `AerospikeInternalError`, `AerospikeUnknownError`.
2. Import the Aerospike exception module as `from aerospike import exception as ax`.
3. Implement `classify(exc) -> str` returning one of:
   `"not_found"`, `"too_big"`, `"timeout"`, `"connection"`, `"forbidden_ttl"`,
   `"busy"`, `"key_mismatch"`, `"unknown"`. Mapping (verified against skills +
   Aerospike error codes):

| Aerospike exception | classify() | Connector behavior (caller decides) |
|---|---|---|
| `ax.RecordNotFound` | `not_found` | get→None; exists→False; remove_sync→False |
| `ax.RecordTooBig` | `too_big` | put→raise `AerospikeRecordTooBigError` |
| `ax.TimeoutError` | `timeout` | reads→miss; writes→raise |
| `ax.ConnectionError`, `ax.ClientError` (no nodes) | `connection` | raise `AerospikeConnectionError`; ping→1 |
| `ax.AerospikeError` with `e.code == 22` (`FORBIDDEN`) on a TTL write | `forbidden_ttl` | raise `AerospikeTTLConfigError` (point at `nsup-period`) |
| `ax.DeviceOverload`, `ax.QueueFull`, code `14` (`KEY_BUSY`) | `busy` | put→raise `AerospikeBusyError` |
| `ax.RecordKeyMismatch` | `key_mismatch` | raise `AerospikeInternalError` |
| any other `ax.AerospikeError` | `unknown` | raise `AerospikeUnknownError` |

   Aerospike exceptions expose `.code` and `.msg`; use `getattr(exc, "code", None)`.
   Error code `22` = `AEROSPIKE_ERR_FAIL_FORBIDDEN`; `14` = `KEY_BUSY`. Be
   defensive: match on both the exception subclass AND `.code` where the table
   uses a code.
4. `map_aerospike_error(op_name, exc) -> AerospikeConnectorError` returns the
   right typed exception with a message containing `op_name`, the original
   `.code`, and `.msg`.

**Verify (gate):** Unit test constructs fake exceptions (subclasses with `.code`)
and asserts `classify()` returns the expected bucket for every row above.

---

## S4 — `keys.py`: logical key → Aerospike key tuples

**Goal:** Deterministic Aerospike primary keys for the meta record and segment
records. Works for both `CacheEngineKey` and `LayerCacheEngineKey` with no
special path (both implement `.to_string()`).

**Do:**

1. The Aerospike "user key" is a string. Build it from `ck.to_string()`:
   - meta record user key: `f"{ck.to_string()}|m"`
   - segment record user key: `f"{ck.to_string()}|s|{i}"` for `i` in `[0, nseg)`.
2. An Aerospike key tuple is `(namespace, set_name, user_key)`. Implement:

```python
# file sketch: src/lmcache_aerospike/keys.py
def meta_key(ns: str, set_: str, ck) -> tuple:
    return (ns, set_, f"{ck.to_string()}|m")

def segment_key(ns: str, set_: str, ck, i: int) -> tuple:
    return (ns, set_, f"{ck.to_string()}|s|{i}")

def segment_keys(ns: str, set_: str, ck, nseg: int) -> list[tuple]:
    return [segment_key(ns, set_, ck, i) for i in range(nseg)]
```

3. Do NOT enable `send_key` by default; the digest is computed from the user key
   string, so the logical key is fully recoverable in our own code without
   storing it server-side.

**Verify (gate):** Unit test: `meta_key` and `segment_key(...,3)` produce the
exact expected tuples for a synthetic key string; `segment_keys(...,4)` returns
4 tuples with suffixes `|s|0`..`|s|3`.

---

## S5 — `sharding.py`: the adaptive shard planner (pure function)

**Goal:** A pure, no-I/O planner that decides single-record vs N-segment layout.

**Do:**

1. Define a result dataclass `ShardPlan(nseg: int, seg_b: int)`.
2. Implement `plan(payload_bytes, *, target_segment_bytes, max_segment_bytes,
   min_segment_bytes, single_record_threshold_bytes) -> ShardPlan` with this
   exact decision rule:
   1. If `payload_bytes <= single_record_threshold_bytes` AND
      `payload_bytes <= max_segment_bytes`: return `ShardPlan(1, payload_bytes)`.
   2. Else `nseg = ceil(payload_bytes / target_segment_bytes)`,
      `seg_b = ceil(payload_bytes / nseg)`. If `seg_b > max_segment_bytes`,
      raise `AerospikeConfigError` (operator override made target inconsistent).
      Return `ShardPlan(nseg, seg_b)`.
   3. (`min_segment_bytes` is a warn-only floor: if a non-single plan would
      produce `seg_b < min_segment_bytes`, fall back to `ShardPlan(1, payload_bytes)`
      provided `payload_bytes <= max_segment_bytes`.)
   - `payload_bytes == 0` → `ShardPlan(1, 0)` (empty payload is a valid single
     record).
   - Use integer math: `nseg = -(-payload_bytes // target_segment_bytes)`.
3. The last segment length is `payload_bytes - (nseg-1)*seg_b`; the planner does
   not need to return it, the writer computes slices (Step S9).

**Verify (gate):** Unit tests with the production defaults `max=8 MiB`,
`target=4 MiB`, `min=64 KiB`, `single_threshold=4 MiB` (these exercise every
branch and match the shipped default):
- `256` → `(1, 256)`; `4 MiB` → `(1, 4194304)` (single-record fast path);
- `4 MiB + 1` → `(2, …)` with `seg_b <= 4 MiB` and balanced (`seg_b≈2 MiB`);
- `16 MiB` → `(4, 4194304)`; `64 MiB` → `(16, 4194304)`;
- override making `target>max` → `AerospikeConfigError`;
- assert `sum of slice lengths == payload_bytes` for each case.

---

## S6 — `serde.py`: metadata bin encode/decode + payload assembly helpers

**Goal:** Centralize the `save_chunk_meta` handling and the meta-bin packing so
the connector body stays simple. **This is the step most likely to be done
wrong — follow it exactly. It mirrors upstream `FSConnector`.**

**Context (why):** `RemoteBackend` serializes the `MemoryObj` (naive/cachegen/
kivi) *before* calling `connector.put`, and deserializes *after*
`connector.get`. So the connector stores opaque bytes. To rebuild a `MemoryObj`
of the right shape on read, we either (a) store the per-object `RemoteMetadata`
(when `save_chunk_meta` is True), or (b) rely on the fixed full-chunk shape and
`reshape_partial_chunk` (when False).

**Do:**

1. Implement `build_remote_metadata(memory_obj) -> RemoteMetadata`:

```python
# file sketch: src/lmcache_aerospike/serde.py
from lmcache.v1.protocol import RemoteMetadata

def build_remote_metadata(memory_obj) -> RemoteMetadata:
    buf = memory_obj.byte_array
    return RemoteMetadata(
        len(buf),
        memory_obj.get_shapes(),
        memory_obj.get_dtypes(),
        memory_obj.get_memory_format(),
    )
```

2. Implement `meta_bins(*, plan, memory_obj, save_chunk_meta, enable_crc32,
   default_ttl, pinned) -> dict` returning the bins for the META record:
   - Always include: `ver=1` (int), `state="ready"` (str), `nseg` (int),
     `seg_b` (int), `tot_b=len(byte_array)` (int), `created_at` (int epoch s).
   - If `save_chunk_meta` is True: include `md = build_remote_metadata(memory_obj).serialize()`
     (a `bytes` blob). ⚠ DESIGN-CORRECTION: store ONE `md` blob bin instead of
     separate `shape0..shape3`/`dtype`/`fmt` bins — the blob supports
     `num_groups > 1` and is guaranteed bit-compatible with the deserializer.
   - If `save_chunk_meta` is False: do NOT include `md`.
   - If `plan.nseg == 1`: include `b = bytes(memory_obj.byte_array)` (the inline
     payload). For `nseg > 1`, do NOT include `b` (segments hold the payload).
   - If `enable_crc32`: include `crc32 = zlib.crc32(payload) & 0xFFFFFFFF`.
   - `pin`/`ttl_class` bins optional; include `pin=pinned` (bool).
3. Implement `allocate_for_read(local_cpu_backend, base_self, meta_bins) ->
   (memory_obj | None, expect_meta_reshape: bool)`:
   - If `save_chunk_meta` is True and `md` bin present:
     `rm = RemoteMetadata.deserialize(meta_bins["md"])`;
     `mo = local_cpu_backend.allocate(rm.shapes, rm.dtypes, rm.fmt)`;
     return `(mo, False)`.
   - Else: `mo = local_cpu_backend.allocate(base_self.meta_shapes,
     base_self.meta_dtypes, base_self.meta_fmt)`; return `(mo, True)`
     (caller will call `reshape_partial_chunk` after filling `bytes_read`).
   - If `mo is None` return `(None, False)`.
4. Implement `write_payload_into(memory_obj, payload: bytes | memoryview) -> int`:
   copies bytes into `memory_obj.byte_array` and returns bytes written. Guard
   against `len(payload) > len(byte_array)` (log + raise `AerospikeInternalError`).

**Verify (gate):** Unit test (mock `local_cpu_backend.allocate` to return a fake
MemoryObj wrapping a `bytearray`):
- with `save_chunk_meta=True`, `meta_bins` contains `md`, and
  `allocate_for_read` returns `expect_reshape=False` and allocates from the
  deserialized shapes;
- with `save_chunk_meta=False`, no `md`, `allocate_for_read` returns
  `expect_reshape=True`;
- `nseg==1` path includes `b`; `nseg>1` excludes `b`.

---

## S7 — `policies.py`: read/write/batch policy factories

**Goal:** Build Aerospike policy dicts once, from `AerospikeConfig`, mapping the
string config to `aerospike.POLICY_*` constants.

**Do:**

1. Map config strings to constants:
   - `commit_level`: `"all" -> aerospike.POLICY_COMMIT_LEVEL_ALL`,
     `"master" -> aerospike.POLICY_COMMIT_LEVEL_MASTER`.
   - `replica`: `"master"->POLICY_REPLICA_MASTER`, `"any"->POLICY_REPLICA_ANY`,
     `"sequence"->POLICY_REPLICA_SEQUENCE`, `"prefer_rack"->POLICY_REPLICA_PREFER_RACK`.
   - key policy: `POLICY_KEY_SEND` if `send_key` else `POLICY_KEY_DIGEST`.
2. Factories return plain dicts (the Python client takes policy dicts):

```python
# file sketch: src/lmcache_aerospike/policies.py
def read_policy(cfg) -> dict:
    return {
        "total_timeout": cfg.read_timeout_ms,
        "socket_timeout": cfg.read_timeout_ms,
        "replica": _replica(cfg.replica),
        "key": _key_policy(cfg.send_key),
        "max_retries": 2,            # reads are idempotent
    }

def write_policy(cfg) -> dict:
    return {
        "total_timeout": cfg.write_timeout_ms,
        "socket_timeout": cfg.write_timeout_ms,
        "commit_level": _commit(cfg.commit_level),
        "key": _key_policy(cfg.send_key),
        "exists": aerospike.POLICY_EXISTS_IGNORE,   # overwrite-always
        "gen": aerospike.POLICY_GEN_IGNORE,         # no CAS
        "max_retries": 0,            # writes default non-idempotent
    }

def batch_policy(cfg) -> dict:
    return {"total_timeout": max(cfg.read_timeout_ms, cfg.write_timeout_ms)}
```

   Use `getattr(aerospike, NAME)` defensively; if a constant is missing on the
   installed version, raise `AerospikeConfigError` naming it (caught in S0 ideally).
3. TTL is NOT set here — it is applied per-write in S8's `_apply_ttl` helper.

**Verify (gate):** Unit test asserts the dicts contain the mapped integer
constants for each config string and that an unknown `replica` value raises.

---

## S8 — `client.py` (singleton holder) + `limits.py` (server discovery)

**Goal:** One Aerospike client per `(hosts, namespace, tls_name)` per process,
ref-counted; and server-side record-size discovery that runs at connector
construction (NOT in `post_init`).

### S8a — `client.py`: `AerospikeClientHolder`

**Do:**

1. Module-level registry: `_HOLDERS: dict[tuple, AerospikeClientHolder] = {}`
   guarded by a `threading.Lock`.
2. `AerospikeClientHolder` wraps exactly one connected `aerospike.client(config)`.
   The config dict passed to `aerospike.client(...)` is:

```python
{
  "hosts": list(cfg.hosts),                  # [("h",3000), ...]
  "policies": {                              # client-level defaults
      "read":  read_policy(cfg),
      "write": write_policy(cfg),
      "batch": batch_policy(cfg),
  },
  # EE-only fields (username/password/tls) only if provided; inert on CE
}
```

   Construct with `aerospike.client(config)` then `.connect()`. ⚠ The Python
   client connects on `.connect()`; do this once.
3. Provide `get_or_create(cfg) -> AerospikeClientHolder` keyed by
   `(cfg.hosts, cfg.namespace, cfg.tls_name)`. Increment `refcount` on each
   acquire. Provide `release()` that decrements; when it reaches 0, call
   `client.close()` and remove from `_HOLDERS`.
4. Expose `.client` (the live `aerospike.Client`). Holder is thread-safe.
5. Idempotency: `release()` more times than acquired must not raise (log a
   warning and no-op).

> ⚠ DESIGN-CORRECTION on reconnect: `RemoteBackend` may rebuild the connector
> (`init_connection` after failure, or `recreate_backend`). Because the holder
> is keyed and ref-counted, a rebuild that constructs a new connector with the
> same `(hosts, namespace, tls_name)` reuses the live client (refcount goes
> 1→2→1). Ensure `close()` only tears down at refcount 0.

### S8b — `limits.py`: discover the server record-size cap

**Do:**

1. `SAFETY_MARGIN_BYTES = 65536`.
2. `discover_limits(client, namespace) -> ServerLimits` where
   `ServerLimits(server_max_record_bytes:int, source:str)`:
   - Issue `resp = client.info_random_node(f"namespace/{namespace}")`.
     (⚠ DESIGN-CORRECTION: use `namespace/{ns}` — it returns the full namespace
     config+stats as `key=value;key=value`. This is what the official client
     `ttl.py` example uses and is robust across versions. The
     `get-config:context=namespace;id=` form also works but is less consistently
     supported.)
   - The response may be prefixed; strip up to the first tab/`\t` if present,
     then split on `;` and `=` into a dict.
   - Choose the cap, most-specific first:
     - if `max-record-size` present and `> 0` → that (Aerospike 7.1+).
     - elif `write-block-size` present and `> 0` → that (≤7.0).
     - else → raise `AerospikeServerLimitError` (do NOT guess).
   - Validate the chosen value is within `[131072, 8388608]` (128 KiB..8 MiB);
     otherwise raise `AerospikeServerLimitError`.
   - On any `info` exception or empty response → raise
     `AerospikeNamespaceProbeError` containing the raw response.
3. `resolve_segment_limits(cfg, server_limits) -> ResolvedLimits` computes:
   - `effective_max = server_max_record_bytes - SAFETY_MARGIN_BYTES`
   - `max_segment_bytes = min(cfg.max_segment_bytes or effective_max, effective_max)`
     (clamp override down, WARN if clamped).
   - `target_segment_bytes = min(cfg.target_segment_bytes, effective_max)`
     (WARN if clamped).
   - `single_record_threshold_bytes =
     min(cfg.single_record_threshold_bytes or target_segment_bytes, effective_max)`.
   - `min_segment_bytes = cfg.min_segment_bytes`.
   - Return all four + emit the INFO log block (server values, derived,
     effective) exactly as in DESIGN §4.3.6.
4. Also verify the TTL precondition at discovery time: if
   `cfg.default_ttl_seconds > 0`, check that the namespace config has
   `nsup-period` `> 0`; if it is `0`, raise `AerospikeTTLConfigError` with an
   actionable message (the alternative is to wait for the first write to fail
   with code 22 — fail fast instead).

> ⚠ DESIGN-CORRECTION (placement): discovery is invoked from the connector
> constructor (Step S9), guarded by `self._limits_ready`. It is NOT placed only
> in `post_init`, because `post_init` is never called for remote connectors.

**Verify (gate):** Unit tests for the parser with three canned `info`
responses:
- 7.1+: contains `max-record-size=1048576;…;nsup-period=120` → cap `1048576`,
  source `max-record-size`, `effective = 1048576-65536`.
- 7.0: `write-block-size=1048576;…` (no `max-record-size`) → cap from
  `write-block-size`.
- bad: `max-record-size=0` and no `write-block-size` → `AerospikeServerLimitError`.
- `nsup-period=0` with `default_ttl_seconds>0` → `AerospikeTTLConfigError`.
- empty/garbage response → `AerospikeNamespaceProbeError`.

---

## S9 — `connector.py`: `AerospikeRemoteConnector` (the core)

This is the largest step; it is split into S9a–S9d. Implement and unit-test each
sub-step before the next.

### S9a — Class, constructor, executor, lifecycle, helpers

**Do:**

1. Class signature and constructor (⚠ note arg source):

```python
# file sketch: src/lmcache_aerospike/connector.py
import asyncio, time, zlib
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional
from lmcache.logging import init_logger
from lmcache.utils import CacheEngineKey
from lmcache.v1.memory_management import MemoryObj
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector
from lmcache_aerospike.config import AerospikeConfig
from lmcache_aerospike.client import AerospikeClientHolder
from lmcache_aerospike import keys as K
from lmcache_aerospike import serde, policies, limits
from lmcache_aerospike.sharding import plan as shard_plan
from lmcache_aerospike.errors import (map_aerospike_error, classify, ...)

logger = init_logger(__name__)

class AerospikeRemoteConnector(RemoteConnector):
    def __init__(self, *, config, metadata, local_cpu_backend, loop,
                 aerospike_config: AerospikeConfig,
                 client_holder: AerospikeClientHolder):
        super().__init__(config, metadata)   # sets save_chunk_meta, meta_*, etc.
        self.cfg = aerospike_config
        self.local_cpu_backend = local_cpu_backend
        self.loop = loop
        self._holder = client_holder
        self._client = client_holder.client
        self._executor = ThreadPoolExecutor(
            max_workers=aerospike_config.executor_threads,
            thread_name_prefix="as-conn",
        )
        self._batch_sem = asyncio.Semaphore(aerospike_config.batch_max_in_flight)
        self._closed = False
        self._limits_ready = False
        self._resolved = None
        self._ensure_limits()          # ⚠ discovery happens HERE, not in post_init
```

2. `_ensure_limits(self)` (idempotent, guarded):
   - if `self._limits_ready`: return.
   - `server = limits.discover_limits(self._client, self.cfg.namespace)`
   - `self._resolved = limits.resolve_segment_limits(self.cfg, server)`
   - validate TTL/nsup precondition (limits module did it; surface its error).
   - set `self._limits_ready = True`.
   - On `AerospikeNamespaceProbeError`/`AerospikeServerLimitError`/
     `AerospikeTTLConfigError`: log ERROR and re-raise (construction fails fast;
     `RemoteBackend` catches and will retry per its reconnect policy).
3. `post_init(self)` override: call `self._ensure_limits()` then
   `super().post_init()` is NOT needed; just `self._ensure_limits()`. (Harmless
   if upstream ever calls it; discovery is already done in `__init__`.)
4. TTL helper (the ONLY place TTL touches the client):

```python
def _ttl_value(self, pinned: bool) -> int:
    if pinned:
        return -1                      # never expire
    return self.cfg.default_ttl_seconds  # 0 => namespace default, >0 seconds

def _put_meta(self, ttl: int) -> dict:
    # aerospike client <19: meta={"ttl": N} is valid (confirmed in S0).
    # If you pinned client >=19, change THIS function per that version's docs.
    return {"ttl": ttl}
```

5. Executor dispatch helper:

```python
async def _run(self, fn, *args):
    return await self.loop.run_in_executor(self._executor, fn, *args)
```

6. `async def close(self)` (idempotent):
   - if `self._closed`: return.
   - `self._closed = True`
   - `self._executor.shutdown(wait=True)`
   - `self._holder.release()` (tears down client at refcount 0).

> ⚠ Cancellation note: `RemoteBackend.get_blocking` cancels the outer future on
> `blocking_timeout_secs`, but the executor thread keeps running to completion.
> That is acceptable (Aerospike op is short). Do not attempt to kill threads.

**Verify (gate):** Unit test constructs the connector with a mocked client +
mocked `discover_limits`/`resolve_segment_limits`; asserts `_limits_ready` is
True after construction and that `close()` is idempotent and releases the holder.

### S9b — `exists`, `exists_sync`, `get`

**Do:**

1. `def _meta_select_state(self, ck) -> Optional[dict]` (sync): returns the meta
   bins needed for existence/state, or `None` if absent:

```python
def _exists_sync_impl(self, ck) -> bool:
    mk = K.meta_key(self.cfg.namespace, self.cfg.set_name, ck)
    try:
        (_k, meta, bins) = self._client.select(mk, ["state"])
    except ax.RecordNotFound:
        return False
    except ax.AerospikeError as e:
        raise map_aerospike_error("exists", e)
    return meta is not None
```

   (`select(key, ["state"])` is one round trip and returns existence. Treat any
   present meta as a hit — the atomicity protocol writes meta last with
   `state="ready"`, so a present meta is effectively ready. Reading `state` is
   for future strictness.)
2. `def exists_sync(self, key) -> bool`: call `_exists_sync_impl` directly on the
   calling thread (LMCache may call from background threads; the client is
   thread-safe).
3. `async def exists(self, key) -> bool`: `return await self._run(self._exists_sync_impl, key)`.
4. `def _get_sync_impl(self, ck) -> Optional[MemoryObj]`:
   1. `mk = K.meta_key(...)`; `(_k, meta, bins) = self._client.get(mk)` →
      `RecordNotFound` ⇒ return None.
   2. if `bins.get("state") != "ready"`: return None.
   3. `nseg = bins["nseg"]`; build a meta-bins dict for `serde.allocate_for_read`.
   4. `(mo, expect_reshape) = serde.allocate_for_read(self.local_cpu_backend, self, bins)`;
      if `mo is None`: return None (CPU backend full).
   5. **Single record (`nseg == 1`)**: `payload = bins["b"]` (a `bytes`);
      `n = serde.write_payload_into(mo, payload)`.
   6. **Multi-segment (`nseg > 1`)**:
      `seg_keys = K.segment_keys(..., nseg)`;
      `brs = self._client.batch_read(seg_keys, ["b"])`;
      iterate `brs.batch_records` IN ORDER; for each `r`: if `r.result != 0` or
      `r.record is None` → log WARNING ("missing/in-flight segment") and return
      None; else append `r.record[2]["b"]`. Concatenate in order into `mo`
      via repeated `write_payload_into` at the right offset (or build a
      `bytearray` then copy once). Track total `n`.
   7. if `enable_crc32`: verify `zlib.crc32(payload) & 0xFFFFFFFF == bins["crc32"]`;
      mismatch → log ERROR, `mo.ref_count_down()`, return None.
   8. if `expect_reshape` (i.e. `save_chunk_meta` False): `mo =
      self.reshape_partial_chunk(mo, n)` (n must be a multiple of
      `single_token_size`; it will be for full LMCache chunks).
   9. return `mo`.
5. `async def get(self, key)`: `return await self._run(self._get_sync_impl, key)`.
   On unexpected `AerospikeError`, map via `errors.py`: timeouts → return None
   (miss); connection errors → raise.

**Verify (gate):** Unit tests with a fake client:
- single-record hit returns a MemoryObj with the right bytes;
- multi-segment hit concatenates segments in order;
- missing one segment → None (no raise);
- `state != "ready"` → None;
- CRC mismatch (when enabled) → None;
- `save_chunk_meta=False` path calls `reshape_partial_chunk`.

### S9c — `put` (uses the corrected `BatchRecords` API)

**Do:**

1. Imports at top of module:
   `from aerospike_helpers.batch import records as br`
   `from aerospike_helpers.operations import operations as op`
2. `def _put_sync_impl(self, ck, memory_obj) -> None`:
   1. `view = memory_obj.byte_array` (ensure `memoryview`); `total = len(view)`.
   2. `r = self._resolved` (resolved limits from S8b).
   3. `p = shard_plan(total, target_segment_bytes=r.target_segment_bytes,
      max_segment_bytes=r.max_segment_bytes, min_segment_bytes=r.min_segment_bytes,
      single_record_threshold_bytes=r.single_record_threshold_bytes)`.
   4. `ttl = self._ttl_value(pinned=False)`; `wmeta = self._put_meta(ttl)`.
   5. `mbins = serde.meta_bins(plan=p, memory_obj=memory_obj,
      save_chunk_meta=self.save_chunk_meta, enable_crc32=self.cfg.enable_crc32,
      default_ttl=ttl, pinned=False)`.
   6. **Single record (`p.nseg == 1`)**: `mbins` already includes `b`. One call:
      `self._client.put(meta_key, mbins, meta=wmeta, policy=policies.write_policy(self.cfg))`.
   7. **Multi-segment (`p.nseg > 1`)**:
      - Build segment writes (⚠ DESIGN-CORRECTION — real batch API):

```python
seg_b = p.seg_b
writes = []
for i in range(p.nseg):
    start = i * seg_b
    chunk = bytes(view[start:start + seg_b])   # last slice is shorter
    seg_ops = [op.write("b", chunk)]
    if self.cfg.enable_crc32:
        seg_ops.append(op.write("crc32", zlib.crc32(chunk) & 0xFFFFFFFF))
    writes.append(br.Write(
        key=K.segment_key(self.cfg.namespace, self.cfg.set_name, ck, i),
        ops=seg_ops,
        meta=wmeta,                       # TTL per segment
        policy=policies.write_policy(self.cfg),
    ))
batch = br.BatchRecords(writes)
self._client.batch_write(batch)
# inspect per-record results — top-level success != per-key success
for rec in batch.batch_records:
    if rec.result != 0:
        raise map_aerospike_error("put-segment", _result_to_exc(rec.result))
```

      - THEN commit the meta record LAST (`mbins` has `state="ready"`, no `b`):
        `self._client.put(meta_key, mbins, meta=wmeta, policy=write_policy)`.
   8. Map exceptions: `RecordTooBig` → `AerospikeRecordTooBigError` with payload
      size + caps; code 22 on TTL → `AerospikeTTLConfigError`; `KEY_BUSY`/
      overload → `AerospikeBusyError`; timeout on write → raise (caller retries).
3. `async def put(self, key, memory_obj)`:
   `await self._run(self._put_sync_impl, key, memory_obj)`.
   - Do NOT call `ref_count_down()` — `InstrumentedRemoteConnector.put` does it.
   - Do NOT retain `memory_obj` references after return.
4. Add `_result_to_exc(result_code) -> Exception`: tiny helper that wraps a
   per-record batch result code into something `map_aerospike_error` can
   classify (carry `.code`).

**Verify (gate):** Unit tests with a fake client recording calls:
- 256 B payload → exactly one `put` (single record, `b` present), no `batch_write`;
- payload forcing `nseg=3` → one `batch_write` with a `BatchRecords` of 3
  `Write` records (segments) THEN one `put` for meta with `state="ready"` and no
  `b`; verify segment byte slices reassemble to the original;
- a fake `batch_write` that sets `batch_records[1].result=14` → `AerospikeBusyError`;
- `RecordTooBig` from the fake client → `AerospikeRecordTooBigError`.

### S9d — batched methods, `remove_sync`, `list`, `ping`

**Do:**

1. `support_batched_get -> True`; `async def batched_get(self, keys)`:
   dedupe defensively, then under `self._batch_sem` gather `self.get(k)` for each
   key; return results in the SAME order as input (map dedup back).
2. `support_batched_put -> True`; `async def batched_put(self, keys, memory_objs)`:
   gather `self.put(k, mo)` pairwise under the semaphore. Independent puts.
3. `support_batched_contains -> True`;
   `def batched_contains(self, keys) -> int` (SYNC):
   - dedupe while preserving first-occurrence order is NOT needed here because we
     need consecutive-prefix semantics on the ORIGINAL order — build
     `meta_keys = [meta_key(k) for k in keys]`.
   - `brs = self._client.batch_read(meta_keys, [])` (empty bins = metadata only).
     ⚠ DESIGN-CORRECTION: replaces `exists_many`.
   - Walk `brs.batch_records` in order; count consecutive `rec.result == 0`
     (record found). Return the count at the first miss. (A present meta is
     treated as ready per the atomicity protocol.)
   - On any exception → return `0` (the upstream `RemoteBackend.batched_contains`
     wraps this in try/except and treats failure as 0 anyway, but be explicit).
4. `support_batched_async_contains` already True in base; override
   `async def batched_async_contains(self, lookup_id, keys, pin=False) -> int`:
   `return await self._run(self._batched_contains_sync, keys)` where
   `_batched_contains_sync` is the body of step 3. Ignore `pin` in Phase 1
   (document it; upstream FS connector ignores it too).
5. `support_batched_get_non_blocking` already True in base; the base
   implementation (gather + release-on-failure) is correct for us, BUT it calls
   `self.get` per key without the semaphore. Override to add the semaphore and
   keep identical release semantics:
   - `results = await asyncio.gather(*(self._sem_get(k) for k in keys), return_exceptions=True)`
   - then replicate the base loop: append consecutive `MemoryObj`s; on first
     `None`/`Exception`, set `found_failure=True` and `ref_count_down()` every
     subsequent `MemoryObj`. Return the prefix list.
6. `def remove_sync(self, key) -> bool`:
   - read meta first to learn `nseg` (best-effort `select(mk, ["nseg"])`).
   - `self._client.remove(meta_key)` → `RecordNotFound` ⇒ return False.
   - if `nseg > 1`: best-effort `batch_write` of `br.Remove(segment_key(i))`
     for each segment; failures here are logged WARNING and ignored (TTL cleans
     up). ⚠ CE: do NOT pass `durable_delete=True` (EE-only).
   - return True on meta removal success.
7. `async def list(self) -> List[str]`:
   - if not `cfg.enable_list`: log INFO once and return `[]`.
   - if enabled: `scan = self._client.scan(namespace, set_name)`; collect user
     keys whose suffix is `|m`; strip the `|m`; return `key.to_string()` values.
     Mark as expensive/debug-only. Run via executor.
8. `support_ping -> True`; `async def ping(self) -> int`:
   - `def _ping(): return 0 if (self._client.is_connected() and
     self._client.get_node_names()) else 1`; dispatch via executor; on any
     exception return `1`.

**Verify (gate):** Unit tests:
- `batched_contains([T,T,F,T]) == 2` (fake `batch_read` returns results
  `[0,0,2,0]` where 2 = RECORD_NOT_FOUND);
- `batched_get` preserves order with a duplicate key in the input;
- `batched_get_non_blocking` releases trailing MemoryObjs after a None
  (assert `ref_count_down` called on them);
- `remove_sync` removes meta then issues segment removes for `nseg>1`;
- `list()` returns `[]` when disabled.

---

## S10 — `adapter.py`: `AerospikeConnectorAdapter`

**Goal:** The class LMCache loads via `module_path`/`class_name`. It is
instantiated with **no arguments**, then `create_connector(context)` is called.

**Do:**

```python
# file sketch: src/lmcache_aerospike/adapter.py
from lmcache.logging import init_logger
from lmcache.v1.storage_backend.connector import (
    ConnectorAdapter, ConnectorContext, extract_plugin_type,
)
from lmcache_aerospike.config import AerospikeConfig
from lmcache_aerospike.client import AerospikeClientHolder
from lmcache_aerospike.connector import AerospikeRemoteConnector

logger = init_logger(__name__)

class AerospikeConnectorAdapter(ConnectorAdapter):
    def __init__(self) -> None:           # ⚠ NO required args
        super().__init__("aerospike://")

    def can_parse(self, url: str) -> bool:
        if url.startswith("aerospike://"):
            return True
        if url.startswith("plugin://"):    # LMCache passes plugin://{plugin_name}
            return extract_plugin_type(url[len("plugin://"):]) == "aerospike"
        return False

    def create_connector(self, context: ConnectorContext):
        # ⚠ canonical upstream pattern: prefer local_cpu_backend.config/metadata
        lcb = context.local_cpu_backend
        config = getattr(lcb, "config", None) or context.config
        metadata = getattr(lcb, "metadata", None) or context.metadata
        plugin_name = context.plugin_name or "aerospike"
        as_cfg = AerospikeConfig.from_extra_config(
            config.extra_config if config else None, plugin_name
        )
        holder = AerospikeClientHolder.get_or_create(as_cfg)
        return AerospikeRemoteConnector(
            config=config,
            metadata=metadata,
            local_cpu_backend=lcb,
            loop=context.loop,
            aerospike_config=as_cfg,
            client_holder=holder,
        )
```

**Notes / gotchas:**
- `plugin_name` is the instance name (`aerospike` or `aerospike.primary`); pass
  it straight to `AerospikeConfig.from_extra_config` so instance-scoped config
  keys resolve.
- If `create_connector` raises (e.g. discovery fails), `RemoteBackend` catches
  it, logs, and will retry per `min_reconnect_interval`. That is acceptable —
  fail loud, not silently.
- Restore the `from lmcache_aerospike.adapter import AerospikeConnectorAdapter`
  line in `__init__.py` now (S1).

**Verify (gate):** Unit test:
- `AerospikeConnectorAdapter()` constructs with no args;
- `can_parse("plugin://aerospike")` and `can_parse("plugin://aerospike.primary")`
  are True; `can_parse("redis://x")` is False;
- `create_connector` with a fake context (mock `local_cpu_backend` having
  `.config`/`.metadata`, mocked holder + discovery) returns an
  `AerospikeRemoteConnector`.

---

## S11 — `metrics.py`: optional Prometheus hooks (opt-in)

**Goal:** Zero hard dependency on `prometheus_client`; metrics only register if
it is importable.

**Do:**

1. `try: import prometheus_client … except ImportError: prometheus_client = None`.
2. If available, define module-level collectors (created once):
   - `aerospike_op_total{op,result}` Counter
   - `aerospike_op_latency_seconds{op}` Histogram (buckets 1ms..10s)
   - `aerospike_segment_count` Histogram
   - `aerospike_segment_bytes` Histogram
   - `aerospike_concurrent_in_flight` Gauge
3. Provide no-op fallbacks (functions that do nothing) when `prometheus_client`
   is None, so the connector body calls `metrics.observe_op(...)` unconditionally.
4. Wire calls from the connector: increment `op_total` with the right `result`
   bucket (`hit`/`miss`/`ok`/`timeout`/`record_too_big`/`busy`/`error`) and time
   each op. Keep this lightweight; never let a metrics error break an op (wrap in
   try/except).

**Verify (gate):** Unit test: import `metrics` with `prometheus_client` absent
(monkeypatch `sys.modules`) and confirm `observe_op(...)` is a no-op that does
not raise; with it present, confirm counters increment.

---

## S12 — Unit test suite (no network) — consolidate and run green

**Goal:** Every module's gate test lives under `tests/unit/` and the whole suite
passes with no Aerospike server.

**Do:**

1. Create a shared fake client in `tests/unit/conftest.py`:
   - `FakeMemoryObj`: wraps a `bytearray`; implements `byte_array` (memoryview),
     `get_size`, `get_shapes`, `get_dtypes`, `get_memory_format`,
     `ref_count_down` (increments a counter), `meta.shape`, `meta.dtype`.
   - `FakeLocalCPUBackend`: `.allocate(shapes, dtypes, fmt)` returns a
     `FakeMemoryObj` sized from shapes×dtype itemsize (or a fixed size for tests);
     carries `.config` and `.metadata`.
   - `FakeClient`: records `put`/`batch_write`/`batch_read`/`select`/`get`/
     `remove`/`info_random_node` calls; configurable return values and
     per-record results; raises configured `aerospike.exception` types.
   - `FakeBatchRecord(result, record)` and a `FakeBatchRecords(list)` exposing
     `.batch_records`.
2. Provide canned `info_random_node` responses (the three from S8b).
3. Ensure `RemoteMetadata` works in tests by calling
   `init_remote_metadata_info(num_groups)` in a fixture (num_groups from the
   fake metadata's `get_num_groups()`); set `num_groups=1` for the basic tests.
4. Port every per-step gate test here. Add these cross-cutting tests:
   - **Atomicity:** simulate a crash between segment writes and meta commit
     (fake client: segments present, meta absent) → `get` returns None.
   - **`batched_contains` parity:** `[T,T,F,T] -> 2`.
   - **Error matrix:** every row of S3 maps correctly.
   - **Round-trip in-memory:** put then get through the fake client for sizes
     that exercise `nseg=1` and `nseg=3`, asserting byte-exact recovery.
5. Run `pytest tests/unit -q`.

**Verify (gate):** `pytest tests/unit -q` is fully green. No network access
occurs (no real `aerospike.client().connect()` in unit tests).

---

## S13 — Docker integration environment (single-node CE)

**Goal:** A reproducible single-node Aerospike CE with a `lmcache` namespace and
`nsup-period > 0` so TTL writes succeed.

**Do:**

1. `docker/aerospike.conf` — minimal CE config. Key requirements:
   - a namespace named `lmcache`;
   - `nsup-period 120` (⚠ MUST be `> 0` or positive-TTL writes fail with code 22);
   - `default-ttl 0` (let the client decide TTL);
   - storage-engine memory (or a small device file) sized for tests;
   - on Aerospike 7.1+, optionally set `max-record-size 1M` (so the discovery
     test has a known cap to assert).

```text
# file: docker/aerospike.conf  (CE; adjust to the image's base config)
service { }
logging { console { context any info } }
network {
  service { address any; port 3000; }
  heartbeat { mode multicast; multicast-group 239.1.99.222; port 9918; interval 150; timeout 10; }
  fabric { port 3001; }
  info { port 3003; }
}
namespace lmcache {
  replication-factor 1
  nsup-period 120
  default-ttl 0
  storage-engine memory { data-size 1G }
  # On 7.1+, uncomment to fix a known cap for the discovery test:
  # max-record-size 1M
}
```

2. `docker/docker-compose.yml`:

```yaml
# file: docker/docker-compose.yml
services:
  aerospike:
    image: aerospike/aerospike-server:latest
    container_name: lmcache-aerospike-ce
    ports: ["3000:3000", "3001:3001", "3003:3003"]
    volumes:
      - ./aerospike.conf:/etc/aerospike/aerospike.conf:ro
    command: ["--config-file", "/etc/aerospike/aerospike.conf"]
```

3. Add `tests/integration/conftest.py` that:
   - is skipped entirely unless `RUN_INTEGRATION=1` is set in the env;
   - starts compose (or assumes it is already up — document both), waits until
     `info_random_node("build")` succeeds (poll up to ~30s);
   - yields a connected `aerospike.Client` and tears down.

**Verify (gate):**
- `docker compose -f docker/docker-compose.yml up -d` starts the container.
- `python -c "import aerospike; c=aerospike.client({'hosts':[('127.0.0.1',3000)]}).connect(); print(c.info_random_node('namespace/lmcache')); c.close()"`
  prints a config string containing `nsup-period=120` and (on 7.1+)
  `max-record-size=...`.

---

## S14 — Integration tests (real CE; gated by `RUN_INTEGRATION=1`)

**Goal:** Prove the connector works end-to-end against a real CE node.

**Do:** Implement these tests in `tests/integration/` (all skipped unless
`RUN_INTEGRATION=1`). For each, build a real `AerospikeConfig` pointing at
`127.0.0.1:3000`, namespace `lmcache`, set `it_chunks`, and a real
`LocalCPUBackend` from a minimal `LMCacheEngineConfig` + `LMCacheMetadata`
(construct the smallest valid metadata; if that is hard, use a tiny real
LMCache engine init helper).

1. **Discovery log/values:** construct the connector; assert
   `connector._resolved.max_segment_bytes == server_cap - 65536` and that the
   INFO discovery lines were emitted.
2. **Round-trip size matrix:** for payloads `256B, 64KiB, 1MiB, 4MiB, 16MiB,
   64MiB`: put then get; assert byte-exact; assert `meta["nseg"]` matches the
   expected shard count for the discovered cap.
3. **TTL expiry:** `default_ttl_seconds=2`; put; `sleep(5)`; `get` → None;
   `exists` → False.
4. **Pinned key:** put with TTL `-1` (pinned path); `sleep(default_ttl+slack)`;
   assert still present. (Requires building the put-pinned path; if pin is not
   exposed in Phase 1 public API, test via a direct `_put_sync_impl` with a
   pinned flag.)
5. **Crash mid-write:** write segments only (call segment batch_write), do NOT
   write meta; `get` → None; then confirm TTL eventually removes segments.
6. **TTL/NSUP precondition:** point at a namespace with `nsup-period 0` (a second
   namespace or reconfigured container) and `default_ttl_seconds>0`; assert
   construction raises `AerospikeTTLConfigError` (fail fast, not on first write).
7. **Multi-instance:** two configs (`aerospike.primary`, `aerospike.dr`) to the
   same node but different sets; write to one, miss from the other.
8. **Layerwise/MLA smoke (⚠ gap from review):** if feasible, build metadata with
   `use_layerwise=True` (forces `save_chunk_meta=True`); round-trip a
   `LayerCacheEngineKey`-shaped payload; assert byte-exact. Document if deferred.

**Verify (gate):** `RUN_INTEGRATION=1 pytest tests/integration -q` is green
against the S13 container. Capture the discovery INFO lines in test output.

---

## S15 — Benchmark harness + vLLM smoke test

**Goal:** Numbers to confirm the 4 MiB default behaves well on the target
deployment and to confirm real cache reuse. These gate the move from `0.1.x`
alpha to `0.2.0` stable.

**Do (bench, `tests/bench/`):**
1. `pytest-benchmark`-driven synthetic chunk stream. Keep the default
   `target_segment_bytes = 4 MiB` as the primary configuration (LMCache authors'
   MB-scale sweet spot, arXiv:2510.09665) and ALSO sweep neighbors
   (`1MiB, 2MiB, 4MiB, 8MiB`) purely as a **sensitivity check**, on a payload
   band representative of the target model (e.g. Llama-class TP=8 chunks).
2. Measure and print: `get` p50/p95/p99 (100% hit and 100% miss), `put`
   p50/p95/p99, sustained bytes/s under `batch_max_in_flight=64`, connector
   thread-pool CPU%.
3. **Decision output:** a short table across segment sizes. The default stays
   **4 MiB**; only recommend deviating for a specific deployment if it is proven
   device-saturated (per DESIGN §4.6's fallback recipe: halve to 2 MiB, then
   1 MiB, never below `min_segment_bytes`). Record any deployment-specific
   override and its measured rationale; do NOT change the shipped default.

**Do (vLLM smoke, optional but recommended):**
1. A pytest fixture launches vLLM + LMCache configured with this connector and a
   tiny model (e.g. Llama-3.2-1B). Send two identical long prompts across a
   worker restart; assert the second request's TTFT is materially lower (cache
   hit through Aerospike). Gate behind `RUN_VLLM=1` and document GPU/CPU needs.

**Verify (gate):** Bench runs and emits the comparison table; vLLM smoke (if
run) shows TTFT drop on the second prompt.

---

## S16 — CI, docs, and DESIGN.md reconciliation

**Goal:** Land CI, user docs, and fold the plan's corrections back into
`DESIGN.md` so the two documents agree.

**Do:**
1. **CI** (`.github/workflows/ci.yml`): matrix Python 3.10/3.11/3.12/3.13 on
   Linux x86_64. Jobs: lint + `pytest tests/unit` on every PR; a separate job
   running `RUN_INTEGRATION=1 pytest tests/integration` with the compose service
   (use a service container or `docker compose up -d`) on merge to `main`.
2. **README:** install (`pip install lmcache lmcache-aerospike`), the YAML config
   snippet (from DESIGN §4.5.1 but with the corrected default
   `target_segment_bytes`), and the compatibility matrix.
3. **DESIGN.md reconciliation — apply these edits** (so design == implementation):
   - §1/§4.4.0/§4.3.6: discovery runs at connector construction, not `post_init`.
   - §4.4.4 / §4.4.10: `batch_write(BatchRecords([...]))`, not tuple lists.
   - §4.4.7: `batch_read(meta_keys, [])` for existence, not `exists_many`.
   - §4.3.1: store one `md` (`RemoteMetadata.serialize()`) blob bin; honor
     `save_chunk_meta`; drop the rigid `shape0..shape3` bins.
   - §4.4.3: allocate from stored metadata when `save_chunk_meta`, else from
     `self.meta_*` + `reshape_partial_chunk` — remove the contradiction.
   - §2.2: fix the *Aerospike* sweet-spot to "1–10 KiB" (match §4.6 and skills);
     cite Aerospike docs for the 8 MiB cap and the 7.1+ `max-record-size`
     default of 1 MiB. Keep this separate from the LMCache chunk sizing below.
   - §4.3.4 / §4.5.2: **keep `target_segment_bytes = 4 MiB`** as the default and
     cite the LMCache paper (arXiv:2510.09665) for the MB-scale chunk-transfer
     sweet spot. Add one sentence clarifying these are two different "sweet
     spots": Aerospike's 1–10 KiB record sweet spot is ops-throughput guidance,
     while LMCache's MB-scale chunk is byte-throughput guidance; the connector
     deliberately follows the LMCache value and clamps it to the server's
     record-size cap.
   - Add a note that the connector is serde-agnostic and that MLA/layerwise key
     rewriting happens in `RemoteBackend` above the connector.
   - Pin the `aerospike` client version and note the `meta["ttl"]` deprecation
     boundary (v19.1.0).

**Verify (gate):** CI is green on a PR (unit) and on `main` (integration);
README install works in a clean venv; `DESIGN.md` no longer contradicts this
plan (grep for `exists_many`, `post_init`, `shape0`, `1-128 KiB` and confirm
each is updated).

---

## Final acceptance checklist (Phase 1 "done")

Mirrors DESIGN §1.5, with corrections. All must be true:

- [ ] `pip install -e ".[dev]"` works on Python 3.10–3.13; `import lmcache_aerospike` clean.
- [ ] `pytest tests/unit -q` fully green (no network).
- [ ] Discovery runs at construction; bad namespace/`nsup-period 0`/out-of-range
      cap fail fast with the typed errors (proven by integration tests).
- [ ] Round-trip byte-exact for `256B,64KiB,1MiB,4MiB,16MiB,64MiB` with correct
      `nseg` for the discovered cap.
- [ ] `batched_contains` returns the consecutive-prefix count
      (`[T,T,F,T] -> 2`), matching the Redis connector's semantics.
- [ ] TTL expiry and pinned (`-1`) behavior verified on a real CE node.
- [ ] No use of `exists_many`/`get_many`/`select_many`; batch ops use
      `batch_read`/`batch_write` + `BatchRecords`.
- [ ] TTL set in exactly one helper; client version pinned.
- [ ] `save_chunk_meta` honored (layerwise/MLA round-trips).
- [ ] vLLM smoke shows cache reuse across a worker restart (if run).
- [ ] Bench harness reports p50/p95/p99 + bytes/s and the segment-size decision.
- [ ] `DESIGN.md` reconciled with this plan.

## Appendix A — Upstream facts this plan relies on (so you can re-verify)

- `RemoteConnector` abstract set and `support_*` defaults:
  `lmcache/v1/storage_backend/connector/base_connector.py`.
- Adapter loading, no-arg adapter instantiation, `plugin://{name}` URL,
  `ConnectorContext`: `…/connector/__init__.py` and
  `…/storage_backend/remote_backend.py` (`init_connection`) and
  `…/storage_backend/__init__.py` (`CreateStorageBackends`).
- `post_init` is defined+forwarded but NOT called in the remote path:
  confirmed across `remote_backend.py`, `storage_manager.py`,
  `instrumented_connector.py`.
- Serde applied above connector: `RemoteBackend.__init__` (`CreateSerde`) and
  `submit_put_task`/`get_blocking`.
- `save_chunk_meta` handling pattern: `…/connector/fs_connector.py`
  (`put`/`get`), `RemoteMetadata` in `…/v1/protocol.py`.
- Aerospike client API (`batch_write`/`batch_read`/`info_random_node`/`put` TTL):
  official Python client reference (pin a version; legacy `*_many` removed).

