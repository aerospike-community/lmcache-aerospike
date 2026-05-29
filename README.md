# lmcache-aerospike

An Aerospike storage backend for [LMCache](https://github.com/LMCache/LMCache).
LMCache caches LLM attention key/value tensors so repeated long contexts in
vLLM, SGLang, and similar serving engines do not have to be prefilled again.
This package plugs Aerospike in as a durable, shared remote KV-cache tier,
sitting between LMCache's CPU/disk tiers and any cold object store.

## Status

**Phase 1 (Python `RemoteConnector`)** and **Phase 2** (`StoragePluginInterface` + L2 `plugin`) are implemented on `main`:

- Adaptive sharded meta + segment records, server cap discovery at construction
- Batch APIs (`batched_get`, `batched_put`, `batched_contains`, …)
- Phase 2: `AerospikeStoragePlugin` (single-process) and `AerospikeL2Plugin` (multiprocess L2)
- Unit tests (no network) and integration tests against Aerospike CE in CI
- Optional Prometheus metrics (`pip install -e ".[metrics]"`)

See [`DESIGN.md`](DESIGN.md) for the full contract and [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md) for the build steps.

## Install

Python **3.10–3.13** (matches LMCache):

```bash
pip install "lmcache>=0.4.5,<0.5" lmcache-aerospike
```

Development:

```bash
git clone https://github.com/aerospike-community/lmcache-aerospike.git
cd lmcache-aerospike
pip install -e ".[dev]"
python scripts/preflight.py
pytest tests/unit -q
```

Optional metrics:

```bash
pip install -e ".[metrics]"
```

## LMCache configuration

Register the plugin and point `extra_config` at your Aerospike cluster
(namespace must have `nsup-period > 0` when using positive TTL):

```yaml
remote_storage_plugins: ["aerospike"]

extra_config:
  remote_storage_plugin.aerospike.hosts: "127.0.0.1:3000"
  remote_storage_plugin.aerospike.namespace: lmcache
  remote_storage_plugin.aerospike.set: kv_chunks
  remote_storage_plugin.aerospike.target_segment_bytes: 4194304
  remote_storage_plugin.aerospike.default_ttl_seconds: 86400
```

Instance-scoped keys (`aerospike.primary`, `aerospike.dr`) use the same prefix
with the instance name instead of `aerospike`.

### Phase 2: storage plugin (single-process)

```yaml
storage_plugins: ["aerospike"]
extra_config:
  storage_plugin.aerospike.module_path: lmcache_aerospike.storage_plugin
  storage_plugin.aerospike.class_name: AerospikeStoragePlugin
  storage_plugin.aerospike.hosts: "127.0.0.1:3000"
  storage_plugin.aerospike.namespace: lmcache
  storage_plugin.aerospike.set: kv_chunks
```

### Phase 2: L2 plugin (multiprocess)

```json
{
  "type": "plugin",
  "module_path": "lmcache_aerospike.l2_plugin",
  "class_name": "AerospikeL2Plugin",
  "adapter_params": {
    "hosts": "127.0.0.1:3000",
    "namespace": "lmcache",
    "set": "kv_chunks"
  }
}
```

Phase 2 uses the same Aerospike record layout as Phase 1. The **storage plugin** works with PyPI `lmcache` 0.4.x. The **L2 plugin** needs LMCache multiprocess L2 APIs (`L2StoreResult` on `lmcache.v1.distributed.internal_api`), which are not in PyPI 0.4.5 yet — use an LMCache `dev` build (or a future release) plus `native_storage_ops`.

## Compatibility

| Component | Version |
|-----------|---------|
| Python | 3.10 – 3.13 |
| `lmcache` | `>=0.4.5,<0.5` |
| `aerospike` (Python client) | `>=14.0.0,<19.0.0` |
| Aerospike server (tested) | CE 7.x / 8.x via Docker |

TTL is set via `meta={"ttl": N}` on writes (valid for client `<19`; see
`DESIGN.md` if upgrading to client 19+).

## Integration tests (local)

```bash
./scripts/start_aerospike_ce.sh
set -a && source .aerospike-ci.env && set +a
pytest tests/integration -v
./scripts/stop_aerospike_ce.sh
```

Large payloads (16–64 MiB): `RUN_LARGE_INTEGRATION=1`.

**L2 plugin** (`test_l2_plugin_*.py`): live tests against Aerospike CE via `PluginL2AdapterConfig`, aligned with LMCache’s meaningful upstream patterns (`test_mock_l2_adapter`, RESP L2 integration, `lmc_external_l2_adapter`) — not LMCache’s full mocked suite. Requires [LMCache](https://github.com/LMCache/LMCache) `dev` in `LMCache/` (or `LMCACHE_SRC`). Install: `./scripts/ci_integration_install.sh`.

## Benchmarks

Benchmark code lives under **`benchmarks/`** (not included in the PyPI wheel).

```bash
pip install -r benchmarks/requirements.txt
./scripts/start_aerospike_ce.sh && source .aerospike-ci.env
python benchmarks/run.py --profile smoke
```

Micro (FakeClient, no server): `RUN_BENCH=1 pytest benchmarks/micro --benchmark-only`

See [`benchmarks/README.md`](benchmarks/README.md).

## License

Apache-2.0 (see [`LICENSE`](LICENSE)).
