# lmcache-aerospike

An Aerospike storage backend for [LMCache](https://github.com/LMCache/LMCache).
LMCache caches LLM attention key/value tensors so repeated long contexts in
vLLM, SGLang, and similar serving engines do not have to be prefilled again.
This package plugs Aerospike in as a durable, shared remote KV-cache tier,
sitting between LMCache's CPU/disk tiers and any cold object store.

## Status

**Phase 1 (Python `RemoteConnector`)** is implemented on `main`:

- Adaptive sharded meta + segment records, server cap discovery at construction
- Batch APIs (`batched_get`, `batched_put`, `batched_contains`, …)
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

## Benchmarks

```bash
RUN_BENCH=1 pytest tests/bench -v --benchmark-only
```

See [`tests/bench/README.md`](tests/bench/README.md).

## License

Apache-2.0 (see [`LICENSE`](LICENSE)).
