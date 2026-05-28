# lmcache-aerospike

An Aerospike storage backend for [LMCache](https://github.com/LMCache/LMCache).
LMCache caches LLM attention key/value tensors so repeated long contexts in
vLLM, SGLang, and similar serving engines do not have to be prefilled again.
This project plugs Aerospike in as a durable, shared remote KV-cache tier,
sitting between LMCache's CPU/disk tiers and any cold object store.

## Status

Pre-implementation. No source code yet. The design is committed to
[`DESIGN.md`](DESIGN.md) as a multi-phase plan:

- **Phase 1 (implementation-ready):** a Python `RemoteConnector` plugin
  (`remote_storage_plugins: ["aerospike"]`) backed by an adaptive sharded
  data model tuned for the ~4 MiB chunk band on Aerospike Community Edition.
- **Phase 2 (architectural):** `StoragePluginInterface` and `L2AdapterInterface`
  for richer LMCache integration, including pin/unpin and multiprocess mode.
- **Phase 3 (architectural):** a native C++ connector against `libaerospike`
  for sustained multi-GB/s per-worker throughput.

Read [`DESIGN.md`](DESIGN.md) for the full contract, data model, configuration
surface, error handling, testing plan, and roll-out strategy.

## License

Apache-2.0 (see [`LICENSE`](LICENSE)).
