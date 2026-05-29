#!/usr/bin/env python3
"""Truncate Aerospike L2 benchmark sets (best-effort, for compare.sh)."""

from __future__ import annotations

import os
import sys


def main() -> int:
    host = os.environ.get("AEROSPIKE_TEST_HOST", "127.0.0.1")
    port = int(os.environ.get("AEROSPIKE_TEST_PORT", "3000"))
    namespace = os.environ.get("AEROSPIKE_TEST_NAMESPACE", "lmcache")
    sets = [
        os.environ.get("AEROSPIKE_BENCH_L2_SET", "kv_chunks_bench_l2"),
        os.environ.get("AEROSPIKE_NATIVE_BENCH_L2_SET", "kv_chunks_bench_l2_native"),
    ]

    try:
        import aerospike
    except ImportError as exc:
        print(f"truncate skipped: aerospike package not installed ({exc})", file=sys.stderr)
        return 0

    client = aerospike.client({"hosts": [(host, port)]}).connect()
    try:
        for set_name in dict.fromkeys(sets):
            try:
                client.truncate(namespace, set_name, 0)
                print(f"truncated {namespace}/{set_name}")
            except Exception as exc:  # noqa: BLE001
                print(f"truncate {namespace}/{set_name}: {exc}", file=sys.stderr)
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
