"""Render L2 adapter JSON for local bench runs (connection overrides)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def _load_template(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def render_aerospike(
    template: dict[str, Any],
    *,
    host: str,
    port: str,
    namespace: str,
    set_name: str,
    num_workers: int = 0,
) -> dict[str, Any]:
    spec = dict(template)
    params = dict(spec.get("adapter_params") or {})
    params["hosts"] = f"{host}:{port}"
    params["namespace"] = namespace
    if spec.get("type") == "native_plugin":
        params["set_name"] = set_name
        if num_workers > 0:
            params["num_workers"] = int(num_workers)
    else:
        params["set"] = set_name
    spec["adapter_params"] = params
    return spec


def render_resp(
    template: dict[str, Any],
    *,
    host: str,
    port: int,
    num_workers: int,
) -> dict[str, Any]:
    spec = dict(template)
    spec["host"] = host
    spec["port"] = int(port)
    if num_workers > 0:
        spec["num_workers"] = int(num_workers)
    return spec


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "backend",
        choices=("aerospike", "aerospike-native", "as-native", "native", "resp", "redis"),
    )
    parser.add_argument("template", type=Path)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True)
    parser.add_argument("--namespace", default="lmcache")
    parser.add_argument("--set", dest="set_name", default="kv_chunks_bench_l2")
    parser.add_argument("--num-workers", type=int, default=8)
    args = parser.parse_args()

    template = _load_template(args.template)
    backend = "resp" if args.backend == "redis" else args.backend
    if backend in {"as-native", "native"}:
        backend = "aerospike-native"

    if backend in {"aerospike", "aerospike-native"}:
        spec = render_aerospike(
            template,
            host=args.host,
            port=args.port,
            namespace=args.namespace,
            set_name=args.set_name,
            num_workers=args.num_workers,
        )
    else:
        spec = render_resp(
            template,
            host=args.host,
            port=int(args.port),
            num_workers=args.num_workers,
        )

    json.dump(spec, sys.stdout, separators=(",", ":"))


if __name__ == "__main__":
    main()
