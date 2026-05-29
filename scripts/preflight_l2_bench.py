# file: scripts/preflight_l2_bench.py
"""Verify the environment can run ``lmcache bench l2`` with AerospikeL2Plugin."""

from __future__ import annotations

import importlib
import shutil
import subprocess
import sys
from pathlib import Path


def _fail(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root / "benchmarks" / "l2"))
    from bootstrap import bootstrap  # noqa: PLC0415

    if shutil.which("lmcache") is None:
        _fail("lmcache CLI not on PATH (install LMCache dev via scripts/setup_l2_bench.sh)")

    try:
        from lmcache.v1.distributed.internal_api import L2StoreResult  # noqa: F401
    except ImportError as exc:
        _fail(
            "L2StoreResult missing — PyPI lmcache 0.4.x is not enough; "
            f"install LMCache dev: {exc}"
        )

    try:
        importlib.import_module("lmcache.cli.commands.bench.l2_adapter_bench.command")
    except ImportError as exc:
        _fail(f"lmcache bench l2 subcommand not found: {exc}")

    try:
        importlib.import_module("openai")
    except ImportError as exc:
        _fail(f"openai package required for lmcache CLI: {exc}")

    l2_mod = importlib.import_module("lmcache_aerospike.l2_plugin")
    if not l2_mod.L2_MP_AVAILABLE:
        _fail("AerospikeL2Plugin L2_MP_AVAILABLE is false after LMCache install")

    bootstrap()

    help_out = subprocess.run(
        ["lmcache", "bench", "l2", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    if help_out.returncode != 0:
        _fail(
            "lmcache bench l2 --help failed:\n"
            f"{help_out.stderr or help_out.stdout}"
        )
    if "--l2-adapter" not in (help_out.stdout + help_out.stderr):
        _fail("lmcache bench l2 --help missing --l2-adapter")

    print("OK: preflight_l2_bench passed")


if __name__ == "__main__":
    main()
