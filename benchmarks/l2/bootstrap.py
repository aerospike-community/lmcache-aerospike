"""Runtime bootstrap for ``lmcache bench l2`` against AerospikeL2Plugin."""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _lmcache_src() -> Path:
    env = os.environ.get("LMCACHE_SRC")
    if env:
        return Path(env).expanduser().resolve()
    return _repo_root().parent / "LMCache"


def install_native_storage_ops_fallback() -> None:
    """Use LMCache test fallback when ``lmcache.native_storage_ops`` is incomplete."""
    try:
        native = __import__("lmcache.native_storage_ops", fromlist=["Bitmap"])
        if hasattr(native, "Bitmap") and hasattr(native, "TTLLock"):
            return
    except Exception:
        pass

    utils = _lmcache_src() / "tests/v1/storage_backend/raw_block_test_utils.py"
    if not utils.is_file():
        raise RuntimeError(
            "lmcache.native_storage_ops is unavailable and LMCache test utils were not found.\n"
            f"  Expected: {utils}\n"
            "  Set LMCACHE_SRC to your LMCache dev clone, or build LMCache with native extensions."
        )
    spec = importlib.util.spec_from_file_location("lmcache_raw_block_test_utils", utils)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load LMCache test utils from {utils}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.install_native_storage_ops_fallback()


def bootstrap() -> None:
    install_native_storage_ops_fallback()
    from lmcache.v1.protocol import init_remote_metadata_info

    init_remote_metadata_info(1)


def main() -> None:
    bootstrap()
    print("OK: L2 bench bootstrap")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)
