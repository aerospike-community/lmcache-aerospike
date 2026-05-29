"""Build optional native Aerospike connector extension."""

from __future__ import annotations

import ctypes.util
import os
from pathlib import Path

from setuptools import Extension, find_packages, setup

ROOT = Path(__file__).resolve().parent


def _lmcache_include_dirs() -> list[str]:
    candidates: list[Path] = []
    if os.environ.get("LMCACHE_SRC"):
        candidates.append(Path(os.environ["LMCACHE_SRC"]) / "csrc" / "storage_backends")
    candidates.append(ROOT / "LMCache" / "csrc" / "storage_backends")
    return [str(path) for path in candidates if (path / "connector_base.h").exists()]


def _has_aerospike_headers() -> bool:
    if os.environ.get("AEROSPIKE_INCLUDE_DIR"):
        header = Path(os.environ["AEROSPIKE_INCLUDE_DIR"]) / "aerospike" / "aerospike.h"
        return header.exists()
    for base in (Path("/usr/include"), Path("/usr/local/include")):
        if (base / "aerospike" / "aerospike.h").exists():
            return True
    return False


def _has_aerospike_library() -> bool:
    if os.environ.get("AEROSPIKE_LIBRARY_DIR"):
        lib_dir = Path(os.environ["AEROSPIKE_LIBRARY_DIR"])
        return (lib_dir / "libaerospike.so").exists() or (
            lib_dir / "libaerospike.a"
        ).exists()
    return ctypes.util.find_library("aerospike") is not None


def _native_extensions() -> list[Extension]:
    if os.environ.get("LMCACHE_AEROSPIKE_NO_NATIVE") == "1":
        return []

    force = os.environ.get("LMCACHE_AEROSPIKE_FORCE_NATIVE") == "1"
    lmcache_includes = _lmcache_include_dirs()
    has_aerospike = _has_aerospike_headers() and _has_aerospike_library()
    if (not lmcache_includes or not has_aerospike) and not force:
        return []

    try:
        import pybind11
    except ImportError:
        if not force:
            return []
        raise

    include_dirs = [
        str(ROOT / "csrc" / "aerospike"),
        pybind11.get_include(),
        *lmcache_includes,
    ]
    if os.environ.get("AEROSPIKE_INCLUDE_DIR"):
        include_dirs.append(os.environ["AEROSPIKE_INCLUDE_DIR"])

    library_dirs = []
    if os.environ.get("AEROSPIKE_LIBRARY_DIR"):
        library_dirs.append(os.environ["AEROSPIKE_LIBRARY_DIR"])

    return [
        Extension(
            "lmcache_aerospike._native",
            sources=[
                "csrc/aerospike/pybind.cpp",
                "csrc/aerospike/connector.cpp",
            ],
            include_dirs=include_dirs,
            library_dirs=library_dirs,
            libraries=["aerospike"],
            language="c++",
            extra_compile_args=["-O3", "-std=c++17"],
        )
    ]


setup(
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    ext_modules=_native_extensions(),
)
