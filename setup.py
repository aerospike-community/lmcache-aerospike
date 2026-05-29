"""Build optional native Aerospike connector extension."""

from __future__ import annotations

import ctypes.util
import os
from pathlib import Path

from setuptools import Extension, find_packages, setup

ROOT = Path(__file__).resolve().parent
DEPS_CLIENT = ROOT / ".deps" / "aerospike-client-c"
DEPS_INSTALL = ROOT / ".deps" / "aerospike-install"
DEPS_YAML_LIB = ROOT / ".deps" / "libyaml-install" / "usr" / "lib" / "x86_64-linux-gnu"


def _local_aerospike_target_dirs() -> list[Path]:
    if not DEPS_CLIENT.is_dir():
        return []
    return sorted(DEPS_CLIENT.glob("target/Linux*"))


def _lmcache_include_dirs() -> list[str]:
    candidates: list[Path] = []
    if os.environ.get("LMCACHE_SRC"):
        candidates.append(Path(os.environ["LMCACHE_SRC"]) / "csrc" / "storage_backends")
    candidates.append(ROOT / "LMCache" / "csrc" / "storage_backends")
    return [str(path) for path in candidates if (path / "connector_base.h").exists()]


def _aerospike_header_path(base: Path) -> Path:
    return base / "aerospike" / "aerospike.h"


def _aerospike_include_dir() -> Path | None:
    if os.environ.get("AEROSPIKE_INCLUDE_DIR"):
        candidate = Path(os.environ["AEROSPIKE_INCLUDE_DIR"])
        if _aerospike_header_path(candidate).exists():
            return candidate
    for base in (
        DEPS_INSTALL / "usr" / "include",
        Path("/usr/include"),
        Path("/usr/local/include"),
    ):
        if _aerospike_header_path(base).exists():
            return base
    for target in _local_aerospike_target_dirs():
        include = target / "include"
        if _aerospike_header_path(include).exists():
            return include
    return None


def _aerospike_library_dir() -> Path | None:
    if os.environ.get("AEROSPIKE_LIBRARY_DIR"):
        lib_dir = Path(os.environ["AEROSPIKE_LIBRARY_DIR"])
        if (lib_dir / "libaerospike.so").exists() or (
            lib_dir / "libaerospike.a"
        ).exists():
            return lib_dir
    for target in _local_aerospike_target_dirs():
        lib_dir = target / "lib"
        if (lib_dir / "libaerospike.so").exists() or (
            lib_dir / "libaerospike.a"
        ).exists():
            return lib_dir
    deps_lib = DEPS_INSTALL / "usr" / "lib"
    if (deps_lib / "libaerospike.so").exists() or (deps_lib / "libaerospike.a").exists():
        return deps_lib
    if ctypes.util.find_library("aerospike") is not None:
        return None
    return None


def _has_aerospike_headers() -> bool:
    return _aerospike_include_dir() is not None


def _has_aerospike_library() -> bool:
    return _aerospike_library_dir() is not None


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

    aerospike_include = _aerospike_include_dir()
    aerospike_lib_dir = _aerospike_library_dir()
    if aerospike_include is None or aerospike_lib_dir is None:
        if force:
            raise RuntimeError(
                "Aerospike C client headers or library not found; run "
                "./scripts/build_libaerospike.sh or set AEROSPIKE_INCLUDE_DIR "
                "and AEROSPIKE_LIBRARY_DIR"
            )
        return []

    include_dirs = [
        str(ROOT / "csrc" / "aerospike"),
        pybind11.get_include(),
        str(aerospike_include),
        *lmcache_includes,
    ]

    library_dirs = [str(aerospike_lib_dir)]
    yaml_static = DEPS_YAML_LIB / "libyaml.a"
    yaml_shared = DEPS_YAML_LIB / "libyaml.so"
    if yaml_static.exists() or yaml_shared.exists():
        library_dirs.append(str(DEPS_YAML_LIB))
    static_lib = aerospike_lib_dir / "libaerospike.a"
    shared_lib = aerospike_lib_dir / "libaerospike.so"
    prefer_static = os.environ.get("AEROSPIKE_STATIC", "0") == "1"
    use_static = prefer_static and static_lib.exists() and not shared_lib.exists()
    if prefer_static and static_lib.exists() and shared_lib.exists():
        # Static .a omits some transitive symbols (e.g. libyaml); prefer .so when both exist.
        use_static = False
    extra_objects: list[str] = []
    libraries = ["aerospike"]
    # libaerospike leaves OpenSSL/libuv (and static .a: libyaml) unresolved at link time.
    aerospike_extra_libs = ["ssl", "crypto", "pthread", "z", "rt"]
    if os.environ.get("AEROSPIKE_EVENT_LIB", "libuv") == "libuv":
        aerospike_extra_libs.append("uv")
    if use_static:
        extra_objects = [str(static_lib)]
        libraries = aerospike_extra_libs + ["yaml"]
    else:
        # libaerospike.so leaves yaml/openssl/libuv unresolved; list aerospike first.
        libraries = ["aerospike"]
        if yaml_shared.exists() or ctypes.util.find_library("yaml"):
            libraries.append("yaml")
        elif yaml_static.exists():
            extra_objects.append(str(yaml_static))
        libraries.extend(aerospike_extra_libs)

    runtime_dirs: list[str] = []
    if shared_lib.exists() and not use_static:
        runtime_dirs.append(str(aerospike_lib_dir))

    return [
        Extension(
            "lmcache_aerospike._native",
            sources=[
                "csrc/aerospike/pybind.cpp",
                "csrc/aerospike/connector.cpp",
            ],
            include_dirs=include_dirs,
            library_dirs=library_dirs,
            libraries=libraries,
            extra_objects=extra_objects,
            runtime_library_dirs=runtime_dirs,
            language="c++",
            extra_compile_args=["-O3", "-std=c++17"],
            extra_link_args=["-Wl,--no-as-needed"],
        )
    ]


setup(
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    ext_modules=_native_extensions(),
)
