#!/usr/bin/env bash
# CI install path for Phase 3 native L2 connector (build _native + test deps).
#
# Usage (from repo root, after LMCache/ is present):
#   ./scripts/ci_native_install.sh
#   source .aerospike-ci.env
#   RUN_NATIVE=1 pytest tests/integration/test_native_l2_*.py -v

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ ! -d LMCache ]]; then
  echo "LMCache/ not found; clone dev beside this repo or set LMCACHE_SRC." >&2
  exit 1
fi

python -m pip install --upgrade pip wheel
pip install "setuptools>=77.0.3,<81.0.0"
pip install "pybind11>=2.12"
pip install "torch" --index-url https://download.pytorch.org/whl/cpu
pip install "setuptools>=77.0.3,<81.0.0" --force-reinstall

echo "==> Aerospike C client (prebuilt, .deps/)"
chmod +x scripts/build_libaerospike.sh
./scripts/build_libaerospike.sh
# shellcheck source=/dev/null
source "${ROOT}/.deps/aerospike-client-c.env"

echo "==> LMCache dev (L2 APIs; no GPU/CUDA extensions required)"
NO_GPU_EXT=1 pip install -e ./LMCache --no-build-isolation

echo "==> lmcache-aerospike with native extension"
export LMCACHE_SRC="${ROOT}/LMCache"
# --no-deps: keep LMCache dev editable; do not replace with PyPI lmcache.
LMCACHE_AEROSPIKE_FORCE_NATIVE=1 pip install -e . --no-build-isolation --no-deps
pip install "aerospike>=14.0.0,<19.0.0"
pip install pytest pytest-asyncio

python -c "from lmcache_aerospike import _native; print('OK:', _native.AerospikeNativeClient)"

echo "CI native install complete."
