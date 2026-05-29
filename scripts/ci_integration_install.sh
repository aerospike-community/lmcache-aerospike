#!/usr/bin/env bash
# Mirror the integration job dependency install (for local CI reproduction).
#
# Usage (from repo root):
#   ./scripts/ci_integration_install.sh
#   ./scripts/start_aerospike_ce.sh && source .aerospike-ci.env && pytest tests/integration -v

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ ! -d LMCache ]]; then
  echo "LMCache/ not found; clone dev: git clone --depth 1 -b dev https://github.com/LMCache/LMCache.git LMCache" >&2
  exit 1
fi

python -m pip install --upgrade pip wheel
pip install "setuptools>=77.0.3,<81.0.0"
pip install "torch" --index-url https://download.pytorch.org/whl/cpu
pip install "setuptools>=77.0.3,<81.0.0" --force-reinstall
NO_NATIVE_EXT=1 pip install -e ./LMCache --no-build-isolation
pip install -e . --no-deps
# --no-deps skips aerospike; required for start_aerospike_ce.sh host probe and tests.
pip install "aerospike>=14.0.0,<19.0.0"
pip install pytest pytest-asyncio

echo "CI integration install complete."
