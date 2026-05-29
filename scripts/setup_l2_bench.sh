#!/usr/bin/env bash
# Install deps for ``lmcache bench l2`` + AerospikeL2Plugin (LMCache dev required).
#
# Usage:
#   ./scripts/setup_l2_bench.sh
#   LMCACHE_SRC=/path/to/LMCache ./scripts/setup_l2_bench.sh
#
# Does not start Aerospike or run the benchmark.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LMCACHE_SRC="${LMCACHE_SRC:-${ROOT}/../LMCache}"
if [[ ! -d "${LMCACHE_SRC}" ]]; then
  echo "LMCache dev clone not found at: ${LMCACHE_SRC}" >&2
  echo "Clone https://github.com/LMCache/LMCache (dev branch) and set LMCACHE_SRC." >&2
  exit 1
fi

echo "==> Installing lmcache-aerospike (editable)"
pip install -e .

echo "==> Installing LMCache from ${LMCACHE_SRC} (editable, --no-build-isolation)"
if ! python3 -c "import torch" 2>/dev/null; then
  echo "torch not found; installing a CPU build first (adjust for your CUDA stack if needed)"
  pip install "torch>=2.0"
fi
pip install -e "${LMCACHE_SRC}" --no-build-isolation

echo "==> Installing L2 bench harness requirements"
pip install -r benchmarks/l2/requirements.txt

echo "==> Preflight"
python3 scripts/preflight_l2_bench.py

echo ""
echo "Setup complete. When ready to benchmark:"
echo "  ./scripts/start_aerospike_ce.sh"
echo "  set -a && source .aerospike-ci.env && set +a"
echo "  ./benchmarks/l2/run.sh"
echo ""
echo "See benchmarks/l2/README.md"
