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
PYTHON="${LMCACHE_BENCH_PYTHON:-python3}"

LMCACHE_SRC="${LMCACHE_SRC:-${ROOT}/../LMCache}"
if [[ ! -d "${LMCACHE_SRC}" ]]; then
  echo "LMCache dev clone not found at: ${LMCACHE_SRC}" >&2
  echo "Clone https://github.com/LMCache/LMCache (dev branch) and set LMCACHE_SRC." >&2
  exit 1
fi

if [[ "${LMCACHE_AEROSPIKE_SKIP_NATIVE_DEPS:-0}" != "1" ]]; then
  echo "==> Aerospike C client + native extension prerequisites"
  "${ROOT}/scripts/build_libaerospike.sh"
  # shellcheck source=/dev/null
  source "${ROOT}/.deps/aerospike-client-c.env"
fi

echo "==> Installing lmcache-aerospike (editable, native extension when deps present)"
LMCACHE_AEROSPIKE_FORCE_NATIVE="${LMCACHE_AEROSPIKE_FORCE_NATIVE:-1}" \
  "${PYTHON}" -m pip install -e . --no-build-isolation

echo "==> Installing LMCache from ${LMCACHE_SRC} (editable, --no-build-isolation)"
if ! "${PYTHON}" -c "import torch" 2>/dev/null; then
  echo "torch not found; installing a CPU build first (adjust for your CUDA stack if needed)"
  "${PYTHON}" -m pip install "torch>=2.0"
fi
NO_GPU_EXT="${NO_GPU_EXT:-1}" "${PYTHON}" -m pip install -e "${LMCACHE_SRC}" --no-build-isolation

echo "==> Installing L2 bench harness requirements"
"${PYTHON}" -m pip install -r benchmarks/l2/requirements.txt

echo "==> Preflight (Aerospike plugin + native + RESP extension)"
"${PYTHON}" scripts/preflight_l2_bench.py --resp --native-aerospike

echo ""
echo "Setup complete. When ready to benchmark:"
echo "  ./scripts/start_aerospike_ce.sh"
echo "  ./scripts/start_redis_bench.sh"
echo "  set -a && source .aerospike-ci.env && source .redis-bench.env && set +a"
echo "  ./benchmarks/l2/compare.sh          # Aerospike native then Redis, same load"
echo "  ./benchmarks/l2/run.sh --backend aerospike"
echo "  ./benchmarks/l2/run.sh --backend aerospike-native"
echo "  ./benchmarks/l2/run.sh --backend resp"
echo ""
echo "See benchmarks/l2/README.md"
