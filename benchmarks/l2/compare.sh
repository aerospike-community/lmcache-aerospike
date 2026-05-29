#!/usr/bin/env bash
# Run the same ``lmcache bench l2`` profile against Aerospike then Redis (RESP), sequentially.
#
# Backends are never benchmarked concurrently. Redis is FLUSHALL'd immediately before its run.
#
# Usage:
#   ./scripts/setup_l2_bench.sh
#   ./scripts/start_aerospike_ce.sh && ./scripts/start_redis_bench.sh
#   set -a && source .aerospike-ci.env && source .redis-bench.env && set +a
#   ./benchmarks/l2/compare.sh
#   ./benchmarks/l2/compare.sh --profile stress
#   ./benchmarks/l2/compare.sh --backend aerospike      # old Python L2 path

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
L2_DIR="${ROOT}/benchmarks/l2"
RUN="${L2_DIR}/run.sh"
PROFILE="smoke"
AEROSPIKE_BACKEND="aerospike-native"
EXTRA=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="${2:?--profile requires a name}"
      shift 2
      ;;
    --backend)
      AEROSPIKE_BACKEND="${2:?--backend requires aerospike or aerospike-native}"
      shift 2
      ;;
    --python)
      AEROSPIKE_BACKEND="aerospike"
      shift
      ;;
    --native)
      AEROSPIKE_BACKEND="aerospike-native"
      shift
      ;;
    --)
      shift
      EXTRA=("$@")
      break
      ;;
    *)
      EXTRA+=("$1")
      shift
      ;;
  esac
done

case "${AEROSPIKE_BACKEND}" in
  aerospike | as) AEROSPIKE_BACKEND="aerospike" ;;
  aerospike-native | as-native | native) AEROSPIKE_BACKEND="aerospike-native" ;;
  *)
    echo "unknown --backend: ${AEROSPIKE_BACKEND} (use aerospike or aerospike-native)" >&2
    exit 1
    ;;
esac

if [[ -f "${ROOT}/.aerospike-ci.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${ROOT}/.aerospike-ci.env"
  set +a
fi
if [[ -f "${ROOT}/.redis-bench.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${ROOT}/.redis-bench.env"
  set +a
fi
if [[ -f "${L2_DIR}/.env.local" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${L2_DIR}/.env.local"
  set +a
fi

REDIS_HOST="${REDIS_BENCH_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_BENCH_PORT:-6399}"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS_DIR="${L2_DIR}/results/${TS}-${PROFILE}"
mkdir -p "$RESULTS_DIR"

echo "============================================================"
echo "L2 compare: aerospike_backend=${AEROSPIKE_BACKEND} profile=${PROFILE}  results=${RESULTS_DIR}"
echo "============================================================"

echo ""
if [[ "${AEROSPIKE_BACKEND}" == "aerospike-native" ]]; then
  echo "[1/2] Aerospike native (AerospikeNativeConnector / native_plugin) ..."
else
  echo "[1/2] Aerospike Python (AerospikeL2Plugin / plugin) ..."
fi
"${RUN}" --backend "${AEROSPIKE_BACKEND}" --profile "${PROFILE}" "${EXTRA[@]}" 2>&1 | tee "${RESULTS_DIR}/${AEROSPIKE_BACKEND}.log"

echo ""
echo "[2/2] Redis (LMCache RESP / native L2 adapter) ..."
if command -v redis-cli >/dev/null 2>&1; then
  redis-cli -h "${REDIS_HOST}" -p "${REDIS_PORT}" FLUSHALL
elif docker ps --format '{{.Names}}' | grep -qx "${REDIS_BENCH_CONTAINER_NAME:-lmcache-redis-bench}"; then
  docker exec "${REDIS_BENCH_CONTAINER_NAME:-lmcache-redis-bench}" redis-cli FLUSHALL
else
  echo "warning: could not FLUSHALL Redis; results may include prior keys" >&2
fi

"${RUN}" --backend resp --profile "${PROFILE}" "${EXTRA[@]}" 2>&1 | tee "${RESULTS_DIR}/resp.log"

echo ""
echo "Done. Logs:"
echo "  ${RESULTS_DIR}/${AEROSPIKE_BACKEND}.log"
echo "  ${RESULTS_DIR}/resp.log"
