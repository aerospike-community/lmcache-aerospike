#!/usr/bin/env bash
# Run ``lmcache bench l2`` against Aerospike Python L2, Aerospike native L2, or LMCache RESP (Redis).
#
# Prereqs: scripts/setup_l2_bench.sh; backend must be running:
#   Aerospike: ./scripts/start_aerospike_ce.sh
#   Redis:     ./scripts/start_redis_bench.sh
#
# Usage:
#   ./benchmarks/l2/run.sh --backend aerospike
#   ./benchmarks/l2/run.sh --backend aerospike-native
#   ./benchmarks/l2/run.sh --backend resp
#   ./benchmarks/l2/compare.sh              # both backends, sequential (see compare.sh)
#   ./benchmarks/l2/run.sh --profile stress --only store

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
L2_DIR="${ROOT}/benchmarks/l2"
cd "$ROOT"

# Match the interpreter that provides ``lmcache`` (often python3.11, not system python3).
PYTHON="${LMCACHE_BENCH_PYTHON:-}"
if [[ -z "${PYTHON}" ]] && command -v lmcache >/dev/null 2>&1; then
  _shebang="$(head -1 "$(command -v lmcache)" 2>/dev/null || true)"
  if [[ "${_shebang}" == \#!* ]]; then
    PYTHON="${_shebang#\#!}"
  fi
fi
PYTHON="${PYTHON:-python3}"

BACKEND="aerospike"
PROFILE="smoke"
ADAPTER_JSON=""
EXTRA=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --backend)
      BACKEND="${2:?--backend requires aerospike, aerospike-native, or resp}"
      shift 2
      ;;
    --profile)
      PROFILE="${2:?--profile requires a name}"
      shift 2
      ;;
    --adapter)
      ADAPTER_JSON="${2:?--adapter requires a filename under benchmarks/l2/adapters/}"
      shift 2
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

case "${BACKEND}" in
  aerospike | as) BACKEND="aerospike" ;;
  aerospike-native | as-native | native) BACKEND="aerospike-native" ;;
  resp | redis) BACKEND="resp" ;;
  *)
    echo "unknown --backend: ${BACKEND} (use aerospike, aerospike-native, or resp)" >&2
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

if [[ -z "${L2_ADAPTER_JSON:-}" ]]; then
  if [[ -z "${ADAPTER_JSON}" ]]; then
    if [[ "${BACKEND}" == "aerospike" ]]; then
      ADAPTER_JSON="aerospike_${PROFILE}.json"
      if [[ ! -f "${L2_DIR}/adapters/${ADAPTER_JSON}" ]]; then
        ADAPTER_JSON="aerospike_smoke.json"
      fi
    elif [[ "${BACKEND}" == "aerospike-native" ]]; then
      ADAPTER_JSON="aerospike_native_${PROFILE}.json"
      if [[ ! -f "${L2_DIR}/adapters/${ADAPTER_JSON}" ]]; then
        ADAPTER_JSON="aerospike_native_smoke.json"
      fi
    else
      ADAPTER_JSON="resp_${PROFILE}.json"
      if [[ ! -f "${L2_DIR}/adapters/${ADAPTER_JSON}" ]]; then
        ADAPTER_JSON="resp_smoke.json"
      fi
    fi
  fi
  ADAPTER_PATH="${L2_DIR}/adapters/${ADAPTER_JSON}"
  if [[ ! -f "${ADAPTER_PATH}" ]]; then
    echo "adapter file not found: ${ADAPTER_PATH}" >&2
    exit 1
  fi

  if [[ "${BACKEND}" == "aerospike" || "${BACKEND}" == "aerospike-native" ]]; then
    HOST="${AEROSPIKE_TEST_HOST:-127.0.0.1}"
    PORT="${AEROSPIKE_TEST_PORT:-3000}"
    NS="${AEROSPIKE_TEST_NAMESPACE:-lmcache}"
    if [[ "${BACKEND}" == "aerospike-native" ]]; then
      SET="${AEROSPIKE_NATIVE_BENCH_L2_SET:-kv_chunks_bench_l2_native}"
      WORKERS="${AEROSPIKE_NATIVE_NUM_WORKERS:-8}"
    else
      SET="${AEROSPIKE_BENCH_L2_SET:-kv_chunks_bench_l2}"
      WORKERS="0"
    fi
    export L2_ADAPTER_JSON
    L2_ADAPTER_JSON="$(
      "${PYTHON}" "${L2_DIR}/render_adapter.py" "${BACKEND}" "${ADAPTER_PATH}" \
        --host "${HOST}" --port "${PORT}" --namespace "${NS}" --set "${SET}" \
        --num-workers "${WORKERS}"
    )"
  else
    HOST="${REDIS_BENCH_HOST:-127.0.0.1}"
    PORT="${REDIS_BENCH_PORT:-6399}"
    WORKERS="${REDIS_BENCH_NUM_WORKERS:-8}"
    export L2_ADAPTER_JSON
    L2_ADAPTER_JSON="$(
      "${PYTHON}" "${L2_DIR}/render_adapter.py" resp "${ADAPTER_PATH}" \
        --host "${HOST}" --port "${PORT}" --num-workers "${WORKERS}"
    )"
  fi
fi

PROFILE_ENV="${L2_DIR}/profiles/${PROFILE}.env"
if [[ ! -f "${PROFILE_ENV}" ]]; then
  echo "unknown profile: ${PROFILE} (missing ${PROFILE_ENV})" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "${PROFILE_ENV}"

BENCH_ARGS=()
if [[ -n "${BENCH_L2_EXTRA_ARGS:-}" ]]; then
  # shellcheck disable=SC2206
  BENCH_ARGS+=(${BENCH_L2_EXTRA_ARGS})
fi
BENCH_ARGS+=("${EXTRA[@]}")

echo "==> lmcache bench l2 (backend=${BACKEND} profile=${PROFILE})" >&2
exec "${PYTHON}" - "${BENCH_ARGS[@]}" <<'PY'
import sys

from benchmarks.l2.bootstrap import bootstrap
from lmcache.cli.main import main

bootstrap()
sys.argv = ["lmcache", "bench", "l2", *sys.argv[1:]]
main()
PY
