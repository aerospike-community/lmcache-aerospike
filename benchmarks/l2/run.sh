#!/usr/bin/env bash
# Run ``lmcache bench l2`` against AerospikeL2Plugin.
#
# Prereqs: scripts/setup_l2_bench.sh, Aerospike CE (./scripts/start_aerospike_ce.sh).
#
# Usage:
#   ./benchmarks/l2/run.sh                          # smoke profile, default adapter
#   ./benchmarks/l2/run.sh --profile stress
#   ./benchmarks/l2/run.sh --adapter aerospike_stress.json --only store
#   ./benchmarks/l2/run.sh -- --only load           # extra args after -- go to lmcache bench l2

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
L2_DIR="${ROOT}/benchmarks/l2"
cd "$ROOT"

PROFILE="smoke"
ADAPTER_JSON=""
EXTRA=()

while [[ $# -gt 0 ]]; do
  case "$1" in
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

if [[ -f "${ROOT}/.aerospike-ci.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${ROOT}/.aerospike-ci.env"
  set +a
fi

if [[ -f "${L2_DIR}/.env.local" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${L2_DIR}/.env.local"
  set +a
fi

HOST="${AEROSPIKE_TEST_HOST:-127.0.0.1}"
PORT="${AEROSPIKE_TEST_PORT:-3000}"
NS="${AEROSPIKE_TEST_NAMESPACE:-lmcache}"
SET="${AEROSPIKE_BENCH_L2_SET:-kv_chunks_bench_l2}"

if [[ -z "${L2_ADAPTER_JSON:-}" ]]; then
  if [[ -z "${ADAPTER_JSON}" ]]; then
    ADAPTER_JSON="aerospike_smoke.json"
  fi
  ADAPTER_PATH="${L2_DIR}/adapters/${ADAPTER_JSON}"
  if [[ ! -f "${ADAPTER_PATH}" ]]; then
    echo "adapter file not found: ${ADAPTER_PATH}" >&2
    exit 1
  fi
  export L2_ADAPTER_JSON
  L2_ADAPTER_JSON="$(
    python3 - "${ADAPTER_PATH}" "${HOST}" "${PORT}" "${NS}" "${SET}" <<'PY'
import json
import sys
from pathlib import Path

path, host, port, ns, set_name = sys.argv[1:6]
spec = json.loads(Path(path).read_text())
params = spec.setdefault("adapter_params", {})
params["hosts"] = f"{host}:{port}"
params["namespace"] = ns
params["set"] = set_name
print(json.dumps(spec, separators=(",", ":")))
PY
  )"
fi

PROFILE_ENV="${L2_DIR}/profiles/${PROFILE}.env"
if [[ ! -f "${PROFILE_ENV}" ]]; then
  echo "unknown profile: ${PROFILE} (missing ${PROFILE_ENV})" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "${PROFILE_ENV}"

python3 "${L2_DIR}/bootstrap.py"

BENCH_ARGS=()
if [[ -n "${BENCH_L2_EXTRA_ARGS:-}" ]]; then
  # shellcheck disable=SC2206
  BENCH_ARGS+=(${BENCH_L2_EXTRA_ARGS})
fi
BENCH_ARGS+=("${EXTRA[@]}")

exec lmcache bench l2 "${BENCH_ARGS[@]}"
