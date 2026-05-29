#!/usr/bin/env bash
# Print a compact table from compare.sh result logs.
#
# Usage:
#   ./benchmarks/l2/summarize_results.sh benchmarks/l2/results/20260529T120000Z-stress

set -euo pipefail

RESULTS_DIR="${1:?usage: summarize_results.sh <results-dir>}"
if [[ ! -d "${RESULTS_DIR}" ]]; then
  echo "not a directory: ${RESULTS_DIR}" >&2
  exit 1
fi

extract_op() {
  local file="$1" op="$2" field="$3"
  sed -n "/---------------------------- ${op} ----------------------------/,/^---------------------------- /p" "${file}" \
    | grep -F "${field}" \
    | head -1 \
    | awk '{print $NF}'
}

print_file() {
  local label="$1" file="$2"
  if [[ ! -f "${file}" ]]; then
    printf "%-18s  (missing %s)\n" "${label}" "$(basename "${file}")"
    return
  fi
  local store_ms load_ms lookup_ms store_mbps load_mbps
  store_ms="$(extract_op "${file}" Store "Duration avg (ms):")"
  load_ms="$(extract_op "${file}" Load "Duration avg (ms):")"
  lookup_ms="$(extract_op "${file}" Lookup "Duration avg (ms):")"
  store_mbps="$(extract_op "${file}" Store "Throughput avg (MB/s):")"
  load_mbps="$(extract_op "${file}" Load "Throughput avg (MB/s):")"
  printf "%-18s  store %8s ms  %8s MB/s | lookup %8s ms | load %8s ms  %8s MB/s\n" \
    "${label}" "${store_ms:-?}" "${store_mbps:-?}" "${lookup_ms:-?}" "${load_ms:-?}" "${load_mbps:-?}"
}

echo "Results: ${RESULTS_DIR}"
echo "================================================================"
for f in "${RESULTS_DIR}"/*.log; do
  [[ -f "${f}" ]] || continue
  base="$(basename "${f}" .log)"
  print_file "${base}" "${f}"
done
echo "================================================================"
