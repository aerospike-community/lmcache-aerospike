#!/usr/bin/env bash
# Stop Redis bench container started by start_redis_bench.sh.

set -euo pipefail

CONTAINER_NAME="${REDIS_BENCH_CONTAINER_NAME:-lmcache-redis-bench}"
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
echo "Stopped ${CONTAINER_NAME} (if it was running)."
