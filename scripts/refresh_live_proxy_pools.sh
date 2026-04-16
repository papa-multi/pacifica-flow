#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "${ROOT_DIR}/config/runtime.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/config/runtime.env"
  set +a
fi

SOURCE_FILE="${PACIFICA_PROXY_SOURCE_FILE:-}"
if [[ -z "${SOURCE_FILE}" ]]; then
  if [[ -f "/root/pacifica_phase1_proxy_list_20260331_healthy.txt" ]]; then
    SOURCE_FILE="/root/pacifica_phase1_proxy_list_20260331_healthy.txt"
  elif [[ -f "/root/pacifica_phase1_proxy_list_20260331.txt" ]]; then
    SOURCE_FILE="/root/pacifica_phase1_proxy_list_20260331.txt"
  else
    SOURCE_FILE="${ROOT_DIR}/data/indexer/working_proxies.txt"
  fi
fi

WORKING_OUT="${ROOT_DIR}/data/indexer/working_proxies.txt"
PROBE_OUT="${ROOT_DIR}/data/indexer/proxy_test_results.json"
POSITIONS_OUT="${ROOT_DIR}/data/live_positions/positions_proxies.txt"
DETECTOR_OUT="${ROOT_DIR}/data/live_positions/detector_proxies.txt"

TEST_CONCURRENCY="${PACIFICA_PROXY_TEST_CONCURRENCY:-96}"
TEST_TIMEOUT_MS="${PACIFICA_PROXY_TEST_TIMEOUT_MS:-6000}"
POSITIONS_MIN_OK="${PACIFICA_POSITIONS_PROXY_POOL_MIN_OK:-200}"
DETECTOR_MIN_OK="${PACIFICA_DETECTOR_PROXY_POOL_MIN_OK:-200}"

echo "[proxy-refresh] source=${SOURCE_FILE}"

node "${ROOT_DIR}/scripts/test_pacifica_proxies.js" \
  --file "${SOURCE_FILE}" \
  --url "https://api.pacifica.fi/api/v1/info/prices" \
  --timeout-ms "${TEST_TIMEOUT_MS}" \
  --concurrency "${TEST_CONCURRENCY}" \
  --out "${PROBE_OUT}" \
  --working-out "${WORKING_OUT}"

"${ROOT_DIR}/scripts/rebuild_positions_proxy_pool.sh" \
  --source "${WORKING_OUT}" \
  --out "${POSITIONS_OUT}" \
  --parallel 48 \
  --timeout 8 \
  --passes 1 \
  --max-latency-ms 6000 \
  --min-ok "${POSITIONS_MIN_OK}"

"${ROOT_DIR}/scripts/rebuild_detector_proxy_pool.sh" \
  --source "${SOURCE_FILE}" \
  --out "${DETECTOR_OUT}" \
  --parallel 64 \
  --timeout 8 \
  --passes 1 \
  --max-latency-ms 6000 \
  --min-ok "${DETECTOR_MIN_OK}"

echo "[proxy-refresh] working_out=${WORKING_OUT} positions_out=${POSITIONS_OUT} detector_out=${DETECTOR_OUT}"
