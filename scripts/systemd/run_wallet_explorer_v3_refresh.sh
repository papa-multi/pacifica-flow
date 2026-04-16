#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/root/pacifica-flow"
LOCK_FILE="/run/pacifica-wallet-explorer-v3-refresh.lock"

mkdir -p /run
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "wallet_explorer_v3_refresh already running"
  exit 0
fi

cd "${ROOT_DIR}"

if [[ -f "${ROOT_DIR}/config/runtime.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/config/runtime.env"
  set +a
fi

export NODE_OPTIONS="${NODE_OPTIONS:---max-old-space-size=4096}"
export PACIFICA_PUBLIC_DETAIL_ENRICH_LIMIT="${PACIFICA_PUBLIC_DETAIL_ENRICH_LIMIT:-300}"
export PACIFICA_PUBLIC_DETAIL_ENRICH_CONCURRENCY="${PACIFICA_PUBLIC_DETAIL_ENRICH_CONCURRENCY:-3}"
export PACIFICA_PUBLIC_DETAIL_WORKER_SHARD_COUNT="${PACIFICA_PUBLIC_DETAIL_WORKER_SHARD_COUNT:-2}"
export PACIFICA_PUBLIC_DETAIL_WORKER_SHARD_INDEX="${PACIFICA_PUBLIC_DETAIL_WORKER_SHARD_INDEX:-0}"
export PACIFICA_PUBLIC_DETAIL_LOCAL_SUBSHARD_COUNT="${PACIFICA_PUBLIC_DETAIL_LOCAL_SUBSHARD_COUNT:-1}"
export PACIFICA_PUBLIC_DETAIL_LOCAL_SUBSHARD_INDEX="${PACIFICA_PUBLIC_DETAIL_LOCAL_SUBSHARD_INDEX:-0}"
export PACIFICA_PUBLIC_DETAIL_PROXY_SERVER_SHARD_COUNT="${PACIFICA_PUBLIC_DETAIL_PROXY_SERVER_SHARD_COUNT:-${PACIFICA_PUBLIC_DETAIL_WORKER_SHARD_COUNT}}"
export PACIFICA_PUBLIC_DETAIL_PROXY_SERVER_SHARD_INDEX="${PACIFICA_PUBLIC_DETAIL_PROXY_SERVER_SHARD_INDEX:-${PACIFICA_PUBLIC_DETAIL_WORKER_SHARD_INDEX}}"
export PACIFICA_PUBLIC_DETAIL_LANE_MODE="${PACIFICA_PUBLIC_DETAIL_LANE_MODE:-hot}"
export PACIFICA_BUILD_REVIEW_SNAPSHOT_ON_V3_REFRESH="${PACIFICA_BUILD_REVIEW_SNAPSHOT_ON_V3_REFRESH:-false}"

if [[ -n "${PACIFICA_AUX_PUBLIC_DETAIL_WORKER_HOST:-}" ]]; then
  bash scripts/systemd/import_aux_public_detail_cache.sh || true
fi

node scripts/enrich_public_endpoint_wallet_details.js || true
node scripts/refresh_wallet_explorer_v3_incremental.js "$@"
if [[ "${PACIFICA_BUILD_REVIEW_SNAPSHOT_ON_V3_REFRESH}" == "true" ]]; then
  python3 scripts/wallet_explorer_v2/build_review_pipeline_snapshot.py
fi
