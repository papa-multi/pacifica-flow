#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/root/pacifica-flow"
LOCK_FILE="/run/pacifica-wallet-explorer-public-detail-worker.lock"

mkdir -p /run
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "wallet_explorer_public_detail_worker already running"
  exit 0
fi

cd "${ROOT_DIR}"

if [[ -f "${ROOT_DIR}/config/runtime.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/config/runtime.env"
  set +a
fi

export NODE_OPTIONS="${NODE_OPTIONS:---max-old-space-size=3072}"
export PACIFICA_PUBLIC_DETAIL_ENRICH_LIMIT="${PACIFICA_PUBLIC_DETAIL_ENRICH_LIMIT:-150}"
export PACIFICA_PUBLIC_DETAIL_ENRICH_CONCURRENCY="${PACIFICA_PUBLIC_DETAIL_ENRICH_CONCURRENCY:-3}"
export PACIFICA_PUBLIC_DETAIL_WORKER_SHARD_COUNT="${PACIFICA_PUBLIC_DETAIL_WORKER_SHARD_COUNT:-2}"
export PACIFICA_PUBLIC_DETAIL_WORKER_SHARD_INDEX="${PACIFICA_PUBLIC_DETAIL_WORKER_SHARD_INDEX:-1}"
export PACIFICA_PUBLIC_DETAIL_LOCAL_WORKERS="${PACIFICA_PUBLIC_DETAIL_LOCAL_WORKERS:-8}"
export PACIFICA_PUBLIC_DETAIL_PROXY_SERVER_SHARD_COUNT="${PACIFICA_PUBLIC_DETAIL_PROXY_SERVER_SHARD_COUNT:-${PACIFICA_PUBLIC_DETAIL_WORKER_SHARD_COUNT}}"
export PACIFICA_PUBLIC_DETAIL_PROXY_SERVER_SHARD_INDEX="${PACIFICA_PUBLIC_DETAIL_PROXY_SERVER_SHARD_INDEX:-${PACIFICA_PUBLIC_DETAIL_WORKER_SHARD_INDEX}}"
export PACIFICA_PUBLIC_DETAIL_HOT_LIMIT="${PACIFICA_PUBLIC_DETAIL_HOT_LIMIT:-160}"
export PACIFICA_PUBLIC_DETAIL_NON_HOT_LIMIT="${PACIFICA_PUBLIC_DETAIL_NON_HOT_LIMIT:-60}"

run_lane() {
  local lane_mode="$1"
  local lane_limit="$2"
  local lane_concurrency="$3"
  local -a lane_pids=()

  for ((worker_index=0; worker_index<PACIFICA_PUBLIC_DETAIL_LOCAL_WORKERS; worker_index+=1)); do
    export PACIFICA_PUBLIC_DETAIL_LOCAL_SUBSHARD_COUNT="${PACIFICA_PUBLIC_DETAIL_LOCAL_WORKERS}"
    export PACIFICA_PUBLIC_DETAIL_LOCAL_SUBSHARD_INDEX="${worker_index}"
    export PACIFICA_PUBLIC_WALLET_DETAIL_CACHE_PATH="${ROOT_DIR}/data/wallet_explorer_v3/public_endpoint_wallet_details.worker-${worker_index}.json"
    export PACIFICA_PUBLIC_DETAIL_STATE_PATH="${ROOT_DIR}/data/wallet_explorer_v3/public_endpoint_wallet_details.worker-${worker_index}.state.json"
    export PACIFICA_PUBLIC_DETAIL_LANE_MODE="${lane_mode}"
    export PACIFICA_PUBLIC_DETAIL_ENRICH_LIMIT="${lane_limit}"
    export PACIFICA_PUBLIC_DETAIL_ENRICH_CONCURRENCY="${lane_concurrency}"
    worker_files+=("${PACIFICA_PUBLIC_WALLET_DETAIL_CACHE_PATH}")
    node scripts/enrich_public_endpoint_wallet_details.js &
    lane_pids+=("$!")
  done

  for pid in "${lane_pids[@]}"; do
    if ! wait "${pid}"; then
      status=1
    fi
  done
}

declare -a worker_files=()
status=0

run_lane hot "${PACIFICA_PUBLIC_DETAIL_HOT_LIMIT}" "${PACIFICA_PUBLIC_DETAIL_ENRICH_CONCURRENCY}"
run_lane non_hot "${PACIFICA_PUBLIC_DETAIL_NON_HOT_LIMIT}" "1"

unset PACIFICA_PUBLIC_DETAIL_LOCAL_SUBSHARD_COUNT
unset PACIFICA_PUBLIC_DETAIL_LOCAL_SUBSHARD_INDEX
unset PACIFICA_PUBLIC_WALLET_DETAIL_CACHE_PATH
unset PACIFICA_PUBLIC_DETAIL_STATE_PATH
unset PACIFICA_PUBLIC_DETAIL_LANE_MODE

if [[ "${#worker_files[@]}" -gt 0 ]]; then
  node scripts/merge_public_endpoint_wallet_details.js "${worker_files[@]}"
fi

exit "${status}"
