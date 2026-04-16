#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/root/pacifica-flow"
REMOTE="${PACIFICA_REMOTE_WRITER_HOST:-root@91.107.129.58}"
REMOTE_ROOT="${PACIFICA_REMOTE_WRITER_ROOT:-/root/pacifica-flow}"
LOCK_FILE="/run/pacifica-wallet-explorer-v3-pull.lock"
RSYNC_RSH="${PACIFICA_REMOTE_RSH:-ssh -o StrictHostKeyChecking=accept-new}"

mkdir -p /run
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "wallet_explorer_v3_pull already running"
  exit 0
fi

mkdir -p \
  "${ROOT_DIR}/data/wallet_explorer_v3/shards" \
  "${ROOT_DIR}/data/nextgen/state" \
  "${ROOT_DIR}/data/ui"

remote_exists() {
  ${RSYNC_RSH} "${REMOTE}" "test -f '$1'"
}

STAGING_DIR="${ROOT_DIR}/data/wallet_explorer_v3/.pull-staging"
STAGING_SHARDS_DIR="${STAGING_DIR}/shards"
LIVE_SHARDS_DIR="${ROOT_DIR}/data/wallet_explorer_v3/shards"
PREV_SHARDS_DIR="${ROOT_DIR}/data/wallet_explorer_v3/shards.prev"

rm -rf "${STAGING_DIR}"
mkdir -p "${STAGING_SHARDS_DIR}"

rsync -az --delete -e "${RSYNC_RSH}" \
  "${REMOTE}:${REMOTE_ROOT}/data/wallet_explorer_v3/shards/" \
  "${STAGING_SHARDS_DIR}/"

rsync -az -e "${RSYNC_RSH}" \
  "${REMOTE}:${REMOTE_ROOT}/data/wallet_explorer_v3/manifest.json" \
  "${STAGING_DIR}/manifest.json"

if remote_exists "${REMOTE_ROOT}/data/wallet_explorer_v3/refresh_state.json"; then
  rsync -az -e "${RSYNC_RSH}" \
    "${REMOTE}:${REMOTE_ROOT}/data/wallet_explorer_v3/refresh_state.json" \
    "${STAGING_DIR}/refresh_state.json"
fi

if remote_exists "${REMOTE_ROOT}/data/wallet_explorer_v3/public_endpoint_sources.json"; then
  rsync -az -e "${RSYNC_RSH}" \
    "${REMOTE}:${REMOTE_ROOT}/data/wallet_explorer_v3/public_endpoint_sources.json" \
    "${STAGING_DIR}/public_endpoint_sources.json"
fi

if remote_exists "${REMOTE_ROOT}/data/wallet_explorer_v3/public_endpoint_wallet_details.json"; then
  rsync -az -e "${RSYNC_RSH}" \
    "${REMOTE}:${REMOTE_ROOT}/data/wallet_explorer_v3/public_endpoint_wallet_details.json" \
    "${STAGING_DIR}/public_endpoint_wallet_details.json"
fi

if remote_exists "${REMOTE_ROOT}/data/nextgen/state/wallet-current-state.json"; then
  rsync -az -e "${RSYNC_RSH}" \
    "${REMOTE}:${REMOTE_ROOT}/data/nextgen/state/wallet-current-state.json" \
    "${STAGING_DIR}/wallet-current-state.json"
fi

if remote_exists "${REMOTE_ROOT}/data/nextgen/state/wallet-live-status.json"; then
  rsync -az -e "${RSYNC_RSH}" \
    "${REMOTE}:${REMOTE_ROOT}/data/nextgen/state/wallet-live-status.json" \
    "${STAGING_DIR}/wallet-live-status.json"
fi

if remote_exists "${REMOTE_ROOT}/data/ui/system_status_summary.json"; then
  rsync -az -e "${RSYNC_RSH}" \
    "${REMOTE}:${REMOTE_ROOT}/data/ui/system_status_summary.json" \
    "${STAGING_DIR}/system_status_summary.json"
fi

rm -rf "${PREV_SHARDS_DIR}"
if [[ -d "${LIVE_SHARDS_DIR}" ]]; then
  mv "${LIVE_SHARDS_DIR}" "${PREV_SHARDS_DIR}"
fi
mv "${STAGING_SHARDS_DIR}" "${LIVE_SHARDS_DIR}"
mv "${STAGING_DIR}/manifest.json" "${ROOT_DIR}/data/wallet_explorer_v3/manifest.json"

if [[ -f "${STAGING_DIR}/refresh_state.json" ]]; then
  mv "${STAGING_DIR}/refresh_state.json" "${ROOT_DIR}/data/wallet_explorer_v3/refresh_state.json"
fi
if [[ -f "${STAGING_DIR}/public_endpoint_sources.json" ]]; then
  mv "${STAGING_DIR}/public_endpoint_sources.json" "${ROOT_DIR}/data/wallet_explorer_v3/public_endpoint_sources.json"
fi
if [[ -f "${STAGING_DIR}/public_endpoint_wallet_details.json" ]]; then
  mv "${STAGING_DIR}/public_endpoint_wallet_details.json" "${ROOT_DIR}/data/wallet_explorer_v3/public_endpoint_wallet_details.json"
fi
if [[ -f "${STAGING_DIR}/wallet-current-state.json" ]]; then
  mv "${STAGING_DIR}/wallet-current-state.json" "${ROOT_DIR}/data/nextgen/state/wallet-current-state.json"
fi
if [[ -f "${STAGING_DIR}/wallet-live-status.json" ]]; then
  mv "${STAGING_DIR}/wallet-live-status.json" "${ROOT_DIR}/data/nextgen/state/wallet-live-status.json"
fi
if [[ -f "${STAGING_DIR}/system_status_summary.json" ]]; then
  mv "${STAGING_DIR}/system_status_summary.json" "${ROOT_DIR}/data/ui/system_status_summary.json"
fi

rm -rf "${PREV_SHARDS_DIR}" "${STAGING_DIR}"
