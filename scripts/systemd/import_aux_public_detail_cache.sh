#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/root/pacifica-flow"
if [[ -f "${ROOT_DIR}/config/runtime.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/config/runtime.env"
  set +a
fi
AUX_HOST="${PACIFICA_AUX_PUBLIC_DETAIL_WORKER_HOST:-}"
AUX_USER="${PACIFICA_AUX_PUBLIC_DETAIL_WORKER_USER:-root}"
AUX_KEY="${PACIFICA_AUX_PUBLIC_DETAIL_WORKER_KEY:-/root/.ssh/pacifica_aux_worker}"
AUX_REMOTE_CACHE_PATH="${PACIFICA_AUX_PUBLIC_DETAIL_REMOTE_CACHE_PATH:-/root/pacifica-flow/data/wallet_explorer_v3/public_endpoint_wallet_details.json}"
STAGING_DIR="${PACIFICA_AUX_PUBLIC_DETAIL_STAGING_DIR:-${ROOT_DIR}/data/wallet_explorer_v3/.aux_import}"

if [[ -z "${AUX_HOST}" ]]; then
  echo "aux worker host not configured"
  exit 0
fi

mkdir -p "${STAGING_DIR}"
STAGING_FILE="${STAGING_DIR}/$(echo "${AUX_HOST}" | tr '.:' '__')_public_endpoint_wallet_details.json"

SSH_ARGS=(
  -o StrictHostKeyChecking=accept-new
)
if [[ -f "${AUX_KEY}" ]]; then
  SSH_ARGS+=(-i "${AUX_KEY}")
fi

rsync -az -e "ssh ${SSH_ARGS[*]}" "${AUX_USER}@${AUX_HOST}:${AUX_REMOTE_CACHE_PATH}" "${STAGING_FILE}"
cd "${ROOT_DIR}"
node scripts/merge_public_endpoint_wallet_details.js "${STAGING_FILE}"
