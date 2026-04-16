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
AUX_ROOT_DIR="${PACIFICA_AUX_PUBLIC_DETAIL_WORKER_ROOT_DIR:-/root/pacifica-flow}"

if [[ -z "${AUX_HOST}" ]]; then
  echo "aux worker host not configured"
  exit 0
fi

SSH_ARGS=(
  -o StrictHostKeyChecking=accept-new
)
if [[ -f "${AUX_KEY}" ]]; then
  SSH_ARGS+=(-i "${AUX_KEY}")
fi

ssh "${SSH_ARGS[@]}" "${AUX_USER}@${AUX_HOST}" "mkdir -p '${AUX_ROOT_DIR}/data'"
rsync -az --delete -e "ssh ${SSH_ARGS[*]}" \
  "${ROOT_DIR}/data/wallet_explorer_v3/" \
  "${AUX_USER}@${AUX_HOST}:${AUX_ROOT_DIR}/data/wallet_explorer_v3/"
