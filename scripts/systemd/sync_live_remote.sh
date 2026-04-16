#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BOOTSTRAP_STATE="false"

if [[ "${1:-}" == "--bootstrap-state" ]]; then
  BOOTSTRAP_STATE="true"
fi

if [[ -f "${ROOT_DIR}/config/runtime.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/config/runtime.env"
  set +a
fi

LIVE_ROLE="$(printf '%s' "${PACIFICA_LIVE_ROLE:-ui}" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')"
if [[ "${LIVE_ROLE}" != "worker" ]]; then
  echo "sync_live_remote.sh is worker-only (PACIFICA_LIVE_ROLE=worker)" >&2
  exit 0
fi

REMOTE_HOST="${PACIFICA_LIVE_REMOTE_HOST:-}"
REMOTE_USER="${PACIFICA_LIVE_REMOTE_USER:-root}"
REMOTE_PORT="${PACIFICA_LIVE_REMOTE_PORT:-22}"
REMOTE_ROOT="${PACIFICA_LIVE_REMOTE_ROOT:-/root/pacifica-flow}"

if [[ -z "${REMOTE_HOST}" ]]; then
  echo "PACIFICA_LIVE_REMOTE_HOST is not set" >&2
  exit 1
fi

SSH_OPTS=(
  -p "${REMOTE_PORT}"
  -o BatchMode=yes
  -o ConnectTimeout=15
  -o StrictHostKeyChecking=accept-new
)

REMOTE="${REMOTE_USER}@${REMOTE_HOST}"

python3 "${ROOT_DIR}/scripts/build_live_wallet_universe.py"

ssh "${SSH_OPTS[@]}" "${REMOTE}" "mkdir -p '${REMOTE_ROOT}/data/live_positions' '${REMOTE_ROOT}/data/ui'"
ssh "${SSH_OPTS[@]}" "${REMOTE}" "mkdir -p '${REMOTE_ROOT}/data/wallet_explorer_v3'"

RSYNC_RSH="ssh -p ${REMOTE_PORT} -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new"

if [[ -f "${ROOT_DIR}/data/wallet_explorer_v3/manifest.json" ]]; then
  rsync -az -e "${RSYNC_RSH}" \
    "${ROOT_DIR}/data/wallet_explorer_v3/" \
    "${REMOTE}:${REMOTE_ROOT}/data/wallet_explorer_v3/"
fi

if [[ -f "${ROOT_DIR}/data/ui/exchange_wallet_metric_history.json" ]]; then
  rsync -az -e "${RSYNC_RSH}" \
    "${ROOT_DIR}/data/ui/exchange_wallet_metric_history.json" \
    "${REMOTE}:${REMOTE_ROOT}/data/ui/exchange_wallet_metric_history.json"
fi

rsync -az -e "${RSYNC_RSH}" \
  "${ROOT_DIR}/data/live_positions/live_wallet_universe.json" \
  "${REMOTE}:${REMOTE_ROOT}/data/live_positions/live_wallet_universe.json"

rsync -az -e "${RSYNC_RSH}" \
  "${ROOT_DIR}/data/live_positions/positions_proxies.txt" \
  "${REMOTE}:${REMOTE_ROOT}/data/live_positions/positions_proxies.txt"

if [[ "${BOOTSTRAP_STATE}" == "true" ]]; then
  rsync -az -e "${RSYNC_RSH}" \
    "${ROOT_DIR}"/data/live_positions/wallet_first_*.json \
    "${ROOT_DIR}/data/live_positions/wallet_activity_triggers.ndjson" \
    "${REMOTE}:${REMOTE_ROOT}/data/live_positions/"
fi

echo "synced live remote state to ${REMOTE_HOST}"
