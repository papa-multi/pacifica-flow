#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ -f "${ROOT_DIR}/config/runtime.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/config/runtime.env"
  set +a
fi

LIVE_ROLE="$(printf '%s' "${PACIFICA_LIVE_ROLE:-ui}" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')"
if [[ "${LIVE_ROLE}" != "ui" ]]; then
  echo "pull_live_remote.sh is ui-only (PACIFICA_LIVE_ROLE=ui)" >&2
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

LOCAL_DIR="${ROOT_DIR}/data/live_positions"
mkdir -p "${LOCAL_DIR}"
PARTIAL_DIR="${LOCAL_DIR}/.rsync-partial.$$"
mkdir -p "${PARTIAL_DIR}"
LOCAL_UI_DIR="${ROOT_DIR}/data/ui"
LOCAL_REVIEW_DIR="${ROOT_DIR}/data/wallet_explorer_v2/review_pipeline"
mkdir -p "${LOCAL_UI_DIR}" "${LOCAL_REVIEW_DIR}"

SSH_OPTS=(
  -p "${REMOTE_PORT}"
  -o BatchMode=yes
  -o ConnectTimeout=15
  -o ServerAliveInterval=15
  -o ServerAliveCountMax=3
  -o StrictHostKeyChecking=accept-new
)

REMOTE="${REMOTE_USER}@${REMOTE_HOST}"
RSYNC_RSH="ssh -p ${REMOTE_PORT} -o BatchMode=yes -o ConnectTimeout=15 -o ServerAliveInterval=15 -o ServerAliveCountMax=3 -o StrictHostKeyChecking=accept-new"

REMOTE_FILES=(
  "live_wallet_universe.json"
  "wallet_activity_triggers.ndjson"
  "wallet_first_shard_0.json"
  "wallet_first_shard_1.json"
  "wallet_first_shard_2.json"
  "wallet_first_shard_3.json"
  "wallet_first_account_shard_0.json"
  "wallet_first_account_shard_1.json"
  "wallet_first_account_shard_2.json"
  "wallet_first_account_shard_3.json"
)
REMOTE_COPY_FILES=(
  "data/ui/system_status_summary.json"
  "data/wallet_explorer_v2/review_pipeline/state.json"
  "data/wallet_explorer_v2/review_pipeline/wallet_status.json"
)

ssh "${SSH_OPTS[@]}" "${REMOTE}" "test -d '${REMOTE_ROOT}/data/live_positions'"

pull_file() {
  local remote_path="$1"
  local local_dir="$2"
  local attempt=1
  local max_attempts=4

  while (( attempt <= max_attempts )); do
    if rsync -az --whole-file --delay-updates --partial --partial-dir="${PARTIAL_DIR}" --timeout=90 \
      -e "${RSYNC_RSH}" \
      "${REMOTE}:${REMOTE_ROOT}/${remote_path}" \
      "${local_dir}/"; then
      return 0
    fi

    sleep "${attempt}"
    attempt=$((attempt + 1))
  done

  return 1
}

pull_dir() {
  local remote_dir="$1"
  local local_dir="$2"
  local attempt=1
  local max_attempts=4

  while (( attempt <= max_attempts )); do
    if rsync -az --whole-file --delay-updates --partial --partial-dir="${PARTIAL_DIR}" --timeout=120 \
      -e "${RSYNC_RSH}" \
      "${REMOTE}:${REMOTE_ROOT}/${remote_dir}/" \
      "${local_dir}/"; then
      return 0
    fi

    sleep "${attempt}"
    attempt=$((attempt + 1))
  done

  return 1
}

remaining=("${REMOTE_FILES[@]}")
successes=0
failures=0
max_rounds=3

for ((round=1; round<=max_rounds; round++)); do
  if (( ${#remaining[@]} == 0 )); then
    break
  fi

  declare -a next_remaining=()
  for remote_file in "${remaining[@]}"; do
    if pull_file "data/live_positions/${remote_file}" "${LOCAL_DIR}"; then
      successes=$((successes + 1))
    else
      next_remaining+=("${remote_file}")
    fi
  done
  remaining=("${next_remaining[@]}")

  if (( ${#remaining[@]} > 0 )); then
    sleep $((round))
  fi
done

failures=${#remaining[@]}

if (( successes == 0 )); then
  echo "ERROR: no live remote files were pulled from ${REMOTE_HOST}" >&2
  exit 1
fi

dir_successes=0
dir_failures=0
for remote_file in "${REMOTE_COPY_FILES[@]}"; do
  case "${remote_file}" in
    data/ui/system_status_summary.json)
      local_dir="${LOCAL_UI_DIR}"
      ;;
    data/wallet_explorer_v2/review_pipeline/state.json|data/wallet_explorer_v2/review_pipeline/wallet_status.json)
      local_dir="${LOCAL_REVIEW_DIR}"
      ;;
    *)
      continue
      ;;
  esac
  if pull_file "${remote_file}" "${local_dir}"; then
    dir_successes=$((dir_successes + 1))
  else
    dir_failures=$((dir_failures + 1))
  fi
done

if (( failures > 0 || dir_failures > 0 )); then
  echo "pulled ${successes}/${#REMOTE_FILES[@]} live remote files and ${dir_successes}/${#REMOTE_COPY_FILES[@]} state payloads from ${REMOTE_HOST} with ${failures} file deferred and ${dir_failures} dir/file deferred" >&2
else
  echo "pulled live remote state and review snapshots from ${REMOTE_HOST}"
fi

rm -rf "${PARTIAL_DIR}"
