#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ -f "${ROOT_DIR}/config/runtime.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/config/runtime.env"
  set +a
fi

REMOTE_HOST="${PACIFICA_LIVE_REMOTE_HOST:-}"
REMOTE_USER="${PACIFICA_LIVE_REMOTE_USER:-root}"
REMOTE_PORT="${PACIFICA_LIVE_REMOTE_PORT:-22}"
REMOTE_ROOT="${PACIFICA_LIVE_REMOTE_ROOT:-/root/pacifica-flow}"
LOCAL_V2_DIR="${ROOT_DIR}/data/wallet_explorer_v2"
REMOTE_V2_DIR="${REMOTE_ROOT}/data/wallet_explorer_v2"
LOCAL_PHASE1_REMOTE_DIR="${LOCAL_V2_DIR}/phase1_remote"
REMOTE_PHASE1_DIR="${REMOTE_V2_DIR}/phase1"
LOCAL_REVIEW_PIPELINE_DIR="${LOCAL_V2_DIR}/review_pipeline"
REMOTE_REVIEW_PIPELINE_DIR="${REMOTE_V2_DIR}/review_pipeline"

if [[ -z "${REMOTE_HOST}" ]]; then
  echo "PACIFICA_LIVE_REMOTE_HOST is not set" >&2
  exit 1
fi

mkdir -p "${LOCAL_V2_DIR}" "${LOCAL_PHASE1_REMOTE_DIR}" "${LOCAL_REVIEW_PIPELINE_DIR}"

SSH_OPTS=(
  -p "${REMOTE_PORT}"
  -o BatchMode=yes
  -o ConnectTimeout=10
  -o StrictHostKeyChecking=accept-new
)

RSYNC_RSH="ssh -p ${REMOTE_PORT} -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new"
REMOTE="${REMOTE_USER}@${REMOTE_HOST}"

for file_name in pending_lane_state.json pending_lane_candidates.json pending_isolated_wallets.json; do
  remote_path="${REMOTE_V2_DIR}/${file_name}"
  local_path="${LOCAL_V2_DIR}/${file_name}"
  if ssh "${SSH_OPTS[@]}" "${REMOTE}" "test -f '${remote_path}'"; then
    rsync -az -e "${RSYNC_RSH}" "${REMOTE}:${remote_path}" "${local_path}" || true
  fi
done

REMOTE_PHASE1_STATE_LIST="${LOCAL_PHASE1_REMOTE_DIR}/.remote_phase1_state_files.txt"
REMOTE_PHASE1_CANDIDATE_LIST="${LOCAL_PHASE1_REMOTE_DIR}/.remote_phase1_candidate_files.txt"

if ssh "${SSH_OPTS[@]}" "${REMOTE}" "test -d '${REMOTE_PHASE1_DIR}'"; then
  find "${LOCAL_PHASE1_REMOTE_DIR}" -maxdepth 1 -type f \( -name 'pending_lane_state_*.json' -o -name 'pending_lane_candidates_*.json' \) -delete
  ssh "${SSH_OPTS[@]}" "${REMOTE}" "find '${REMOTE_PHASE1_DIR}' -maxdepth 1 -type f \\( -name 'pending_lane_state_probe_*.json' -o -name 'pending_lane_state_recovery_*.json' -o -name 'pending_lane_state_proof_*.json' \\) -printf '%f\n' | sort" > "${REMOTE_PHASE1_STATE_LIST}" || true
  ssh "${SSH_OPTS[@]}" "${REMOTE}" "find '${REMOTE_PHASE1_DIR}' -maxdepth 1 -type f \\( -name 'pending_lane_candidates_probe_*.json' -o -name 'pending_lane_candidates_recovery_*.json' -o -name 'pending_lane_candidates_proof_*.json' \\) -printf '%f\n' | sort" > "${REMOTE_PHASE1_CANDIDATE_LIST}" || true
  if [[ -s "${REMOTE_PHASE1_STATE_LIST}" ]]; then
    rsync -az -e "${RSYNC_RSH}" \
      --files-from="${REMOTE_PHASE1_STATE_LIST}" \
      "${REMOTE}:${REMOTE_PHASE1_DIR}/" \
      "${LOCAL_PHASE1_REMOTE_DIR}/" || true
  fi
  if [[ -s "${REMOTE_PHASE1_CANDIDATE_LIST}" ]]; then
    rsync -az -e "${RSYNC_RSH}" \
      --files-from="${REMOTE_PHASE1_CANDIDATE_LIST}" \
      "${REMOTE}:${REMOTE_PHASE1_DIR}/" \
      "${LOCAL_PHASE1_REMOTE_DIR}/" || true
  fi
fi

REMOTE_REVIEW_STATE_PATH="${REMOTE_REVIEW_PIPELINE_DIR}/state.json"
LOCAL_REVIEW_STATE_PATH="${LOCAL_REVIEW_PIPELINE_DIR}/state.json"
if ssh "${SSH_OPTS[@]}" "${REMOTE}" "test -f '${REMOTE_REVIEW_STATE_PATH}'"; then
  rsync -az -e "${RSYNC_RSH}" "${REMOTE}:${REMOTE_REVIEW_STATE_PATH}" "${LOCAL_REVIEW_STATE_PATH}" || true
fi

REMOTE_REVIEW_STATUS_PATH="${REMOTE_REVIEW_PIPELINE_DIR}/wallet_status.json"
LOCAL_REVIEW_STATUS_PATH="${LOCAL_REVIEW_PIPELINE_DIR}/wallet_status.json"
if ssh "${SSH_OPTS[@]}" "${REMOTE}" "test -f '${REMOTE_REVIEW_STATUS_PATH}'"; then
  rsync -az -e "${RSYNC_RSH}" "${REMOTE}:${REMOTE_REVIEW_STATUS_PATH}" "${LOCAL_REVIEW_STATUS_PATH}" || true
fi

echo "synced wallet explorer pending state from ${REMOTE_HOST}"
