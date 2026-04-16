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
LOCAL_V3_MANIFEST="${ROOT_DIR}/data/wallet_explorer_v3/manifest.json"
LOCAL_HEAVY_DATASET="${LOCAL_V2_DIR}/heavy_wallet_review_v3.json"
LOCAL_HEAVY_WALLETS="${LOCAL_V2_DIR}/heavy_wallets_remote.txt"
LOCAL_SYNC_STATE_DIR="${ROOT_DIR}/data/runtime/status"
LOCAL_LAST_MATERIALIZE_MARKER="${LOCAL_SYNC_STATE_DIR}/wallet_explorer_heavy_remote_materialize_ms"
REMOTE_V2_DIR="${REMOTE_ROOT}/data/wallet_explorer_v2"

HEAVY_MIN_PAGES="${PACIFICA_WALLET_EXPLORER_V2_HEAVY_MIN_PAGES:-120}"
HEAVY_MIN_ROWS="${PACIFICA_WALLET_EXPLORER_V2_HEAVY_MIN_ROWS:-50000}"
HEAVY_MIN_VOLUME_USD="${PACIFICA_WALLET_EXPLORER_V2_HEAVY_MIN_VOLUME_USD:-25000000}"

if [[ -z "${REMOTE_HOST}" ]]; then
  echo "PACIFICA_LIVE_REMOTE_HOST is not set" >&2
  exit 1
fi

mkdir -p "${LOCAL_V2_DIR}" "${LOCAL_SYNC_STATE_DIR}"

python3 - <<'PY' "${LOCAL_V3_MANIFEST}" "${LOCAL_HEAVY_DATASET}" "${LOCAL_HEAVY_WALLETS}" "${HEAVY_MIN_PAGES}" "${HEAVY_MIN_ROWS}" "${HEAVY_MIN_VOLUME_USD}"
import json
import sys
import time
from pathlib import Path

v3_manifest_path = Path(sys.argv[1])
heavy_dataset_path = Path(sys.argv[2])
wallets_path = Path(sys.argv[3])
min_pages = max(1, int(sys.argv[4]))
min_rows = max(1, int(sys.argv[5]))
min_volume_usd = float(sys.argv[6])

payload = {}
if v3_manifest_path.exists():
    try:
        payload = json.loads(v3_manifest_path.read_text())
    except Exception:
        payload = {}

rows = payload.get("rows") or []
if not isinstance(rows, list):
    rows = []

selected = []
for row in rows:
    if not isinstance(row, dict):
        continue
    if bool(row.get("backfillComplete")):
        continue
    wallet = str(row.get("wallet") or "").strip()
    if not wallet:
        continue
    trade_pages = int(((row.get("tradePagination") or {}).get("fetchedPages")) or 0)
    funding_pages = int(((row.get("fundingPagination") or {}).get("fetchedPages")) or 0)
    trade_rows = int(row.get("tradeRowsLoaded") or row.get("trades") or 0)
    volume_usd = float(row.get("volumeUsdRaw") or 0.0)
    if max(trade_pages, funding_pages) < min_pages and trade_rows < min_rows and volume_usd < min_volume_usd:
        continue
    selected.append(row)

selected.sort(
    key=lambda row: (
        -int(row.get("tradeRowsLoaded") or row.get("trades") or 0),
        -float(row.get("volumeUsdRaw") or 0.0),
        str(row.get("wallet") or ""),
    )
)

now_ms = int(time.time() * 1000)
dataset_payload = {
    "version": 1,
    "generatedAt": now_ms,
    "sourceGeneratedAt": payload.get("generatedAt"),
    "count": len(selected),
    "rows": selected,
}

heavy_dataset_path.write_text(json.dumps(dataset_payload, separators=(",", ":")))
wallets_path.write_text("".join(f"{row['wallet']}\n" for row in selected))
PY

SSH_OPTS=(
  -p "${REMOTE_PORT}"
  -o BatchMode=yes
  -o ConnectTimeout=15
  -o StrictHostKeyChecking=accept-new
)

RSYNC_RSH="ssh -p ${REMOTE_PORT} -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new"
REMOTE="${REMOTE_USER}@${REMOTE_HOST}"

ssh "${SSH_OPTS[@]}" "${REMOTE}" "mkdir -p '${REMOTE_V2_DIR}/isolated' '${REMOTE_ROOT}/data/runtime/status'"

rsync -az -e "${RSYNC_RSH}" \
  "${LOCAL_HEAVY_DATASET}" \
  "${REMOTE}:${REMOTE_V2_DIR}/heavy_wallet_review_v3.json"

while IFS= read -r wallet; do
  [[ -n "${wallet}" ]] || continue
  local_dir="${LOCAL_V2_DIR}/isolated/${wallet}/"
  [[ -d "${local_dir}" ]] || continue
  rsync -az -e "${RSYNC_RSH}" \
    "${local_dir}" \
    "${REMOTE}:${REMOTE_V2_DIR}/isolated/${wallet}/"
done < "${LOCAL_HEAVY_WALLETS}"

changed=0

for file_name in heavy_lane_state.json heavy_lane_candidates.json; do
  remote_path="${REMOTE_V2_DIR}/${file_name}"
  local_path="${LOCAL_V2_DIR}/${file_name}"
  if ssh "${SSH_OPTS[@]}" "${REMOTE}" "test -f '${remote_path}'"; then
    output="$(rsync -az -i -e "${RSYNC_RSH}" "${REMOTE}:${remote_path}" "${local_path}" || true)"
    if [[ -n "${output}" ]]; then
      changed=1
    fi
  fi
done

while IFS= read -r wallet; do
  [[ -n "${wallet}" ]] || continue
  remote_dir="${REMOTE_V2_DIR}/isolated/${wallet}/"
  local_dir="${LOCAL_V2_DIR}/isolated/${wallet}/"
  if ssh "${SSH_OPTS[@]}" "${REMOTE}" "test -d '${remote_dir}'"; then
    output="$(rsync -az -i -e "${RSYNC_RSH}" "${REMOTE}:${remote_dir}" "${local_dir}" || true)"
    if [[ -n "${output}" ]]; then
      changed=1
    fi
  fi
done < "${LOCAL_HEAVY_WALLETS}"

if [[ "${changed}" == "1" ]]; then
  now_ms="$(date +%s%3N)"
  last_ms="0"
  if [[ -f "${LOCAL_LAST_MATERIALIZE_MARKER}" ]]; then
    last_ms="$(cat "${LOCAL_LAST_MATERIALIZE_MARKER}" 2>/dev/null || echo 0)"
  fi
  if [[ $((now_ms - last_ms)) -ge 300000 ]]; then
    systemctl start --no-block pacifica-wallet-explorer-v2-materializer.service || true
    printf '%s\n' "${now_ms}" > "${LOCAL_LAST_MATERIALIZE_MARKER}"
  fi
fi

echo "synced wallet explorer heavy lane to ${REMOTE_HOST}"
