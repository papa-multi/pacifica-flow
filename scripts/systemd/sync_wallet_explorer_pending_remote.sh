#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOCK_DIR="${ROOT_DIR}/data/runtime/locks"
LOCK_FILE="${LOCK_DIR}/wallet_explorer_pending_remote_sync.lock"

mkdir -p "${LOCK_DIR}"
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "pending remote sync already running" >&2
  exit 0
fi

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
LOCAL_V3_DIR="${ROOT_DIR}/data/wallet_explorer_v3"
LOCAL_V3_MANIFEST="${LOCAL_V3_DIR}/manifest.json"
LOCAL_LIVE_UNIVERSE="${ROOT_DIR}/data/live_positions/live_wallet_universe.json"
LOCAL_DISCOVERY_FILE="${ROOT_DIR}/data/indexer/wallet_discovery.json"
LOCAL_WALLET_METRIC_HISTORY_FILE="${ROOT_DIR}/data/ui/exchange_wallet_metric_history.json"
LOCAL_PROXY_FILE="${ROOT_DIR}/data/indexer/working_proxies.txt"
LOCAL_HEAVY_DATASET="${LOCAL_V2_DIR}/heavy_wallet_review_v3.json"
LOCAL_PENDING_DATASET="${LOCAL_V2_DIR}/pending_wallet_review_v3.json"
LOCAL_PENDING_WALLETS="${LOCAL_V2_DIR}/pending_wallets_remote.txt"
LOCAL_TRACKER_POOL_SNAPSHOT="${LOCAL_V2_DIR}/tracker_pool_snapshot.json"
LOCAL_SHARDS_DIR="${LOCAL_V2_DIR}/shards"
LOCAL_ISOLATED_DIR="${LOCAL_V2_DIR}/isolated"
LOCAL_SYNC_STATE_DIR="${ROOT_DIR}/data/runtime/status"
REMOTE_V2_DIR="${REMOTE_ROOT}/data/wallet_explorer_v2"
LOCAL_PENDING_PATHS="${LOCAL_V2_DIR}/pending_wallet_paths_local.txt"
REMOTE_PENDING_PATHS="${LOCAL_V2_DIR}/pending_wallet_paths_remote.txt"
LOCAL_PENDING_REMOTE_RECORD_PATHS="${LOCAL_V2_DIR}/pending_wallet_record_paths_remote.txt"
LOCAL_RECENT_REMOTE_ISOLATED_PATHS="${LOCAL_V2_DIR}/recent_wallet_paths_remote.txt"
LOCAL_PENDING_REMOTE_RECORD_SYNC_MARKER="${LOCAL_SYNC_STATE_DIR}/wallet_explorer_pending_remote_record_sync_ms"

if [[ -z "${REMOTE_HOST}" ]]; then
  echo "PACIFICA_LIVE_REMOTE_HOST is not set" >&2
  exit 1
fi

mkdir -p "${LOCAL_V2_DIR}" "${LOCAL_SYNC_STATE_DIR}"

SSH_OPTS=(
  -p "${REMOTE_PORT}"
  -o BatchMode=yes
  -o ConnectTimeout=15
  -o StrictHostKeyChecking=accept-new
)

RSYNC_RSH="ssh -p ${REMOTE_PORT} -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new"
REMOTE="${REMOTE_USER}@${REMOTE_HOST}"

push_remote_runtime_inputs() {
  ssh "${SSH_OPTS[@]}" "${REMOTE}" "mkdir -p '${REMOTE_ROOT}/data/indexer' '${REMOTE_ROOT}/data/ui' '${REMOTE_ROOT}/data/live_positions'" || true
  if [[ -f "${LOCAL_PROXY_FILE}" ]]; then
    rsync -az -e "${RSYNC_RSH}" "${LOCAL_PROXY_FILE}" "${REMOTE}:${REMOTE_ROOT}/data/indexer/working_proxies.txt" || true
  fi
  if [[ -f "${LOCAL_DISCOVERY_FILE}" ]]; then
    rsync -az -e "${RSYNC_RSH}" "${LOCAL_DISCOVERY_FILE}" "${REMOTE}:${REMOTE_ROOT}/data/indexer/wallet_discovery.json" || true
  fi
  if [[ -f "${LOCAL_WALLET_METRIC_HISTORY_FILE}" ]]; then
    rsync -az -e "${RSYNC_RSH}" "${LOCAL_WALLET_METRIC_HISTORY_FILE}" "${REMOTE}:${REMOTE_ROOT}/data/ui/exchange_wallet_metric_history.json" || true
  fi
  if [[ -d "${LOCAL_V3_DIR}" && -f "${LOCAL_V3_DIR}/manifest.json" ]]; then
    rsync -az -e "${RSYNC_RSH}" "${LOCAL_V3_DIR}/" "${REMOTE}:${REMOTE_ROOT}/data/wallet_explorer_v3/" || true
  fi
  if [[ -f "${LOCAL_LIVE_UNIVERSE}" ]]; then
    rsync -az -e "${RSYNC_RSH}" "${LOCAL_LIVE_UNIVERSE}" "${REMOTE}:${REMOTE_ROOT}/data/live_positions/live_wallet_universe.json" || true
  fi
}

push_remote_runtime_inputs

pull_remote_pending_fast_state() {
  local file_name remote_path local_path
  for file_name in pending_lane_state.json pending_lane_candidates.json pending_isolated_wallets.json; do
    remote_path="${REMOTE_V2_DIR}/${file_name}"
    local_path="${LOCAL_V2_DIR}/${file_name}"
    if ssh "${SSH_OPTS[@]}" "${REMOTE}" "test -f '${remote_path}'"; then
      rsync -az -e "${RSYNC_RSH}" "${REMOTE}:${remote_path}" "${local_path}" || true
    fi
  done
}

pull_remote_recent_pending_records() {
  local initial_lookback_minutes now_ms last_sync_ms cutoff_ms
  initial_lookback_minutes="${PACIFICA_PENDING_REMOTE_SYNC_INITIAL_RECORD_LOOKBACK_MINUTES:-15}"
  now_ms="$(date +%s%3N)"
  if [[ -f "${LOCAL_PENDING_REMOTE_RECORD_SYNC_MARKER}" ]]; then
    last_sync_ms="$(cat "${LOCAL_PENDING_REMOTE_RECORD_SYNC_MARKER}" 2>/dev/null || echo 0)"
  else
    last_sync_ms="$(( now_ms - (initial_lookback_minutes * 60 * 1000) ))"
  fi
  cutoff_ms="$(( last_sync_ms - 300000 ))"
  if ssh "${SSH_OPTS[@]}" "${REMOTE}" "test -d '${REMOTE_V2_DIR}/isolated'"; then
    ssh "${SSH_OPTS[@]}" "${REMOTE}" "python3 - <<'PY' '${REMOTE_V2_DIR}/isolated' '${cutoff_ms}'
import os
import sys
from pathlib import Path

isolated_dir = Path(sys.argv[1])
cutoff_ms = max(0, int(sys.argv[2]))

rows = []
if isolated_dir.is_dir():
    for wallet_dir in isolated_dir.iterdir():
        if not wallet_dir.is_dir():
            continue
        record_path = wallet_dir / 'wallet_record.json'
        if not record_path.exists():
            continue
        try:
            mtime_ms = int(record_path.stat().st_mtime * 1000)
        except Exception:
            continue
        if mtime_ms >= cutoff_ms:
            rows.append((wallet_dir.name, mtime_ms))

for wallet, _mtime_ms in sorted(rows, key=lambda item: (item[1], item[0]), reverse=True):
    print(f'{wallet}/wallet_record.json')
PY" > "${LOCAL_PENDING_REMOTE_RECORD_PATHS}" || true
    if [[ -s "${LOCAL_PENDING_REMOTE_RECORD_PATHS}" ]]; then
      mkdir -p "${LOCAL_V2_DIR}/isolated"
      rsync -az -e "${RSYNC_RSH}" \
        --files-from="${LOCAL_PENDING_REMOTE_RECORD_PATHS}" \
        "${REMOTE}:${REMOTE_V2_DIR}/isolated/" \
        "${LOCAL_V2_DIR}/isolated/" || true
    fi
    printf '%s\n' "${now_ms}" > "${LOCAL_PENDING_REMOTE_RECORD_SYNC_MARKER}"
  fi
}

pull_remote_recent_activity_histories() {
  local lookback_hours wallet_limit lookback_ms
  lookback_hours="${PACIFICA_PENDING_REMOTE_SYNC_LOOKBACK_HOURS:-12}"
  wallet_limit="${PACIFICA_PENDING_REMOTE_SYNC_RECENT_WALLET_LIMIT:-500}"
  lookback_ms=$(( lookback_hours * 60 * 60 * 1000 ))
  if ssh "${SSH_OPTS[@]}" "${REMOTE}" "test -f '${REMOTE_V2_DIR}/review_pipeline/wallet_status.json'"; then
    ssh "${SSH_OPTS[@]}" "${REMOTE}" "python3 - <<'PY' '${REMOTE_V2_DIR}/review_pipeline/wallet_status.json' '${REMOTE_V2_DIR}/isolated' '${lookback_ms}' '${wallet_limit}'
import json
import os
import sys
from pathlib import Path

status_path = Path(sys.argv[1])
isolated_dir = Path(sys.argv[2])
lookback_ms = max(60_000, int(sys.argv[3]))
wallet_limit = max(100, int(sys.argv[4]))
now_ms = int(__import__('time').time() * 1000)
cutoff = now_ms - lookback_ms

try:
    payload = json.loads(status_path.read_text())
except Exception:
    payload = {}
rows = []
if isinstance(payload, dict):
    rows = payload.get('rows') or payload.get('wallets') or []
elif isinstance(payload, list):
    rows = payload

ranked = []
for row in rows:
    if not isinstance(row, dict):
        continue
    wallet = str(row.get('wallet') or '').strip()
    if not wallet:
        continue
    last_trade_at = int(row.get('last_trade_at') or 0)
    verified_at = int(row.get('verified_through_at') or 0)
    updated_at = int(row.get('updated_at') or 0)
    freshness = max(last_trade_at, verified_at, updated_at)
    if freshness < cutoff:
        continue
    ranked.append((freshness, wallet))

seen = set()
selected_wallets = []
for _freshness, wallet in sorted(ranked, reverse=True):
    if wallet in seen:
        continue
    seen.add(wallet)
    selected_wallets.append(wallet)
    if len(selected_wallets) >= wallet_limit:
        break

names = ['wallet_state.json', 'wallet_history.json', 'wallet_record.json', 'wallet_history_log.ndjson']
for wallet in selected_wallets:
    base = isolated_dir / wallet
    if not base.is_dir():
        continue
    for name in names:
        path = base / name
        if path.exists():
            print(f'{wallet}/{name}')
PY" > "${LOCAL_RECENT_REMOTE_ISOLATED_PATHS}" || true
    if [[ -s "${LOCAL_RECENT_REMOTE_ISOLATED_PATHS}" ]]; then
      mkdir -p "${LOCAL_V2_DIR}/isolated"
      rsync -az -e "${RSYNC_RSH}" \
        --files-from="${LOCAL_RECENT_REMOTE_ISOLATED_PATHS}" \
        "${REMOTE}:${REMOTE_V2_DIR}/isolated/" \
        "${LOCAL_V2_DIR}/isolated/" || true
    fi
  fi
}

pull_remote_pending_fast_state
pull_remote_recent_pending_records
pull_remote_recent_activity_histories

python3 - <<'PY' "${LOCAL_V3_MANIFEST}" "${LOCAL_LIVE_UNIVERSE}" "${LOCAL_DISCOVERY_FILE}" "${LOCAL_HEAVY_DATASET}" "${LOCAL_SHARDS_DIR}" "${LOCAL_ISOLATED_DIR}" "${LOCAL_PENDING_DATASET}" "${LOCAL_PENDING_WALLETS}" "${LOCAL_TRACKER_POOL_SNAPSHOT}"
import csv
import json
import os
import sys
import time
from pathlib import Path

v3_manifest_path = Path(sys.argv[1])
live_path = Path(sys.argv[2])
discovery_path = Path(sys.argv[3])
heavy_path = Path(sys.argv[4])
shards_dir = Path(sys.argv[5])
isolated_dir = Path(sys.argv[6])
pending_dataset_path = Path(sys.argv[7])
wallets_path = Path(sys.argv[8])
pool_snapshot_path = Path(sys.argv[9])

NEW_INTAKE_WINDOW_MS = max(60_000, int(os.getenv("PACIFICA_WALLET_EXPLORER_V2_NEW_INTAKE_WINDOW_MS", str(48 * 60 * 60 * 1000))))
TRACKED_LIVE_WINDOW_MS = max(60_000, int(os.getenv("PACIFICA_WALLET_EXPLORER_V2_TRACKED_LIVE_WINDOW_MS", str(15 * 60 * 1000))))
STEADY_INTAKE_PCT = max(0, min(100, int(os.getenv("PACIFICA_WALLET_EXPLORER_V2_STEADY_INTAKE_LANE_PCT", "2"))))
STEADY_REFRESH_PCT = max(0, min(100, int(os.getenv("PACIFICA_WALLET_EXPLORER_V2_STEADY_REFRESH_LANE_PCT", "80"))))
LIVE_POSITIONS_TARGET_PCT = max(0, min(100, int(os.getenv("PACIFICA_LIVE_POSITIONS_TARGET_PCT", "15"))))
FOCUS_COHORT_CSV = str(os.getenv("PACIFICA_WALLET_EXPLORER_V2_FOCUS_COHORT_CSV", "") or "").strip()

def load_json(path: Path, fallback):
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text())
    except Exception:
        return fallback

def normalize_wallet(value):
    text = str(value or "").strip()
    return text or None

def load_focus_wallets(path_text: str):
    path_text = str(path_text or "").strip()
    if not path_text:
        return set()
    path = Path(path_text)
    if not path.exists():
        return set()
    wallets = set()
    try:
        with path.open(newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames and "wallet" in reader.fieldnames:
                for row in reader:
                    wallet = normalize_wallet((row or {}).get("wallet"))
                    if wallet:
                        wallets.add(wallet)
            else:
                handle.seek(0)
                for line in handle:
                    wallet = normalize_wallet(str(line or "").split(",")[0])
                    if wallet and wallet.lower() != "wallet":
                        wallets.add(wallet)
    except Exception:
        return set()
    return wallets

def stable_shard_hash(value: str) -> int:
    text = str(value or "")
    hash_value = 5381
    for char in text:
        hash_value = ((hash_value << 5) + hash_value + ord(char)) & 0xFFFFFFFF
    return hash_value

def shard_index_for_wallet(wallet: str, shard_count: int = 8) -> int:
    return stable_shard_hash(f"wallet:{normalize_wallet(wallet) or ''}") % max(1, int(shard_count))

def read_wallet_pack(wallet: str):
    shard_index = shard_index_for_wallet(wallet)
    owner_base = shards_dir / f"shard_{shard_index}"
    owner_state_path = owner_base / "wallet_state" / f"{wallet}.json"
    owner_history_path = owner_base / "wallet_history" / f"{wallet}.json"
    owner_record_path = owner_base / "wallet_records" / f"{wallet}.json"
    isolated_base = isolated_dir / wallet
    isolated_state_path = isolated_base / "wallet_state.json"
    isolated_history_path = isolated_base / "wallet_history.json"
    isolated_record_path = isolated_base / "wallet_record.json"
    return {
        "owner": {
            "shardIndex": shard_index,
            "statePath": owner_state_path,
            "historyPath": owner_history_path,
            "recordPath": owner_record_path,
            "state": load_json(owner_state_path, None),
            "history": load_json(owner_history_path, None),
            "record": load_json(owner_record_path, None),
        },
        "isolated": {
            "statePath": isolated_state_path,
            "historyPath": isolated_history_path,
            "recordPath": isolated_record_path,
            "state": load_json(isolated_state_path, None),
            "history": load_json(isolated_history_path, None),
            "record": load_json(isolated_record_path, None),
        },
    }

def state_score(state, history):
    state = state if isinstance(state, dict) else {}
    history = history if isinstance(history, dict) else {}
    streams = state.get("streams") or {}
    trades = streams.get("trades") or {}
    funding = streams.get("funding") or {}
    trade_rows = len(history.get("trades") or [])
    funding_rows = len(history.get("funding") or [])
    return (
        1 if trades.get("exhausted") else 0,
        1 if funding.get("exhausted") else 0,
        trade_rows,
        funding_rows,
        int(trades.get("pagesFetched") or 0),
        int(funding.get("pagesFetched") or 0),
        int(state.get("updatedAt") or 0),
        int(history.get("updatedAt") or 0),
    )

def choose_best_pack(packs):
    best = None
    best_score = None
    for label, pack in packs.items():
        score = state_score(pack.get("state"), pack.get("history"))
        if best_score is None or score > best_score:
            best = (label, pack)
            best_score = score
    return best

def record_score(record):
    record = record if isinstance(record, dict) else {}
    proof = record.get("proof") if isinstance(record.get("proof"), dict) else {}
    local = proof.get("local") if isinstance(proof.get("local"), dict) else {}
    head = proof.get("head") if isinstance(proof.get("head"), dict) else {}
    return (
        1 if bool(record.get("historyVerifiedThroughNow")) else 0,
        1 if bool(head.get("complete")) else 0,
        1 if bool(local.get("complete")) or bool(record.get("backfillComplete")) else 0,
        int(record.get("tradeRowsLoaded") or 0),
        int(record.get("fundingRowsLoaded") or 0),
        int(record.get("updatedAt") or 0),
    )

def choose_best_record_pack(packs):
    best = None
    best_score = None
    for label, pack in packs.items():
        score = record_score(pack.get("record"))
        if best_score is None or score > best_score:
            best = (label, pack)
            best_score = score
    return best

def latest_timestamp(rows, key="createdAt"):
    latest = 0
    for row in rows or []:
        value = int((row or {}).get(key) or 0)
        if value > latest:
            latest = value
    return latest

def compute_local_proof(wallet: str, pack: dict, compact_row: dict | None):
    state = pack.get("state") if isinstance(pack.get("state"), dict) else {}
    history = pack.get("history") if isinstance(pack.get("history"), dict) else {}
    record = pack.get("record") if isinstance(pack.get("record"), dict) else {}
    record_proof = record.get("proof") if isinstance(record.get("proof"), dict) else {}
    record_proof_local = record_proof.get("local") if isinstance(record_proof.get("local"), dict) else {}
    trade_stream = ((state.get("streams") or {}).get("trades")) or {}
    funding_stream = ((state.get("streams") or {}).get("funding")) or {}
    trades = history.get("trades") or []
    funding = history.get("funding") or []
    now = int(time.time() * 1000)
    retry_pending = any(
        not stream.get("exhausted") and int(stream.get("nextEligibleAt") or 0) > now
        for stream in (trade_stream, funding_stream)
    ) or bool(trade_stream.get("failedCursor")) or bool(funding_stream.get("failedCursor"))
    trade_frontier_clear = not bool(trade_stream.get("frontierCursor")) and not bool(trade_stream.get("failedCursor"))
    funding_frontier_clear = not bool(funding_stream.get("frontierCursor")) and not bool(funding_stream.get("failedCursor"))
    trade_rows_loaded = len(trades)
    funding_rows_loaded = len(funding)
    record_trade_rows = int(record.get("tradeRowsLoaded") or 0)
    record_funding_rows = int(record.get("fundingRowsLoaded") or 0)
    files_present = {
        "state": bool(pack.get("statePath") and pack["statePath"].exists()),
        "history": bool(pack.get("historyPath") and pack["historyPath"].exists()),
        "record": bool(pack.get("recordPath") and pack["recordPath"].exists()),
    }
    local_complete = (
        files_present["state"]
        and files_present["history"]
        and files_present["record"]
        and bool(trade_stream.get("exhausted"))
        and bool(funding_stream.get("exhausted"))
        and trade_frontier_clear
        and funding_frontier_clear
        and not retry_pending
        and bool(record.get("backfillComplete"))
        and record_trade_rows == trade_rows_loaded
        and record_funding_rows == funding_rows_loaded
    )
    record_last_trade = int(record.get("lastTrade") or 0)
    record_last_funding = int(record.get("lastFundingAt") or 0)
    compact_last_trade = int((compact_row or {}).get("lastTrade") or 0)
    if local_complete:
        proof_class = "local_complete_head_pending"
    elif any(files_present.values()) or trade_rows_loaded > 0 or funding_rows_loaded > 0:
        proof_class = "partial_backfill"
    elif compact_last_trade > 0:
        proof_class = "compact_only_unproven"
    else:
        proof_class = "untracked"
    if record_proof_local:
        local_complete = bool(record_proof_local.get("complete")) or local_complete
        proof_class = "local_complete_head_pending" if local_complete else proof_class
    return {
        "source": pack.get("label") or "owner",
        "filesPresent": files_present,
        "tradeExhausted": bool(record_proof_local.get("tradeExhausted")) if record_proof_local else bool(trade_stream.get("exhausted")),
        "fundingExhausted": bool(record_proof_local.get("fundingExhausted")) if record_proof_local else bool(funding_stream.get("exhausted")),
        "tradeFrontierClear": bool(record_proof_local.get("tradeFrontierClear")) if record_proof_local else trade_frontier_clear,
        "fundingFrontierClear": bool(record_proof_local.get("fundingFrontierClear")) if record_proof_local else funding_frontier_clear,
        "retryClear": bool(record_proof_local.get("retryClear")) if record_proof_local else (not retry_pending),
        "recordBackfillComplete": bool(record_proof_local.get("recordBackfillComplete")) if record_proof_local else bool(record.get("backfillComplete")),
        "tradeRowsLoaded": int(record_proof_local.get("tradeRowsLoaded") or trade_rows_loaded) if record_proof_local else trade_rows_loaded,
        "fundingRowsLoaded": int(record_proof_local.get("fundingRowsLoaded") or funding_rows_loaded) if record_proof_local else funding_rows_loaded,
        "recordTradeRowsLoaded": int(record_proof_local.get("recordTradeRowsLoaded") or record_trade_rows) if record_proof_local else record_trade_rows,
        "recordFundingRowsLoaded": int(record_proof_local.get("recordFundingRowsLoaded") or record_funding_rows) if record_proof_local else record_funding_rows,
        "tradeRowsMatch": bool(record_proof_local.get("tradeRowsMatch")) if record_proof_local else (record_trade_rows == trade_rows_loaded),
        "fundingRowsMatch": bool(record_proof_local.get("fundingRowsMatch")) if record_proof_local else (record_funding_rows == funding_rows_loaded),
        "lastTradeAt": (int(record_proof_local.get("lastTradeAt") or 0) or None) if record_proof_local else (record_last_trade or latest_timestamp(trades) or None),
        "lastFundingAt": (int(record_proof_local.get("lastFundingAt") or 0) or None) if record_proof_local else (record_last_funding or latest_timestamp(funding) or None),
        "complete": local_complete,
        "proofClass": proof_class,
    }

def read_head_proof(record: dict):
    proof = record.get("proof") if isinstance(record.get("proof"), dict) else {}
    head = proof.get("head") if isinstance(proof.get("head"), dict) else {}
    trades = head.get("trades") if isinstance(head.get("trades"), dict) else {}
    funding = head.get("funding") if isinstance(head.get("funding"), dict) else {}
    trade_status = str(trades.get("status") or "").strip().lower()
    funding_status = str(funding.get("status") or "").strip().lower()
    complete = trade_status == "verified" and funding_status == "verified"
    return {
        "checkedAt": int(head.get("checkedAt") or 0) or None,
        "complete": complete,
        "trades": trades,
        "funding": funding,
    }

def compute_progress(local_proof: dict, head_proof: dict, best_record: dict):
    trade_rows_loaded = int(local_proof.get("tradeRowsLoaded") or best_record.get("tradeRowsLoaded") or 0)
    funding_rows_loaded = int(local_proof.get("fundingRowsLoaded") or best_record.get("fundingRowsLoaded") or 0)
    trade_exhausted = bool(local_proof.get("tradeExhausted"))
    funding_exhausted = bool(local_proof.get("fundingExhausted"))
    trade_started = trade_rows_loaded > 0 or int(((best_record.get("tradePagination") or {}).get("fetchedPages")) or 0) > 0
    funding_started = funding_rows_loaded > 0 or int(((best_record.get("fundingPagination") or {}).get("fetchedPages")) or 0) > 0
    stage_score = sum(
        1
        for item in (
            trade_started,
            trade_exhausted,
            funding_started,
            funding_exhausted,
        )
        if item
    )
    local_progress = 70.0 if bool(local_proof.get("complete")) else (stage_score / 4.0) * 70.0
    head_progress = 30.0 if bool(head_proof.get("complete")) else 0.0
    return round(min(100.0, local_progress + head_progress), 1)

def compute_remaining_label(local_proof: dict, head_proof: dict, best_record: dict):
    if bool(local_proof.get("complete")) and bool(head_proof.get("complete")):
        return "verified through now"
    if bool(local_proof.get("complete")):
        trade_status = str(((head_proof.get("trades") or {}).get("status") or "")).strip().lower()
        funding_status = str(((head_proof.get("funding") or {}).get("status") or "")).strip().lower()
        if trade_status in {"remote_newer", "remote_empty"} or funding_status in {"remote_newer", "remote_empty"}:
            return "head proof mismatch"
        if trade_status == "error" or funding_status == "error":
            return "head proof error"
        return "head proof pending"
    trade_exhausted = bool(local_proof.get("tradeExhausted"))
    funding_exhausted = bool(local_proof.get("fundingExhausted"))
    trade_pages_pending = int((((best_record.get("tradePagination") or {}).get("pendingPages")) or 0))
    funding_pages_pending = int((((best_record.get("fundingPagination") or {}).get("pendingPages")) or 0))
    if not trade_exhausted:
        return f"{trade_pages_pending or 1} trade pages pending"
    if not funding_exhausted:
        return f"{funding_pages_pending or 1} funding pages pending"
    return "local proof pending"

def classify_pool(local_proof: dict, head_proof: dict, compact_row: dict | None, best_record: dict, live_row: dict | None, discovery_row: dict | None, now_ms: int):
    trade_rows_loaded = int(local_proof.get("tradeRowsLoaded") or best_record.get("tradeRowsLoaded") or 0)
    funding_rows_loaded = int(local_proof.get("fundingRowsLoaded") or best_record.get("fundingRowsLoaded") or 0)
    compact_trades = int((compact_row or {}).get("trades") or (compact_row or {}).get("tradeRowsLoaded") or 0)
    compact_volume = float((compact_row or {}).get("volumeUsdRaw") or (compact_row or {}).get("volumeUsd") or 0.0)
    compact_last_trade = int((compact_row or {}).get("lastTrade") or 0)
    discovery_first_seen_at = int((discovery_row or {}).get("firstSeenBlockTimeMs") or (discovery_row or {}).get("firstSeenAt") or 0)
    live_open_positions = int((live_row or {}).get("openPositions") or 0)
    has_local_start = (
        bool(local_proof.get("filesPresent", {}).get("state")) or
        bool(local_proof.get("filesPresent", {}).get("history")) or
        bool(local_proof.get("filesPresent", {}).get("record")) or
        trade_rows_loaded > 0 or
        funding_rows_loaded > 0 or
        int((((best_record.get("tradePagination") or {}).get("fetchedPages")) or 0)) > 0 or
        int((((best_record.get("fundingPagination") or {}).get("fetchedPages")) or 0)) > 0
    )
    has_metrics = trade_rows_loaded > 0 or compact_trades > 0 or compact_volume > 0 or compact_last_trade > 0
    is_new_intake = discovery_first_seen_at > 0 and discovery_first_seen_at >= now_ms - NEW_INTAKE_WINDOW_MS

    if bool(local_proof.get("complete")) and bool(head_proof.get("complete")):
        return "tracked_pool"
    if is_new_intake and not has_local_start:
        return "new_intake"
    if not has_metrics and not bool(head_proof.get("complete")):
        return "untracked"
    if not has_local_start and not has_metrics:
        return "untracked"
    if bool(local_proof.get("complete")) and not bool(head_proof.get("complete")):
        return "catch_up"
    if live_open_positions > 0 and not has_metrics:
        return "untracked"
    return "catch_up"

def tracked_live(best_record: dict, head_proof: dict, live_row: dict | None, now_ms: int):
    freshness_ts = max(
        int(head_proof.get("checkedAt") or 0),
        int(best_record.get("headTradeSyncAt") or 0),
        int((live_row or {}).get("updatedAt") or (live_row or {}).get("liveScannedAt") or 0),
    )
    return freshness_ts > 0 and freshness_ts >= now_ms - TRACKED_LIVE_WINDOW_MS

def build_item(wallet: str, compact_row: dict | None, best_record: dict, live_row: dict | None, discovery_row: dict | None, local_proof: dict, head_proof: dict, pool_name: str, now_ms: int):
    compact_safe = compact_row if isinstance(compact_row, dict) else {}
    trade_rows_loaded = int(
        best_record.get("tradeRowsLoaded")
        or local_proof.get("tradeRowsLoaded")
        or compact_safe.get("tradeRowsLoaded")
        or compact_safe.get("trades")
        or 0
    )
    funding_rows_loaded = int(
        best_record.get("fundingRowsLoaded")
        or local_proof.get("fundingRowsLoaded")
        or compact_safe.get("fundingRowsLoaded")
        or 0
    )
    progress_pct = compute_progress(local_proof, head_proof, best_record)
    proof_complete = bool(local_proof.get("complete")) and bool(head_proof.get("complete"))
    open_positions = int(
        (live_row or {}).get("openPositions")
        or best_record.get("openPositions")
        or compact_safe.get("openPositions")
        or 0
    )
    live_updated_at = int((live_row or {}).get("updatedAt") or (live_row or {}).get("liveScannedAt") or 0) or None
    last_trade = int(best_record.get("lastTrade") or local_proof.get("lastTradeAt") or compact_safe.get("lastTrade") or 0) or None
    volume_usd_raw = round(
        float(
            best_record.get("volumeUsdRaw")
            or compact_safe.get("volumeUsdRaw")
            or compact_safe.get("volumeUsd")
            or 0.0
        ),
        2,
    )
    trade_head_status = str(((head_proof.get("trades") or {}).get("status") or "")).strip().lower()
    funding_head_status = str(((head_proof.get("funding") or {}).get("status") or "")).strip().lower()
    local_complete = bool(local_proof.get("complete"))
    tracked_live_now = proof_complete and tracked_live(best_record, head_proof, live_row, now_ms)
    if proof_complete and tracked_live_now:
        status = "good"
    elif proof_complete:
        status = "warn"
    elif local_complete:
        status = "warn" if trade_head_status not in {"error", "remote_newer", "remote_empty"} and funding_head_status not in {"error", "remote_newer", "remote_empty"} else "bad"
    elif trade_rows_loaded > 0 or funding_rows_loaded > 0:
        status = "warn"
    else:
        status = "muted"
    return {
        "wallet": wallet,
        "pool": pool_name,
        "status": status,
        "progressPct": progress_pct,
        "backfillComplete": local_complete,
        "proofLocalComplete": local_complete,
        "proofHeadComplete": bool(head_proof.get("complete")),
        "historyVerifiedThroughNow": proof_complete,
        "trackedLive": tracked_live_now,
        "remainingLabel": compute_remaining_label(local_proof, head_proof, best_record),
        "updatedAt": int(best_record.get("updatedAt") or 0) or None,
        "liveUpdatedAt": live_updated_at,
        "openPositions": open_positions,
        "lastTrade": last_trade,
        "volumeUsdRaw": volume_usd_raw,
        "tradeRowsLoaded": trade_rows_loaded,
        "fundingRowsLoaded": funding_rows_loaded,
        "tradePagesPending": int((((best_record.get("tradePagination") or {}).get("pendingPages")) or 0)),
        "fundingPagesPending": int((((best_record.get("fundingPagination") or {}).get("pendingPages")) or 0)),
        "tradePagesFailed": int((((best_record.get("tradePagination") or {}).get("failedPages")) or 0)),
        "fundingPagesFailed": int((((best_record.get("fundingPagination") or {}).get("failedPages")) or 0)),
        "proofClass": "verified_through_now" if proof_complete else str(local_proof.get("proofClass") or "untracked"),
        "summaryLabel": None,
        "metaLabel": None,
    }

compact_payload = load_json(v3_manifest_path, {}) or {}
compact_rows = compact_payload.get("rows") or []
if not isinstance(compact_rows, list):
    compact_rows = []
compact_by_wallet = {}
for row in compact_rows:
    if not isinstance(row, dict):
        continue
    wallet = str(row.get("wallet") or "").strip()
    if wallet:
        compact_by_wallet[wallet] = row

heavy_payload = load_json(heavy_path, {}) or {}
heavy_rows = heavy_payload.get("rows") or []
if not isinstance(heavy_rows, list):
    heavy_rows = []
heavy_wallets = {
    str(row.get("wallet") or "").strip()
    for row in heavy_rows
    if isinstance(row, dict) and str(row.get("wallet") or "").strip()
}
focus_wallets = load_focus_wallets(FOCUS_COHORT_CSV)
if focus_wallets:
    heavy_wallets = {wallet for wallet in heavy_wallets if wallet not in focus_wallets}

live_payload = load_json(live_path, {}) or {}
live_rows = live_payload.get("wallets") or []
if not isinstance(live_rows, list):
    live_rows = []
live_by_wallet = {}
for row in live_rows:
    if not isinstance(row, dict):
        continue
    wallet = normalize_wallet(row.get("wallet"))
    if wallet:
        live_by_wallet[wallet] = row

discovery_payload = load_json(discovery_path, {}) or {}
discovery_wallets_raw = discovery_payload.get("wallets") or {}
discovery_by_wallet = {}
if isinstance(discovery_wallets_raw, dict):
    for wallet_raw, details in discovery_wallets_raw.items():
        wallet = normalize_wallet(wallet_raw)
        if wallet and isinstance(details, dict):
            discovery_by_wallet[wallet] = details

wallets = (
    set(focus_wallets)
    if focus_wallets
    else (set(compact_by_wallet.keys()) | set(live_by_wallet.keys()) | set(discovery_by_wallet.keys()))
)
selected = []
pool_rows = {
    "untracked": [],
    "new_intake": [],
    "catch_up": [],
    "tracked_pool": [],
    "heavy": [],
}
pool_items = {
    "untracked": [],
    "new_intake": [],
    "catch_up": [],
    "tracked_pool": [],
    "heavy": [],
}
now_ms = int(time.time() * 1000)
for wallet in wallets:
    if not wallet or wallet in heavy_wallets:
        continue
    compact_row = compact_by_wallet.get(wallet)
    live_row = live_by_wallet.get(wallet)
    discovery_row = discovery_by_wallet.get(wallet)
    packs = read_wallet_pack(wallet)
    best_label, best_pack = choose_best_pack(packs)
    best_pack = dict(best_pack or {})
    best_pack["label"] = best_label
    best_record_label, best_record_pack = choose_best_record_pack(packs)
    best_record_pack = dict(best_record_pack or best_pack)
    if not best_record_pack.get("label"):
        best_record_pack["label"] = best_record_label or best_label
    best_record = best_record_pack.get("record") if isinstance(best_record_pack.get("record"), dict) else {}
    local_proof = compute_local_proof(wallet, best_record_pack, compact_row)
    head_proof = read_head_proof(best_record)
    head_complete = bool(head_proof.get("complete"))
    trade_head_status = str(((head_proof.get("trades") or {}).get("status") or "")).strip().lower()
    funding_head_status = str(((head_proof.get("funding") or {}).get("status") or "")).strip().lower()
    head_has_signal = bool(head_proof.get("checkedAt")) or bool(trade_head_status) or bool(funding_head_status)
    base = {}
    if isinstance(compact_row, dict):
        base.update(compact_row)
    if isinstance(best_record, dict):
        base.update(best_record)
    live_updated_at = int((live_row or {}).get("updatedAt") or (live_row or {}).get("liveScannedAt") or 0)
    discovery_first_seen_at = int((discovery_row or {}).get("firstSeenBlockTimeMs") or (discovery_row or {}).get("firstSeenAt") or 0)
    discovery_last_seen_at = int((discovery_row or {}).get("lastSeenBlockTimeMs") or (discovery_row or {}).get("lastSeenAt") or (discovery_row or {}).get("updatedAt") or 0)
    if head_complete:
        proof_class = "verified_through_now"
    else:
        proof_class = local_proof["proofClass"]
    pool_name = classify_pool(local_proof, head_proof, compact_row, best_record, live_row, discovery_row, now_ms)
    row = dict(base)
    row["wallet"] = wallet
    row["backfillComplete"] = bool(best_record.get("backfillComplete"))
    row["tradeRowsLoaded"] = int(best_record.get("tradeRowsLoaded") or row.get("tradeRowsLoaded") or row.get("trades") or 0)
    row["fundingRowsLoaded"] = int(best_record.get("fundingRowsLoaded") or row.get("fundingRowsLoaded") or 0)
    row["tradePagination"] = best_record.get("tradePagination") or row.get("tradePagination") or {}
    row["fundingPagination"] = best_record.get("fundingPagination") or row.get("fundingPagination") or {}
    row["openPositions"] = int((live_row or {}).get("openPositions") or row.get("openPositions") or 0)
    row["liveUpdatedAt"] = live_updated_at or None
    row["liveScannedAt"] = int((live_row or {}).get("liveScannedAt") or live_updated_at or 0) or None
    row["discoveryFirstSeenAt"] = discovery_first_seen_at or None
    row["discoveryLastSeenAt"] = discovery_last_seen_at or None
    row["proofLocalComplete"] = bool(local_proof["complete"])
    row["proofHeadComplete"] = head_complete
    row["proofClass"] = proof_class
    row["historyProofStatus"] = (
        "verified_through_now"
        if head_complete and local_proof["complete"]
        else "local_complete_head_pending"
        if local_proof["complete"]
        else "backfill_incomplete"
    )
    row["historyVerifiedThroughNow"] = bool(head_complete and local_proof["complete"])
    row["proof"] = {
        "local": local_proof,
        "head": head_proof,
        "complete": bool(head_complete and local_proof["complete"]),
    }
    row["poolName"] = pool_name
    row["sourceGroup"] = pool_name
    row["proofSource"] = best_label
    pool_rows.setdefault(pool_name, []).append(row)
    pool_items.setdefault(pool_name, []).append(
        build_item(
            wallet,
            compact_row,
            best_record,
            live_row,
            discovery_row,
            local_proof,
            head_proof,
            pool_name,
            now_ms,
        )
    )
    if pool_name != "tracked_pool":
        selected.append(row)

for wallet in heavy_wallets:
    if not wallet:
        continue
    compact_row = compact_by_wallet.get(wallet)
    live_row = live_by_wallet.get(wallet)
    discovery_row = discovery_by_wallet.get(wallet)
    packs = read_wallet_pack(wallet)
    best_label, best_pack = choose_best_pack(packs)
    best_pack = dict(best_pack or {})
    best_pack["label"] = best_label
    best_record_label, best_record_pack = choose_best_record_pack(packs)
    best_record_pack = dict(best_record_pack or best_pack)
    if not best_record_pack.get("label"):
        best_record_pack["label"] = best_record_label or best_label
    best_record = best_record_pack.get("record") if isinstance(best_record_pack.get("record"), dict) else {}
    local_proof = compute_local_proof(wallet, best_record_pack, compact_row)
    head_proof = read_head_proof(best_record)
    row = {}
    if isinstance(compact_row, dict):
        row.update(compact_row)
    if isinstance(best_record, dict):
        row.update(best_record)
    row["wallet"] = wallet
    row["poolName"] = "heavy"
    row["sourceGroup"] = "heavy"
    row["proofLocalComplete"] = bool(local_proof["complete"])
    row["proofHeadComplete"] = bool(head_proof.get("complete"))
    row["proofClass"] = "verified_through_now" if bool(local_proof["complete"]) and bool(head_proof.get("complete")) else str(local_proof.get("proofClass") or "partial_backfill")
    row["proof"] = {"local": local_proof, "head": head_proof, "complete": bool(local_proof["complete"]) and bool(head_proof.get("complete"))}
    pool_rows["heavy"].append(row)
    pool_items["heavy"].append(
        build_item(
            wallet,
            compact_row,
            best_record,
            live_row,
            discovery_row,
            local_proof,
            head_proof,
            "heavy",
            now_ms,
        )
    )

priority = {
    "new_intake": 0,
    "untracked": 1,
    "compact_only_unproven": 2,
    "partial_backfill": 3,
    "local_complete_head_pending": 4,
    "verified_through_now": 5,
}
rows = list(selected)
rows.sort(
    key=lambda row: (
        0 if str(row.get("poolName") or "") == "new_intake" else 1 if str(row.get("poolName") or "") == "untracked" else 2,
        priority.get(str(row.get("proofClass") or ""), 99),
        -int(row.get("liveUpdatedAt") or row.get("liveScannedAt") or 0),
        -int(row.get("discoveryLastSeenAt") or row.get("updatedAt") or 0),
        -int(row.get("tradeRowsLoaded") or row.get("trades") or 0),
        str(row.get("wallet") or ""),
    )
)

local_complete_count = sum(1 for row in rows if bool(row.get("proofLocalComplete")))
head_complete_count = sum(1 for row in rows if bool(row.get("proofHeadComplete")))
dataset_payload = {
    "version": 1,
    "generatedAt": now_ms,
    "scope": "focus_cohort" if focus_wallets else "global",
    "focusWalletCount": len(focus_wallets),
    "sourceGeneratedAt": compact_payload.get("generatedAt"),
    "liveGeneratedAt": live_payload.get("generatedAt"),
    "discoveryGeneratedAt": discovery_payload.get("generatedAt"),
    "count": len(rows),
    "localProofComplete": local_complete_count,
    "headProofComplete": head_complete_count,
    "rows": rows,
}

pending_dataset_path.write_text(json.dumps(dataset_payload, separators=(",", ":")))
wallets_path.write_text("".join(f"{row['wallet']}\n" for row in rows))

def summarize_group(label, key, rows_in_group):
    items = list(pool_items.get(key) or [])
    if key == "tracked_pool":
        items.sort(
            key=lambda item: (
                0 if not item.get("trackedLive") else 1,
                -(int(item.get("liveUpdatedAt") or 0)),
                -(int(item.get("updatedAt") or 0)),
                item.get("wallet") or "",
            )
        )
    elif key == "new_intake":
        items.sort(
            key=lambda item: (
                -(int(item.get("liveUpdatedAt") or 0)),
                -(int(item.get("updatedAt") or 0)),
                item.get("wallet") or "",
            )
        )
    else:
        items.sort(
            key=lambda item: (
                -(float(item.get("progressPct") or 0.0)),
                -(int(item.get("liveUpdatedAt") or 0)),
                -(int(item.get("updatedAt") or 0)),
                item.get("wallet") or "",
            )
        )
    count = len(items)
    progress_pct = round(sum(float(item.get("progressPct") or 0.0) for item in items) / count, 1) if count > 0 else 100.0
    tracked_live_count = sum(1 for item in items if bool(item.get("trackedLive")))
    pending_proof_count = sum(1 for item in items if bool(item.get("proofLocalComplete")) and not bool(item.get("proofHeadComplete")))
    return {
        "label": label,
        "count": count,
        "active": 0,
        "complete": sum(1 for item in items if bool(item.get("historyVerifiedThroughNow"))),
        "progressPct": progress_pct,
        "trackedLiveCount": tracked_live_count,
        "pendingProofCount": pending_proof_count,
        "updatedAt": max([int(item.get("updatedAt") or 0) for item in items] + [0]) or None,
        "wallets": items[:12],
    }

previous_snapshot = load_json(pool_snapshot_path, {}) or {}
previous_progress = previous_snapshot.get("progress") if isinstance(previous_snapshot.get("progress"), dict) else {}

groups = {
    "newIntake": summarize_group("New Intake", "new_intake", pool_rows.get("new_intake") or []),
    "untracked": summarize_group("Untracked / Empty Metrics", "untracked", pool_rows.get("untracked") or []),
    "catchUp": summarize_group("Catch-Up Pool", "catch_up", pool_rows.get("catch_up") or []),
    "trackedPool": summarize_group("Tracked Pool", "tracked_pool", pool_rows.get("tracked_pool") or []),
    "heavy": summarize_group("Heavy Wallets", "heavy", pool_rows.get("heavy") or []),
}

tracked_count = groups["trackedPool"]["count"]
tracked_live_count = groups["trackedPool"]["trackedLiveCount"]
pending_proof_count = groups["catchUp"]["pendingProofCount"]
recovery_backlog = groups["newIntake"]["count"] + groups["untracked"]["count"] + groups["catchUp"]["count"]
mode = "recovery" if recovery_backlog > 0 else "steady_state"
pool_progress_pct = round((tracked_count / max(1, len(wallets) - len(heavy_wallets))) * 100.0, 1) if (len(wallets) - len(heavy_wallets)) > 0 else 100.0
bottlenecks = {
    "localProofPending": sum(1 for row in rows if not bool(row.get("proofLocalComplete"))),
    "headProofPending": sum(1 for row in rows if bool(row.get("proofLocalComplete")) and not bool(row.get("proofHeadComplete"))),
    "headProofMismatch": sum(
        1
        for row in rows
        if str((((row.get("proof") or {}).get("head") or {}).get("trades") or {}).get("status") or "").strip().lower() in {"remote_newer", "remote_empty"}
        or str((((row.get("proof") or {}).get("head") or {}).get("funding") or {}).get("status") or "").strip().lower() in {"remote_newer", "remote_empty"}
    ),
    "headProofError": sum(
        1
        for row in rows
        if str((((row.get("proof") or {}).get("head") or {}).get("trades") or {}).get("status") or "").strip().lower() == "error"
        or str((((row.get("proof") or {}).get("head") or {}).get("funding") or {}).get("status") or "").strip().lower() == "error"
    ),
}

pool_snapshot = {
    "version": 1,
    "generatedAt": now_ms,
    "mode": mode,
    "scope": "focus_cohort" if focus_wallets else "global",
    "focusWalletCount": len(focus_wallets),
    "targets": {
        "steadyState": {
            "newIntakePct": STEADY_INTAKE_PCT,
            "trackedRefreshPct": STEADY_REFRESH_PCT,
            "livePositionsPct": LIVE_POSITIONS_TARGET_PCT,
        }
    },
    "progress": {
        "knownWallets": len(wallets),
        "nonHeavyWallets": max(0, len(wallets) - len(heavy_wallets)),
        "heavyWallets": len(heavy_wallets),
        "trackedPool": tracked_count,
        "trackedLive": tracked_live_count,
        "pendingProof": pending_proof_count,
        "catchUp": groups["catchUp"]["count"],
        "untracked": groups["untracked"]["count"],
        "newIntake": groups["newIntake"]["count"],
        "poolProgressPct": pool_progress_pct,
        "trackedPoolDelta": tracked_count - int(previous_progress.get("trackedPool") or 0),
        "trackedLiveDelta": tracked_live_count - int(previous_progress.get("trackedLive") or 0),
    },
    "bottlenecks": bottlenecks,
    "groups": groups,
}

pool_snapshot_path.write_text(json.dumps(pool_snapshot, separators=(",", ":")))
PY

ssh "${SSH_OPTS[@]}" "${REMOTE}" "mkdir -p '${REMOTE_V2_DIR}/isolated' '${REMOTE_ROOT}/data/runtime/status'"

rsync -az -e "${RSYNC_RSH}" \
  "${LOCAL_PENDING_DATASET}" \
  "${REMOTE}:${REMOTE_V2_DIR}/pending_wallet_review_v3.json"

rsync -az -e "${RSYNC_RSH}" \
  "${LOCAL_PENDING_WALLETS}" \
  "${REMOTE}:${REMOTE_V2_DIR}/pending_wallets_remote.txt"

python3 - <<'PY' "${LOCAL_PENDING_WALLETS}" "${LOCAL_V2_DIR}/isolated" "${LOCAL_PENDING_PATHS}"
import sys
from pathlib import Path

wallets_path = Path(sys.argv[1])
isolated_dir = Path(sys.argv[2])
out_path = Path(sys.argv[3])
wallets = [line.strip() for line in wallets_path.read_text().splitlines() if line.strip()] if wallets_path.exists() else []
names = ["wallet_state.json", "wallet_history.json", "wallet_record.json", "trace.json", "wallet_history_log.ndjson"]
paths = []
for wallet in wallets:
    base = isolated_dir / wallet
    if not base.is_dir():
        continue
    for name in names:
        path = base / name
        if path.exists():
            paths.append(f"{wallet}/{name}")
out_path.write_text("".join(f"{path}\n" for path in paths))
PY

if [[ -s "${LOCAL_PENDING_PATHS}" ]]; then
  rsync -az -e "${RSYNC_RSH}" \
    --files-from="${LOCAL_PENDING_PATHS}" \
    "${LOCAL_V2_DIR}/isolated/" \
    "${REMOTE}:${REMOTE_V2_DIR}/isolated/"
fi

pull_remote_pending_fast_state

ssh "${SSH_OPTS[@]}" "${REMOTE}" "python3 - <<'PY' '${REMOTE_V2_DIR}/isolated' '${REMOTE_V2_DIR}/pending_wallets_remote.txt'
from pathlib import Path
import sys

isolated_dir = Path(sys.argv[1])
wallets_path = Path(sys.argv[2])
wallets = [line.strip() for line in wallets_path.read_text().splitlines() if line.strip()] if wallets_path.exists() else []
names = ['wallet_state.json', 'wallet_history.json', 'wallet_record.json', 'trace.json', 'wallet_history_log.ndjson']
for wallet in wallets:
    base = isolated_dir / wallet
    if not base.is_dir():
        continue
    for name in names:
        path = base / name
        if path.exists():
            print(f'{wallet}/{name}')
PY" > "${REMOTE_PENDING_PATHS}"

if [[ -s "${REMOTE_PENDING_PATHS}" ]]; then
  rsync -az -e "${RSYNC_RSH}" \
    --files-from="${REMOTE_PENDING_PATHS}" \
    "${REMOTE}:${REMOTE_V2_DIR}/isolated/" \
    "${LOCAL_V2_DIR}/isolated/" || true
fi

if command -v systemctl >/dev/null 2>&1; then
  systemctl start --no-block pacifica-shared-wallet-trade-store.service >/dev/null 2>&1 || true
fi

echo "synced wallet explorer pending lane to ${REMOTE_HOST}"
