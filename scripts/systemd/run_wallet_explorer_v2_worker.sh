#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SHARD_INDEX="${1:?missing shard index}"

if [[ -f "${ROOT_DIR}/config/runtime.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/config/runtime.env"
  set +a
fi

SHARD_COUNT="${PACIFICA_WALLET_EXPLORER_V2_SHARDS:-8}"
CPU_WORKERS="${PACIFICA_WALLET_EXPLORER_V2_CPU_WORKERS:-4}"
MASTER_PROXY_FILE="${PACIFICA_MULTI_EGRESS_PROXY_FILE:-${ROOT_DIR}/data/indexer/working_proxies.txt}"
PROXY_SHARD_DIR="${ROOT_DIR}/data/wallet_explorer_v2/proxy_shards"
PROXY_SHARD_FILE="${PROXY_SHARD_DIR}/proxies_${SHARD_INDEX}.txt"
WALLET_CONCURRENCY="${PACIFICA_WALLET_EXPLORER_V2_WALLET_CONCURRENCY:-4}"
ACTIVE_BATCH_MULTIPLIER="${PACIFICA_WALLET_EXPLORER_V2_ACTIVE_BATCH_MULTIPLIER:-8}"
HISTORY_LIMIT="${PACIFICA_WALLET_EXPLORER_V2_HISTORY_LIMIT:-120}"
TIMEOUT_SECONDS="${PACIFICA_WALLET_EXPLORER_V2_TIMEOUT_SECONDS:-20}"
REQUEST_ATTEMPTS="${PACIFICA_WALLET_EXPLORER_V2_REQUEST_ATTEMPTS:-2}"
LOOP_SLEEP_SECONDS="${PACIFICA_WALLET_EXPLORER_V2_LOOP_SLEEP_SECONDS:-5}"
PRIORITIZE_FIRST_TRADE="${PACIFICA_WALLET_EXPLORER_V2_PRIORITIZE_FIRST_TRADE:-true}"
COMPLETION_LANE_PCT="${PACIFICA_WALLET_EXPLORER_V2_COMPLETION_LANE_PCT:-35}"
RESERVED_PROXY_COUNT="${PACIFICA_WALLET_EXPLORER_V2_RESERVED_PROXY_COUNT:-40}"
TRADE_PAGE_BUDGET="${PACIFICA_WALLET_EXPLORER_V2_TRADE_PAGE_BUDGET:-12}"
FUNDING_PAGE_BUDGET="${PACIFICA_WALLET_EXPLORER_V2_FUNDING_PAGE_BUDGET:-0}"
FLUSH_INTERVAL_PAGES="${PACIFICA_WALLET_EXPLORER_V2_FLUSH_INTERVAL_PAGES:-4}"
WALLET_REFRESH_MS="${PACIFICA_WALLET_EXPLORER_V2_WALLET_REFRESH_MS:-30000}"
REFRESH_LANE_PCT="${PACIFICA_WALLET_EXPLORER_V2_REFRESH_LANE_PCT:-10}"
REFRESH_TRADE_PAGE_BUDGET="${PACIFICA_WALLET_EXPLORER_V2_REFRESH_TRADE_PAGE_BUDGET:-2}"
REFRESH_FUNDING_PAGE_BUDGET="${PACIFICA_WALLET_EXPLORER_V2_REFRESH_FUNDING_PAGE_BUDGET:-1}"

mkdir -p "${PROXY_SHARD_DIR}"

python3 - <<'PY' "${MASTER_PROXY_FILE}" "${PROXY_SHARD_FILE}" "${SHARD_INDEX}" "${SHARD_COUNT}" "${RESERVED_PROXY_COUNT}"
import pathlib, sys

src = pathlib.Path(sys.argv[1])
dst = pathlib.Path(sys.argv[2])
idx = int(sys.argv[3])
count = int(sys.argv[4])
reserved = max(0, int(sys.argv[5]))
rows = []
if src.exists():
    rows = [
        line.strip()
        for line in src.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
if not rows:
    dst.write_text("")
    raise SystemExit(0)
usable = list(rows)
if reserved > 0 and len(rows) - reserved >= count:
    usable = rows[:-reserved]
if not usable:
    usable = list(rows)
per = max(1, (len(usable) + count - 1) // count)
start = idx * per
end = min(len(usable), start + per)
subset = usable[start:end]
dst.write_text("\n".join(subset) + ("\n" if subset else ""))
PY

EXTRA_ARGS=()
if [[ "${PRIORITIZE_FIRST_TRADE,,}" == "true" || "${PRIORITIZE_FIRST_TRADE}" == "1" ]]; then
  EXTRA_ARGS+=(--prioritize-first-trade)
fi

if command -v taskset >/dev/null 2>&1; then
  CPU_INDEX=$(( SHARD_INDEX % CPU_WORKERS ))
  exec taskset -c "${CPU_INDEX}" python3 "${ROOT_DIR}/scripts/wallet_explorer_v2/worker.py" \
    --shard-index "${SHARD_INDEX}" \
    --shard-count "${SHARD_COUNT}" \
    --proxy-file "${PROXY_SHARD_FILE}" \
    --wallet-concurrency "${WALLET_CONCURRENCY}" \
    --active-batch-multiplier "${ACTIVE_BATCH_MULTIPLIER}" \
    --history-limit "${HISTORY_LIMIT}" \
    --timeout-seconds "${TIMEOUT_SECONDS}" \
    --request-attempts "${REQUEST_ATTEMPTS}" \
    --loop-sleep-seconds "${LOOP_SLEEP_SECONDS}" \
    --completion-lane-pct "${COMPLETION_LANE_PCT}" \
    --trade-page-budget "${TRADE_PAGE_BUDGET}" \
    --funding-page-budget "${FUNDING_PAGE_BUDGET}" \
    --flush-interval-pages "${FLUSH_INTERVAL_PAGES}" \
    --wallet-refresh-ms "${WALLET_REFRESH_MS}" \
    --refresh-lane-pct "${REFRESH_LANE_PCT}" \
    --refresh-trade-page-budget "${REFRESH_TRADE_PAGE_BUDGET}" \
    --refresh-funding-page-budget "${REFRESH_FUNDING_PAGE_BUDGET}" \
    "${EXTRA_ARGS[@]}"
fi

exec python3 "${ROOT_DIR}/scripts/wallet_explorer_v2/worker.py" \
  --shard-index "${SHARD_INDEX}" \
  --shard-count "${SHARD_COUNT}" \
  --proxy-file "${PROXY_SHARD_FILE}" \
  --wallet-concurrency "${WALLET_CONCURRENCY}" \
  --active-batch-multiplier "${ACTIVE_BATCH_MULTIPLIER}" \
  --history-limit "${HISTORY_LIMIT}" \
  --timeout-seconds "${TIMEOUT_SECONDS}" \
  --request-attempts "${REQUEST_ATTEMPTS}" \
  --loop-sleep-seconds "${LOOP_SLEEP_SECONDS}" \
  --completion-lane-pct "${COMPLETION_LANE_PCT}" \
  --trade-page-budget "${TRADE_PAGE_BUDGET}" \
  --funding-page-budget "${FUNDING_PAGE_BUDGET}" \
  --flush-interval-pages "${FLUSH_INTERVAL_PAGES}" \
  --wallet-refresh-ms "${WALLET_REFRESH_MS}" \
  --refresh-lane-pct "${REFRESH_LANE_PCT}" \
  --refresh-trade-page-budget "${REFRESH_TRADE_PAGE_BUDGET}" \
  --refresh-funding-page-budget "${REFRESH_FUNDING_PAGE_BUDGET}" \
  "${EXTRA_ARGS[@]}"
