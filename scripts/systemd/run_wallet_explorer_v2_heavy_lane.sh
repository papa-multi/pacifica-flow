#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

PRESERVE_ENV_KEYS=(
  PACIFICA_MULTI_EGRESS_PROXY_FILE
  PACIFICA_WALLET_EXPLORER_V2_HEAVY_STATE_PATH
  PACIFICA_WALLET_EXPLORER_V2_HEAVY_CANDIDATES_PATH
  PACIFICA_WALLET_EXPLORER_V2_ISOLATED_WALLETS_PATH
  PACIFICA_WALLET_EXPLORER_V2_HEAVY_PROXY_DIR
  PACIFICA_WALLET_EXPLORER_V2_HEAVY_HISTORY_LIMIT
  PACIFICA_WALLET_EXPLORER_V2_HEAVY_MIN_PAGES
  PACIFICA_WALLET_EXPLORER_V2_HEAVY_MIN_ROWS
  PACIFICA_WALLET_EXPLORER_V2_HEAVY_MIN_VOLUME_USD
  PACIFICA_WALLET_EXPLORER_V2_HEAVY_MAX_ACTIVE
  PACIFICA_WALLET_EXPLORER_V2_HEAVY_PARALLELISM
  PACIFICA_WALLET_EXPLORER_V2_HEAVY_LOOP_SLEEP_SECONDS
  PACIFICA_WALLET_EXPLORER_V2_HEAVY_FLUSH_INTERVAL_PAGES
  PACIFICA_WALLET_EXPLORER_V2_HEAVY_RECORD_FLUSH_PAGES
  PACIFICA_WALLET_EXPLORER_V2_TAIL_MODE_THRESHOLD
  PACIFICA_WALLET_EXPLORER_V2_HEAVY_DIRECT_ONLY
)

declare -A PRESERVED_ENV=()
for env_key in "${PRESERVE_ENV_KEYS[@]}"; do
  if [[ "${!env_key+x}" == "x" ]]; then
    PRESERVED_ENV["${env_key}"]="${!env_key}"
  fi
done

if [[ -f "${ROOT_DIR}/config/runtime.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/config/runtime.env"
  set +a
fi

for env_key in "${!PRESERVED_ENV[@]}"; do
  export "${env_key}=${PRESERVED_ENV[$env_key]}"
done

MASTER_PROXY_FILE="${PACIFICA_MULTI_EGRESS_PROXY_FILE:-${ROOT_DIR}/data/indexer/working_proxies.txt}"
PROXY_SHARD_DIR="${PACIFICA_WALLET_EXPLORER_V2_HEAVY_PROXY_DIR:-${ROOT_DIR}/data/wallet_explorer_v2/proxy_shards}"
SHARD_COUNT="${PACIFICA_WALLET_EXPLORER_V2_SHARDS:-8}"
CPU_WORKERS="${PACIFICA_WALLET_EXPLORER_V2_CPU_WORKERS:-4}"
RESERVED_PROXY_COUNT="${PACIFICA_WALLET_EXPLORER_V2_RESERVED_PROXY_COUNT:-80}"
HISTORY_LIMIT="${PACIFICA_WALLET_EXPLORER_V2_HEAVY_HISTORY_LIMIT:-${PACIFICA_WALLET_EXPLORER_V2_HISTORY_LIMIT:-240}}"
TIMEOUT_SECONDS="${PACIFICA_WALLET_EXPLORER_V2_TIMEOUT_SECONDS:-25}"
REQUEST_ATTEMPTS="${PACIFICA_WALLET_EXPLORER_V2_REQUEST_ATTEMPTS:-2}"
HEAVY_MIN_PAGES="${PACIFICA_WALLET_EXPLORER_V2_HEAVY_MIN_PAGES:-120}"
HEAVY_MIN_ROWS="${PACIFICA_WALLET_EXPLORER_V2_HEAVY_MIN_ROWS:-50000}"
HEAVY_MIN_VOLUME_USD="${PACIFICA_WALLET_EXPLORER_V2_HEAVY_MIN_VOLUME_USD:-25000000}"
HEAVY_MAX_ACTIVE="${PACIFICA_WALLET_EXPLORER_V2_HEAVY_MAX_ACTIVE:-4}"
HEAVY_PARALLELISM="${PACIFICA_WALLET_EXPLORER_V2_HEAVY_PARALLELISM:-2}"
HEAVY_LOOP_SLEEP_SECONDS="${PACIFICA_WALLET_EXPLORER_V2_HEAVY_LOOP_SLEEP_SECONDS:-5}"
HEAVY_FLUSH_INTERVAL_PAGES="${PACIFICA_WALLET_EXPLORER_V2_HEAVY_FLUSH_INTERVAL_PAGES:-8}"
HEAVY_RECORD_FLUSH_PAGES="${PACIFICA_WALLET_EXPLORER_V2_HEAVY_RECORD_FLUSH_PAGES:-24}"
TAIL_MODE_THRESHOLD="${PACIFICA_WALLET_EXPLORER_V2_TAIL_MODE_THRESHOLD:-0}"
HEAVY_DIRECT_ONLY="${PACIFICA_WALLET_EXPLORER_V2_HEAVY_DIRECT_ONLY:-false}"

mkdir -p "${PROXY_SHARD_DIR}"

if [[ "${HEAVY_DIRECT_ONLY,,}" == "true" || "${HEAVY_DIRECT_ONLY}" == "1" ]]; then
  python3 - <<'PY' "${PROXY_SHARD_DIR}" "${HEAVY_PARALLELISM}"
import pathlib, sys

out_dir = pathlib.Path(sys.argv[1])
parallelism = max(1, int(sys.argv[2]))
for lane in range(parallelism):
    dst = out_dir / f"proxies_heavy_{lane}.txt"
    dst.write_text("")
PY
else
python3 - <<'PY' "${MASTER_PROXY_FILE}" "${PROXY_SHARD_DIR}" "${RESERVED_PROXY_COUNT}" "${HEAVY_PARALLELISM}"
import pathlib, sys

src = pathlib.Path(sys.argv[1])
out_dir = pathlib.Path(sys.argv[2])
reserved = max(1, int(sys.argv[3]))
parallelism = max(1, int(sys.argv[4]))
rows = []
if src.exists():
    rows = [
        line.strip()
        for line in src.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
subset = rows[-reserved:] if len(rows) > reserved else rows
for lane in range(parallelism):
    dst = out_dir / f"proxies_heavy_{lane}.txt"
    dst.write_text("")
if not subset:
    raise SystemExit(0)
per = max(1, (len(subset) + parallelism - 1) // parallelism)
for lane in range(parallelism):
    start = lane * per
    end = min(len(subset), start + per)
    lane_rows = subset[start:end]
    dst = out_dir / f"proxies_heavy_{lane}.txt"
    dst.write_text("\n".join(lane_rows) + ("\n" if lane_rows else ""))
PY
fi

CPU_SET="0-$((CPU_WORKERS - 1))"

if command -v taskset >/dev/null 2>&1; then
  exec taskset -c "${CPU_SET}" python3 "${ROOT_DIR}/scripts/wallet_explorer_v2/heavy_lane.py" \
    --proxy-dir "${PROXY_SHARD_DIR}" \
    --shard-count "${SHARD_COUNT}" \
    --history-limit "${HISTORY_LIMIT}" \
    --timeout-seconds "${TIMEOUT_SECONDS}" \
    --request-attempts "${REQUEST_ATTEMPTS}" \
    --min-pages "${HEAVY_MIN_PAGES}" \
    --min-rows "${HEAVY_MIN_ROWS}" \
    --min-volume-usd "${HEAVY_MIN_VOLUME_USD}" \
    --max-active "${HEAVY_MAX_ACTIVE}" \
    --parallelism "${HEAVY_PARALLELISM}" \
    --loop-sleep-seconds "${HEAVY_LOOP_SLEEP_SECONDS}" \
    --flush-interval-pages "${HEAVY_FLUSH_INTERVAL_PAGES}" \
    --record-flush-pages "${HEAVY_RECORD_FLUSH_PAGES}" \
    --tail-mode-threshold "${TAIL_MODE_THRESHOLD}"
fi

exec python3 "${ROOT_DIR}/scripts/wallet_explorer_v2/heavy_lane.py" \
  --proxy-dir "${PROXY_SHARD_DIR}" \
  --shard-count "${SHARD_COUNT}" \
  --history-limit "${HISTORY_LIMIT}" \
  --timeout-seconds "${TIMEOUT_SECONDS}" \
  --request-attempts "${REQUEST_ATTEMPTS}" \
  --min-pages "${HEAVY_MIN_PAGES}" \
  --min-rows "${HEAVY_MIN_ROWS}" \
  --min-volume-usd "${HEAVY_MIN_VOLUME_USD}" \
  --max-active "${HEAVY_MAX_ACTIVE}" \
  --parallelism "${HEAVY_PARALLELISM}" \
  --loop-sleep-seconds "${HEAVY_LOOP_SLEEP_SECONDS}" \
  --flush-interval-pages "${HEAVY_FLUSH_INTERVAL_PAGES}" \
  --record-flush-pages "${HEAVY_RECORD_FLUSH_PAGES}" \
  --tail-mode-threshold "${TAIL_MODE_THRESHOLD}"
