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

SHARD_COUNT="${PACIFICA_INDEXER_MULTI_WORKER_SHARDS:-20}"
if ! [[ "${SHARD_COUNT}" =~ ^[0-9]+$ ]] || [[ "${SHARD_COUNT}" -lt 1 ]]; then
  SHARD_COUNT="20"
fi
if ! [[ "${SHARD_INDEX}" =~ ^[0-9]+$ ]] || [[ "${SHARD_INDEX}" -lt 0 ]]; then
  echo "invalid shard index: ${SHARD_INDEX}" >&2
  exit 1
fi
if [[ "${SHARD_INDEX}" -ge "${SHARD_COUNT}" ]]; then
  echo "shard index ${SHARD_INDEX} out of range for shard count ${SHARD_COUNT}" >&2
  exit 1
fi

MASTER_PROXY_FILE="${PACIFICA_MULTI_EGRESS_PROXY_FILE:-${ROOT_DIR}/data/indexer/working_proxies.txt}"
PROXY_SHARD_DIR="${ROOT_DIR}/data/indexer/proxy_shards"
PROXY_SHARD_FILE="${PROXY_SHARD_DIR}/proxies_${SHARD_INDEX}.txt"
INDEXER_SHARD_DIR="${ROOT_DIR}/data/indexer/shards/shard_${SHARD_INDEX}"

mkdir -p "${PROXY_SHARD_DIR}" "${INDEXER_SHARD_DIR}"

python3 - <<'PY' "${MASTER_PROXY_FILE}" "${PROXY_SHARD_FILE}" "${SHARD_INDEX}" "${SHARD_COUNT}"
import pathlib, sys

src = pathlib.Path(sys.argv[1])
dst = pathlib.Path(sys.argv[2])
idx = int(sys.argv[3])
count = int(sys.argv[4])
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
per = max(1, (len(rows) + count - 1) // count)
start = idx * per
end = min(len(rows), start + per)
subset = rows[start:end]
dst.write_text("\n".join(subset) + ("\n" if subset else ""))
PY

python3 "${ROOT_DIR}/scripts/repair_indexer_shard.py" \
  "${INDEXER_SHARD_DIR}" \
  "${SHARD_INDEX}" \
  "${SHARD_COUNT}" >/dev/null 2>&1 || true

export PORT="$((3210 + SHARD_INDEX))"
export HOST="127.0.0.1"
export PACIFICA_SERVICE="api"
export PACIFICA_WS_ENABLED="false"
export PACIFICA_SNAPSHOT_ENABLED="false"
export PACIFICA_GLOBAL_KPI_ENABLED="false"
export PACIFICA_INDEXER_ENABLED="true"
export PACIFICA_INDEXER_RUNNER_ENABLED="true"
export PACIFICA_ONCHAIN_ENABLED="false"
export PACIFICA_ONCHAIN_RUNNER_ENABLED="false"
export PACIFICA_INDEXER_DISCOVERY_ONLY="false"
export PACIFICA_SOLANA_LOG_TRIGGER_ENABLED="false"
export PACIFICA_LIVE_TRADE_ATTRIBUTION_DB_ENABLED="false"
export PACIFICA_LIVE_TRADE_WALLET_ATTRIBUTION_ENABLED="false"
export PACIFICA_LIVE_TRADE_TX_ATTRIBUTION_ENABLED="false"
export PACIFICA_LIVE_WALLET_FIRST_ENABLED="false"
export PACIFICA_POSITION_LIFECYCLE_LOOP_ENABLED="false"
export PACIFICA_WALLET_METRICS_HISTORY_LOOP_ENABLED="false"
export PACIFICA_INDEXER_EXTERNAL_SHARDS_ENABLED="false"
export PACIFICA_INDEXER_WORKER_SHARD_COUNT="${SHARD_COUNT}"
export PACIFICA_INDEXER_WORKER_SHARD_INDEX="${SHARD_INDEX}"
export PACIFICA_INDEXER_DATA_DIR="${INDEXER_SHARD_DIR}"
export PACIFICA_MULTI_EGRESS_INCLUDE_DIRECT="false"
export PACIFICA_MULTI_EGRESS_PROXY_FILE="${PROXY_SHARD_FILE}"

if command -v taskset >/dev/null 2>&1; then
  exec taskset -c "${SHARD_INDEX}" /usr/bin/node "${ROOT_DIR}/server.js"
fi

exec /usr/bin/node "${ROOT_DIR}/server.js"
