#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ -f "${ROOT_DIR}/config/runtime.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/config/runtime.env"
  set +a
fi

SHARD_COUNT="${PACIFICA_WALLET_EXPLORER_V2_SHARDS:-8}"

exec python3 "${ROOT_DIR}/scripts/wallet_explorer_v2/materialize.py" --shards "${SHARD_COUNT}"
