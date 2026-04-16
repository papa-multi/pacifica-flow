#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ -f "${ROOT_DIR}/config/runtime.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/config/runtime.env"
  set +a
fi

export PACIFICA_LIVE_TRADE_BASE_DIR="${PACIFICA_LIVE_TRADE_BASE_DIR:-${ROOT_DIR}/data/live_trade_leaderboard}"
export PACIFICA_LIVE_TRADE_HOST="${PACIFICA_LIVE_TRADE_HOST:-0.0.0.0}"
export PACIFICA_LIVE_TRADE_PORT="${PACIFICA_LIVE_TRADE_PORT:-3335}"
export PACIFICA_LIVE_TRADE_NEW_EVENT_MAX_LAG_MS="${PACIFICA_LIVE_TRADE_NEW_EVENT_MAX_LAG_MS:-5000}"

NODE_BIN="${NODE_BIN:-$(command -v node || command -v nodejs || true)}"
if [[ -z "${NODE_BIN}" ]]; then
  echo "node binary not found" >&2
  exit 127
fi

exec "${NODE_BIN}" "${ROOT_DIR}/scripts/live_trade_leaderboard_server.js"
