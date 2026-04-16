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
export PACIFICA_LIVE_TRADE_PORTFOLIO_PROXY_FILE="${PACIFICA_LIVE_TRADE_PORTFOLIO_PROXY_FILE:-${ROOT_DIR}/data/live_positions/positions_proxies.txt}"
export PACIFICA_LIVE_TRADE_PORTFOLIO_ENRICH_LIMIT="${PACIFICA_LIVE_TRADE_PORTFOLIO_ENRICH_LIMIT:-500}"
export PACIFICA_LIVE_TRADE_PORTFOLIO_ENRICH_CONCURRENCY="${PACIFICA_LIVE_TRADE_PORTFOLIO_ENRICH_CONCURRENCY:-10}"
export PACIFICA_LIVE_TRADE_PORTFOLIO_PROXY_INCLUDE_DIRECT="${PACIFICA_LIVE_TRADE_PORTFOLIO_PROXY_INCLUDE_DIRECT:-true}"

NODE_BIN="${NODE_BIN:-$(command -v node || command -v nodejs || true)}"
if [[ -z "${NODE_BIN}" ]]; then
  echo "node binary not found" >&2
  exit 127
fi

"${NODE_BIN}" "${ROOT_DIR}/scripts/build_live_trade_leaderboard_universe.js"
"${NODE_BIN}" "${ROOT_DIR}/scripts/enrich_live_trade_leaderboard_portfolio.js"
