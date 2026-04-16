#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "Please run as root (or with sudo)." >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
UNIT_SRC_DIR="${ROOT_DIR}/systemd"
UNIT_DST_DIR="/etc/systemd/system"
LIVE_SHARDS=""
INDEXER_SHARDS=""
START_NOW="false"
ENABLE_LIVE_POSITIONS=""
ENABLE_LIVE_POSITIONS_EXPLICIT="false"
LIVE_ROLE="${PACIFICA_LIVE_ROLE:-ui}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --now)
      START_NOW="true"
      shift
      ;;
    --live-position-shards)
      LIVE_SHARDS="${2:-}"
      shift 2
      ;;
    --wallet-indexer-shards)
      INDEXER_SHARDS="${2:-}"
      shift 2
      ;;
    --enable-live-positions)
      ENABLE_LIVE_POSITIONS="true"
      ENABLE_LIVE_POSITIONS_EXPLICIT="true"
      shift
      ;;
    --disable-live-positions)
      ENABLE_LIVE_POSITIONS="false"
      ENABLE_LIVE_POSITIONS_EXPLICIT="true"
      shift
      ;;
    --live-role)
      LIVE_ROLE="${2:-ui}"
      shift 2
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

if [[ ! -d "${UNIT_SRC_DIR}" ]]; then
  echo "Missing unit directory: ${UNIT_SRC_DIR}" >&2
  exit 1
fi

chmod +x "${ROOT_DIR}/scripts/systemd/run_live_positions_shard.sh"
chmod +x "${ROOT_DIR}/scripts/systemd/run_live_positions_census_shard.sh"
chmod +x "${ROOT_DIR}/scripts/systemd/run_live_positions_materializer_shard.sh"
chmod +x "${ROOT_DIR}/scripts/systemd/run_wallet_indexer_shard.sh"
chmod +x "${ROOT_DIR}/scripts/systemd/run_wallet_materializer.sh"
chmod +x "${ROOT_DIR}/scripts/systemd/run_wallet_explorer_v2_mux.sh"
chmod +x "${ROOT_DIR}/scripts/systemd/run_wallet_explorer_v2_heavy_lane.sh"
chmod +x "${ROOT_DIR}/scripts/systemd/run_wallet_explorer_v2_materializer.sh"
chmod +x "${ROOT_DIR}/scripts/systemd/run_wallet_explorer_v3_refresh.sh"
chmod +x "${ROOT_DIR}/scripts/systemd/pull_wallet_explorer_v3_remote.sh"
chmod +x "${ROOT_DIR}/scripts/systemd/run_storage_prune.sh"
chmod +x "${ROOT_DIR}/scripts/systemd/run_repo_state_sync.sh"
chmod +x "${ROOT_DIR}/scripts/systemd/resume_from_repo_state.sh"
chmod +x "${ROOT_DIR}/scripts/telegram_copilot_bot.js"
chmod +x "/root/pacifica-proxy-lab/scripts/fetch_additional_sources.sh" 2>/dev/null || true
chmod +x "/root/pacifica-proxy-lab/scripts/build_candidate_list.sh" 2>/dev/null || true
chmod +x "/root/pacifica-proxy-lab/scripts/run_proxy_lab.sh" 2>/dev/null || true
chmod +x "/root/pacifica-proxy-lab/scripts/export_for_pacifica.sh" 2>/dev/null || true
chmod +x "/root/pacifica-proxy-lab/scripts/apply_to_pacifica.sh" 2>/dev/null || true
chmod +x "/root/pacifica-proxy-lab/scripts/refresh_and_apply.sh" 2>/dev/null || true

render_unit() {
  local src="$1"
  local dst="$2"
  sed "s/__PACIFICA_LIVE_SHARDS__/${LIVE_SHARDS}/g" "${src}" > "${dst}"
}

if [[ -z "${LIVE_SHARDS}" ]]; then
  LIVE_SHARDS="$(
    (
      find "${ROOT_DIR}/data/live_positions/proxy_shards" -maxdepth 1 -type f -name 'proxies_*.txt' 2>/dev/null || true
    ) | wc -l | tr -d ' '
  )"
fi

if ! [[ "${LIVE_SHARDS}" =~ ^[0-9]+$ ]] || [[ "${LIVE_SHARDS}" -lt 1 ]]; then
  LIVE_SHARDS="4"
fi

if [[ -z "${INDEXER_SHARDS}" ]]; then
  INDEXER_SHARDS="${PACIFICA_INDEXER_MULTI_WORKER_SHARDS:-1}"
fi

if ! [[ "${INDEXER_SHARDS}" =~ ^[0-9]+$ ]] || [[ "${INDEXER_SHARDS}" -lt 1 ]]; then
  INDEXER_SHARDS="1"
fi

LIVE_ROLE="$(printf '%s' "${LIVE_ROLE}" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')"
if [[ "${LIVE_ROLE}" != "worker" && "${LIVE_ROLE}" != "ui" ]]; then
  LIVE_ROLE="ui"
fi

if [[ "${ENABLE_LIVE_POSITIONS_EXPLICIT}" != "true" ]]; then
  if [[ "${LIVE_ROLE}" == "worker" ]]; then
    ENABLE_LIVE_POSITIONS="true"
  else
    ENABLE_LIVE_POSITIONS="false"
  fi
fi

cp -f "${UNIT_SRC_DIR}/pacifica-wallet-indexer.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-wallet-indexer@.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-wallet-indexers.target" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-onchain-discovery.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-global-kpi.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-ui-api.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-telegram-report-bot.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-telegram-copilot-bot.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-wallet-materializer.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-wallet-materializer.timer" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-live-positions-census@.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-live-positions-materializer@.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-live-remote-sync.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-live-remote-sync.timer" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-live-remote-pull.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-live-remote-pull.timer" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-live-positions-watchdog.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-live-positions-watchdog.timer" "${UNIT_DST_DIR}/"
render_unit "${UNIT_SRC_DIR}/pacifica-live-positions@.service" "${UNIT_DST_DIR}/pacifica-live-positions@.service"
cp -f "${UNIT_SRC_DIR}/pacifica-live-positions.target" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-wallet-explorer-v2-mux@.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-wallet-explorer-v2-heavy.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-wallet-explorer-v2-materializer.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-wallet-explorer-v2-materializer.timer" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-wallet-explorer-v2.target" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-wallet-explorer-v3-refresh.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-wallet-explorer-v3-refresh.timer" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-wallet-explorer-v3-pull.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-wallet-explorer-v3-pull.timer" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-storage-prune.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-storage-prune.timer" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-proxy-refresh.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-proxy-refresh.timer" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-repo-state-sync.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-repo-state-sync.timer" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-flow.target" "${UNIT_DST_DIR}/"

POSITIONS_PROXY_FILE="${ROOT_DIR}/data/live_positions/positions_proxies.txt"
INDEXER_PROXY_FILE="${ROOT_DIR}/data/indexer/working_proxies.txt"
if [[ ! -f "${POSITIONS_PROXY_FILE}" ]]; then
  if [[ -x "${ROOT_DIR}/scripts/rebuild_positions_proxy_pool.sh" ]]; then
    bash "${ROOT_DIR}/scripts/rebuild_positions_proxy_pool.sh" \
      --source "${INDEXER_PROXY_FILE}" \
      --out "${POSITIONS_PROXY_FILE}" \
      >/dev/null 2>&1 || true
  fi
  if [[ ! -f "${POSITIONS_PROXY_FILE}" && -f "${INDEXER_PROXY_FILE}" ]]; then
    mkdir -p "$(dirname "${POSITIONS_PROXY_FILE}")"
    cp -f "${INDEXER_PROXY_FILE}" "${POSITIONS_PROXY_FILE}"
  fi
fi

if [[ -f "${ROOT_DIR}/data/live_positions/positions_proxies.txt" && -x "${ROOT_DIR}/scripts/split_live_positions_proxies.sh" ]]; then
  bash "${ROOT_DIR}/scripts/split_live_positions_proxies.sh" "${LIVE_SHARDS}"
fi

systemctl daemon-reload
systemctl enable pacifica-flow.target
systemctl disable pacifica-wallet-indexer.service 2>/dev/null || true
systemctl disable pacifica-wallet-indexers.target 2>/dev/null || true
systemctl disable pacifica-wallet-materializer.timer 2>/dev/null || true
systemctl disable pacifica-wallet-explorer-v2.target 2>/dev/null || true
systemctl disable pacifica-wallet-explorer-v2-heavy.service 2>/dev/null || true
systemctl disable pacifica-wallet-explorer-v2-materializer.timer 2>/dev/null || true
systemctl disable pacifica-wallet-explorer-v2-pending-remote-sync.timer 2>/dev/null || true
systemctl disable pacifica-wallet-explorer-v2-pending-state-remote-sync.timer 2>/dev/null || true
systemctl disable pacifica-shared-wallet-trade-store.timer 2>/dev/null || true
systemctl enable pacifica-storage-prune.timer
systemctl enable pacifica-repo-state-sync.timer
systemctl enable pacifica-telegram-report-bot.service
systemctl enable pacifica-telegram-copilot-bot.service
if [[ "${LIVE_ROLE}" == "worker" ]]; then
  systemctl enable pacifica-live-remote-sync.timer
  systemctl disable pacifica-live-remote-pull.timer 2>/dev/null || true
  systemctl enable pacifica-live-positions.target
  systemctl enable pacifica-live-positions-watchdog.timer
else
  systemctl disable pacifica-live-remote-sync.timer 2>/dev/null || true
  systemctl enable pacifica-live-remote-pull.timer
  systemctl disable pacifica-live-positions.target 2>/dev/null || true
  systemctl disable pacifica-live-positions-watchdog.timer 2>/dev/null || true
fi
systemctl enable pacifica-proxy-refresh.timer

mapfile -t LIVE_INSTANCE_UNITS < <(systemctl list-unit-files 'pacifica-live-positions@*.service' --no-legend 2>/dev/null | awk '{print $1}')
for unit in "${LIVE_INSTANCE_UNITS[@]}"; do
  if [[ "${unit}" =~ ^pacifica-live-positions@([0-9]+)\.service$ ]]; then
    idx="${BASH_REMATCH[1]}"
    if [[ "${ENABLE_LIVE_POSITIONS}" != "true" ]]; then
      systemctl disable "${unit}" 2>/dev/null || true
      systemctl stop "${unit}" 2>/dev/null || true
      continue
    fi
    if [[ "${idx}" -ge "${LIVE_SHARDS}" ]]; then
      systemctl disable "${unit}" 2>/dev/null || true
      if [[ "${START_NOW}" == "true" ]]; then
        systemctl stop "${unit}" 2>/dev/null || true
      fi
    fi
  fi
done

if [[ "${ENABLE_LIVE_POSITIONS}" == "true" ]]; then
  for ((i=0; i<LIVE_SHARDS; i+=1)); do
    systemctl disable "pacifica-live-positions@${i}.service" 2>/dev/null || true
    systemctl enable "pacifica-live-positions-census@${i}.service"
    systemctl enable "pacifica-live-positions-materializer@${i}.service"
  done
else
  mapfile -t LIVE_POSITION_INSTANCE_UNITS < <(systemctl list-unit-files 'pacifica-live-positions@*.service' --no-legend 2>/dev/null | awk '{print $1}')
  for unit in "${LIVE_POSITION_INSTANCE_UNITS[@]}"; do
    if [[ "${unit}" =~ ^pacifica-live-positions@([0-9]+)\.service$ ]]; then
      systemctl disable "${unit}" 2>/dev/null || true
      systemctl stop "${unit}" 2>/dev/null || true
    fi
  done
  mapfile -t LIVE_POSITION_WORKER_UNITS < <(systemctl list-unit-files 'pacifica-live-positions-census@*.service' 'pacifica-live-positions-materializer@*.service' --no-legend 2>/dev/null | awk '{print $1}')
  for unit in "${LIVE_POSITION_WORKER_UNITS[@]}"; do
    systemctl disable "${unit}" 2>/dev/null || true
    systemctl stop "${unit}" 2>/dev/null || true
  done
fi

mapfile -t INDEXER_INSTANCE_UNITS < <(systemctl list-unit-files 'pacifica-wallet-indexer@*.service' --no-legend 2>/dev/null | awk '{print $1}')
for unit in "${INDEXER_INSTANCE_UNITS[@]}"; do
  if [[ "${unit}" =~ ^pacifica-wallet-indexer@([0-9]+)\.service$ ]]; then
    idx="${BASH_REMATCH[1]}"
    systemctl disable "${unit}" 2>/dev/null || true
    if [[ "${START_NOW}" == "true" ]]; then
      systemctl stop "${unit}" 2>/dev/null || true
    fi
  fi
done

mapfile -t V2_MUX_INSTANCE_UNITS < <(systemctl list-unit-files 'pacifica-wallet-explorer-v2-mux@*.service' --no-legend 2>/dev/null | awk '{print $1}')
for unit in "${V2_MUX_INSTANCE_UNITS[@]}"; do
  systemctl disable "${unit}" 2>/dev/null || true
  if [[ "${START_NOW}" == "true" ]]; then
    systemctl stop "${unit}" 2>/dev/null || true
  fi
done

if [[ "${START_NOW}" == "true" ]]; then
  systemctl start pacifica-flow.target
  systemctl start pacifica-storage-prune.timer
  systemctl start pacifica-repo-state-sync.timer
  systemctl start pacifica-telegram-report-bot.service || true
  if [[ "${ENABLE_LIVE_POSITIONS}" == "true" ]]; then
    systemctl start pacifica-live-positions.target
  fi
  systemctl start pacifica-proxy-refresh.timer
fi

echo "Installed PacificaFlow systemd units."
echo "Use: systemctl start pacifica-flow.target"
echo "Use: systemctl status pacifica-flow.target"
echo "Live role: ${LIVE_ROLE}"
if [[ "${ENABLE_LIVE_POSITIONS}" == "true" ]]; then
  echo "Enabled live-position shards: ${LIVE_SHARDS}"
else
  echo "Enabled live-position shards: 0 (explicitly disabled)"
fi
echo "Legacy wallet explorer v2 workers are disabled by default."
echo "Enabled storage prune timer: pacifica-storage-prune.timer"
echo "Enabled proxy refresh timer: pacifica-proxy-refresh.timer"
echo "Enabled repo state sync timer: pacifica-repo-state-sync.timer"
echo "Enabled Telegram report bot service: pacifica-telegram-report-bot.service"
