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
START_NOW="false"

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
  LIVE_SHARDS="$(find "${ROOT_DIR}/data/live_positions/proxy_shards" -maxdepth 1 -type f -name 'proxies_*.txt' 2>/dev/null | wc -l | tr -d ' ')"
fi

if ! [[ "${LIVE_SHARDS}" =~ ^[0-9]+$ ]] || [[ "${LIVE_SHARDS}" -lt 1 ]]; then
  LIVE_SHARDS="4"
fi

cp -f "${UNIT_SRC_DIR}/pacifica-wallet-indexer.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-onchain-discovery.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-global-kpi.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-ui-api.service" "${UNIT_DST_DIR}/"
render_unit "${UNIT_SRC_DIR}/pacifica-live-positions@.service" "${UNIT_DST_DIR}/pacifica-live-positions@.service"
cp -f "${UNIT_SRC_DIR}/pacifica-live-positions.target" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-proxy-refresh.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-proxy-refresh.timer" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-flow.target" "${UNIT_DST_DIR}/"

if [[ -f "${ROOT_DIR}/data/live_positions/positions_proxies.txt" && -x "${ROOT_DIR}/scripts/split_live_positions_proxies.sh" ]]; then
  bash "${ROOT_DIR}/scripts/split_live_positions_proxies.sh" "${LIVE_SHARDS}"
fi

systemctl daemon-reload
systemctl enable pacifica-flow.target
systemctl enable pacifica-live-positions.target
systemctl enable pacifica-proxy-refresh.timer

mapfile -t LIVE_INSTANCE_UNITS < <(systemctl list-unit-files 'pacifica-live-positions@*.service' --no-legend 2>/dev/null | awk '{print $1}')
for unit in "${LIVE_INSTANCE_UNITS[@]}"; do
  if [[ "${unit}" =~ ^pacifica-live-positions@([0-9]+)\.service$ ]]; then
    idx="${BASH_REMATCH[1]}"
    if [[ "${idx}" -ge "${LIVE_SHARDS}" ]]; then
      systemctl disable "${unit}" 2>/dev/null || true
      if [[ "${START_NOW}" == "true" ]]; then
        systemctl stop "${unit}" 2>/dev/null || true
      fi
    fi
  fi
done

for ((i=0; i<LIVE_SHARDS; i+=1)); do
  systemctl enable "pacifica-live-positions@${i}.service"
done

if [[ "${START_NOW}" == "true" ]]; then
  systemctl start pacifica-flow.target
  systemctl start pacifica-proxy-refresh.timer
fi

echo "Installed PacificaFlow systemd units."
echo "Use: systemctl start pacifica-flow.target"
echo "Use: systemctl status pacifica-flow.target"
echo "Enabled live-position shards: ${LIVE_SHARDS}"
echo "Enabled proxy refresh timer: pacifica-proxy-refresh.timer"
