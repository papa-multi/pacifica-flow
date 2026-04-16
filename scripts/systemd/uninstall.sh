#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "Please run as root (or with sudo)." >&2
  exit 1
fi

UNIT_DST_DIR="/etc/systemd/system"

systemctl stop pacifica-flow.target 2>/dev/null || true
systemctl stop pacifica-wallet-indexers.target 2>/dev/null || true
systemctl stop pacifica-live-positions.target 2>/dev/null || true
systemctl stop pacifica-repo-state-sync.timer 2>/dev/null || true
systemctl stop pacifica-repo-state-sync.service 2>/dev/null || true
systemctl stop pacifica-proxy-refresh.timer 2>/dev/null || true
systemctl stop pacifica-proxy-refresh.service 2>/dev/null || true
systemctl stop pacifica-telegram-report-bot.service 2>/dev/null || true
systemctl stop pacifica-telegram-copilot-bot.service 2>/dev/null || true
systemctl disable pacifica-flow.target 2>/dev/null || true
systemctl disable pacifica-wallet-indexers.target 2>/dev/null || true
systemctl disable pacifica-live-positions.target 2>/dev/null || true
systemctl disable pacifica-repo-state-sync.timer 2>/dev/null || true
systemctl disable pacifica-proxy-refresh.timer 2>/dev/null || true
systemctl disable pacifica-telegram-report-bot.service 2>/dev/null || true
systemctl disable pacifica-telegram-copilot-bot.service 2>/dev/null || true

while read -r unit _; do
  if [[ "${unit}" =~ ^pacifica-live-positions@[0-9]+\.service$ ]]; then
    systemctl stop "${unit}" 2>/dev/null || true
    systemctl disable "${unit}" 2>/dev/null || true
  fi
done < <(systemctl list-unit-files 'pacifica-live-positions@*.service' --no-legend 2>/dev/null || true)

while read -r unit _; do
  if [[ "${unit}" =~ ^pacifica-wallet-indexer@[0-9]+\.service$ ]]; then
    systemctl stop "${unit}" 2>/dev/null || true
    systemctl disable "${unit}" 2>/dev/null || true
  fi
done < <(systemctl list-unit-files 'pacifica-wallet-indexer@*.service' --no-legend 2>/dev/null || true)

rm -f "${UNIT_DST_DIR}/pacifica-wallet-indexer.service"
rm -f "${UNIT_DST_DIR}/pacifica-wallet-indexer@.service"
rm -f "${UNIT_DST_DIR}/pacifica-wallet-indexers.target"
rm -f "${UNIT_DST_DIR}/pacifica-onchain-discovery.service"
rm -f "${UNIT_DST_DIR}/pacifica-global-kpi.service"
rm -f "${UNIT_DST_DIR}/pacifica-ui-api.service"
rm -f "${UNIT_DST_DIR}/pacifica-telegram-report-bot.service"
rm -f "${UNIT_DST_DIR}/pacifica-telegram-copilot-bot.service"
rm -f "${UNIT_DST_DIR}/pacifica-live-positions@.service"
rm -f "${UNIT_DST_DIR}/pacifica-live-positions.target"
rm -f "${UNIT_DST_DIR}/pacifica-repo-state-sync.service"
rm -f "${UNIT_DST_DIR}/pacifica-repo-state-sync.timer"
rm -f "${UNIT_DST_DIR}/pacifica-proxy-refresh.service"
rm -f "${UNIT_DST_DIR}/pacifica-proxy-refresh.timer"
rm -f "${UNIT_DST_DIR}/pacifica-flow.target"

systemctl daemon-reload

echo "Removed PacificaFlow systemd units."
