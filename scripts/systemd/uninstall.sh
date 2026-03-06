#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "Please run as root (or with sudo)." >&2
  exit 1
fi

UNIT_DST_DIR="/etc/systemd/system"

systemctl stop pacifica-flow.target 2>/dev/null || true
systemctl disable pacifica-flow.target 2>/dev/null || true

rm -f "${UNIT_DST_DIR}/pacifica-wallet-indexer.service"
rm -f "${UNIT_DST_DIR}/pacifica-onchain-discovery.service"
rm -f "${UNIT_DST_DIR}/pacifica-global-kpi.service"
rm -f "${UNIT_DST_DIR}/pacifica-ui-api.service"
rm -f "${UNIT_DST_DIR}/pacifica-flow.target"

systemctl daemon-reload

echo "Removed PacificaFlow systemd units."
