#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "Please run as root (or with sudo)." >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
UNIT_SRC_DIR="${ROOT_DIR}/systemd"
UNIT_DST_DIR="/etc/systemd/system"

if [[ ! -d "${UNIT_SRC_DIR}" ]]; then
  echo "Missing unit directory: ${UNIT_SRC_DIR}" >&2
  exit 1
fi

cp -f "${UNIT_SRC_DIR}/pacifica-wallet-indexer.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-onchain-discovery.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-global-kpi.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-ui-api.service" "${UNIT_DST_DIR}/"
cp -f "${UNIT_SRC_DIR}/pacifica-flow.target" "${UNIT_DST_DIR}/"

systemctl daemon-reload
systemctl enable pacifica-flow.target

if [[ "${1:-}" == "--now" ]]; then
  systemctl start pacifica-flow.target
fi

echo "Installed PacificaFlow systemd units."
echo "Use: systemctl start pacifica-flow.target"
echo "Use: systemctl status pacifica-flow.target"
