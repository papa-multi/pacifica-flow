#!/usr/bin/env bash
set -euo pipefail

TARGET_UNIT="${PACIFICA_UI_API_TARGET_UNIT:-pacifica-ui-api.service}"
HEALTH_URL="${PACIFICA_UI_API_HEALTH_URL:-http://127.0.0.1:3200/live-trade/}"
MAX_TIME_SECONDS="${PACIFICA_UI_API_HEALTH_MAX_TIME_SECONDS:-6}"
LOCK_FILE="/tmp/pacifica-ui-api-watchdog.lock"

exec 9>"${LOCK_FILE}"
flock -n 9 || exit 0

if ! curl -fsS --max-time "${MAX_TIME_SECONDS}" "${HEALTH_URL}" > /dev/null; then
  echo "[ui-api-watchdog] healthcheck failed for ${HEALTH_URL}; restarting ${TARGET_UNIT}"
  systemctl restart "${TARGET_UNIT}"
  exit 0
fi

echo "[ui-api-watchdog] healthy ${HEALTH_URL}"

