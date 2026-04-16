#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LIVE_DIR="${ROOT_DIR}/data/live_positions"
TARGET_UNIT="${PACIFICA_LIVE_POSITIONS_TARGET_UNIT:-pacifica-live-positions.target}"
STALE_AFTER_SECONDS="${PACIFICA_LIVE_POSITIONS_WATCHDOG_STALE_AFTER_SECONDS:-300}"
LOCK_FILE="${LIVE_DIR}/.watchdog.lock"

if [[ -f "${ROOT_DIR}/config/runtime.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/config/runtime.env"
  set +a
fi

LIVE_ROLE="$(printf '%s' "${PACIFICA_LIVE_ROLE:-ui}" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')"
if [[ "${LIVE_ROLE}" != "worker" ]]; then
  echo "[live-positions-watchdog] skipping on role=${LIVE_ROLE}"
  exit 0
fi

mkdir -p "${LIVE_DIR}"

exec 9>"${LOCK_FILE}"
flock -n 9 || exit 0

shopt -s nullglob
files=("${LIVE_DIR}"/wallet_first_shard_*.json "${LIVE_DIR}"/wallet_first_account_shard_*.json)
shopt -u nullglob

if [[ "${#files[@]}" -eq 0 ]]; then
  echo "[live-positions-watchdog] no shard files found; restarting ${TARGET_UNIT}"
  systemctl restart "${TARGET_UNIT}"
  exit 0
fi

now_epoch="$(date +%s)"
latest_epoch=0
latest_status_epoch=0
latest_positions=0
latest_open_events=0

for file in "${files[@]}"; do
  if [[ ! -f "${file}" ]]; then
    continue
  fi
  file_epoch="$(stat -c '%Y' "${file}" 2>/dev/null || echo 0)"
  if [[ "${file_epoch}" -gt "${latest_epoch}" ]]; then
    latest_epoch="${file_epoch}"
  fi
  status_epoch="$(node - <<'NODE' "${file}"
const fs = require("fs");
const filePath = process.argv[2];
const payload = JSON.parse(fs.readFileSync(filePath, "utf8"));
const status = payload && typeof payload.status === "object" ? payload.status : {};
const latest = Math.max(
  Number(status.lastSuccessAt || 0),
  Number(status.lastEventAt || 0),
  Number(payload.generatedAt || 0)
);
process.stdout.write(String(Number.isFinite(latest) ? latest : 0));
NODE
)"
  # Some payload fields are epoch milliseconds; normalize to seconds for stale checks.
  if [[ "${status_epoch}" -gt 1000000000000 ]]; then
    status_epoch=$((status_epoch / 1000))
  fi
  if [[ "${status_epoch}" -gt "${latest_status_epoch}" ]]; then
    latest_status_epoch="${status_epoch}"
  fi
  positions_count="$(node - <<'NODE' "${file}"
const fs = require("fs");
const filePath = process.argv[2];
const payload = JSON.parse(fs.readFileSync(filePath, "utf8"));
const count = Array.isArray(payload.positions) ? payload.positions.length : 0;
process.stdout.write(String(count));
NODE
)"
  if [[ "${positions_count}" -gt "${latest_positions}" ]]; then
    latest_positions="${positions_count}"
  fi
  opened_count="$(node - <<'NODE' "${file}"
const fs = require("fs");
const filePath = process.argv[2];
const payload = JSON.parse(fs.readFileSync(filePath, "utf8"));
const count = Array.isArray(payload.positionOpenedEvents) ? payload.positionOpenedEvents.length : 0;
process.stdout.write(String(count));
NODE
)"
  if [[ "${opened_count}" -gt "${latest_open_events}" ]]; then
    latest_open_events="${opened_count}"
  fi
done

latest_activity_epoch="${latest_status_epoch}"
if [[ "${latest_epoch}" -gt "${latest_activity_epoch}" ]]; then
  latest_activity_epoch="${latest_epoch}"
fi

if [[ "${latest_positions}" -le 0 && "${latest_open_events}" -le 0 ]]; then
  echo "[live-positions-watchdog] no live rows or open events yet; starting ${TARGET_UNIT}"
  systemctl restart "${TARGET_UNIT}"
  exit 0
fi

stale_age_seconds="$((now_epoch - latest_activity_epoch))"
if [[ "${stale_age_seconds}" -gt "${STALE_AFTER_SECONDS}" ]]; then
  echo "[live-positions-watchdog] stale for ${stale_age_seconds}s; restarting ${TARGET_UNIT}"
  systemctl restart "${TARGET_UNIT}"
  exit 0
fi

echo "[live-positions-watchdog] healthy latest_activity_age=${stale_age_seconds}s positions=${latest_positions} open_events=${latest_open_events}"
