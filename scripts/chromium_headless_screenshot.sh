#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <output.png> <url> [extra chromium args...]" >&2
  exit 64
fi

OUTPUT_PATH="$1"
TARGET_URL="$2"
shift 2

mkdir -p "$(dirname "$OUTPUT_PATH")"

export HOME="${HOME:-/tmp/chromium-headless-home}"
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/tmp/chromium-headless-runtime}"
export DBUS_SESSION_BUS_ADDRESS=""
mkdir -p "$HOME" "$XDG_RUNTIME_DIR"
chmod 700 "$XDG_RUNTIME_DIR" 2>/dev/null || true

PROFILE_DIR="$(mktemp -d /tmp/chromium-headless-profile.XXXXXX)"
LOG_PATH="$(mktemp /tmp/chromium-headless-log.XXXXXX)"
trap 'rm -rf "$PROFILE_DIR" "$LOG_PATH"' EXIT

VIRTUAL_TIME_BUDGET="${CHROMIUM_VIRTUAL_TIME_BUDGET:-16000}"
WINDOW_SIZE="${CHROMIUM_WINDOW_SIZE:-1440,2600}"

if /snap/bin/chromium \
  --headless \
  --disable-gpu \
  --no-sandbox \
  --hide-scrollbars \
  --run-all-compositor-stages-before-draw \
  --virtual-time-budget="$VIRTUAL_TIME_BUDGET" \
  --window-size="$WINDOW_SIZE" \
  --user-data-dir="$PROFILE_DIR" \
  --disable-background-networking \
  --disable-component-update \
  --disable-default-apps \
  --disable-domain-reliability \
  --disable-sync \
  --metrics-recording-only \
  --no-first-run \
  --disable-features=MediaRouter,OptimizationHints,AutofillServerCommunication,CertificateTransparencyComponentUpdater,OptimizationGuideModelDownloading,Translate \
  --password-store=basic \
  --use-mock-keychain \
  --enable-logging=stderr \
  --log-level=3 \
  --screenshot="$OUTPUT_PATH" \
  "$TARGET_URL" \
  "$@" >"$LOG_PATH" 2>&1; then
  if [ -f "$OUTPUT_PATH" ]; then
    echo "Wrote $OUTPUT_PATH"
    exit 0
  fi
fi

echo "Screenshot failed for $TARGET_URL" >&2
tail -n 40 "$LOG_PATH" >&2 || true
exit 1
