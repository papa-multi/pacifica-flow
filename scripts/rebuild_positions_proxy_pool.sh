#!/usr/bin/env bash
set -euo pipefail

# Rebuild a positions-endpoint-healthy proxy subset for wallet-first live scanning.
#
# Usage:
#   scripts/rebuild_positions_proxy_pool.sh
#   scripts/rebuild_positions_proxy_pool.sh --source data/indexer/working_proxies_all.txt --out data/indexer/working_proxies.txt

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_FILE="$ROOT_DIR/data/indexer/working_proxies_all.txt"
if [[ ! -f "$SOURCE_FILE" ]]; then
  SOURCE_FILE="$ROOT_DIR/data/indexer/working_proxies.txt"
fi
OUT_FILE="$ROOT_DIR/data/live_positions/positions_proxies.txt"
TMP_DIR="/tmp"
PARALLEL=32
TIMEOUT_SEC=10
TEST_ACCOUNT="11111111111111111111111111111111"
URL="https://api.pacifica.fi/api/v1/positions?account=${TEST_ACCOUNT}"
MIN_OK=10

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source)
      SOURCE_FILE="$2"
      shift 2
      ;;
    --out)
      OUT_FILE="$2"
      shift 2
      ;;
    --parallel)
      PARALLEL="$2"
      shift 2
      ;;
    --timeout)
      TIMEOUT_SEC="$2"
      shift 2
      ;;
    --min-ok)
      MIN_OK="$2"
      shift 2
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

if [[ ! -f "$SOURCE_FILE" ]]; then
  echo "Source proxy file not found: $SOURCE_FILE" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUT_FILE")"

UNIQ_FILE="$TMP_DIR/pf_positions_proxy_uniq_$$.txt"
OK_FILE="$TMP_DIR/pf_positions_proxy_ok_$$.txt"
PROBE_SCRIPT="$TMP_DIR/pf_probe_proxy_$$.sh"

cleanup() {
  rm -f "$UNIQ_FILE" "$OK_FILE" "$PROBE_SCRIPT"
}
trap cleanup EXIT

cat >"$PROBE_SCRIPT" <<'EOS'
#!/usr/bin/env bash
set -euo pipefail
p="$1"
u="$2"
timeout_sec="$3"
resp="$(curl --silent --show-error --max-time "$timeout_sec" --proxy "$p" --write-out '\n__CODE__:%{http_code}' "$u" 2>/dev/null || true)"
code="$(printf "%s" "$resp" | awk -F: '/__CODE__/{print $2}' | tail -n1)"
body="$(printf "%s" "$resp" | sed '/__CODE__:/d')"
if [[ "$code" == "200" || "$code" == "429" ]]; then
  if printf "%s" "$body" | grep -Eq '"success"[[:space:]]*:[[:space:]]*true|Rate limit exceeded'; then
    printf "%s\n" "$p"
  fi
fi
EOS
chmod +x "$PROBE_SCRIPT"

sed '/^[[:space:]]*$/d' "$SOURCE_FILE" | sort -u >"$UNIQ_FILE"
TOTAL="$(wc -l <"$UNIQ_FILE" | tr -d ' ')"

cat "$UNIQ_FILE" | xargs -I{} -P "$PARALLEL" "$PROBE_SCRIPT" {} "$URL" "$TIMEOUT_SEC" >"$OK_FILE"
OK_COUNT="$(wc -l <"$OK_FILE" | tr -d ' ')"

if [[ "$OK_COUNT" -lt "$MIN_OK" ]]; then
  echo "Probe result too small (ok=$OK_COUNT < min_ok=$MIN_OK). Keeping existing output file unchanged." >&2
  exit 2
fi

cp "$OUT_FILE" "${OUT_FILE}.bak.$(date -u +%Y%m%dT%H%M%SZ)" 2>/dev/null || true
cp "$OK_FILE" "$OUT_FILE"

echo "positions-proxy-pool rebuilt: total=$TOTAL ok=$OK_COUNT out=$OUT_FILE"
