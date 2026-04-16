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
PASSES=2
MAX_LATENCY_MS=3500
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
    --passes)
      PASSES="$2"
      shift 2
      ;;
    --max-latency-ms)
      MAX_LATENCY_MS="$2"
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
STRICT_FILE="$TMP_DIR/pf_positions_proxy_strict_$$.txt"
FALLBACK_FILE="$TMP_DIR/pf_positions_proxy_fallback_$$.txt"
PROBE_SCRIPT="$TMP_DIR/pf_probe_proxy_$$.sh"

cleanup() {
  rm -f "$UNIQ_FILE" "$OK_FILE" "$STRICT_FILE" "$FALLBACK_FILE" "$PROBE_SCRIPT"
}
trap cleanup EXIT

cat >"$PROBE_SCRIPT" <<'EOS'
#!/usr/bin/env bash
set -euo pipefail
p="$1"
u="$2"
timeout_sec="$3"
resp="$(curl --silent --show-error --max-time "$timeout_sec" --proxy "$p" --write-out '\n__CODE__:%{http_code}\n__TIME__:%{time_total}' "$u" 2>/dev/null || true)"
code="$(printf "%s" "$resp" | awk -F: '/__CODE__/{print $2}' | tail -n1)"
time_total="$(printf "%s" "$resp" | awk -F: '/__TIME__/{print $2}' | tail -n1)"
body="$(printf "%s" "$resp" | sed '/__CODE__:/d')"
if [[ "$code" == "200" || "$code" == "429" ]]; then
  if printf "%s" "$body" | grep -Eq '"success"[[:space:]]*:[[:space:]]*true|Rate limit exceeded'; then
    printf "%s\t%s\n" "$p" "${time_total:-0}"
  fi
fi
EOS
chmod +x "$PROBE_SCRIPT"

sed '/^[[:space:]]*$/d' "$SOURCE_FILE" | sort -u >"$UNIQ_FILE"
TOTAL="$(wc -l <"$UNIQ_FILE" | tr -d ' ')"

for ((pass=1; pass<=PASSES; pass+=1)); do
  cat "$UNIQ_FILE" | xargs -I{} -P "$PARALLEL" "$PROBE_SCRIPT" {} "$URL" "$TIMEOUT_SEC" >>"$OK_FILE"
done
awk -F'\t' -v max_ms="$MAX_LATENCY_MS" '
  NF >= 2 {
    proxy=$1;
    latency_ms=int(($2+0)*1000);
    seen[proxy]+=1;
    sum[proxy]+=latency_ms;
    if (latency_ms <= max_ms) fast[proxy]+=1;
  }
  END {
    for (proxy in seen) {
      avg=int(sum[proxy]/seen[proxy]);
      printf "%s\t%d\t%d\t%d\n", proxy, seen[proxy], fast[proxy]+0, avg;
    }
  }
' "$OK_FILE" | sort -t$'\t' -k4,4n > "$FALLBACK_FILE"

awk -F'\t' -v passes="$PASSES" '
  NF >= 4 {
    proxy=$1;
    seen=$2+0;
    fast=$3+0;
    avg=$4+0;
    if (seen >= passes && fast >= passes) {
      printf "%s\t%d\n", proxy, avg;
    }
  }
' "$FALLBACK_FILE" | sort -t$'\t' -k2,2n | cut -f1 > "$STRICT_FILE"

OK_COUNT="$(wc -l <"$STRICT_FILE" | tr -d ' ')"

if [[ "$OK_COUNT" -lt "$MIN_OK" ]]; then
  awk -F'\t' 'NF >= 4 { print $1 }' "$FALLBACK_FILE" | head -n "$MIN_OK" > "$STRICT_FILE"
  OK_COUNT="$(wc -l <"$STRICT_FILE" | tr -d ' ')"
  if [[ "$OK_COUNT" -lt "$MIN_OK" ]]; then
    echo "Probe result too small (ok=$OK_COUNT < min_ok=$MIN_OK). Keeping existing output file unchanged." >&2
    exit 2
  fi
fi

cp "$OUT_FILE" "${OUT_FILE}.bak.$(date -u +%Y%m%dT%H%M%SZ)" 2>/dev/null || true
cp "$STRICT_FILE" "$OUT_FILE"

echo "positions-proxy-pool rebuilt: total=$TOTAL ok=$OK_COUNT passes=$PASSES max_latency_ms=$MAX_LATENCY_MS out=$OUT_FILE"
