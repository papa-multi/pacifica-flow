#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SHARDS="${1:-4}"
BUDGET="${2:-${PACIFICA_LIVE_POSITIONS_PROXY_BUDGET:-0}}"
SOURCE_FILE="${3:-$ROOT_DIR/data/live_positions/positions_proxies.txt}"
OUT_DIR="${4:-$ROOT_DIR/data/live_positions/proxy_shards}"

if [[ ! -f "$SOURCE_FILE" ]]; then
  echo "Missing live positions proxy file: $SOURCE_FILE" >&2
  exit 1
fi

if ! [[ "$SHARDS" =~ ^[0-9]+$ ]] || [[ "$SHARDS" -lt 1 ]]; then
  echo "Shard count must be a positive integer." >&2
  exit 1
fi

if ! [[ "$BUDGET" =~ ^[0-9]+$ ]] || [[ "$BUDGET" -lt 0 ]]; then
  echo "Proxy budget must be a non-negative integer." >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
find "$OUT_DIR" -maxdepth 1 -type f -name 'proxies_*.txt' -delete

for ((i=0; i<SHARDS; i+=1)); do
  : > "$OUT_DIR/proxies_${i}.txt"
done

idx=0
selected=0
while IFS= read -r proxy; do
  [[ -n "$proxy" ]] || continue
  if [[ "$BUDGET" -gt 0 && "$selected" -ge "$BUDGET" ]]; then
    break
  fi
  shard=$((idx % SHARDS))
  printf '%s\n' "$proxy" >> "$OUT_DIR/proxies_${shard}.txt"
  idx=$((idx + 1))
  selected=$((selected + 1))
done < <(awk 'NF && !seen[$0]++ { print $0 }' "$SOURCE_FILE")

for ((i=0; i<SHARDS; i+=1)); do
  count="$(wc -l < "$OUT_DIR/proxies_${i}.txt" | tr -d ' ')"
  echo "shard=$i proxies=$count file=$OUT_DIR/proxies_${i}.txt"
done
echo "selected_proxies=$selected budget=$BUDGET source=$SOURCE_FILE"
