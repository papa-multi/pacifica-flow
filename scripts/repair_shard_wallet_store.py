#!/usr/bin/env python3
import json
import re
from pathlib import Path


def stable_shard_hash(value: str) -> int:
    text = str(value or "")
    hash_value = 5381
    for char in text:
        hash_value = ((hash_value << 5) + hash_value + ord(char)) & 0xFFFFFFFF
    return hash_value


def shard_index_for_wallet(wallet: str, shard_count: int) -> int:
    return stable_shard_hash(f"wallet:{wallet.strip()}") % shard_count


def main() -> int:
    base = Path("/root/pacifica-flow/data/indexer/shards")
    shards = sorted([path for path in base.iterdir() if path.is_dir()])
    shard_count = len(shards)
    summary = []

    for shard in shards:
      match = re.match(r"shard_(\d+)$", shard.name)
      if not match:
          continue
      shard_index = int(match.group(1))
      store_path = shard / "wallets.json"
      if not store_path.exists():
          continue
      payload = json.load(open(store_path))
      if isinstance(payload, dict) and isinstance(payload.get("wallets"), dict):
          before = len(payload["wallets"])
          kept = {
              wallet: record
              for wallet, record in payload["wallets"].items()
              if shard_index_for_wallet(wallet, shard_count) == shard_index
          }
          if len(kept) != before:
              payload["wallets"] = kept
              with open(store_path, "w") as handle:
                  json.dump(payload, handle, separators=(",", ":"))
          summary.append((shard.name, before, len(kept)))
      elif isinstance(payload, list):
          before = len(payload)
          kept = [
              record
              for record in payload
              if shard_index_for_wallet((record or {}).get("wallet", ""), shard_count) == shard_index
          ]
          if len(kept) != before:
              with open(store_path, "w") as handle:
                  json.dump(kept, handle, separators=(",", ":"))
          summary.append((shard.name, before, len(kept)))

    print(json.dumps({
        "summary": summary,
        "totalAfter": sum(after for _, _, after in summary),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
