#!/usr/bin/env python3
import json
import sys
from pathlib import Path


def stable_shard_hash(value: str) -> int:
    text = str(value or "")
    hash_value = 5381
    for char in text:
        hash_value = ((hash_value << 5) + hash_value + ord(char)) & 0xFFFFFFFF
    return hash_value


def shard_index_for_wallet(wallet: str, shard_count: int) -> int:
    return stable_shard_hash(f"wallet:{str(wallet or '').strip()}") % shard_count


def read_json(path: Path):
    if not path.exists():
        return None
    with open(path) as handle:
        return json.load(handle)


def write_json_atomic(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as handle:
        json.dump(payload, handle, separators=(",", ":"))
    tmp.replace(path)


def normalize_wallet_row(record: dict, wallet: str, shard_dir: Path, shard_index: int) -> dict:
    next_record = dict(record or {})
    next_record["wallet"] = wallet
    storage = dict(next_record.get("storage") or {})
    storage["indexerShardId"] = f"indexer_shard_{shard_index}"
    storage["indexerStatePath"] = str(shard_dir / "indexer_state.json")
    storage["walletStorePath"] = str(shard_dir / "wallets.json")
    storage["walletHistoryPath"] = str(shard_dir / "wallet_history" / f"{wallet}.json")
    storage.pop("mergedWalletStorePath", None)
    next_record["storage"] = storage
    all_bucket = next_record.get("all") if isinstance(next_record.get("all"), dict) else {}
    for key in (
        "trades",
        "volumeUsd",
        "feesUsd",
        "feesPaidUsd",
        "liquidityPoolFeesUsd",
        "feeRebatesUsd",
        "netFeesUsd",
        "fundingPayoutUsd",
        "revenueUsd",
        "pnlUsd",
        "wins",
        "losses",
        "winRatePct",
        "firstTrade",
        "lastTrade",
    ):
        if next_record.get(key) is None and key in all_bucket:
            next_record[key] = all_bucket.get(key)
    return next_record


def main() -> int:
    if len(sys.argv) != 4:
        raise SystemExit("usage: repair_indexer_shard.py <shard-dir> <shard-index> <shard-count>")
    shard_dir = Path(sys.argv[1]).resolve()
    shard_index = int(sys.argv[2])
    shard_count = int(sys.argv[3])
    state_path = shard_dir / "indexer_state.json"
    store_path = shard_dir / "wallets.json"
    history_dir = shard_dir / "wallet_history"
    history_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        "shard": shard_index,
        "walletStoreBefore": 0,
        "walletStoreAfter": 0,
        "stateWalletsBefore": 0,
        "stateWalletsAfter": 0,
        "historyFilesBefore": 0,
        "historyFilesAfter": 0,
    }

    store_payload = read_json(store_path)
    if isinstance(store_payload, dict) and isinstance(store_payload.get("wallets"), dict):
        summary["walletStoreBefore"] = len(store_payload["wallets"])
        kept_wallets = {}
        for wallet, record in store_payload["wallets"].items():
            normalized = str(wallet or "").strip()
            if not normalized or shard_index_for_wallet(normalized, shard_count) != shard_index:
                continue
            kept_wallets[normalized] = normalize_wallet_row(
                dict(record or {}), normalized, shard_dir, shard_index
            )
        store_payload["wallets"] = kept_wallets
        summary["walletStoreAfter"] = len(kept_wallets)
        write_json_atomic(store_path, store_payload)

    state_payload = read_json(state_path)
    if isinstance(state_payload, dict):
        wallet_states = state_payload.get("walletStates") or {}
        if isinstance(wallet_states, dict):
            summary["stateWalletsBefore"] = len(wallet_states)
            kept_states = {}
            for wallet, row in wallet_states.items():
                normalized = str((row or {}).get("wallet") or wallet or "").strip()
                if not normalized or shard_index_for_wallet(normalized, shard_count) != shard_index:
                    continue
                next_row = dict(row or {})
                next_row["wallet"] = normalized
                next_row["storage"] = {
                    **dict(next_row.get("storage") or {}),
                    "indexerShardId": f"indexer_shard_{shard_index}",
                    "indexerStatePath": str(state_path),
                    "walletHistoryDir": str(history_dir),
                }
                kept_states[normalized] = next_row
            state_payload["walletStates"] = kept_states
            summary["stateWalletsAfter"] = len(kept_states)
        for key in ("knownWallets", "liveWallets", "priorityQueue", "continuationQueue"):
            rows = state_payload.get(key)
            if not isinstance(rows, list):
                continue
            seen = set()
            filtered = []
            for wallet in rows:
                normalized = str(wallet or "").strip()
                if not normalized or shard_index_for_wallet(normalized, shard_count) != shard_index:
                    continue
                if normalized in seen:
                    continue
                seen.add(normalized)
                filtered.append(normalized)
            state_payload[key] = filtered
        write_json_atomic(state_path, state_payload)

    history_files = list(history_dir.glob("*.json"))
    summary["historyFilesBefore"] = len(history_files)
    kept_history = 0
    for file_path in history_files:
        wallet = file_path.stem.strip()
        if not wallet or shard_index_for_wallet(wallet, shard_count) != shard_index:
            file_path.unlink(missing_ok=True)
            continue
        kept_history += 1
    summary["historyFilesAfter"] = kept_history

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
