#!/usr/bin/env python3
import json
import os
import tempfile
from pathlib import Path


ROOT_DIR = Path("/root/pacifica-flow")
DATA_DIR = ROOT_DIR / "data"
INDEXER_DISCOVERY_PATH = DATA_DIR / "indexer" / "wallet_discovery.json"
WALLET_ADDRESSES_PATH = DATA_DIR / "wallet_explorer_v2" / "wallet_addresses.json"
LIVE_POSITIONS_DIR = DATA_DIR / "live_positions"
OUTPUT_PATH = LIVE_POSITIONS_DIR / "live_wallet_universe.json"


def now_ms() -> int:
    return int(__import__("time").time() * 1000)


def read_json(path: Path, default):
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def write_json_atomic(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=str(path.parent), delete=False, encoding="utf-8") as handle:
        json.dump(payload, handle, separators=(",", ":"), ensure_ascii=True)
        handle.write("\n")
        temp_path = Path(handle.name)
    os.replace(temp_path, path)


def file_mtime_ms(path: Path) -> int:
    try:
        return int(path.stat().st_mtime * 1000)
    except Exception:
        return 0


def normalize_wallet(value) -> str:
    return str(value or "").strip()


def normalize_ts(*values, fallback: int = 0) -> int:
    for value in values:
        try:
            num = int(float(value))
        except Exception:
            continue
        if num <= 0:
            continue
        return num if num >= 10**12 else num * 1000
    return int(fallback or 0)


def should_refresh(output_path: Path, source_paths: list[Path]) -> bool:
    output_mtime = file_mtime_ms(output_path)
    if output_mtime <= 0:
        return True
    latest_source_mtime = max((file_mtime_ms(path) for path in source_paths), default=0)
    return latest_source_mtime > output_mtime


def upsert_wallet(rows: dict, wallet: str, *, updated_at: int = 0, live_scanned_at: int = 0, open_positions: int | None = None, source: str | None = None) -> None:
    wallet = normalize_wallet(wallet)
    if not wallet:
        return
    row = rows.setdefault(
        wallet,
        {
            "wallet": wallet,
            "updatedAt": 0,
            "liveScannedAt": 0,
            "openPositions": 0,
            "sources": [],
        },
    )
    if updated_at > int(row.get("updatedAt") or 0):
        row["updatedAt"] = int(updated_at)
    if live_scanned_at > int(row.get("liveScannedAt") or 0):
        row["liveScannedAt"] = int(live_scanned_at)
    if open_positions is not None:
        row["openPositions"] = max(int(row.get("openPositions") or 0), max(0, int(open_positions)))
    if source:
        sources = row.setdefault("sources", [])
        if source not in sources:
            sources.append(source)


def merge_discovery(rows: dict, fallback_ts: int) -> None:
    payload = read_json(INDEXER_DISCOVERY_PATH, {}) or {}
    wallets = payload.get("wallets") or {}
    if isinstance(wallets, dict):
        for wallet, meta in wallets.items():
            meta = meta if isinstance(meta, dict) else {}
            rank_ts = normalize_ts(
                meta.get("updatedAt"),
                meta.get("lastSeenAt"),
                meta.get("firstSeenAt"),
                payload.get("updatedAt"),
                fallback=fallback_ts,
            )
            upsert_wallet(
                rows,
                wallet,
                updated_at=rank_ts,
                live_scanned_at=rank_ts,
                source="wallet_discovery",
            )
    elif isinstance(wallets, list):
        for item in wallets:
            if isinstance(item, dict):
                wallet = item.get("wallet") or item.get("address")
                rank_ts = normalize_ts(
                    item.get("updatedAt"),
                    item.get("lastSeenAt"),
                    item.get("firstSeenAt"),
                    payload.get("updatedAt"),
                    fallback=fallback_ts,
                )
            else:
                wallet = item
                rank_ts = normalize_ts(payload.get("updatedAt"), fallback=fallback_ts)
            upsert_wallet(
                rows,
                wallet,
                updated_at=rank_ts,
                live_scanned_at=rank_ts,
                source="wallet_discovery",
            )


def merge_wallet_addresses(rows: dict, fallback_ts: int) -> None:
    payload = read_json(WALLET_ADDRESSES_PATH, {}) or {}
    wallets = payload.get("wallets") or []
    generated_at = normalize_ts(payload.get("generatedAt"), fallback=fallback_ts)
    for item in wallets:
        if isinstance(item, dict):
            wallet = item.get("wallet") or item.get("address")
            item_sources = item.get("sources") if isinstance(item.get("sources"), list) else []
        else:
            wallet = item
            item_sources = []
        upsert_wallet(
            rows,
            wallet,
            updated_at=generated_at,
            live_scanned_at=generated_at,
            source="wallet_addresses",
        )
        for source in item_sources:
            upsert_wallet(rows, wallet, source=str(source or "").strip())


def merge_live_snapshots(rows: dict, fallback_ts: int) -> None:
    freshness_cutoff = now_ms() - 10 * 60 * 1000
    for path in sorted(LIVE_POSITIONS_DIR.glob("wallet_first_shard_*.json")):
        if file_mtime_ms(path) < freshness_cutoff:
            continue
        payload = read_json(path, {}) or {}
        generated_at = normalize_ts(payload.get("generatedAt"), fallback=fallback_ts)
        position_counts = {}
        for position in payload.get("positions") or []:
            if not isinstance(position, dict):
                continue
            wallet = position.get("wallet")
            observed_at = normalize_ts(
                position.get("observedAt"),
                position.get("updatedAt"),
                position.get("openedAt"),
                generated_at,
                fallback=fallback_ts,
            )
            position_counts[normalize_wallet(wallet)] = position_counts.get(normalize_wallet(wallet), 0) + 1
            upsert_wallet(
                rows,
                wallet,
                updated_at=observed_at,
                live_scanned_at=observed_at,
                source="live_positions_snapshot",
            )
        for wallet, count in position_counts.items():
            upsert_wallet(
                rows,
                wallet,
                updated_at=generated_at,
                live_scanned_at=generated_at,
                open_positions=count,
                source="live_positions_snapshot",
            )
        for hint in payload.get("accountOpenHints") or []:
            if not isinstance(hint, dict):
                continue
            wallet = hint.get("wallet")
            hint_ts = normalize_ts(
                hint.get("lastAccountAt"),
                hint.get("updatedAt"),
                generated_at,
                fallback=fallback_ts,
            )
            upsert_wallet(
                rows,
                wallet,
                updated_at=hint_ts,
                live_scanned_at=hint_ts,
                open_positions=max(0, int(hint.get("positionsCount") or 0)),
                source="account_open_hint",
            )


def build_payload() -> dict:
    current_ms = now_ms()
    rows = {}
    merge_discovery(rows, current_ms)
    merge_wallet_addresses(rows, current_ms)
    merge_live_snapshots(rows, current_ms)
    wallets = sorted(
        rows.values(),
        key=lambda row: (
            -(int(row.get("openPositions") or 0)),
            -(int(row.get("liveScannedAt") or 0)),
            -(int(row.get("updatedAt") or 0)),
            str(row.get("wallet") or ""),
        ),
    )
    return {
        "generatedAt": current_ms,
        "wallets": wallets,
    }


def main() -> int:
    source_paths = [INDEXER_DISCOVERY_PATH, WALLET_ADDRESSES_PATH]
    source_paths.extend(sorted(LIVE_POSITIONS_DIR.glob("wallet_first_shard_*.json")))
    if not should_refresh(OUTPUT_PATH, source_paths):
        print(f"live wallet universe already current: {OUTPUT_PATH}")
        return 0
    payload = build_payload()
    write_json_atomic(OUTPUT_PATH, payload)
    print(f"wrote {OUTPUT_PATH} wallets={len(payload.get('wallets') or [])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
