#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
ISOLATED_DIR = ROOT / "data" / "wallet_explorer_v2" / "isolated"
SHARDS_DIR = ROOT / "data" / "wallet_explorer_v2" / "shards"
SOURCE_PATH = ROOT / "data" / "ui" / ".build" / "wallet_dataset_compact_source.json"
NEXTGEN_CURRENT_PATH = ROOT / "data" / "nextgen" / "state" / "wallet-current-state.json"
NEXTGEN_LIVE_PATH = ROOT / "data" / "nextgen" / "state" / "wallet-live-status.json"
BUILD_V3_SCRIPT = ROOT / "scripts" / "build_wallet_storage_v3.js"
REPAIR_V3_SCRIPT = ROOT / "scripts" / "repair_wallet_v3_last_activity.js"
REVIEW_SNAPSHOT_SCRIPT = ROOT / "scripts" / "wallet_explorer_v2" / "build_review_pipeline_snapshot.py"


def load_json(path: Path, fallback=None):
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return fallback


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=True, separators=(",", ":"))
    os.replace(tmp, path)


def to_timestamp(value) -> int:
    try:
        number = int(float(value))
    except Exception:
        return 0
    return number if number > 0 else 0


def latest_opened_at_from_payload(payload) -> Optional[int]:
    trades = []
    if isinstance(payload, dict):
        trades = payload.get("trades")
    if not isinstance(trades, list):
        return None
    last_opened_at = 0
    for row in trades:
        if not isinstance(row, dict):
            continue
        side = str(row.get("side") or "").strip().lower()
        if not side.startswith("open_"):
            continue
        opened_at = max(
            to_timestamp(row.get("timestamp")),
            to_timestamp(row.get("created_at")),
            to_timestamp(row.get("createdAt")),
            to_timestamp(row.get("openedAt")),
        )
        if opened_at > last_opened_at:
            last_opened_at = opened_at
    return last_opened_at or None


def set_activity_fields(payload: dict, last_opened_at: Optional[int]) -> bool:
    if not isinstance(payload, dict):
        return False
    next_value = int(last_opened_at) if last_opened_at else None
    changed = False
    for key in (
        "lastOpenedAt",
        "lastActivity",
        "lastActivityAt",
        "lastActiveAt",
        "last_opened_at",
        "last_activity_at",
    ):
        if payload.get(key) != next_value:
            payload[key] = next_value
            changed = True
    all_bucket = payload.get("all")
    if isinstance(all_bucket, dict):
        for key in ("lastOpenedAt", "lastActivity", "lastActivityAt", "lastActiveAt"):
            if all_bucket.get(key) != next_value:
                all_bucket[key] = next_value
                changed = True
    summary = payload.get("summary")
    if isinstance(summary, dict):
        for key in ("lastOpenedAt", "lastActivity", "lastActivityAt", "lastActiveAt"):
            if summary.get(key) != next_value:
                summary[key] = next_value
                changed = True
    return changed


def iter_wallet_dirs(root: Path) -> Iterable[Tuple[str, Path]]:
    if not root.exists():
        return
    for entry in root.iterdir():
        if entry.is_dir():
            yield entry.name, entry


def repair_isolated(mapping: Dict[str, Optional[int]], counters: dict) -> None:
    for wallet, wallet_dir in iter_wallet_dirs(ISOLATED_DIR):
        history_path = wallet_dir / "wallet_history.json"
        history_payload = load_json(history_path, None) if history_path.exists() else None
        last_opened_at = latest_opened_at_from_payload(history_payload)
        mapping[wallet] = last_opened_at
        if isinstance(history_payload, dict) and set_activity_fields(history_payload, last_opened_at):
            write_json(history_path, history_payload)
            counters["isolated_history"] += 1
        for file_name in ("wallet_record.json", "wallet_state.json"):
            target = wallet_dir / file_name
            payload = load_json(target, None) if target.exists() else None
            if isinstance(payload, dict) and set_activity_fields(payload, last_opened_at):
                write_json(target, payload)
                counters["isolated_meta"] += 1


def repair_shards(mapping: Dict[str, Optional[int]], counters: dict) -> None:
    if not SHARDS_DIR.exists():
        return
    for shard_dir in sorted(SHARDS_DIR.iterdir()):
        if not shard_dir.is_dir():
            continue
        history_dir = shard_dir / "wallet_history"
        if history_dir.exists():
            for history_path in history_dir.glob("*.json"):
                wallet = history_path.stem
                payload = load_json(history_path, None)
                last_opened_at = latest_opened_at_from_payload(payload)
                if wallet not in mapping or mapping[wallet] is None:
                    mapping[wallet] = last_opened_at
                if isinstance(payload, dict) and set_activity_fields(payload, last_opened_at):
                    write_json(history_path, payload)
                    counters["shard_history"] += 1
        for subdir_name in ("wallet_records", "wallet_states"):
            subdir = shard_dir / subdir_name
            if not subdir.exists():
                continue
            for path in subdir.glob("*.json"):
                wallet = path.stem
                payload = load_json(path, None)
                if not isinstance(payload, dict):
                    continue
                last_opened_at = mapping.get(wallet)
                if wallet not in mapping:
                    last_opened_at = None
                    mapping[wallet] = None
                if set_activity_fields(payload, last_opened_at):
                    write_json(path, payload)
                    counters["shard_meta"] += 1


def repair_source_dataset(mapping: Dict[str, Optional[int]], counters: dict) -> None:
    if not SOURCE_PATH.exists():
        counters["source_rows"] = 0
        counters["source_rows_cleared"] = 0
        return
    payload = load_json(SOURCE_PATH, None)
    if not isinstance(payload, dict):
        raise RuntimeError(f"unexpected source payload structure at {SOURCE_PATH}")
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise RuntimeError(f"unexpected source rows structure at {SOURCE_PATH}")
    changed = 0
    cleared = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("wallet") or "").strip()
        if not wallet:
            continue
        has_mapping = wallet in mapping
        last_opened_at = mapping.get(wallet)
        if not has_mapping and not row.get("lastOpenedAt") and row.get("lastActivity") == row.get("updatedAt"):
            last_opened_at = None
        elif not has_mapping:
            continue
        next_value = int(last_opened_at) if last_opened_at else None
        row_changed = False
        for key in ("lastOpenedAt", "lastActivity", "lastActivityAt", "lastActiveAt"):
            if row.get(key) != next_value:
                row[key] = next_value
                row_changed = True
        if row_changed:
            changed += 1
            if next_value is None:
                cleared += 1
    if changed:
        payload["generatedAt"] = int(time.time() * 1000)
        payload["lastActivitySource"] = "isolated_wallet_open_history"
        write_json(SOURCE_PATH, payload)
    counters["source_rows"] = changed
    counters["source_rows_cleared"] = cleared


def repair_nextgen_state(path: Path, mapping: Dict[str, Optional[int]], counters: dict, counter_key: str) -> None:
    payload = load_json(path, None)
    if not isinstance(payload, dict):
        return
    changed = 0
    for wallet, row in payload.items():
        if not isinstance(row, dict):
            continue
        if wallet not in mapping:
            continue
        next_value = int(mapping[wallet]) if mapping[wallet] else None
        row_changed = False
        for key in ("last_opened_at", "last_activity_at"):
            if row.get(key) != next_value:
                row[key] = next_value
                row_changed = True
        if "lastOpenedAt" in row and row.get("lastOpenedAt") != next_value:
            row["lastOpenedAt"] = next_value
            row_changed = True
        if "lastActivity" in row and row.get("lastActivity") != next_value:
            row["lastActivity"] = next_value
            row_changed = True
        if "lastActivityAt" in row and row.get("lastActivityAt") != next_value:
            row["lastActivityAt"] = next_value
            row_changed = True
        if row_changed:
            changed += 1
    if changed:
        write_json(path, payload)
    counters[counter_key] = changed


def run_step(command: list[str], env: Optional[dict] = None) -> None:
    merged_env = dict(os.environ)
    if env:
        merged_env.update(env)
    if command and Path(command[0]).name == "node":
        merged_env.setdefault("NODE_OPTIONS", "--max-old-space-size=8192")
    completed = subprocess.run(command, cwd=str(ROOT), env=merged_env, check=True)
    if completed.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(command)}")


def main() -> int:
    started_at = time.time()
    mapping: Dict[str, Optional[int]] = {}
    counters = {
        "isolated_history": 0,
        "isolated_meta": 0,
        "shard_history": 0,
        "shard_meta": 0,
        "source_rows": 0,
        "source_rows_cleared": 0,
        "nextgen_current": 0,
        "nextgen_live": 0,
    }

    repair_isolated(mapping, counters)
    repair_shards(mapping, counters)
    repair_source_dataset(mapping, counters)
    repair_nextgen_state(NEXTGEN_CURRENT_PATH, mapping, counters, "nextgen_current")
    repair_nextgen_state(NEXTGEN_LIVE_PATH, mapping, counters, "nextgen_live")
    if SOURCE_PATH.exists():
        run_step(["node", str(BUILD_V3_SCRIPT), f"--source={SOURCE_PATH}", f"--root={ROOT}"])
    run_step(["node", str(REPAIR_V3_SCRIPT)])
    if REVIEW_SNAPSHOT_SCRIPT.exists():
        run_step(["python3", str(REVIEW_SNAPSHOT_SCRIPT)])

    elapsed = round(time.time() - started_at, 2)
    wallets_with_open = sum(1 for value in mapping.values() if value)
    wallets_without_open = sum(1 for value in mapping.values() if not value)
    report = {
        "done": True,
        "elapsedSeconds": elapsed,
        "walletsMapped": len(mapping),
        "walletsWithOpenHistory": wallets_with_open,
        "walletsWithoutOpenHistory": wallets_without_open,
        **counters,
    }
    print(json.dumps(report, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
