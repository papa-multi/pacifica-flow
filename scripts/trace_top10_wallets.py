#!/usr/bin/env python3
import json
import sys
import math
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


ACTIVE_SHARD_COUNT = 8
API_BASE = "https://api.pacifica.fi/api/v1"
TRADES_LIMIT = 120
FUNDING_LIMIT = 120
MAX_RETRIES = 2
BASE_DIR = Path("/root/pacifica-flow/data/indexer/shards")
OUT_PATH = Path("/tmp/top10_wallet_trace.json")


def read_json(path: Path):
    with path.open() as handle:
      return json.load(handle)


def active_shard_dirs():
    return [BASE_DIR / f"shard_{index}" for index in range(ACTIVE_SHARD_COUNT)]


def all_shard_dirs():
    return sorted(path for path in BASE_DIR.glob("shard_*") if path.is_dir())


def wallet_row_timestamp(row):
    if not isinstance(row, dict):
        return 0
    all_bucket = row.get("all") if isinstance(row.get("all"), dict) else {}
    return max(
        int(row.get("updatedAt") or 0),
        int(all_bucket.get("computedAt") or 0),
        int(row.get("createdAt") or 0),
    )


def state_timestamp(row):
    if not isinstance(row, dict):
        return 0
    fields = (
        "updatedAt",
        "lastSuccessAt",
        "lastAttemptAt",
        "lastScannedAt",
        "lastFailureAt",
        "retryQueuedAt",
        "backfillCompletedAt",
        "remoteHistoryVerifiedAt",
        "discoveredAt",
    )
    return max(int(row.get(field) or 0) for field in fields)


def to_num(value, fallback=0.0):
    try:
        num = float(value)
    except (TypeError, ValueError):
        return fallback
    return num if math.isfinite(num) else fallback


def fetch_json(url: str):
    with urllib.request.urlopen(url, timeout=12) as response:
        return json.load(response)


def fetch_cursor_stream(endpoint: str, wallet: str, limit: int):
    rows = []
    page_count = 0
    cursor = None
    first_cursor = None
    last_cursor = None
    while True:
        params = {"account": wallet, "limit": str(limit)}
        if cursor:
            params["cursor"] = cursor
        url = f"{API_BASE}/{endpoint}?{urllib.parse.urlencode(params)}"
        payload = None
        error_message = None
        for attempt in range(MAX_RETRIES):
            try:
                payload = fetch_json(url)
                break
            except urllib.error.HTTPError as error:
                error_message = f"http_{error.code}"
                if error.code in (429, 500, 502, 503, 504):
                    time.sleep(min(2 ** attempt, 8))
                    continue
                raise
            except Exception as error:  # noqa: BLE001
                error_message = str(error)
                time.sleep(min(2 ** attempt, 8))
        if payload is None:
            raise RuntimeError(f"{endpoint} fetch failed for {wallet}: {error_message}")
        data = payload.get("data") if isinstance(payload, dict) else None
        next_cursor = payload.get("next_cursor") if isinstance(payload, dict) else None
        has_more = bool(payload.get("has_more")) if isinstance(payload, dict) else False
        if first_cursor is None:
            first_cursor = cursor
        last_cursor = next_cursor
        page_count += 1
        if isinstance(data, list):
            rows.extend(data)
        if not has_more:
            return {
                "rows": rows,
                "pageCount": page_count,
                "firstCursor": first_cursor,
                "lastCursor": last_cursor,
                "exhausted": True,
            }
        cursor = next_cursor
        if not cursor:
            raise RuntimeError(f"{endpoint} has_more without next_cursor for {wallet}")


def compute_trade_metrics(rows):
    total_volume = 0.0
    fees_paid = 0.0
    fee_rebates = 0.0
    pnl = 0.0
    wins = 0
    losses = 0
    first_trade = None
    last_trade = None
    for row in rows:
        amount = abs(to_num(row.get("amount"), 0.0))
        price = to_num(row.get("price"), 0.0)
        fee = to_num(row.get("fee"), 0.0)
        trade_pnl = to_num(row.get("pnl"), 0.0)
        timestamp = int(row.get("created_at") or row.get("timestamp") or 0)
        total_volume += amount * price
        if fee > 0:
            fees_paid += fee
        elif fee < 0:
            fee_rebates += abs(fee)
        pnl += trade_pnl
        if trade_pnl > 0:
            wins += 1
        elif trade_pnl < 0:
            losses += 1
        if timestamp > 0:
            first_trade = timestamp if first_trade is None else min(first_trade, timestamp)
            last_trade = timestamp if last_trade is None else max(last_trade, timestamp)
    return {
        "tradeCount": len(rows),
        "volumeUsd": round(total_volume, 6),
        "feesPaidUsd": round(fees_paid, 6),
        "feeRebatesUsd": round(fee_rebates, 6),
        "pnlUsd": round(pnl, 6),
        "wins": wins,
        "losses": losses,
        "firstTrade": first_trade,
        "lastTrade": last_trade,
    }


def compute_funding_metrics(rows):
    payout = 0.0
    first_created = None
    last_created = None
    for row in rows:
        payout += to_num(row.get("payout"), 0.0)
        created = int(row.get("created_at") or row.get("createdAt") or 0)
        if created > 0:
            first_created = created if first_created is None else min(first_created, created)
            last_created = created if last_created is None else max(last_created, created)
    return {
        "fundingCount": len(rows),
        "fundingPayoutUsd": round(payout, 6),
        "firstFunding": first_created,
        "lastFunding": last_created,
    }


def summarize_state_row(row):
    if not isinstance(row, dict):
        return {}
    return {
        "tradeDone": bool(row.get("tradeDone")),
        "fundingDone": bool(row.get("fundingDone")),
        "remoteHistoryVerified": bool(row.get("remoteHistoryVerified")),
        "tradeHasMore": bool(row.get("tradeHasMore")),
        "fundingHasMore": bool(row.get("fundingHasMore")),
        "tradeCursor": row.get("tradeCursor"),
        "fundingCursor": row.get("fundingCursor"),
        "historyPhase": row.get("historyPhase"),
        "lifecycleStage": row.get("lifecycleStage"),
        "retryPending": bool(row.get("retryPending")),
        "retryReason": row.get("retryReason"),
        "forceHeadRefetch": bool(row.get("forceHeadRefetch")),
        "storage": row.get("storage") if isinstance(row.get("storage"), dict) else {},
        "tradePagination": row.get("tradePagination") if isinstance(row.get("tradePagination"), dict) else {},
        "fundingPagination": row.get("fundingPagination") if isinstance(row.get("fundingPagination"), dict) else {},
    }


def load_wallet_top10():
    freshest = {}
    for shard_dir in active_shard_dirs():
        wallet_path = shard_dir / "wallets.json"
        if not wallet_path.exists():
            continue
        payload = read_json(wallet_path)
        wallet_rows = payload.get("wallets") if isinstance(payload, dict) else {}
        for wallet, row in (wallet_rows or {}).items():
            if not isinstance(row, dict):
                continue
            timestamp = wallet_row_timestamp(row)
            volume = to_num((row.get("all") or {}).get("volumeUsd"), to_num(row.get("volumeUsd"), 0.0))
            current = freshest.get(wallet)
            if current is None or timestamp >= current["timestamp"]:
                freshest[wallet] = {
                    "wallet": wallet,
                    "timestamp": timestamp,
                    "volumeUsd": volume,
                    "row": row,
                    "rowShard": shard_dir.name,
                }
    ordered = sorted(
        freshest.values(),
        key=lambda item: (item["volumeUsd"], item["timestamp"], item["wallet"]),
        reverse=True,
    )
    return ordered[:10]


def collect_wallet_occurrences(wallet):
    row_occurrences = []
    state_occurrences = []
    history_paths = []
    for shard_dir in all_shard_dirs():
        wallet_path = shard_dir / "wallets.json"
        if wallet_path.exists():
            payload = read_json(wallet_path)
            row = ((payload.get("wallets") if isinstance(payload, dict) else {}) or {}).get(wallet)
            if isinstance(row, dict):
                row_occurrences.append(
                    {
                        "shard": shard_dir.name,
                        "path": str(wallet_path),
                        "timestamp": wallet_row_timestamp(row),
                        "volumeUsd": to_num((row.get("all") or {}).get("volumeUsd"), to_num(row.get("volumeUsd"), 0.0)),
                        "trades": int(to_num((row.get("all") or {}).get("trades"), to_num(row.get("trades"), 0))),
                        "storage": row.get("storage") if isinstance(row.get("storage"), dict) else {},
                    }
                )
        state_path = shard_dir / "indexer_state.json"
        if state_path.exists():
            payload = read_json(state_path)
            row = ((payload.get("walletStates") if isinstance(payload, dict) else {}) or {}).get(wallet)
            if isinstance(row, dict):
                state_occurrences.append(
                    {
                        "shard": shard_dir.name,
                        "path": str(state_path),
                        "timestamp": state_timestamp(row),
                        "state": summarize_state_row(row),
                    }
                )
        history_path = shard_dir / "wallet_history" / f"{wallet}.json"
        if history_path.exists():
            history_paths.append(str(history_path))
    row_occurrences.sort(key=lambda item: item["timestamp"], reverse=True)
    state_occurrences.sort(key=lambda item: item["timestamp"], reverse=True)
    return row_occurrences, state_occurrences, history_paths


def load_local_history(history_paths):
    if not history_paths:
        return None
    payload = read_json(Path(history_paths[0]))
    trades = payload.get("trades") if isinstance(payload, dict) else []
    funding = payload.get("funding") if isinstance(payload, dict) else []
    return {
        "path": history_paths[0],
        "tradeMetrics": compute_trade_metrics(trades or []),
        "fundingMetrics": compute_funding_metrics(funding or []),
        "tradeCountStored": len(trades or []),
        "fundingCountStored": len(funding or []),
        "audit": payload.get("audit") if isinstance(payload, dict) else {},
    }


def build_wallet_trace(wallet_entry):
    wallet = wallet_entry["wallet"]
    print(f"trace_start {wallet}", file=sys.stderr, flush=True)
    row_occurrences, state_occurrences, history_paths = collect_wallet_occurrences(wallet)
    remote_trades = fetch_cursor_stream("trades/history", wallet, TRADES_LIMIT)
    remote_funding = fetch_cursor_stream("funding/history", wallet, FUNDING_LIMIT)
    remote_trade_metrics = compute_trade_metrics(remote_trades["rows"])
    remote_funding_metrics = compute_funding_metrics(remote_funding["rows"])
    local_history = load_local_history(history_paths)
    freshest_row = row_occurrences[0] if row_occurrences else None
    freshest_state = state_occurrences[0] if state_occurrences else None
    row_bucket = ((wallet_entry.get("row") or {}).get("all") if isinstance(wallet_entry.get("row"), dict) else {}) or {}
    row_metrics = {
        "tradeCount": int(to_num(row_bucket.get("trades"), 0)),
        "volumeUsd": round(to_num(row_bucket.get("volumeUsd"), 0.0), 6),
        "feesPaidUsd": round(to_num(row_bucket.get("feesUsd"), 0.0), 6),
        "fundingPayoutUsd": round(to_num(row_bucket.get("fundingPayoutUsd"), 0.0), 6),
        "pnlUsd": round(to_num(row_bucket.get("pnlUsd"), 0.0), 6),
        "wins": int(to_num(row_bucket.get("wins"), 0)),
        "losses": int(to_num(row_bucket.get("losses"), 0)),
        "firstTrade": row_bucket.get("firstTrade"),
        "lastTrade": row_bucket.get("lastTrade"),
    }
    defects = []
    if len(row_occurrences) > 1:
        defects.append(f"wallet_row_present_in_{len(row_occurrences)}_shards")
    if len(state_occurrences) > 1:
        defects.append(f"wallet_state_present_in_{len(state_occurrences)}_shards")
    if freshest_state:
        state_storage = freshest_state["state"].get("storage") or {}
        shard_id = str(state_storage.get("indexerShardId") or "")
        if shard_id.startswith("indexer_shard_"):
            owner_index = int(shard_id.split("_")[-1])
            if owner_index >= ACTIVE_SHARD_COUNT:
                defects.append("state_points_to_stale_20_shard_owner")
    if freshest_row and freshest_state:
        row_storage = freshest_row.get("storage") or {}
        state_storage = freshest_state["state"].get("storage") or {}
        if row_storage.get("indexerShardId") != state_storage.get("indexerShardId"):
            defects.append("row_storage_owner_differs_from_state_owner")
    if round(remote_trade_metrics["volumeUsd"], 2) != round(row_metrics["volumeUsd"], 2):
        defects.append("row_volume_mismatch_vs_remote")
    if remote_trade_metrics["tradeCount"] != row_metrics["tradeCount"]:
        defects.append("row_trade_count_mismatch_vs_remote")
    if local_history:
        if round(local_history["tradeMetrics"]["volumeUsd"], 2) != round(remote_trade_metrics["volumeUsd"], 2):
            defects.append("local_history_trade_volume_mismatch_vs_remote")
        if local_history["tradeCountStored"] != remote_trade_metrics["tradeCount"]:
            defects.append("local_history_trade_count_mismatch_vs_remote")
        if round(local_history["fundingMetrics"]["fundingPayoutUsd"], 6) != round(remote_funding_metrics["fundingPayoutUsd"], 6):
            defects.append("local_history_funding_mismatch_vs_remote")
    result = {
        "wallet": wallet,
        "selectedFrom": {
            "rowShard": wallet_entry["rowShard"],
            "volumeUsd": round(wallet_entry["volumeUsd"], 6),
            "timestamp": wallet_entry["timestamp"],
        },
        "rowOccurrences": row_occurrences,
        "stateOccurrences": state_occurrences,
        "historyPaths": history_paths,
        "remote": {
            "trades": {
                **remote_trade_metrics,
                "pageCount": remote_trades["pageCount"],
            },
            "funding": {
                **remote_funding_metrics,
                "pageCount": remote_funding["pageCount"],
            },
        },
        "rowMetrics": row_metrics,
        "localHistory": local_history,
        "defects": defects,
    }
    print(
        f"trace_done {wallet} defects={','.join(defects) if defects else 'none'} "
        f"remote_trades={remote_trade_metrics['tradeCount']} remote_pages={remote_trades['pageCount']}",
        file=sys.stderr,
        flush=True,
    )
    return result


def main():
    top10 = load_wallet_top10()
    traces = [build_wallet_trace(entry) for entry in top10]
    report = {
        "activeShardCount": ACTIVE_SHARD_COUNT,
        "generatedAt": int(time.time() * 1000),
        "wallets": traces,
    }
    OUT_PATH.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
