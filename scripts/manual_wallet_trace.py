#!/usr/bin/env python3
import argparse
import json
import math
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


BASE_DIR = Path("/root/pacifica-flow/data/indexer/shards")
DEBUG_DIR = Path("/root/pacifica-flow/data/debug")
API_BASE = "https://api.pacifica.fi/api/v1"
ACTIVE_SHARDS = 8
DEFAULT_WALLET = "F8Q2Kx7G7erDmj8jojeXyTtVF1aHSsAV4z6nRnFvR7id"


def log_section(title: str) -> None:
    print(f"\n=== {title} ===")


def log_line(label: str, value) -> None:
    print(f"{label}: {value}")


def stable_shard_hash(value: str) -> int:
    text = str(value or "")
    hash_value = 5381
    for char in text:
        hash_value = ((hash_value << 5) + hash_value + ord(char)) & 0xFFFFFFFF
    return hash_value


def shard_index_for_wallet(wallet: str, shard_count: int = ACTIVE_SHARDS) -> int:
    return stable_shard_hash(f"wallet:{str(wallet or '').strip()}") % shard_count


def read_json(path: Path):
    if not path.exists():
        return None
    with open(path) as handle:
        return json.load(handle)


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as handle:
        json.dump(payload, handle, indent=2)


def to_num(value, fallback=0.0) -> float:
    try:
        num = float(value)
    except Exception:
        return fallback
    return num if math.isfinite(num) else fallback


def aggregate_trades(rows) -> dict:
    volume_usd = 0.0
    fees_usd = 0.0
    pnl_usd = 0.0
    wins = 0
    losses = 0
    first_trade = None
    last_trade = None
    for row in rows or []:
        created_at = int(row.get("created_at") or row.get("timestamp") or 0)
        amount = abs(to_num(row.get("amount"), 0.0))
        price = to_num(row.get("price"), 0.0)
        fee = to_num(row.get("fee"), 0.0)
        pnl = to_num(row.get("pnl"), 0.0)
        volume_usd += amount * price
        if fee > 0:
            fees_usd += fee
        pnl_usd += pnl
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
        if created_at > 0:
            first_trade = created_at if first_trade is None else min(first_trade, created_at)
            last_trade = created_at if last_trade is None else max(last_trade, created_at)
    return {
        "trades": len(rows or []),
        "volumeUsd": round(volume_usd, 2),
        "feesUsd": round(fees_usd, 2),
        "pnlUsd": round(pnl_usd, 2),
        "wins": wins,
        "losses": losses,
        "firstTrade": first_trade,
        "lastTrade": last_trade,
    }


def aggregate_funding(rows) -> dict:
    payout = 0.0
    first_funding = None
    last_funding = None
    for row in rows or []:
        created_at = int(row.get("created_at") or row.get("createdAt") or 0)
        payout += to_num(row.get("payout"), 0.0)
        if created_at > 0:
            first_funding = created_at if first_funding is None else min(first_funding, created_at)
            last_funding = created_at if last_funding is None else max(last_funding, created_at)
    return {
        "fundingCount": len(rows or []),
        "fundingPayoutUsd": round(payout, 6),
        "firstFunding": first_funding,
        "lastFunding": last_funding,
    }


def fetch_cursor_stream(wallet: str, endpoint: str, limit: int, sleep_s: float, retries: int):
    rows = []
    cursor = None
    pages = []
    page_number = 0
    while True:
        page_number += 1
        params = {"account": wallet, "limit": str(limit)}
        if cursor:
            params["cursor"] = cursor
        url = f"{API_BASE}/{endpoint}?{urllib.parse.urlencode(params)}"
        response = None
        last_error = None
        for attempt in range(retries):
            try:
                with urllib.request.urlopen(url, timeout=20) as handle:
                    response = json.load(handle)
                break
            except urllib.error.HTTPError as exc:
                last_error = f"http_{exc.code}"
            except Exception as exc:
                last_error = str(exc)
            wait_s = min(15, 1 + attempt * 2)
            print(
                f"[fetch] endpoint={endpoint} page={page_number} cursor={cursor or '__head__'} attempt={attempt + 1}/{retries} error={last_error} wait_s={wait_s}"
            )
            time.sleep(wait_s)
        if response is None:
            raise RuntimeError(f"{endpoint} failed wallet={wallet} cursor={cursor or '__head__'} error={last_error}")

        page_rows = response.get("data") or []
        next_cursor = response.get("next_cursor")
        has_more = bool(response.get("has_more"))
        rows.extend(page_rows)
        pages.append(
            {
                "page": page_number,
                "cursor": cursor,
                "rows": len(page_rows),
                "hasMore": has_more,
                "nextCursor": next_cursor,
            }
        )
        print(
            f"[fetch] endpoint={endpoint} page={page_number} cursor={cursor or '__head__'} rows={len(page_rows)} has_more={has_more} next_cursor={next_cursor}"
        )
        if not has_more:
            return {"rows": rows, "pages": pages}
        if not next_cursor:
            raise RuntimeError(f"{endpoint} missing next_cursor wallet={wallet} page={page_number}")
        cursor = next_cursor
        time.sleep(sleep_s)


def find_local_wallet_occurrences(wallet: str) -> dict:
    result = {
        "rowOccurrences": [],
        "stateOccurrences": [],
        "historyOccurrences": [],
    }
    for shard in range(ACTIVE_SHARDS):
        shard_dir = BASE_DIR / f"shard_{shard}"
        wallet_store = read_json(shard_dir / "wallets.json") or {}
        row = (wallet_store.get("wallets") or {}).get(wallet)
        if isinstance(row, dict):
            result["rowOccurrences"].append({"shard": shard, "row": row})
        state_payload = read_json(shard_dir / "indexer_state.json") or {}
        state = (state_payload.get("walletStates") or {}).get(wallet)
        if isinstance(state, dict):
            result["stateOccurrences"].append({"shard": shard, "state": state})
            result.setdefault("queueMembership", []).append(
                {
                    "shard": shard,
                    "known": wallet in (state_payload.get("knownWallets") or []),
                    "live": wallet in (state_payload.get("liveWallets") or []),
                    "priority": wallet in (state_payload.get("priorityQueue") or []),
                    "continuation": wallet in (state_payload.get("continuationQueue") or []),
                }
            )
        history_path = shard_dir / "wallet_history" / f"{wallet}.json"
        history = read_json(history_path)
        if isinstance(history, dict):
            result["historyOccurrences"].append(
                {
                    "shard": shard,
                    "path": str(history_path),
                    "history": history,
                }
            )
    return result


def summarize_local_row(row: dict | None) -> dict | None:
    if not isinstance(row, dict):
        return None
    all_bucket = row.get("all") if isinstance(row.get("all"), dict) else {}
    return {
        "wallet": row.get("wallet"),
        "updatedAt": row.get("updatedAt"),
        "storage": row.get("storage"),
        "flat": {k: row.get(k) for k in ["trades", "volumeUsd", "feesUsd", "pnlUsd", "wins", "losses", "firstTrade", "lastTrade"]},
        "all": {k: all_bucket.get(k) for k in ["trades", "volumeUsd", "feesUsd", "pnlUsd", "wins", "losses", "firstTrade", "lastTrade"]},
    }


def summarize_local_state(state: dict | None) -> dict | None:
    if not isinstance(state, dict):
        return None
    return {
        "wallet": state.get("wallet"),
        "lifecycleStage": state.get("lifecycleStage"),
        "historyPhase": state.get("historyPhase"),
        "tradeDone": state.get("tradeDone"),
        "fundingDone": state.get("fundingDone"),
        "tradeHasMore": state.get("tradeHasMore"),
        "fundingHasMore": state.get("fundingHasMore"),
        "tradeCursor": state.get("tradeCursor"),
        "fundingCursor": state.get("fundingCursor"),
        "remoteHistoryVerified": state.get("remoteHistoryVerified"),
        "retryPending": state.get("retryPending"),
        "retryReason": state.get("retryReason"),
        "tradeRowsLoaded": state.get("tradeRowsLoaded"),
        "fundingRowsLoaded": state.get("fundingRowsLoaded"),
        "tradePagesLoaded": state.get("tradePagesLoaded"),
        "fundingPagesLoaded": state.get("fundingPagesLoaded"),
        "tradePagination": state.get("tradePagination"),
        "fundingPagination": state.get("fundingPagination"),
        "historyAudit": state.get("historyAudit"),
        "storage": state.get("storage"),
    }


def summarize_history(history: dict | None) -> dict | None:
    if not isinstance(history, dict):
        return None
    return {
        "version": history.get("version"),
        "tradesStored": len(history.get("trades") or []),
        "fundingStored": len(history.get("funding") or []),
        "audit": history.get("audit"),
        "pagination": history.get("pagination"),
    }


def build_manual_read_model(wallet: str, trade_metrics: dict, funding_metrics: dict, owner_shard: int):
    return {
        "wallet": wallet,
        "shard": owner_shard,
        "trades": trade_metrics["trades"],
        "volumeUsd": trade_metrics["volumeUsd"],
        "feesUsd": trade_metrics["feesUsd"],
        "pnlUsd": trade_metrics["pnlUsd"],
        "wins": trade_metrics["wins"],
        "losses": trade_metrics["losses"],
        "firstTrade": trade_metrics["firstTrade"],
        "lastTrade": trade_metrics["lastTrade"],
        "fundingCount": funding_metrics["fundingCount"],
        "fundingPayoutUsd": funding_metrics["fundingPayoutUsd"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wallet", default=DEFAULT_WALLET)
    parser.add_argument("--limit", type=int, default=120)
    parser.add_argument("--sleep-ms", type=int, default=500)
    parser.add_argument("--retries", type=int, default=8)
    parser.add_argument("--write-artifact", action="store_true")
    args = parser.parse_args()

    wallet = str(args.wallet or "").strip()
    if not wallet:
        raise SystemExit("wallet is required")

    owner_shard = shard_index_for_wallet(wallet, ACTIVE_SHARDS)
    local = find_local_wallet_occurrences(wallet)

    log_section("Selected Wallet")
    log_line("wallet", wallet)
    log_line("ownerShard", owner_shard)
    log_line("activeShards", ACTIVE_SHARDS)

    log_section("Ownership Decision")
    log_line("hashKey", f"wallet:{wallet}")
    log_line("shardIndex", owner_shard)
    log_line("rowOccurrences", len(local["rowOccurrences"]))
    log_line("stateOccurrences", len(local["stateOccurrences"]))
    log_line("historyOccurrences", len(local["historyOccurrences"]))

    log_section("Queue And State Presence")
    for entry in local.get("queueMembership", []):
        print(json.dumps(entry, indent=2))
    owner_state = next((entry["state"] for entry in local["stateOccurrences"] if entry["shard"] == owner_shard), None)
    print(json.dumps(summarize_local_state(owner_state), indent=2))

    log_section("Current Local Row")
    owner_row = next((entry["row"] for entry in local["rowOccurrences"] if entry["shard"] == owner_shard), None)
    print(json.dumps(summarize_local_row(owner_row), indent=2))

    log_section("Current Local History")
    owner_history = next((entry["history"] for entry in local["historyOccurrences"] if entry["shard"] == owner_shard), None)
    print(json.dumps(summarize_history(owner_history), indent=2)[:5000])

    log_section("Raw API Fetches")
    sleep_s = max(0.1, args.sleep_ms / 1000.0)
    remote_trades = fetch_cursor_stream(wallet, "trades/history", args.limit, sleep_s, args.retries)
    remote_funding = fetch_cursor_stream(wallet, "funding/history", args.limit, sleep_s, args.retries)

    log_section("Metric Calculation")
    trade_metrics = aggregate_trades(remote_trades["rows"])
    funding_metrics = aggregate_funding(remote_funding["rows"])
    print(json.dumps({"tradeMetrics": trade_metrics, "fundingMetrics": funding_metrics}, indent=2))

    log_section("Persistence/Write Step")
    write_plan = {
        "ownerShard": owner_shard,
        "walletStorePath": str(BASE_DIR / f"shard_{owner_shard}" / "wallets.json"),
        "indexerStatePath": str(BASE_DIR / f"shard_{owner_shard}" / "indexer_state.json"),
        "walletHistoryPath": str(BASE_DIR / f"shard_{owner_shard}" / "wallet_history" / f"{wallet}.json"),
        "rowPayload": build_manual_read_model(wallet, trade_metrics, funding_metrics, owner_shard),
    }
    print(json.dumps(write_plan, indent=2))

    log_section("Materialized / Read Model Output")
    materialized = build_manual_read_model(wallet, trade_metrics, funding_metrics, owner_shard)
    print(json.dumps(materialized, indent=2))

    log_section("Final Diagnosis")
    local_row_metrics = summarize_local_row(owner_row)
    local_history_summary = summarize_history(owner_history)
    diagnosis = {
        "wallet": wallet,
        "ownerShard": owner_shard,
        "rowShardCount": len(local["rowOccurrences"]),
        "stateShardCount": len(local["stateOccurrences"]),
        "historyShardCount": len(local["historyOccurrences"]),
        "rowVolumeUsd": ((local_row_metrics or {}).get("all") or {}).get("volumeUsd"),
        "remoteVolumeUsd": trade_metrics["volumeUsd"],
        "localHistoryTradesStored": (local_history_summary or {}).get("tradesStored"),
        "remoteTrades": trade_metrics["trades"],
        "stateRetryReason": (owner_state or {}).get("retryReason"),
        "stateRemoteHistoryVerified": (owner_state or {}).get("remoteHistoryVerified"),
        "likelyBreaks": [
            "duplicate_shard_occurrences" if len(local["rowOccurrences"]) > 1 or len(local["stateOccurrences"]) > 1 else None,
            "state_staler_than_history" if owner_state and owner_history else None,
            "flat_vs_all_metric_mismatch"
            if owner_row and any((owner_row.get(k) is None) for k in ["trades", "volumeUsd", "feesUsd"])
            else None,
        ],
    }
    diagnosis["likelyBreaks"] = [item for item in diagnosis["likelyBreaks"] if item]
    print(json.dumps(diagnosis, indent=2))

    if args.write_artifact:
        artifact = {
            "selectedWallet": wallet,
            "ownerShard": owner_shard,
            "local": {
                "rowOccurrences": [
                    {"shard": entry["shard"], "row": summarize_local_row(entry["row"])}
                    for entry in local["rowOccurrences"]
                ],
                "stateOccurrences": [
                    {"shard": entry["shard"], "state": summarize_local_state(entry["state"])}
                    for entry in local["stateOccurrences"]
                ],
                "historyOccurrences": [
                    {"shard": entry["shard"], "path": entry["path"], "history": summarize_history(entry["history"])}
                    for entry in local["historyOccurrences"]
                ],
                "queueMembership": local.get("queueMembership", []),
            },
            "remote": {
                "tradesPages": remote_trades["pages"],
                "fundingPages": remote_funding["pages"],
                "tradeMetrics": trade_metrics,
                "fundingMetrics": funding_metrics,
            },
            "writePlan": write_plan,
            "materialized": materialized,
            "diagnosis": diagnosis,
        }
        artifact_path = DEBUG_DIR / f"manual_wallet_trace_{wallet}.json"
        write_json(artifact_path, artifact)
        log_section("Artifact Written")
        log_line("path", artifact_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
