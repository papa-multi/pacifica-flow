#!/usr/bin/env python3
import json
import math
from pathlib import Path


INDEXER_DIR = Path("/root/pacifica-flow/data/indexer")
BASE_DIR = INDEXER_DIR / "shards"
ROOT_HISTORY_DIR = INDEXER_DIR / "wallet_history"
ROOT_DISCOVERY_PATH = INDEXER_DIR / "wallet_discovery.json"
ACTIVE_SHARDS = 8
DAY_MS = 24 * 60 * 60 * 1000


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


def to_num(value, fallback=0.0) -> float:
    try:
        num = float(value)
    except Exception:
        return fallback
    return num if math.isfinite(num) else fallback


def row_freshness(row: dict) -> int:
    all_bucket = row.get("all") if isinstance(row.get("all"), dict) else {}
    return max(
        int(row.get("updatedAt") or 0),
        int(row.get("createdAt") or 0),
        int(row.get("lastTrade") or 0),
        int(all_bucket.get("computedAt") or 0),
        int(all_bucket.get("lastTrade") or 0),
    )


def choose_better_row(current, incoming):
    if current is None:
        return incoming
    if incoming is None:
        return current
    left = row_freshness(current)
    right = row_freshness(incoming)
    if right > left:
        return incoming
    if left > right:
        return current
    left_all = current.get("all") if isinstance(current.get("all"), dict) else {}
    right_all = incoming.get("all") if isinstance(incoming.get("all"), dict) else {}
    left_volume = to_num(left_all.get("volumeUsd") or current.get("volumeUsd"), 0.0)
    right_volume = to_num(right_all.get("volumeUsd") or incoming.get("volumeUsd"), 0.0)
    return incoming if right_volume >= left_volume else current


def state_freshness(row: dict) -> int:
    return max(
        int(row.get("updatedAt") or 0),
        int(row.get("lastSuccessAt") or 0),
        int(row.get("lastAttemptAt") or 0),
        int(row.get("lastScannedAt") or 0),
        int(row.get("lastFailureAt") or 0),
        int(row.get("retryQueuedAt") or 0),
        int(row.get("backfillCompletedAt") or 0),
        int(row.get("liveTrackingSince") or 0),
        int(row.get("discoveredAt") or 0),
    )


def normalize_pagination_summary(summary=None):
    if not isinstance(summary, dict):
        return None
    return {
        "mode": "cursor",
        "exhausted": bool(summary.get("exhausted")),
        "frontierCursor": summary.get("frontierCursor"),
        "highestFetchedPage": int(summary.get("highestFetchedPage") or 0),
        "highestKnownPage": int(summary.get("highestKnownPage") or 0),
        "totalKnownPages": None
        if summary.get("totalKnownPages") is None
        else int(summary.get("totalKnownPages") or 0),
        "fetchedPages": int(summary.get("fetchedPages") or 0),
        "verifiedPages": int(summary.get("verifiedPages") or summary.get("fetchedPages") or 0),
        "retryPages": int(summary.get("retryPages") or 0),
        "pendingPages": int(summary.get("pendingPages") or 0),
        "lastSuccessfulPage": None
        if summary.get("lastSuccessfulPage") in (None, "")
        else int(summary.get("lastSuccessfulPage") or 0),
        "lastSuccessfulCursor": summary.get("lastSuccessfulCursor"),
        "lastSuccessfulAt": None
        if summary.get("lastSuccessfulAt") in (None, "")
        else int(summary.get("lastSuccessfulAt") or 0),
        "lastAttemptedPage": None
        if summary.get("lastAttemptedPage") in (None, "")
        else int(summary.get("lastAttemptedPage") or 0),
        "lastAttemptedCursor": summary.get("lastAttemptedCursor"),
        "lastAttemptedAt": None
        if summary.get("lastAttemptedAt") in (None, "")
        else int(summary.get("lastAttemptedAt") or 0),
        "missingPageRanges": [
            {
                "page": None
                if (entry or {}).get("page") in (None, "")
                else int((entry or {}).get("page") or 0),
                "cursor": (entry or {}).get("cursor"),
                "reason": str((entry or {}).get("reason") or "retry_pending"),
            }
            for entry in (summary.get("missingPageRanges") or [])
            if isinstance(entry, dict)
        ],
    }


def pagination_complete(summary=None) -> bool:
    safe = normalize_pagination_summary(summary)
    if not safe:
        return False
    return bool(safe["exhausted"]) and int(safe["retryPages"] or 0) <= 0 and not safe["frontierCursor"]


def pagination_score(summary=None) -> int:
    safe = normalize_pagination_summary(summary)
    if not safe:
        return -1
    return (
        (1_000_000 if pagination_complete(safe) else 0)
        + (100_000 if safe["exhausted"] else 0)
        + safe["verifiedPages"] * 1_000
        + safe["fetchedPages"] * 100
        + safe["highestFetchedPage"] * 10
        - safe["retryPages"] * 500
        - safe["pendingPages"] * 5
        + int((safe["lastSuccessfulAt"] or 0) / 1_000_000_000_000)
    )


def choose_better_summary(current=None, incoming=None):
    current_score = pagination_score(current)
    incoming_score = pagination_score(incoming)
    if incoming_score > current_score:
        return normalize_pagination_summary(incoming)
    if current_score >= 0:
        return normalize_pagination_summary(current)
    return normalize_pagination_summary(incoming)


def state_truth_score(row: dict) -> int:
    history_audit = row.get("historyAudit") if isinstance(row.get("historyAudit"), dict) else {}
    file_audit = history_audit.get("file") if isinstance(history_audit.get("file"), dict) else {}
    file_pagination = file_audit.get("pagination") if isinstance(file_audit.get("pagination"), dict) else {}
    trade_summary = choose_better_summary(row.get("tradePagination"), file_pagination.get("trades"))
    funding_summary = choose_better_summary(row.get("fundingPagination"), file_pagination.get("funding"))
    trade_complete = pagination_complete(trade_summary)
    funding_complete = pagination_complete(funding_summary)
    return (
        (1_000_000_000 if trade_complete else 0)
        + (1_000_000_000 if funding_complete else 0)
        + (500_000_000 if (row.get("remoteHistoryVerified") or (trade_complete and funding_complete)) else 0)
        + (100_000_000 if file_audit else 0)
        + int(row.get("tradeRowsLoaded") or file_audit.get("tradesStored") or 0) * 1000
        + int(row.get("fundingRowsLoaded") or file_audit.get("fundingStored") or 0) * 100
        - (10_000_000 if row.get("retryPending") else 0)
        + pagination_score(trade_summary)
        + pagination_score(funding_summary)
    )


def choose_better_state(current, incoming):
    if current is None:
        return incoming
    if incoming is None:
        return current
    left_truth = state_truth_score(current)
    right_truth = state_truth_score(incoming)
    if right_truth > left_truth:
        return incoming
    if left_truth > right_truth:
        return current
    return incoming if state_freshness(incoming) >= state_freshness(current) else current


def history_score(payload):
    if not isinstance(payload, dict):
        return (-1, -1, -1, -1)
    trades = payload.get("trades") if isinstance(payload.get("trades"), list) else []
    funding = payload.get("funding") if isinstance(payload.get("funding"), list) else []
    audit = payload.get("audit") if isinstance(payload.get("audit"), dict) else {}
    pagination = audit.get("pagination") if isinstance(audit.get("pagination"), dict) else {}
    trades_pg = normalize_pagination_summary(pagination.get("trades"))
    funding_pg = normalize_pagination_summary(pagination.get("funding"))
    exhausted = int(bool(trades_pg and trades_pg.get("exhausted"))) + int(
        bool(funding_pg and funding_pg.get("exhausted"))
    )
    return (
        len(trades) + len(funding),
        exhausted,
        int(audit.get("persistedAt") or payload.get("updatedAt") or 0),
        int(payload.get("updatedAt") or 0),
    )


def maybe_update_best_history(best_history_refs: dict, wallet: str, history_path: Path) -> None:
    payload = read_json(history_path)
    if payload is None:
        return
    candidate_score = history_score(payload)
    current = best_history_refs.get(wallet)
    if current is None or candidate_score >= current["score"]:
        best_history_refs[wallet] = {
            "path": history_path,
            "score": candidate_score,
        }


def normalize_trade_like(row):
    timestamp = int(row.get("timestamp") or row.get("created_at") or row.get("createdAt") or 0)
    return {
        "symbol": str(row.get("symbol") or "").upper(),
        "amount": row.get("amount", "0"),
        "price": row.get("price", "0"),
        "fee": row.get("fee", "0"),
        "liquidityPoolFee": row.get("liquidity_pool_fee")
        if row.get("liquidity_pool_fee") is not None
        else row.get("liquidityPoolFee")
        if row.get("liquidityPoolFee") is not None
        else row.get("lp_fee")
        if row.get("lp_fee") is not None
        else row.get("lpFee")
        if row.get("lpFee") is not None
        else row.get("supply_side_fee")
        if row.get("supply_side_fee") is not None
        else row.get("supplySideFee")
        if row.get("supplySideFee") is not None
        else "0",
        "pnl": row.get("pnl", "0"),
        "timestamp": timestamp if timestamp > 0 else 0,
    }


def normalize_funding_like(row):
    created_at = int(row.get("createdAt") or row.get("created_at") or 0)
    return {
        "symbol": str(row.get("symbol") or "").upper(),
        "payout": row.get("payout", "0"),
        "createdAt": created_at if created_at > 0 else 0,
    }


def aggregate_bucket(trades, funding, now, since_ts=None):
    filtered_trades = [
        row for row in trades if int(row.get("timestamp") or 0) > 0 and (since_ts is None or int(row.get("timestamp") or 0) >= since_ts)
    ]
    filtered_funding = [
        row for row in funding if int(row.get("createdAt") or 0) > 0 and (since_ts is None or int(row.get("createdAt") or 0) >= since_ts)
    ]
    volume_usd = 0.0
    fees_paid_usd = 0.0
    lp_fees_usd = 0.0
    fee_rebates_usd = 0.0
    net_fees_usd = 0.0
    pnl_usd = 0.0
    wins = 0
    losses = 0
    first_trade = None
    last_trade = None
    symbol_volumes = {}
    for row in filtered_trades:
        ts = int(row.get("timestamp") or 0)
        amount = abs(to_num(row.get("amount"), 0.0))
        price = to_num(row.get("price"), 0.0)
        fee_signed = to_num(row.get("fee"), 0.0)
        lp_fee = max(0.0, to_num(row.get("liquidityPoolFee"), 0.0))
        pnl = to_num(row.get("pnl"), 0.0)
        notional = amount * price
        volume_usd += notional
        if fee_signed > 0:
            fees_paid_usd += fee_signed
        elif fee_signed < 0:
            fee_rebates_usd += abs(fee_signed)
        lp_fees_usd += lp_fee
        net_fees_usd += fee_signed
        pnl_usd += pnl
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
        first_trade = ts if first_trade is None else min(first_trade, ts)
        last_trade = ts if last_trade is None else max(last_trade, ts)
        symbol = str(row.get("symbol") or "").upper()
        if symbol:
            symbol_volumes[symbol] = symbol_volumes.get(symbol, 0.0) + notional
    funding_payout_usd = sum(to_num(row.get("payout"), 0.0) for row in filtered_funding)
    trade_count = len(filtered_trades)
    win_rate_pct = (wins / (wins + losses) * 100.0) if (wins + losses) > 0 else 0.0
    return {
        "computedAt": now,
        "trades": trade_count,
        "volumeUsd": volume_usd,
        "feesUsd": fees_paid_usd,
        "feesPaidUsd": fees_paid_usd,
        "liquidityPoolFeesUsd": lp_fees_usd,
        "feeRebatesUsd": fee_rebates_usd,
        "netFeesUsd": net_fees_usd,
        "fundingPayoutUsd": funding_payout_usd,
        "revenueUsd": fees_paid_usd,
        "pnlUsd": pnl_usd,
        "wins": wins,
        "losses": losses,
        "winRatePct": win_rate_pct,
        "firstTrade": first_trade,
        "lastTrade": last_trade,
        "symbolVolumes": symbol_volumes,
    }


def build_wallet_record_from_history(wallet: str, history: dict, source_row: dict | None):
    trades = [normalize_trade_like(row) for row in (history.get("trades") or [])]
    funding = [normalize_funding_like(row) for row in (history.get("funding") or [])]
    now = int(
        (history.get("audit") or {}).get("persistedAt")
        or history.get("updatedAt")
        or (source_row or {}).get("updatedAt")
        or 0
    ) or int(Path("/").stat().st_mtime * 1000)
    all_bucket = aggregate_bucket(trades, funding, now, None)
    d30 = aggregate_bucket(trades, funding, now, now - 30 * DAY_MS)
    d7 = aggregate_bucket(trades, funding, now, now - 7 * DAY_MS)
    d24 = aggregate_bucket(trades, funding, now, now - DAY_MS)
    previous = dict(source_row or {})
    previous_storage = dict(previous.get("storage") or {})
    previous_created_at = int(previous.get("createdAt") or previous.get("updatedAt") or now)
    return {
        **previous,
        "wallet": wallet,
        "updatedAt": now,
        "createdAt": previous_created_at,
        "trades": all_bucket["trades"],
        "volumeUsd": all_bucket["volumeUsd"],
        "feesUsd": all_bucket["feesUsd"],
        "feesPaidUsd": all_bucket["feesPaidUsd"],
        "liquidityPoolFeesUsd": all_bucket["liquidityPoolFeesUsd"],
        "feeRebatesUsd": all_bucket["feeRebatesUsd"],
        "netFeesUsd": all_bucket["netFeesUsd"],
        "fundingPayoutUsd": all_bucket["fundingPayoutUsd"],
        "revenueUsd": all_bucket["revenueUsd"],
        "pnlUsd": all_bucket["pnlUsd"],
        "wins": all_bucket["wins"],
        "losses": all_bucket["losses"],
        "winRatePct": all_bucket["winRatePct"],
        "firstTrade": all_bucket["firstTrade"],
        "lastTrade": all_bucket["lastTrade"],
        "all": all_bucket,
        "d30": d30,
        "d7": d7,
        "d24": d24,
        "storage": previous_storage,
    }


def build_history_audit_for_state(state_row: dict, history: dict | None):
    file_audit = history.get("audit") if isinstance(history, dict) and isinstance(history.get("audit"), dict) else None
    file_pagination = file_audit.get("pagination") if isinstance(file_audit, dict) and isinstance(file_audit.get("pagination"), dict) else {}
    trade_pagination = choose_better_summary(
        state_row.get("tradePagination"),
        file_pagination.get("trades"),
    )
    funding_pagination = choose_better_summary(
        state_row.get("fundingPagination"),
        file_pagination.get("funding"),
    )
    trade_done = bool(state_row.get("tradeDone")) or pagination_complete(trade_pagination)
    funding_done = bool(state_row.get("fundingDone")) or pagination_complete(funding_pagination)
    remote_verified = bool(state_row.get("remoteHistoryVerified")) or (trade_done and funding_done)
    pending_reasons = []
    if not remote_verified:
        pending_reasons.append("remote_history_verification_pending")
    if not trade_done:
        pending_reasons.append("trade_history_incomplete")
    if not funding_done:
        pending_reasons.append("funding_history_incomplete")
    if not trade_done and state_row.get("tradeCursor"):
        pending_reasons.append("trade_cursor_pending")
    if not funding_done and state_row.get("fundingCursor"):
        pending_reasons.append("funding_cursor_pending")
    if not trade_done and state_row.get("tradeHasMore"):
        pending_reasons.append("trade_pages_pending")
    if not funding_done and state_row.get("fundingHasMore"):
        pending_reasons.append("funding_pages_pending")
    if trade_pagination and int(trade_pagination.get("retryPages") or 0) > 0:
        pending_reasons.append("trade_page_retries_pending")
    if funding_pagination and int(funding_pagination.get("retryPages") or 0) > 0:
        pending_reasons.append("funding_page_retries_pending")
    if state_row.get("retryPending"):
        pending_reasons.append(f"retry_pending:{state_row.get('retryReason') or 'unknown'}")
    completeness_state = "complete" if (trade_done and funding_done and remote_verified) else "partial_history"
    if int(state_row.get("tradeRowsLoaded") or 0) <= 0 and int(state_row.get("fundingRowsLoaded") or 0) <= 0:
        completeness_state = "activation_only" if int(state_row.get("liveTrackingSince") or 0) > 0 else "pending"
    return {
        "version": 1,
        "completenessState": completeness_state,
        "backfillComplete": trade_done and funding_done and remote_verified,
        "remoteHistoryVerified": remote_verified,
        "retryPending": bool(state_row.get("retryPending")),
        "retryReason": state_row.get("retryReason"),
        "pendingReasons": sorted(set(pending_reasons)),
        "warnings": [],
        "lastAttemptMode": state_row.get("lastAttemptMode"),
        "lastSuccessMode": state_row.get("lastSuccessMode"),
        "lastFailureMode": state_row.get("lastFailureMode"),
        "file": file_audit,
        "pagination": {
            "trades": trade_pagination,
            "funding": funding_pagination,
        },
    }


def rebuild_state_from_truth(wallet: str, source_state: dict | None, history: dict | None):
    state = dict(source_state or {})
    file_audit = history.get("audit") if isinstance(history, dict) and isinstance(history.get("audit"), dict) else {}
    file_pagination = file_audit.get("pagination") if isinstance(file_audit.get("pagination"), dict) else {}
    trade_pagination = choose_better_summary(state.get("tradePagination"), file_pagination.get("trades"))
    funding_pagination = choose_better_summary(state.get("fundingPagination"), file_pagination.get("funding"))
    trade_complete = pagination_complete(trade_pagination)
    funding_complete = pagination_complete(funding_pagination)
    history_complete = trade_complete and funding_complete
    rebuilt = {
        **state,
        "wallet": wallet,
        "tradePagination": trade_pagination,
        "fundingPagination": funding_pagination,
        "tradeRowsLoaded": max(int(state.get("tradeRowsLoaded") or 0), int(file_audit.get("tradesStored") or 0)),
        "fundingRowsLoaded": max(int(state.get("fundingRowsLoaded") or 0), int(file_audit.get("fundingStored") or 0)),
        "tradePagesLoaded": max(int(state.get("tradePagesLoaded") or 0), int((trade_pagination or {}).get("highestFetchedPage") or 0)),
        "fundingPagesLoaded": max(int(state.get("fundingPagesLoaded") or 0), int((funding_pagination or {}).get("highestFetchedPage") or 0)),
        "tradeDone": trade_complete or bool(state.get("tradeDone")),
        "fundingDone": funding_complete or bool(state.get("fundingDone")),
        "tradeHasMore": False if trade_complete else bool(state.get("tradeHasMore")),
        "fundingHasMore": False if funding_complete else bool(state.get("fundingHasMore")),
        "tradeCursor": None if trade_complete else state.get("tradeCursor"),
        "fundingCursor": None if funding_complete else state.get("fundingCursor"),
        "remoteHistoryVerified": history_complete or bool(state.get("remoteHistoryVerified")),
        "remoteHistoryVerifiedAt": int(
            state.get("remoteHistoryVerifiedAt")
            or state.get("backfillCompletedAt")
            or file_audit.get("persistedAt")
            or state.get("updatedAt")
            or 0
        )
        or None,
        "backfillCompletedAt": int(
            state.get("backfillCompletedAt")
            or state.get("remoteHistoryVerifiedAt")
            or file_audit.get("persistedAt")
            or state.get("updatedAt")
            or 0
        )
        if history_complete
        else (int(state.get("backfillCompletedAt") or 0) or None),
        "retryPending": False if history_complete else bool(state.get("retryPending")),
        "retryReason": None if history_complete else state.get("retryReason"),
        "retryQueuedAt": None if history_complete else (int(state.get("retryQueuedAt") or 0) or None),
        "forceHeadRefetch": False if history_complete else bool(state.get("forceHeadRefetch")),
        "forceHeadRefetchReason": None if history_complete else state.get("forceHeadRefetchReason"),
        "forceHeadRefetchQueuedAt": None
        if history_complete
        else (int(state.get("forceHeadRefetchQueuedAt") or 0) or None),
        "historyPhase": "complete"
        if history_complete
        else state.get("historyPhase")
        or ("deep" if int(state.get("tradeRowsLoaded") or 0) > 0 or int(file_audit.get("tradesStored") or 0) > 0 else "activate"),
    }
    lifecycle = state.get("lifecycleStage")
    if history_complete:
        rebuilt["lifecycleStage"] = "live_tracking" if int(state.get("liveTrackingSince") or 0) > 0 else "fully_indexed"
    else:
        rebuilt["lifecycleStage"] = lifecycle or ("backfilling" if rebuilt["tradeRowsLoaded"] or rebuilt["fundingRowsLoaded"] else "discovered")
    rebuilt["historyAudit"] = build_history_audit_for_state(rebuilt, history)
    return rebuilt


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
    return next_record


def load_discovered_wallets() -> set[str]:
    payload = read_json(ROOT_DISCOVERY_PATH)
    if not isinstance(payload, dict):
        return set()
    wallets = payload.get("wallets")
    if isinstance(wallets, dict):
        return {str(wallet or "").strip() for wallet in wallets.keys() if str(wallet or "").strip()}
    if isinstance(wallets, list):
        result = set()
        for item in wallets:
            if isinstance(item, dict):
                wallet = str(item.get("wallet") or item.get("address") or "").strip()
            else:
                wallet = str(item or "").strip()
            if wallet:
                result.add(wallet)
        return result
    return set()


def main() -> int:
    shards = [BASE_DIR / f"shard_{index}" for index in range(ACTIVE_SHARDS) if (BASE_DIR / f"shard_{index}").is_dir()]
    merged_rows = {}
    merged_states = {}
    best_history_refs = {}
    discovered_wallets = load_discovered_wallets()
    last_discovery_at = 0
    last_scan_at = 0
    discovery_cycles = 0
    scan_cycles = 0

    for shard_dir in shards:
        wallet_payload = read_json(shard_dir / "wallets.json")
        if isinstance(wallet_payload, dict) and isinstance(wallet_payload.get("wallets"), dict):
            for wallet, row in wallet_payload["wallets"].items():
                normalized = str(wallet or "").strip()
                if normalized and isinstance(row, dict):
                    merged_rows[normalized] = choose_better_row(merged_rows.get(normalized), dict(row))

        state_payload = read_json(shard_dir / "indexer_state.json")
        if isinstance(state_payload, dict):
            last_discovery_at = max(last_discovery_at, int(state_payload.get("lastDiscoveryAt") or 0))
            last_scan_at = max(last_scan_at, int(state_payload.get("lastScanAt") or 0))
            discovery_cycles += int(state_payload.get("discoveryCycles") or 0)
            scan_cycles += int(state_payload.get("scanCycles") or 0)
            for wallet, row in (state_payload.get("walletStates") or {}).items():
                normalized = str((row or {}).get("wallet") or wallet or "").strip()
                if normalized and isinstance(row, dict):
                    next_row = dict(row)
                    next_row["wallet"] = normalized
                    merged_states[normalized] = choose_better_state(merged_states.get(normalized), next_row)
            for wallet in (state_payload.get("knownWallets") or []):
                normalized = str(wallet or "").strip()
                if normalized:
                    discovered_wallets.add(normalized)

    if ROOT_HISTORY_DIR.exists():
        for history_path in ROOT_HISTORY_DIR.glob("*.json"):
            wallet = history_path.stem.strip()
            if not wallet:
                continue
            maybe_update_best_history(best_history_refs, wallet, history_path)
            discovered_wallets.add(wallet)

    summary = {"shards": []}
    for shard_index in range(ACTIVE_SHARDS):
        shard_dir = BASE_DIR / f"shard_{shard_index}"
        history_dir = shard_dir / "wallet_history"
        history_dir.mkdir(parents=True, exist_ok=True)
        for file_path in history_dir.glob("*.json"):
            file_path.unlink(missing_ok=True)

        wallet_rows = {}
        wallet_states = {}
        known = []
        live = []
        priority = []
        continuation = []

        for wallet in sorted(set(merged_rows) | set(merged_states) | set(best_history_refs) | discovered_wallets):
            if shard_index_for_wallet(wallet, ACTIVE_SHARDS) != shard_index:
                continue
            history_ref = best_history_refs.get(wallet)
            history = read_json(history_ref["path"]) if history_ref else None
            source_row = merged_rows.get(wallet)
            source_state = merged_states.get(wallet)
            row = build_wallet_record_from_history(wallet, history, source_row) if history else dict(source_row or {"wallet": wallet})
            row = normalize_wallet_row(row, wallet, shard_dir, shard_index)
            wallet_rows[wallet] = row

            rebuilt_state = rebuild_state_from_truth(wallet, source_state, history)
            rebuilt_state["storage"] = {
                **dict(rebuilt_state.get("storage") or {}),
                "indexerShardId": f"indexer_shard_{shard_index}",
                "indexerStatePath": str(shard_dir / "indexer_state.json"),
                "walletHistoryDir": str(history_dir),
            }
            wallet_states[wallet] = rebuilt_state

            known.append(wallet)
            if str(rebuilt_state.get("lifecycleStage") or "").lower() == "live_tracking":
                live.append(wallet)
            if rebuilt_state.get("tradeDone") and rebuilt_state.get("fundingDone") and rebuilt_state.get("remoteHistoryVerified"):
                continue
            if int(rebuilt_state.get("tradeRowsLoaded") or 0) > 0 or int(rebuilt_state.get("fundingRowsLoaded") or 0) > 0:
                continuation.append(wallet)
            else:
                priority.append(wallet)

            if history_ref and history_ref["path"].exists():
                target_history_path = history_dir / f"{wallet}.json"
                try:
                    target_history_path.symlink_to(history_ref["path"])
                except FileExistsError:
                    pass

        write_json_atomic(
            shard_dir / "wallets.json",
            {"version": 2, "wallets": wallet_rows, "updatedAt": max(last_scan_at, last_discovery_at)},
        )
        write_json_atomic(
            shard_dir / "indexer_state.json",
            {
                "version": 5,
                "knownWallets": known,
                "liveWallets": live,
                "priorityQueue": priority,
                "continuationQueue": continuation,
                "scanCursor": 0,
                "liveScanCursor": 0,
                "lastDiscoveryAt": last_discovery_at or None,
                "lastScanAt": last_scan_at or None,
                "discoveryCycles": discovery_cycles,
                "scanCycles": scan_cycles,
                "walletStates": wallet_states,
            },
        )
        summary["shards"].append(
            {
                "shard": shard_index,
                "walletRows": len(wallet_rows),
                "walletStates": len(wallet_states),
                "historyFiles": len(list(history_dir.glob('*.json'))),
                "priorityQueue": len(priority),
                "continuationQueue": len(continuation),
                "liveWallets": len(live),
            }
        )

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
