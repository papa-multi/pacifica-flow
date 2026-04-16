#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import math
import multiprocessing as mp
import os
import subprocess
from pathlib import Path

from common import (
    DEFAULT_SHARD_COUNT,
    SHARDS_DIR,
    UI_DIR,
    V2_DIR,
    build_exchange_overview,
    classify_trade_execution,
    exact_first_trade_queue_path,
    load_phase_state,
    load_wallet_discovery_first_seen,
    isolated_wallet_record_path,
    isolated_wallet_history_log_path,
    isolated_wallet_history_path,
    load_isolated_wallets,
    latest_opened_at_from_history,
    list_wallets_from_sources,
    now_ms,
    normalize_funding_row,
    normalize_trade_row,
    phase_state_path,
    read_json,
    funding_row_key,
    trade_row_key,
    shard_index_for_wallet,
    to_num,
    wallet_history_path,
    wallet_record_path,
    write_json_atomic,
)

VOLUME_PROGRESS_HISTORY_PATH = UI_DIR / "wallet_volume_progress_history.json"
FIRST_TRADE_INDEX_PATH = V2_DIR / "wallet_first_trade_index.json"
EXACT_FIRST_TRADE_QUEUE_PATH = exact_first_trade_queue_path()
PHASE_STATE_PATH = phase_state_path()
EXCHANGE_WALLET_METRIC_HISTORY_PATH = UI_DIR / "exchange_wallet_metric_history.json"
GLOBAL_KPI_PATH = V2_DIR.parent / "pipeline" / "global_kpi.json"
ACTIVE_WALLETS_START_DATE = "2025-09-09"
WALLET_METRICS_HISTORY_DAILY_DIR = V2_DIR.parent / "indexer" / "wallet_metrics_history" / "daily"
NEXTGEN_STATE_DIR = V2_DIR.parent / "nextgen" / "state"
NEXTGEN_WALLET_STATE_PATH = NEXTGEN_STATE_DIR / "wallet-current-state.json"
NEXTGEN_LIVE_STATUS_PATH = NEXTGEN_STATE_DIR / "wallet-live-status.json"
V3_BUILD_SOURCE_PATH = UI_DIR / ".build" / "wallet_dataset_compact_source.json"
NEXTGEN_STATE_CACHE = {
    "walletsMtimeMs": 0,
    "liveMtimeMs": 0,
    "wallets": {},
    "live": {},
}
WALLET_STORAGE_V3_BUILD_SCRIPT = V2_DIR.parent.parent / "scripts" / "build_wallet_storage_v3.js"


def load_wallet_address_dataset():
    dynamic = list_wallets_from_sources()
    if isinstance(dynamic, dict) and isinstance(dynamic.get("wallets"), list) and dynamic.get("wallets"):
        write_json_atomic(V2_DIR / "wallet_addresses.json", dynamic)
        return dynamic
    return read_json(V2_DIR / "wallet_addresses.json", {"wallets": []}) or {"wallets": []}


def build_wallet_storage_v3(source_path: Path) -> None:
    subprocess.run(
        ["node", str(WALLET_STORAGE_V3_BUILD_SCRIPT), f"--source={source_path}"],
        check=True,
        cwd=str(V2_DIR.parent.parent),
    )


def load_market_daily_volume_by_date():
    payload = read_json(GLOBAL_KPI_PATH, {}) or {}
    history = payload.get("history") if isinstance(payload.get("history"), dict) else {}
    daily_by_date = history.get("dailyByDate") if isinstance(history.get("dailyByDate"), dict) else {}
    normalized = {}
    for day_raw, row_raw in daily_by_date.items():
        day = str(day_raw or "").strip()[:10]
        if not day:
            continue
        row = row_raw if isinstance(row_raw, dict) else {}
        volume = float(row.get("dailyVolume") or 0.0)
        if volume > 0:
            normalized[day] = volume
    return normalized


def load_wallet_metrics_daily_snapshot_aggregate(day: str | None):
    safe_day = str(day or "").strip()[:10]
    if not safe_day:
        return None
    path = WALLET_METRICS_HISTORY_DAILY_DIR / f"{safe_day}.json"
    payload = read_json(path, None)
    if not isinstance(payload, dict):
        return None
    wallets = payload.get("wallets") if isinstance(payload.get("wallets"), dict) else {}
    if not wallets:
        return None
    total_trades = 0
    total_volume = 0.0
    total_fees = 0.0
    active_wallets = 0
    for row_raw in wallets.values():
        row = row_raw if isinstance(row_raw, dict) else {}
        d24 = row.get("d24") if isinstance(row.get("d24"), dict) else {}
        trades = max(0, int(d24.get("trades") or 0))
        volume = max(0.0, to_num(d24.get("volumeUsd"), 0.0))
        fees = max(
            0.0,
            to_num(
                d24.get("feesPaidUsd")
                if d24.get("feesPaidUsd") is not None
                else d24.get("feesUsd")
                if d24.get("feesUsd") is not None
                else row.get("feesPaidUsd")
                if row.get("feesPaidUsd") is not None
                else row.get("feesUsd"),
                0.0,
            ),
        )
        total_trades += trades
        total_volume += volume
        total_fees += fees
        if trades > 0 or volume > 0 or fees > 0:
            active_wallets += 1
    return {
        "trades": int(total_trades),
        "volumeUsd": round(total_volume, 2),
        "feesUsd": round(total_fees, 2),
        "activeWallets": int(active_wallets),
        "walletCount": int(payload.get("walletCount") or len(wallets)),
        "generatedAt": int(payload.get("generatedAt") or 0),
    }


def file_mtime_ms(path: Path) -> int:
    try:
        return int(path.stat().st_mtime * 1000)
    except Exception:
        return 0


def load_nextgen_state_overlay():
    wallets_mtime = file_mtime_ms(NEXTGEN_WALLET_STATE_PATH)
    live_mtime = file_mtime_ms(NEXTGEN_LIVE_STATUS_PATH)
    if (
        NEXTGEN_STATE_CACHE["wallets"]
        and NEXTGEN_STATE_CACHE["live"]
        and NEXTGEN_STATE_CACHE["walletsMtimeMs"] == wallets_mtime
        and NEXTGEN_STATE_CACHE["liveMtimeMs"] == live_mtime
    ):
        return NEXTGEN_STATE_CACHE
    wallets_payload = read_json(NEXTGEN_WALLET_STATE_PATH, {}) or {}
    live_payload = read_json(NEXTGEN_LIVE_STATUS_PATH, {}) or {}
    NEXTGEN_STATE_CACHE["walletsMtimeMs"] = wallets_mtime
    NEXTGEN_STATE_CACHE["liveMtimeMs"] = live_mtime
    NEXTGEN_STATE_CACHE["wallets"] = wallets_payload if isinstance(wallets_payload, dict) else {}
    NEXTGEN_STATE_CACHE["live"] = live_payload if isinstance(live_payload, dict) else {}
    return NEXTGEN_STATE_CACHE


def load_all_rows(shard_count: int):
    rows = {}
    for shard_index in range(shard_count):
        shard_dir = SHARDS_DIR / f"shard_{shard_index}" / "wallet_records"
        if not shard_dir.exists():
            continue
        for path in sorted(shard_dir.glob("*.json")):
            payload = read_json(path, None)
            if not isinstance(payload, dict):
                continue
            wallet = str(payload.get("wallet") or "").strip()
            if not wallet:
                continue
            rows[wallet] = payload
    for wallet in sorted(load_isolated_wallets()):
        payload = read_json(isolated_wallet_record_path(wallet), None)
        if not isinstance(payload, dict):
            continue
        normalized = str(payload.get("wallet") or wallet).strip()
        if not normalized:
            continue
        existing = rows.get(normalized)
        if not isinstance(existing, dict):
            rows[normalized] = payload
            continue

        def score(record):
            backfill = 1 if record.get("backfillComplete") else 0
            trade_done = 1 if record.get("tradeDone") else 0
            funding_done = 1 if record.get("fundingDone") else 0
            trade_rows = int(record.get("tradeRowsLoaded") or record.get("trades") or 0)
            funding_rows = int(record.get("fundingRowsLoaded") or 0)
            trade_pages = int(((record.get("tradePagination") or {}).get("fetchedPages")) or 0)
            volume = float(record.get("volumeUsdRaw") or 0.0)
            updated = int(record.get("updatedAt") or 0)
            return (backfill, trade_done, funding_done, trade_rows, funding_rows, trade_pages, volume, updated)

        rows[normalized] = payload if score(payload) >= score(existing) else existing
    return rows


def load_record_last_opened_at(wallet: str, shard_index: int, row: dict | None) -> int | None:
    safe_row = row if isinstance(row, dict) else {}
    direct = int(safe_row.get("lastOpenedAt") or 0) or None
    if direct:
        return direct
    isolated_history = read_json(isolated_wallet_history_path(wallet), None)
    isolated_last_opened_at = latest_opened_at_from_history(isolated_history)
    if isolated_last_opened_at:
        return isolated_last_opened_at
    shard_history = read_json(wallet_history_path(shard_index, wallet), None)
    shard_last_opened_at = latest_opened_at_from_history(shard_history)
    if shard_last_opened_at:
        return shard_last_opened_at
    return None


def load_shard_progress(shard_count: int):
    out = []
    for shard_index in range(shard_count):
        payload = read_json(
            SHARDS_DIR / f"shard_{shard_index}" / "wallet_state" / "__progress__.json",
            None,
        )
        if isinstance(payload, dict):
            proxy_pool = payload.get("proxyPool") or {}
            payload["proxyPool"] = {
                "total": int(proxy_pool.get("total") or 0),
                "active": int(proxy_pool.get("active") or 0),
                "cooling": int(proxy_pool.get("cooling") or 0),
            }
            out.append(payload)
    return out


def count_history_files(shard_count: int) -> int:
    total = 0
    for shard_index in range(shard_count):
        history_dir = SHARDS_DIR / f"shard_{shard_index}" / "wallet_history"
        if not history_dir.exists():
            continue
        total += sum(1 for _ in history_dir.glob("*.json"))
    for wallet in load_isolated_wallets():
        if isolated_wallet_record_path(wallet).exists():
            total += 1
    return total


def build_placeholder_row(wallet: str, generated_at: int):
    return {
        "wallet": wallet,
        "walletRecordId": None,
        "rank": 0,
        "trades": 0,
        "volumeUsd": 0,
        "volumeUsdRaw": 0,
        "volumeUsdVerified": None,
        "rankingVolumeUsd": None,
        "feesUsd": 0,
        "feesPaidUsd": 0,
        "totalWins": 0,
        "totalLosses": 0,
        "pnlUsd": 0,
        "winRate": 0,
        "firstTrade": None,
        "lastTrade": None,
        "lastOpenedAt": None,
        "lastActivity": None,
        "updatedAt": generated_at,
        "openPositions": 0,
        "exposureUsd": 0,
        "unrealizedPnlUsd": 0,
        "totalPnlUsd": 0,
        "tradeRowsLoaded": 0,
        "fundingRowsLoaded": 0,
        "tradeDone": False,
        "fundingDone": False,
        "tradeHasMore": True,
        "fundingHasMore": True,
        "tradePagination": {
            "fetchedPages": 0,
            "failedPages": 0,
            "pendingPages": 1,
            "frontierCursor": None,
            "failedCursor": None,
            "exhausted": False,
            "rowsFetched": 0,
            "lastSuccessAt": None,
            "lastAttemptAt": None,
            "retryCount": 0,
            "lastError": None,
            "nextEligibleAt": 0,
            "lastPageRowCount": 0,
        },
        "fundingPagination": {
            "fetchedPages": 0,
            "failedPages": 0,
            "pendingPages": 1,
            "frontierCursor": None,
            "failedCursor": None,
            "exhausted": False,
            "rowsFetched": 0,
            "lastSuccessAt": None,
            "lastAttemptAt": None,
            "retryCount": 0,
            "lastError": None,
            "nextEligibleAt": 0,
            "lastPageRowCount": 0,
        },
        "retryPending": False,
        "retryReason": None,
        "historyPhase": "backfill",
        "validationStatus": "pending",
        "backfillComplete": False,
        "all": {
            "computedAt": generated_at,
            "trades": 0,
            "volumeUsd": 0,
            "feesUsd": 0,
            "feesPaidUsd": 0,
            "makerTrades": 0,
            "takerTrades": 0,
            "makerVolumeUsd": 0,
            "takerVolumeUsd": 0,
            "feeBearingVolumeUsd": 0,
            "pnlUsd": 0,
            "wins": 0,
            "losses": 0,
            "winRatePct": 0,
            "firstTrade": None,
            "lastTrade": None,
            "symbolVolumes": {},
        },
        "d30": {"computedAt": generated_at, "trades": 0, "volumeUsd": 0, "feesUsd": 0, "feesPaidUsd": 0, "makerTrades": 0, "takerTrades": 0, "makerVolumeUsd": 0, "takerVolumeUsd": 0, "feeBearingVolumeUsd": 0, "pnlUsd": 0, "wins": 0, "losses": 0, "winRatePct": 0, "firstTrade": None, "lastTrade": None, "symbolVolumes": {}},
        "d7": {"computedAt": generated_at, "trades": 0, "volumeUsd": 0, "feesUsd": 0, "feesPaidUsd": 0, "makerTrades": 0, "takerTrades": 0, "makerVolumeUsd": 0, "takerVolumeUsd": 0, "feeBearingVolumeUsd": 0, "pnlUsd": 0, "wins": 0, "losses": 0, "winRatePct": 0, "firstTrade": None, "lastTrade": None, "symbolVolumes": {}},
        "d24": {"computedAt": generated_at, "trades": 0, "volumeUsd": 0, "feesUsd": 0, "feesPaidUsd": 0, "makerTrades": 0, "takerTrades": 0, "makerVolumeUsd": 0, "takerVolumeUsd": 0, "feeBearingVolumeUsd": 0, "pnlUsd": 0, "wins": 0, "losses": 0, "winRatePct": 0, "firstTrade": None, "lastTrade": None, "symbolVolumes": {}},
        "symbols": [],
        "symbolBreakdown": [],
        "completeness": {
            "historyVerified": False,
            "metricsVerified": False,
            "leaderboardEligible": False,
            "trackingState": "tracked_lite",
            "tradeHistoryState": "activation_only",
            "metricsState": "metrics_pending",
            "volumeExactness": "blocked",
            "volumeExactnessLabel": "Blocked",
            "fullBlockedReasons": ["history_backfill_pending"],
            "warnings": [],
            "historyPersistedAt": None,
        },
    }


def sort_rows(rows):
    def sort_key(row):
        trusted = row.get("rankingVolumeUsd")
        trusted_rank = 0 if trusted is not None else 1
        trusted_value = -(float(trusted) if trusted is not None else 0.0)
        raw_value = -float(row.get("volumeUsdRaw") or 0.0)
        return (trusted_rank, trusted_value, raw_value, row.get("wallet") or "")

    ordered = sorted(rows, key=sort_key)
    for index, row in enumerate(ordered, start=1):
        row["rank"] = index
    return ordered


def build_counts(rows):
    total_wallets = len(rows)
    tracked_full = sum(1 for row in rows if (row.get("completeness") or {}).get("historyVerified"))
    tracked_lite = total_wallets - tracked_full
    metrics_complete = sum(1 for row in rows if (row.get("completeness") or {}).get("metricsVerified"))
    leaderboard_eligible = sum(1 for row in rows if (row.get("completeness") or {}).get("leaderboardEligible"))
    activation_only = sum(1 for row in rows if int((row.get("tradePagination") or {}).get("rowsFetched") or 0) <= 0)
    partial_history = sum(
        1
        for row in rows
        if int((row.get("tradePagination") or {}).get("rowsFetched") or 0) > 0
        and not (row.get("completeness") or {}).get("historyVerified")
    )
    metrics_partial = total_wallets - metrics_complete
    trade_pages_fetched = sum(int((row.get("tradePagination") or {}).get("fetchedPages") or 0) for row in rows)
    funding_pages_fetched = sum(int((row.get("fundingPagination") or {}).get("fetchedPages") or 0) for row in rows)
    trade_pages_pending = sum(int((row.get("tradePagination") or {}).get("pendingPages") or 0) for row in rows)
    funding_pages_pending = sum(int((row.get("fundingPagination") or {}).get("pendingPages") or 0) for row in rows)
    trade_pages_failed = sum(int((row.get("tradePagination") or {}).get("failedPages") or 0) for row in rows)
    funding_pages_failed = sum(int((row.get("fundingPagination") or {}).get("failedPages") or 0) for row in rows)
    return {
        "totalWallets": total_wallets,
        "discoveredWallets": total_wallets,
        "trackedWallets": total_wallets,
        "trackedLiteWallets": tracked_lite,
        "trackedFullWallets": tracked_full,
        "tradeHistoryCompleteWallets": tracked_full,
        "metricsCompleteWallets": metrics_complete,
        "metricsPartialWallets": metrics_partial,
        "leaderboardEligibleWallets": leaderboard_eligible,
        "validatedWallets": tracked_full,
        "backfilledWallets": tracked_full,
        "activationOnlyWallets": activation_only,
        "tradeHistoryPartialWallets": partial_history,
        "recentHistoryOnlyWallets": 0,
        "tradePagesFetched": trade_pages_fetched,
        "fundingPagesFetched": funding_pages_fetched,
        "tradePagesPending": trade_pages_pending,
        "fundingPagesPending": funding_pages_pending,
        "tradePagesFailed": trade_pages_failed,
        "fundingPagesFailed": funding_pages_failed,
    }


def build_top_correction(rows):
    top = rows[:100]
    return {
        "cohortTotal": len(top),
        "historyVerified": sum(1 for row in top if (row.get("completeness") or {}).get("historyVerified")),
        "metricsVerified": sum(1 for row in top if (row.get("completeness") or {}).get("metricsVerified")),
        "leaderboardEligible": sum(1 for row in top if (row.get("completeness") or {}).get("leaderboardEligible")),
        "tradePagesPending": sum(int((row.get("tradePagination") or {}).get("pendingPages") or 0) for row in top),
        "fundingPagesPending": sum(int((row.get("fundingPagination") or {}).get("pendingPages") or 0) for row in top),
    }


def build_validation(rows, overview):
    raw_total = round(sum(float(row.get("volumeUsdRaw") or 0.0) for row in rows), 2)
    verified_total = round(
        sum(float(row.get("volumeUsdVerified") or 0.0) for row in rows if row.get("volumeUsdVerified") is not None),
        2,
    )
    market_total = round(float(overview.get("totalHistoricalVolumeUsd") or 0.0), 2)
    gap = round(market_total - raw_total, 2)
    gap_reason = "wallet_history_backfill_pending" if gap > 0 else "matched_or_exceeded"
    return {
        "generatedAt": now_ms(),
        "marketTotalVolumeUsd": market_total,
        "walletTotalRawVolumeUsd": raw_total,
        "walletTotalVerifiedVolumeUsd": verified_total,
        "gapUsd": gap,
        "gapReason": gap_reason,
        "volumeMatch": math.isclose(market_total, raw_total, rel_tol=0, abs_tol=0.01),
    }


def build_first_trade_index(address_dataset, by_wallet):
    items = []
    exact = 0
    known = 0
    for item in address_dataset.get("wallets") or []:
        wallet = item["wallet"]
        row = by_wallet.get(wallet) or {}
        first_trade = row.get("firstTrade")
        trade_done = bool(row.get("tradeDone"))
        if first_trade and trade_done:
            known += 1
            exact += 1
            source = "trade_history_exhausted"
            sort_ts = first_trade
        else:
            source = "pending_exact_first_trade"
            sort_ts = None
        items.append(
            {
                "wallet": wallet,
                "firstTrade": first_trade,
                "firstTradeExact": bool(first_trade and trade_done),
                "firstTradeSource": source,
                "sortTs": sort_ts,
            }
        )
    items.sort(key=lambda entry: (entry["sortTs"] is None, entry["sortTs"] or 253402300799000, entry["wallet"]))
    return {
        "version": 1,
        "generatedAt": now_ms(),
        "totalWallets": len(items),
        "knownFirstTradeWallets": known,
        "exactFirstTradeWallets": exact,
        "rows": items,
    }


def build_exact_first_trade_queue(first_trade_index):
    rows = [
        {
            "wallet": row["wallet"],
            "firstTrade": row["firstTrade"],
            "sortTs": row["sortTs"],
        }
        for row in (first_trade_index.get("rows") or [])
        if row.get("firstTradeExact") and int(row.get("sortTs") or 0) > 0
    ]
    rows.sort(key=lambda row: (int(row["sortTs"]), row["wallet"]))
    return {
        "version": 1,
        "generatedAt": now_ms(),
        "totalWallets": int(first_trade_index.get("totalWallets") or 0),
        "exactWallets": int(first_trade_index.get("exactFirstTradeWallets") or 0),
        "remainingWallets": max(
            0,
            int(first_trade_index.get("totalWallets") or 0) - int(first_trade_index.get("exactFirstTradeWallets") or 0),
        ),
        "rows": rows,
        "wallets": [row["wallet"] for row in rows],
    }


def build_phase_state(first_trade_index):
    total_wallets = int(first_trade_index.get("totalWallets") or 0)
    exact_wallets = int(first_trade_index.get("exactFirstTradeWallets") or 0)
    remaining_wallets = max(0, total_wallets - exact_wallets)
    previous = load_phase_state()
    mode = "completion_first_backfill"
    switched_at = int(previous.get("switchedAt") or 0)
    if mode != str(previous.get("mode") or ""):
        switched_at = now_ms()
    return {
        "version": 1,
        "generatedAt": now_ms(),
        "mode": mode,
        "switchedAt": switched_at or now_ms(),
        "exactFirstTradeWallets": exact_wallets,
        "totalWallets": total_wallets,
        "remainingWallets": remaining_wallets,
        "queuePath": str(EXACT_FIRST_TRADE_QUEUE_PATH),
        "indexPath": str(FIRST_TRADE_INDEX_PATH),
    }


def load_progress_history():
    payload = read_json(VOLUME_PROGRESS_HISTORY_PATH, {"version": 1, "points": []}) or {"version": 1, "points": []}
    points = payload.get("points") or []
    if not isinstance(points, list):
        points = []
    payload["points"] = [point for point in points if isinstance(point, dict)]
    return payload


def update_progress_history(counts, validation, overview, generated_at):
    history = load_progress_history()
    points = history["points"]
    point = {
        "generatedAt": generated_at,
        "walletTotalRawVolumeUsd": round(float(validation.get("walletTotalRawVolumeUsd") or 0.0), 2),
        "walletTotalVerifiedVolumeUsd": round(float(validation.get("walletTotalVerifiedVolumeUsd") or 0.0), 2),
        "marketTotalVolumeUsd": round(float(validation.get("marketTotalVolumeUsd") or 0.0), 2),
        "trackedFullWallets": int(counts.get("trackedFullWallets") or 0),
        "tradePagesFetched": int(counts.get("tradePagesFetched") or 0),
        "fundingPagesFetched": int(counts.get("fundingPagesFetched") or 0),
    }
    if points and int(points[-1].get("generatedAt") or 0) == generated_at:
        points[-1] = point
    else:
        points.append(point)
    if len(points) > 720:
        points = points[-720:]
    history["points"] = points
    history["generatedAt"] = generated_at
    return history


def compute_volume_progress(history, counts, validation, generated_at):
    points = list(history.get("points") or [])
    current = points[-1] if points else {
        "generatedAt": generated_at,
        "walletTotalRawVolumeUsd": round(float(validation.get("walletTotalRawVolumeUsd") or 0.0), 2),
        "walletTotalVerifiedVolumeUsd": round(float(validation.get("walletTotalVerifiedVolumeUsd") or 0.0), 2),
        "trackedFullWallets": int(counts.get("trackedFullWallets") or 0),
    }
    previous = points[-2] if len(points) >= 2 else None
    lookback_cutoff = generated_at - (30 * 60 * 1000)
    baseline = None
    for point in reversed(points[:-1]):
        if int(point.get("generatedAt") or 0) <= lookback_cutoff:
            baseline = point
            break
    if baseline is None and previous is not None:
        baseline = previous
    if baseline is None:
        baseline = current

    interval_ms = max(0, int(current.get("generatedAt") or 0) - int((previous or current).get("generatedAt") or 0))
    window_ms = max(0, int(current.get("generatedAt") or 0) - int(baseline.get("generatedAt") or 0))
    delta_raw = round(
        float(current.get("walletTotalRawVolumeUsd") or 0.0) - float((previous or current).get("walletTotalRawVolumeUsd") or 0.0),
        2,
    )
    delta_verified = round(
        float(current.get("walletTotalVerifiedVolumeUsd") or 0.0)
        - float((previous or current).get("walletTotalVerifiedVolumeUsd") or 0.0),
        2,
    )
    delta_wallets = int(current.get("trackedFullWallets") or 0) - int((previous or current).get("trackedFullWallets") or 0)
    window_delta_raw = round(
        float(current.get("walletTotalRawVolumeUsd") or 0.0) - float(baseline.get("walletTotalRawVolumeUsd") or 0.0),
        2,
    )
    window_delta_verified = round(
        float(current.get("walletTotalVerifiedVolumeUsd") or 0.0) - float(baseline.get("walletTotalVerifiedVolumeUsd") or 0.0),
        2,
    )
    window_delta_wallets = int(current.get("trackedFullWallets") or 0) - int(baseline.get("trackedFullWallets") or 0)

    raw_correction_window_usd = round(min(0.0, window_delta_raw), 2)
    verified_correction_window_usd = round(min(0.0, window_delta_verified), 2)
    raw_growth_window_usd = round(max(0.0, window_delta_raw), 2)
    verified_growth_window_usd = round(max(0.0, window_delta_verified), 2)
    wallet_growth_window = max(0, window_delta_wallets)

    raw_per_hour = round((raw_growth_window_usd * 3600000.0 / window_ms), 2) if window_ms > 0 else 0.0
    verified_per_hour = round((verified_growth_window_usd * 3600000.0 / window_ms), 2) if window_ms > 0 else 0.0
    wallets_per_hour = round((wallet_growth_window * 3600000.0 / window_ms), 2) if window_ms > 0 else 0.0

    total_wallets = int(counts.get("totalWallets") or 0)
    completed_wallets = int(counts.get("trackedFullWallets") or 0)
    remaining_wallets = max(0, total_wallets - completed_wallets)
    gap_usd = max(0.0, float(validation.get("gapUsd") or 0.0))

    eta_wallet_completion_ms = int((remaining_wallets / wallets_per_hour) * 3600000) if wallets_per_hour > 0 else None
    eta_volume_gap_ms = int((gap_usd / raw_per_hour) * 3600000) if raw_per_hour > 0 and gap_usd > 0 else None

    market_total = float(validation.get("marketTotalVolumeUsd") or 0.0)
    raw_total = float(validation.get("walletTotalRawVolumeUsd") or 0.0)
    verified_total = float(validation.get("walletTotalVerifiedVolumeUsd") or 0.0)
    raw_coverage_pct = (raw_total / market_total * 100.0) if market_total > 0 else 0.0
    verified_coverage_pct = (verified_total / market_total * 100.0) if market_total > 0 else 0.0

    progress_state = "stalled"
    if delta_raw > 0 or delta_wallets > 0:
        progress_state = "advancing"
    elif interval_ms <= 0 and points:
        progress_state = "steady"

    return {
        "generatedAt": generated_at,
        "currentRawVolumeUsd": round(raw_total, 2),
        "currentVerifiedVolumeUsd": round(verified_total, 2),
        "marketTotalVolumeUsd": round(market_total, 2),
        "currentCompletedWallets": completed_wallets,
        "remainingWallets": remaining_wallets,
        "snapshotIntervalMs": interval_ms,
        "deltaRawVolumeUsd": delta_raw,
        "deltaVerifiedVolumeUsd": delta_verified,
        "deltaCompletedWallets": delta_wallets,
        "rollingWindowMs": window_ms,
        "windowDeltaRawVolumeUsd": window_delta_raw,
        "windowDeltaVerifiedVolumeUsd": window_delta_verified,
        "windowDeltaCompletedWallets": window_delta_wallets,
        "windowRawVolumeGrowthUsd": raw_growth_window_usd,
        "windowVerifiedVolumeGrowthUsd": verified_growth_window_usd,
        "windowCompletedWalletGrowth": wallet_growth_window,
        "windowRawVolumeCorrectionUsd": raw_correction_window_usd,
        "windowVerifiedVolumeCorrectionUsd": verified_correction_window_usd,
        "rawVolumePerHour": raw_per_hour,
        "verifiedVolumePerHour": verified_per_hour,
        "completedWalletsPerHour": wallets_per_hour,
        "walletCoveragePct": round(raw_coverage_pct, 4),
        "verifiedCoveragePct": round(verified_coverage_pct, 4),
        "etaWalletCompletionMs": eta_wallet_completion_ms,
        "etaVolumeGapMs": eta_volume_gap_ms,
        "progressState": progress_state,
        "pointsTracked": len(points),
    }


def aggregate_runtime(shard_progress, counts, generated_at):
    active_shard_count = len(shard_progress)
    total_wallets = int(counts.get("totalWallets") or 0)
    proxy_total = sum(int(((item.get("proxyPool") or {}).get("total") or 0)) for item in shard_progress)
    proxy_active = sum(int(((item.get("proxyPool") or {}).get("active") or 0)) for item in shard_progress)
    proxy_cooling = sum(int(((item.get("proxyPool") or {}).get("cooling") or 0)) for item in shard_progress)
    last_progress_at = max((int(item.get("generatedAt") or 0) for item in shard_progress), default=0)
    stall_shards = []
    for item in shard_progress:
        guard = item.get("stallGuard") or {}
        mode = str(guard.get("mode") or "normal")
        if mode == "normal":
            continue
        stall_shards.append(
            {
                "shardIndex": int(item.get("shardIndex") or 0),
                "mode": mode,
                "blockedOn": guard.get("blockedOn"),
                "stalledSinceAt": guard.get("stalledSinceAt"),
                "rescueEnteredAt": guard.get("rescueEnteredAt"),
                "fundingBacklogWallets": int(guard.get("fundingBacklogWallets") or 0),
                "tradeBacklogWallets": int(guard.get("tradeBacklogWallets") or 0),
            }
        )
    discovered = int(counts.get("discoveredWallets") or total_wallets)
    completion_ratio = (int(counts.get("trackedFullWallets") or 0) / total_wallets) if total_wallets > 0 else 0.0
    return {
        "progress": {
            "discovered": discovered,
            "validated": int(counts.get("validatedWallets") or 0),
            "backfilled": int(counts.get("backfilledWallets") or 0),
            "currentlyTracked": int(counts.get("trackedWallets") or total_wallets),
            "trackedLite": int(counts.get("trackedLiteWallets") or 0),
            "trackedFull": int(counts.get("trackedFullWallets") or 0),
            "pending": max(0, total_wallets - int(counts.get("trackedFullWallets") or 0)),
            "walletHistoryTimeline": {
                "startTimeMs": None,
                "currentTimeMs": generated_at,
                "endTimeMs": None,
                "remainingMs": None,
                "etaMs": None,
                "percentComplete": round(completion_ratio * 100.0, 2),
                "method": "wallet_completion_ratio",
                "exactness": "derived",
            },
            "discoveryWindow": {
                "startTimeMs": None,
                "currentTimeMs": generated_at,
                "endTimeMs": generated_at,
                "remainingMs": 0,
                "etaMs": 0,
                "percentComplete": 100.0 if discovered >= total_wallets and total_wallets > 0 else 0.0,
                "mode": "complete" if discovered >= total_wallets and total_wallets > 0 else "catch_up",
                "method": "address_corpus",
            },
        },
        "system": {
            "mode": "wallet_explorer_v2",
            "activeShardCount": active_shard_count,
            "proxyHealth": {
                "total": proxy_total,
                "active": proxy_active,
                "cooling": proxy_cooling,
            },
            "stallGuard": {
                "active": bool(stall_shards),
                "stalledShardCount": len(stall_shards),
                "shards": stall_shards,
            },
            "restPace": None,
            "wsHealth": {"activeWallets": 0, "promotionBacklog": 0},
            "lastProgressAt": last_progress_at,
        },
        "freshness": {
            "lastSuccessfulSyncAt": generated_at,
            "lastSuccessfulWalletDiscoveryAt": generated_at,
            "lastSuccessfulExchangeRollupAt": None,
            "lagBehindNowMs": max(0, now_ms() - generated_at),
        },
        "storage": {
            "wallets": {
                "walletRowsPersisted": int(counts.get("trackedFullWallets") or 0),
                "historyStore": {"persistedWallets": 0},
            },
            "materializedViews": {"walletMetricsHistory": {"dailyFiles": 0}},
        },
    }


def build_default_payload(rows, counts, overview, validation, generated_at, runtime):
    page_size = 20
    return {
        "generatedAt": generated_at,
        "timeframe": "all",
        "query": {"q": "", "page": 1, "pageSize": page_size, "sort": "rankingVolumeUsd", "dir": "desc"},
        "sorting": {
            "key": "rankingVolumeUsd",
            "dir": "desc",
            "label": "Trusted Volume Rank",
            "shortLabel": "Trusted Rank",
            "basisField": "rankingVolumeUsd",
            "description": "Leaderboard-eligible wallets rank first, then history-verified wallets, then partial wallets. Inside each trust tier, rows are ordered by raw volume.",
        },
        "total": len(rows),
        "page": 1,
        "pageSize": page_size,
        "pages": max(1, math.ceil(len(rows) / page_size)),
        "rows": rows[:page_size],
        "counts": counts,
        "progress": runtime["progress"],
        "system": runtime["system"],
        "freshness": {
            **runtime["freshness"],
            "lastSuccessfulExchangeRollupAt": overview.get("updatedAt"),
        },
        "storage": runtime["storage"],
        "exchangeOverview": overview,
        "validation": validation,
        "availableSymbols": sorted({symbol for row in rows for symbol in (row.get("symbols") or [])}),
        "warmup": {"persistedPreview": True, "liveEnrichmentPending": False},
        "performance": {"datasetBuiltAt": generated_at},
    }


def build_status_summary(counts, overview, generated_at, runtime, volume_progress, phase_state):
    total_wallets = counts["totalWallets"]
    tracked_full = counts["trackedFullWallets"]
    progress = runtime["progress"]
    progress["walletVolumeProgress"] = volume_progress
    return {
        "generatedAt": generated_at,
        "status": {
            "generatedAt": generated_at,
            "sourceCount": 1,
            "activeSourceCount": 1,
            "summary": {
                "systemMode": "wallet_explorer_v2",
                "system": runtime["system"],
                "walletExplorerProgress": {
                    **progress,
                    "discovered": counts["discoveredWallets"],
                    "validated": counts["validatedWallets"],
                    "backfilled": counts["backfilledWallets"],
                    "currentlyTracked": counts["trackedWallets"],
                    "trackedLite": counts["trackedLiteWallets"],
                    "trackedFull": tracked_full,
                    "metricsComplete": counts["metricsCompleteWallets"],
                    "leaderboardEligible": counts["leaderboardEligibleWallets"],
                    "tradePagesFetched": counts["tradePagesFetched"],
                    "fundingPagesFetched": counts["fundingPagesFetched"],
                    "tradePagesPending": counts["tradePagesPending"],
                    "fundingPagesPending": counts["fundingPagesPending"],
                    "tradePagesFailed": counts["tradePagesFailed"],
                    "fundingPagesFailed": counts["fundingPagesFailed"],
                    "completionRatio": tracked_full / total_wallets if total_wallets > 0 else 0,
                    "mode": "wallet_explorer_v2",
                    "firstTradePhase": phase_state,
                    "updatedAt": generated_at,
                },
                "exchangeOverview": overview,
            },
            "services": [
                {"id": "walletExplorerV2Workers", "name": "Wallet Explorer V2 Workers", "status": "ready"},
                {"id": "walletExplorerV2Materializer", "name": "Wallet Explorer V2 Materializer", "status": "ready"},
            ],
            "startup": {"phase": "ready", "readyAt": generated_at, "steps": {}},
        },
    }


def build_exchange_wallet_kpis(rows, counts, generated_at, metric_history=None):
    first_trade = min(
        (int(row.get("firstTrade") or 0) for row in rows if int(row.get("firstTrade") or 0) > 0),
        default=0,
    )
    last_trade = max((int(row.get("lastTrade") or 0) for row in rows), default=0)
    coverage = metric_history.get("coverage") if isinstance(metric_history, dict) else {}
    total_accounts = int(coverage.get("totalAccounts") or counts.get("totalWallets") or len(rows))
    total_trades = int(coverage.get("totalTrades") or sum(int(row.get("trades") or 0) for row in rows))
    total_fees = round(float(coverage.get("totalFeesUsd") or 0.0), 2) if coverage else round(sum(float(row.get("feesPaidUsd") or 0.0) for row in rows), 2)
    return {
        "generatedAt": generated_at,
        "kpis": {
            "totalAccounts": total_accounts,
            "activeAccounts": total_accounts,
            "totalTrades": int(total_trades),
            "totalFeesUsd": total_fees,
        },
        "coverage": {
            "startTimeMs": first_trade or None,
            "endTimeMs": last_trade or None,
        },
        "source": {
            "mode": "wallet_explorer_v2_materialized_kpis",
            "wallets": total_accounts,
        },
    }


def format_utc_date_from_ms(timestamp_ms):
    try:
        value = int(timestamp_ms or 0)
    except Exception:
        return None
    if value <= 0:
        return None
    return dt.datetime.fromtimestamp(value / 1000, tz=dt.timezone.utc).strftime("%Y-%m-%d")


def add_utc_days(date_str, delta_days):
    try:
        base = dt.date.fromisoformat(str(date_str).strip()[:10])
    except Exception:
        return None
    return (base + dt.timedelta(days=int(delta_days or 0))).isoformat()


def empty_exchange_metric_day():
    return {
        "accountsAdded": 0,
        "accountsTotal": 0,
        "activeWallets": 0,
        "trades": 0,
        "makerTrades": 0,
        "takerTrades": 0,
        "feesUsd": 0.0,
        "feesPaidUsd": 0.0,
        "liquidityPoolFeesUsd": 0.0,
        "volumeUsd": 0.0,
        "makerVolumeUsd": 0.0,
        "takerVolumeUsd": 0.0,
        "feeBearingVolumeUsd": 0.0,
    }


def resolve_active_wallet_metric_timestamp(row):
    safe_row = row if isinstance(row, dict) else {}

    def to_ts(value):
        try:
            value = int(float(value or 0))
        except Exception:
            return 0
        return value if value > 0 else 0

    all_bucket = safe_row.get("all") if isinstance(safe_row.get("all"), dict) else {}
    candidates = [
        to_ts(safe_row.get("lastTrade")),
        to_ts(all_bucket.get("lastTrade")),
        to_ts(safe_row.get("lastActivity")),
        to_ts(safe_row.get("lastActivityAt")),
        to_ts(safe_row.get("lastActiveAt")),
        to_ts(safe_row.get("lastOpenedAt")),
        to_ts(all_bucket.get("lastOpenedAt")),
        to_ts(safe_row.get("last_activity_at")),
        to_ts(safe_row.get("last_opened_at")),
    ]
    return max(candidates) if candidates else 0


def build_active_wallet_days_from_rows(rows_by_wallet, start_date=ACTIVE_WALLETS_START_DATE):
    active_wallet_days = {}
    for wallet, row in (rows_by_wallet or {}).items():
        wallet_key = str(wallet or "").strip()
        if not wallet_key:
            continue
        activity_ts = resolve_active_wallet_metric_timestamp(row)
        activity_day = format_utc_date_from_ms(activity_ts)
        if not activity_day or (start_date and activity_day < start_date):
            continue
        active_wallet_days.setdefault(activity_day, set()).add(wallet_key)
    return active_wallet_days


def record_score(record):
    safe = record if isinstance(record, dict) else {}
    backfill = 1 if safe.get("backfillComplete") else 0
    trade_done = 1 if safe.get("tradeDone") else 0
    funding_done = 1 if safe.get("fundingDone") else 0
    trade_rows = int(safe.get("tradeRowsLoaded") or safe.get("trades") or 0)
    funding_rows = int(safe.get("fundingRowsLoaded") or 0)
    trade_pages = int(((safe.get("tradePagination") or {}).get("fetchedPages")) or 0)
    volume = float(safe.get("volumeUsdRaw") or 0.0)
    updated = int(safe.get("updatedAt") or 0)
    return (backfill, trade_done, funding_done, trade_rows, funding_rows, trade_pages, volume, updated)


def load_effective_isolated_history(wallet: str):
    history = read_json(isolated_wallet_history_path(wallet), {}) or {}
    if not isinstance(history, dict):
        history = {}
    trades = []
    funding = []
    trade_seen = set()
    funding_seen = set()

    for row in history.get("trades") or []:
        normalized = normalize_trade_row(row)
        key = trade_row_key(normalized)
        if key in trade_seen:
            continue
        trade_seen.add(key)
        trades.append(normalized)

    for row in history.get("funding") or []:
        normalized = normalize_funding_row(row)
        key = funding_row_key(normalized)
        if key in funding_seen:
            continue
        funding_seen.add(key)
        funding.append(normalized)

    log_path = isolated_wallet_history_log_path(wallet)
    if log_path.exists():
        try:
            with open(log_path) as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except Exception:
                        continue
                    stream_name = str(payload.get("stream") or "").strip().lower()
                    row = payload.get("row")
                    if stream_name == "trades":
                        normalized = normalize_trade_row(row)
                        key = trade_row_key(normalized)
                        if key in trade_seen:
                            continue
                        trade_seen.add(key)
                        trades.append(normalized)
                    elif stream_name == "funding":
                        normalized = normalize_funding_row(row)
                        key = funding_row_key(normalized)
                        if key in funding_seen:
                            continue
                        funding_seen.add(key)
                        funding.append(normalized)
        except FileNotFoundError:
            pass

    return {
        "wallet": wallet,
        "trades": trades,
        "funding": funding,
    }


def add_trade_rows_to_exchange_daily(daily_by_date, trades, wallet=None, active_wallet_days=None):
    wallet_key = str(wallet or "").strip()
    for row in trades or []:
        normalized = normalize_trade_row(row)
        timestamp = int(normalized.get("timestamp") or 0)
        day = format_utc_date_from_ms(timestamp)
        if not day:
            continue
        bucket = daily_by_date.setdefault(day, empty_exchange_metric_day())
        side = str(normalized.get("side") or "").strip().lower()
        if (
            active_wallet_days is not None
            and wallet_key
            and day >= ACTIVE_WALLETS_START_DATE
            and side.startswith("open_")
        ):
            active_wallet_days.setdefault(day, set()).add(wallet_key)
        amount = abs(to_num(normalized.get("amount"), 0.0))
        price = to_num(normalized.get("price"), 0.0)
        fee = max(0.0, to_num(normalized.get("fee"), 0.0))
        liquidity_pool_fee = max(0.0, to_num(normalized.get("liquidityPoolFee"), 0.0))
        execution_kind = classify_trade_execution(normalized)
        notional = amount * price
        bucket["trades"] += 1
        if execution_kind == "taker":
            bucket["takerTrades"] += 1
            bucket["takerVolumeUsd"] += notional
        else:
            bucket["makerTrades"] += 1
            bucket["makerVolumeUsd"] += notional
        bucket["volumeUsd"] += notional
        if fee > 0 or liquidity_pool_fee > 0:
            bucket["feeBearingVolumeUsd"] += notional
        bucket["feesPaidUsd"] += fee
        bucket["liquidityPoolFeesUsd"] += liquidity_pool_fee
        bucket["feesUsd"] += fee + liquidity_pool_fee


def add_funding_rows_to_exchange_daily(daily_by_date, funding_rows, wallet=None, active_wallet_days=None):
    for row in funding_rows or []:
        normalized = normalize_funding_row(row)
        timestamp = int(normalized.get("created_at") or 0)
        day = format_utc_date_from_ms(timestamp)
        if not day:
            continue
        daily_by_date.setdefault(day, empty_exchange_metric_day())


def merge_exchange_daily_partials(daily_by_date, active_wallet_days, partial_daily, partial_active):
    for day, partial_bucket in (partial_daily or {}).items():
        target_bucket = daily_by_date.setdefault(day, empty_exchange_metric_day())
        for key, value in (partial_bucket or {}).items():
            if key in {"accountsAdded", "accountsTotal", "activeWallets"}:
                continue
            if isinstance(value, (int, float)):
                target_bucket[key] = float(target_bucket.get(key) or 0.0) + float(value)
    for day, wallets in (partial_active or {}).items():
        if not wallets:
            continue
        target_wallets = active_wallet_days.setdefault(day, set())
        target_wallets.update(str(wallet).strip() for wallet in wallets if str(wallet).strip())


def build_exchange_daily_partial_for_shard(shard_index, prefer_isolated):
    daily_by_date = {}
    active_wallet_days = {}
    history_dir = SHARDS_DIR / f"shard_{shard_index}" / "wallet_history"
    if history_dir.exists():
        for history_path in history_dir.glob("*.json"):
            wallet = history_path.stem
            if prefer_isolated.get(wallet):
                continue
            history = read_json(history_path, None)
            if not isinstance(history, dict):
                continue
            add_trade_rows_to_exchange_daily(
                daily_by_date,
                history.get("trades") or [],
                wallet=wallet,
                active_wallet_days=active_wallet_days,
            )
            add_funding_rows_to_exchange_daily(
                daily_by_date,
                history.get("funding") or [],
                wallet=wallet,
                active_wallet_days=active_wallet_days,
            )
    return daily_by_date, {day: sorted(wallets) for day, wallets in active_wallet_days.items()}


def build_exchange_wallet_metric_history(address_dataset, rows_by_wallet, generated_at, shard_count):
    wallet_first_seen = load_wallet_discovery_first_seen()
    wallets = set()
    for item in address_dataset.get("wallets") or []:
        if isinstance(item, dict):
            wallet = str(item.get("wallet") or "").strip()
            if wallet:
                wallets.add(wallet)
    wallets.update(str(wallet).strip() for wallet in rows_by_wallet.keys() if str(wallet).strip())
    wallets.update(str(wallet).strip() for wallet in wallet_first_seen.keys() if str(wallet).strip())

    daily_by_date = {}
    active_wallet_days = {}
    for wallet in sorted(wallets):
        row = rows_by_wallet.get(wallet) or {}
        first_seen_ms = int(wallet_first_seen.get(wallet) or 0)
        if first_seen_ms <= 0:
            first_seen_ms = int(row.get("firstTrade") or 0) or int(row.get("updatedAt") or generated_at)
        first_seen_day = format_utc_date_from_ms(first_seen_ms)
        if not first_seen_day:
            continue
        bucket = daily_by_date.setdefault(first_seen_day, empty_exchange_metric_day())
        bucket["accountsAdded"] += 1

    isolated_wallets = load_isolated_wallets()
    prefer_isolated = {}
    for wallet in sorted(isolated_wallets):
        isolated_history_exists = isolated_wallet_history_path(wallet).exists() or isolated_wallet_history_log_path(wallet).exists()
        # For exchange-wide daily metrics, isolated histories are the more
        # complete/canonical source whenever they exist. Score-based fallback to
        # shard histories was suppressing recent isolated-only activity for some
        # wallets, which corrupted daily Active Wallets / Trades / Fees / Volume.
        prefer_isolated[wallet] = bool(isolated_history_exists)

    worker_count = max(
        1,
        min(
            shard_count,
            int(os.getenv("PACIFICA_EXCHANGE_HISTORY_WORKERS", os.cpu_count() or 1) or 1),
        ),
    )
    shard_args = [(shard_index, prefer_isolated) for shard_index in range(shard_count)]
    if worker_count > 1 and shard_count > 1:
        ctx = mp.get_context("fork")
        with ctx.Pool(processes=worker_count) as pool:
            shard_partials = pool.starmap(build_exchange_daily_partial_for_shard, shard_args)
    else:
        shard_partials = [build_exchange_daily_partial_for_shard(*args) for args in shard_args]

    for partial_daily, partial_active in shard_partials:
        merge_exchange_daily_partials(
            daily_by_date,
            active_wallet_days,
            partial_daily,
            partial_active,
        )

    for wallet, use_isolated in sorted(prefer_isolated.items()):
        if not use_isolated:
            continue
        history = load_effective_isolated_history(wallet)
        add_trade_rows_to_exchange_daily(
            daily_by_date,
            history.get("trades") or [],
            wallet=wallet,
            active_wallet_days=active_wallet_days,
        )
        add_funding_rows_to_exchange_daily(
            daily_by_date,
            history.get("funding") or [],
            wallet=wallet,
            active_wallet_days=active_wallet_days,
        )

    generated_date = format_utc_date_from_ms(generated_at)
    market_daily_volume_by_date = load_market_daily_volume_by_date()
    if not daily_by_date:
        return {
            "version": 1,
            "generatedAt": generated_at,
            "dailyByDate": {},
            "coverage": {
                "startDate": None,
                "endDate": generated_date,
                "totalAccounts": len(wallets),
                "historyWallets": 0,
                "totalTrades": 0,
                "totalFeesUsd": 0.0,
                "totalVolumeUsd": 0.0,
            },
            "source": {
                "mode": "wallet_explorer_v2_materialized_daily_history",
                "wallets": len(wallets),
                "isolatedWallets": len(isolated_wallets),
            },
        }

    active_wallet_days_from_rows = build_active_wallet_days_from_rows(rows_by_wallet)

    start_date = min(daily_by_date.keys())
    end_date = max(max(daily_by_date.keys()), generated_date or max(daily_by_date.keys()))
    running_accounts = 0
    cursor = start_date
    while cursor and cursor <= end_date:
        bucket = daily_by_date.setdefault(cursor, empty_exchange_metric_day())
        running_accounts += int(bucket.get("accountsAdded") or 0)
        snapshot_fallback = load_wallet_metrics_daily_snapshot_aggregate(cursor)
        if snapshot_fallback:
            if int(bucket.get("trades") or 0) <= 0 and int(snapshot_fallback.get("trades") or 0) > 0:
                bucket["trades"] = int(snapshot_fallback.get("trades") or 0)
            if int(bucket.get("activeWallets") or 0) <= 0 and int(snapshot_fallback.get("activeWallets") or 0) > 0:
                bucket["activeWallets"] = int(snapshot_fallback.get("activeWallets") or 0)
            if float(bucket.get("feesUsd") or 0.0) <= 0 and float(snapshot_fallback.get("feesUsd") or 0.0) > 0:
                bucket["feesUsd"] = round(float(snapshot_fallback.get("feesUsd") or 0.0), 2)
                bucket["feesPaidUsd"] = round(float(snapshot_fallback.get("feesUsd") or 0.0), 2)
        bucket["accountsTotal"] = running_accounts
        bucket["activeWallets"] = max(
            int(bucket.get("activeWallets") or 0),
            len(active_wallet_days_from_rows.get(cursor) or ()),
        )
        bucket["trades"] = int(bucket.get("trades") or 0)
        bucket["makerTrades"] = int(bucket.get("makerTrades") or 0)
        bucket["takerTrades"] = int(bucket.get("takerTrades") or 0)
        bucket["feesUsd"] = round(float(bucket.get("feesUsd") or 0.0), 2)
        bucket["feesPaidUsd"] = round(float(bucket.get("feesPaidUsd") or 0.0), 2)
        bucket["liquidityPoolFeesUsd"] = round(float(bucket.get("liquidityPoolFeesUsd") or 0.0), 2)
        bucket["volumeUsd"] = round(float(bucket.get("volumeUsd") or 0.0), 2)
        bucket["makerVolumeUsd"] = round(float(bucket.get("makerVolumeUsd") or 0.0), 2)
        bucket["takerVolumeUsd"] = round(float(bucket.get("takerVolumeUsd") or 0.0), 2)
        bucket["feeBearingVolumeUsd"] = round(float(bucket.get("feeBearingVolumeUsd") or 0.0), 2)
        cursor = add_utc_days(cursor, 1)

    ordered_daily = {day: daily_by_date[day] for day in sorted(daily_by_date.keys())}
    total_trades = sum(int((row or {}).get("trades") or 0) for row in ordered_daily.values())
    total_fees = round(sum(float((row or {}).get("feesUsd") or 0.0) for row in ordered_daily.values()), 2)
    total_volume = round(sum(float((row or {}).get("volumeUsd") or 0.0) for row in ordered_daily.values()), 2)
    return {
        "version": 1,
        "generatedAt": generated_at,
        "dailyByDate": ordered_daily,
        "coverage": {
            "startDate": start_date,
            "endDate": end_date,
            "totalAccounts": len(wallets),
            "historyWallets": sum(1 for row in rows_by_wallet.values() if int((row or {}).get("trades") or 0) > 0),
            "totalTrades": total_trades,
            "totalFeesUsd": total_fees,
            "totalVolumeUsd": total_volume,
        },
        "source": {
            "mode": "wallet_explorer_v2_materialized_daily_history",
            "wallets": len(wallets),
            "isolatedWallets": len(isolated_wallets),
            "activeWalletsMode": "wallet_explorer_last_activity_fallback",
            "feesMode": "actual_trade_event_fee_sum",
            "feesFormula": "sum(trade.fee + trade.liquidityPoolFee)",
            "activeWalletsTimestampPriority": [
                "lastTrade",
                "all.lastTrade",
                "lastActivity",
                "lastActivityAt",
                "lastActiveAt",
                "lastOpenedAt",
                "all.lastOpenedAt",
            ],
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shards", type=int, default=DEFAULT_SHARD_COUNT)
    args = parser.parse_args()

    generated_at = now_ms()
    address_dataset = load_wallet_address_dataset()
    by_wallet = load_all_rows(args.shards)
    rows = []
    for item in address_dataset.get("wallets") or []:
        wallet = item["wallet"]
        rows.append(by_wallet.get(wallet) or build_placeholder_row(wallet, generated_at))
    nextgen_overlay = load_nextgen_state_overlay()
    nextgen_wallets = nextgen_overlay.get("wallets") if isinstance(nextgen_overlay.get("wallets"), dict) else {}
    nextgen_live = nextgen_overlay.get("live") if isinstance(nextgen_overlay.get("live"), dict) else {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("wallet") or "").strip()
        if not wallet:
            continue
        shard_index = shard_index_for_wallet(wallet, args.shards)
        wallet_state = nextgen_wallets.get(wallet) if isinstance(nextgen_wallets, dict) else {}
        live_state = nextgen_live.get(wallet) if isinstance(nextgen_live, dict) else {}
        if not isinstance(wallet_state, dict):
            wallet_state = {}
        if not isinstance(live_state, dict):
            live_state = {}
        canonical_last_opened_at = max(
            int(load_record_last_opened_at(wallet, shard_index, row) or 0),
            int(wallet_state.get("last_opened_at") or 0),
            int(live_state.get("last_opened_at") or 0),
        ) or None
        current_last_trade = max(
            int(wallet_state.get("last_trade_at") or 0),
            int(row.get("lastTrade") or 0),
        )
        current_last_activity = canonical_last_opened_at or 0
        current_open_positions = max(
            int(row.get("openPositions") or 0),
            int(wallet_state.get("open_positions") or 0),
            int(live_state.get("open_positions") or 0),
        )
        current_volume = max(
            float(row.get("volumeUsdRaw") or 0.0),
            float(wallet_state.get("volume_usd") or 0.0),
        )
        current_trades = max(
            int(row.get("trades") or 0),
            int(wallet_state.get("trades") or 0),
        )
        current_total_wins = max(
            int(row.get("totalWins") or 0),
            int(wallet_state.get("total_wins") or 0),
        )
        current_total_losses = max(
            int(row.get("totalLosses") or 0),
            int(wallet_state.get("total_losses") or 0),
        )
        current_realized = float(wallet_state.get("realized_pnl_usd") or row.get("pnlUsd") or 0.0)
        current_unrealized = float(wallet_state.get("unrealized_pnl_usd") or row.get("unrealizedPnlUsd") or 0.0)
        current_first_trade = int(wallet_state.get("first_trade_at") or row.get("firstTrade") or 0) or None
        current_win_rate = max(
            float(row.get("winRate") or 0.0),
            float(wallet_state.get("win_rate") or 0.0),
        )
        current_exposure = float(wallet_state.get("exposure_usd") or row.get("exposureUsd") or 0.0)
        current_activity_at = current_last_trade or canonical_last_opened_at or 0
        current_updated_at = max(
            int(row.get("updatedAt") or 0),
            int(wallet_state.get("updated_at") or 0),
            int(live_state.get("updated_at") or 0),
            int(live_state.get("lastLocalStateUpdateAt") or 0),
            current_activity_at,
        )
        row["lastTrade"] = current_last_trade or row.get("lastTrade")
        row["lastOpenedAt"] = canonical_last_opened_at
        row["lastActivity"] = current_activity_at or None
        row["lastActivityAt"] = current_activity_at or None
        row["lastActiveAt"] = current_activity_at or None
        row["updatedAt"] = current_updated_at or row.get("updatedAt")
        row["openPositions"] = current_open_positions
        row["volumeUsdRaw"] = round(current_volume, 2)
        row["volumeUsd"] = round(current_volume, 2)
        row["trades"] = current_trades
        row["totalWins"] = current_total_wins
        row["totalLosses"] = current_total_losses
        row["pnlUsd"] = round(current_realized, 2)
        row["unrealizedPnlUsd"] = round(current_unrealized, 2)
        row["totalPnlUsd"] = round(current_realized + current_unrealized, 2)
        row["winRate"] = round(current_win_rate, 2)
        row["firstTrade"] = current_first_trade
        row["exposureUsd"] = round(current_exposure, 2)
        row["liveTrackingSince"] = int(wallet_state.get("last_opened_at") or row.get("liveTrackingSince") or 0) or row.get("liveTrackingSince")
        row["liveLastScanAt"] = max(
            int(row.get("liveLastScanAt") or 0),
            int(live_state.get("updated_at") or 0),
            int(live_state.get("lastLocalStateUpdateAt") or 0),
        ) or row.get("liveLastScanAt")
        row["lifecycleStage"] = wallet_state.get("review_stage") or row.get("lifecycleStage")
        row["lifecycleStatus"] = wallet_state.get("review_stage") or row.get("lifecycleStatus")
        symbol_breakdown = wallet_state.get("symbol_breakdown")
        if isinstance(symbol_breakdown, list) and symbol_breakdown:
            row["symbolBreakdown"] = [
                {
                    "symbol": str(item.get("symbol") or "").strip().upper(),
                    "volumeUsd": float(item.get("volumeUsd") or item.get("volume_usd") or 0.0),
                }
                for item in symbol_breakdown
                if isinstance(item, dict) and str(item.get("symbol") or "").strip()
            ]
    rows = sort_rows(rows)
    counts = build_counts(rows)
    overview = build_exchange_overview()
    validation = build_validation(rows, overview)
    shard_progress = load_shard_progress(args.shards)
    runtime = aggregate_runtime(shard_progress, counts, generated_at)
    runtime["freshness"]["lastSuccessfulExchangeRollupAt"] = overview.get("updatedAt")
    runtime["storage"]["wallets"]["walletRowsPersisted"] = len(by_wallet)
    runtime["storage"]["wallets"]["historyStore"]["persistedWallets"] = count_history_files(args.shards)
    history = update_progress_history(counts, validation, overview, generated_at)
    volume_progress = compute_volume_progress(history, counts, validation, generated_at)
    runtime["progress"]["walletVolumeProgress"] = volume_progress
    runtime["progress"]["walletHistoryTimeline"]["remainingMs"] = volume_progress.get("etaWalletCompletionMs")
    runtime["progress"]["walletHistoryTimeline"]["etaMs"] = volume_progress.get("etaWalletCompletionMs")
    first_trade_index = build_first_trade_index(address_dataset, by_wallet)
    exact_first_trade_queue = build_exact_first_trade_queue(first_trade_index)
    phase_state = build_phase_state(first_trade_index)
    exchange_wallet_metric_history = build_exchange_wallet_metric_history(
        address_dataset,
        by_wallet,
        generated_at,
        args.shards,
    )
    compact = {
        "version": 2,
        "generatedAt": generated_at,
        "rows": rows,
        "counts": counts,
        "progress": runtime["progress"],
        "system": runtime["system"],
        "freshness": runtime["freshness"],
        "storage": runtime["storage"],
        "topCorrection": build_top_correction(rows),
        "exchangeOverview": {
            **overview,
            "walletSummedVolumeUsd": validation["walletTotalRawVolumeUsd"],
            "walletVerifiedVolumeUsd": validation["walletTotalVerifiedVolumeUsd"],
            "volumeGapUsd": validation["gapUsd"],
            "volumeRatio": round(
                validation["walletTotalRawVolumeUsd"] / overview["totalHistoricalVolumeUsd"],
                6,
            )
            if overview["totalHistoricalVolumeUsd"] > 0
            else 0,
        },
        "validation": validation,
        "availableSymbols": sorted({symbol for row in rows for symbol in (row.get("symbols") or [])}),
        "firstTradePhase": phase_state,
    }
    default_payload = build_default_payload(
        rows,
        counts,
        compact["exchangeOverview"],
        validation,
        generated_at,
        runtime,
    )
    manifest = {
        "version": 2,
        "generatedAt": generated_at,
        "dataset": {
            "generatedAt": generated_at,
            "counts": counts,
            "exchangeOverview": compact["exchangeOverview"],
            "firstTradePhase": phase_state,
        },
        "files": {
            "walletExplorerV3Path": str(UI_DIR.parent / "wallet_explorer_v3" / "manifest.json"),
            "walletsPage1Default": str(UI_DIR / "wallets_page1_default.json"),
            "systemStatusSummary": str(UI_DIR / "system_status_summary.json"),
            "exchangeWalletKpis": str(UI_DIR / "exchange_wallet_kpis.json"),
            "exchangeWalletMetricHistory": str(EXCHANGE_WALLET_METRIC_HISTORY_PATH),
            "validationReport": str(UI_DIR / "validation_report.json"),
            "firstTradeIndex": str(FIRST_TRADE_INDEX_PATH),
            "exactFirstTradeQueue": str(EXACT_FIRST_TRADE_QUEUE_PATH),
            "phaseState": str(PHASE_STATE_PATH),
        },
    }
    write_json_atomic(V3_BUILD_SOURCE_PATH, compact)
    build_wallet_storage_v3(V3_BUILD_SOURCE_PATH)
    write_json_atomic(
        UI_DIR / "wallets_page1_default.json",
        {
            "version": 1,
            "generatedAt": generated_at,
            "sourceGeneratedAt": generated_at,
            "defaultQuery": {"timeframe": "all", "page": 1, "pageSize": 20, "sort": "rankingVolumeUsd", "dir": "desc"},
            "payload": default_payload,
        },
    )
    write_json_atomic(
        UI_DIR / "system_status_summary.json",
        build_status_summary(counts, compact["exchangeOverview"], generated_at, runtime, volume_progress, phase_state),
    )
    write_json_atomic(
        UI_DIR / "exchange_wallet_kpis.json",
        build_exchange_wallet_kpis(rows, counts, generated_at, exchange_wallet_metric_history),
    )
    write_json_atomic(EXCHANGE_WALLET_METRIC_HISTORY_PATH, exchange_wallet_metric_history)
    write_json_atomic(VOLUME_PROGRESS_HISTORY_PATH, history)
    write_json_atomic(UI_DIR / "validation_report.json", validation)
    write_json_atomic(UI_DIR / "wallet_tracking_manifest.json", manifest)
    write_json_atomic(FIRST_TRADE_INDEX_PATH, first_trade_index)
    write_json_atomic(EXACT_FIRST_TRADE_QUEUE_PATH, exact_first_trade_queue)
    write_json_atomic(PHASE_STATE_PATH, phase_state)
    print(f"wallet_explorer_v2 materialized rows={len(rows)} complete={counts['trackedFullWallets']}", flush=True)


if __name__ == "__main__":
    main()
