#!/usr/bin/env python3
import argparse
import concurrent.futures
import time
from collections import deque
from pathlib import Path

from common import (
    DEFAULT_SHARD_COUNT,
    SHARDS_DIR,
    ProxyPool,
    backoff_seconds,
    build_canonical_wallet_record,
    build_wallet_record_id,
    funding_row_key,
    initial_wallet_history,
    initial_wallet_state,
    list_wallets_from_sources,
    load_live_wallet_universe,
    load_recent_wallet_triggers,
    load_wallet_discovery_recent_seen,
    load_wallet_discovery_first_seen,
    load_proxies,
    load_isolated_wallets,
    latest_opened_at_from_history,
    normalize_funding_row,
    normalize_trade_row,
    now_ms,
    read_json,
    request_endpoint,
    shard_index_for_wallet,
    shard_addresses_path,
    trade_row_key,
    wallet_history_path,
    wallet_record_path,
    wallet_state_path,
    write_json_atomic,
)

STALL_THRESHOLD_MS = 5 * 60 * 1000
WALLET_REFRESH_DEFAULT_MS = 30 * 1000
HOT_HEAD_REFRESH_DEFAULT_MS = 5 * 60 * 1000
HEAD_HISTORY_LIMIT_DEFAULT = 3
HEAD_TIMEOUT_SECONDS_DEFAULT = 12
HEAD_REQUEST_ATTEMPTS_DEFAULT = 3


def scheduler_state_path(shard_index: int) -> Path:
    return SHARDS_DIR / f"shard_{shard_index}" / "scheduler.json"


def load_wallets(shard_index: int, shard_count: int = DEFAULT_SHARD_COUNT) -> list[str]:
    payload = read_json(shard_addresses_path(shard_index), {}) or {}
    isolated_wallets = load_isolated_wallets()
    wallets = {
        str(wallet).strip()
        for wallet in (payload.get("wallets") or [])
        if str(wallet).strip() and str(wallet).strip() not in isolated_wallets
    }
    try:
        dynamic_sources = list_wallets_from_sources()
        for item in dynamic_sources.get("wallets") or []:
            wallet = str((item or {}).get("wallet") or "").strip()
            if (
                wallet
                and wallet not in isolated_wallets
                and shard_index_for_wallet(wallet, shard_count) == shard_index
            ):
                wallets.add(wallet)
    except Exception:
        pass
    return sorted(wallets)


def load_wallet_state(shard_index: int, wallet: str) -> dict:
    path = wallet_state_path(shard_index, wallet)
    return read_json(path, initial_wallet_state(wallet, shard_index)) or initial_wallet_state(wallet, shard_index)


def load_wallet_history(shard_index: int, wallet: str) -> dict:
    path = wallet_history_path(shard_index, wallet)
    return read_json(path, initial_wallet_history(wallet, shard_index)) or initial_wallet_history(wallet, shard_index)


def load_wallet_record(shard_index: int, wallet: str) -> dict:
    return read_json(wallet_record_path(shard_index, wallet), {}) or {}


def default_summary() -> dict:
    return {
        "tradeExhausted": False,
        "fundingExhausted": False,
        "tradeStarted": False,
        "fundingStarted": False,
        "tradePagesFetched": 0,
        "fundingPagesFetched": 0,
        "nextEligibleAt": 0,
        "lastAttemptAt": 0,
        "lastSuccessAt": 0,
        "retryCount": 0,
        "firstTrade": None,
        "firstTradeExact": False,
        "discoveryFirstSeenAt": None,
        "priorityTs": 253402300799000,
        "rawVolumeUsd": 0.0,
        "headTradeLastAttemptAt": 0,
        "headTradeLastSuccessAt": 0,
        "headTradeLastTrade": 0,
        "headTradeNextEligibleAt": 0,
    }


def summarize_wallet_state(
    state: dict | None,
    record: dict | None = None,
    discovery_first_seen_at: int | None = None,
) -> dict:
    if not isinstance(state, dict):
        return default_summary()
    streams = state.get("streams") or {}
    trades = streams.get("trades") or {}
    funding = streams.get("funding") or {}
    head_refresh = ((state.get("headRefresh") or {}).get("trades")) or {}

    def stream_started(stream: dict) -> bool:
        return any(
            [
                int(stream.get("pagesFetched") or 0) > 0,
                int(stream.get("rowsFetched") or 0) > 0,
                bool(stream.get("frontierCursor")),
                bool(stream.get("failedCursor")),
                int(stream.get("lastAttemptAt") or 0) > 0,
                int(stream.get("lastSuccessAt") or 0) > 0,
            ]
        )

    incomplete_next = [
        int(stream.get("nextEligibleAt") or 0)
        for stream in (trades, funding)
        if not bool(stream.get("exhausted")) and int(stream.get("nextEligibleAt") or 0) > 0
    ]
    first_trade = None
    if isinstance(record, dict):
        candidate = int(record.get("firstTrade") or 0)
        if candidate > 0:
            first_trade = candidate
    raw_volume_usd = 0.0
    if isinstance(record, dict):
        try:
            raw_volume_usd = float(record.get("volumeUsdRaw") or 0.0)
        except Exception:
            raw_volume_usd = 0.0
    discovery_ts = int(discovery_first_seen_at or 0) or None
    priority_ts = first_trade or discovery_ts or 253402300799000

    return {
        "tradeExhausted": bool(trades.get("exhausted")),
        "fundingExhausted": bool(funding.get("exhausted")),
        "tradeStarted": stream_started(trades),
        "fundingStarted": stream_started(funding),
        "tradePagesFetched": int(trades.get("pagesFetched") or 0),
        "fundingPagesFetched": int(funding.get("pagesFetched") or 0),
        "nextEligibleAt": min(incomplete_next) if incomplete_next else 0,
        "lastAttemptAt": max(int(trades.get("lastAttemptAt") or 0), int(funding.get("lastAttemptAt") or 0)),
        "lastSuccessAt": max(int(trades.get("lastSuccessAt") or 0), int(funding.get("lastSuccessAt") or 0)),
        "retryCount": int(trades.get("retryCount") or 0) + int(funding.get("retryCount") or 0),
        "firstTrade": first_trade,
        "firstTradeExact": bool(first_trade and trades.get("exhausted")),
        "discoveryFirstSeenAt": discovery_ts,
        "priorityTs": priority_ts,
        "rawVolumeUsd": raw_volume_usd,
        "headTradeLastAttemptAt": int(head_refresh.get("lastAttemptAt") or 0),
        "headTradeLastSuccessAt": int(head_refresh.get("lastSuccessAt") or 0),
        "headTradeLastTrade": int(head_refresh.get("lastTrade") or 0),
        "headTradeNextEligibleAt": int(head_refresh.get("nextEligibleAt") or 0),
    }


def load_scheduler_state(shard_index: int) -> dict:
    payload = read_json(
        scheduler_state_path(shard_index),
        {"version": 2, "queueCursor": 0, "cycle": 0, "lastSavedAt": now_ms(), "stallGuard": {}},
    ) or {"version": 2, "queueCursor": 0, "cycle": 0, "lastSavedAt": now_ms(), "stallGuard": {}}
    payload.setdefault("version", 2)
    payload.setdefault("queueCursor", 0)
    payload.setdefault("cycle", 0)
    payload.setdefault("lastSavedAt", now_ms())
    payload.setdefault("stallGuard", {})
    return payload


def save_scheduler_state(shard_index: int, scheduler_state: dict) -> None:
    scheduler_state["lastSavedAt"] = now_ms()
    write_json_atomic(scheduler_state_path(shard_index), scheduler_state)


def default_stall_guard() -> dict:
    return {
        "mode": "normal",
        "blockedOn": None,
        "stalledSinceAt": None,
        "rescueEnteredAt": None,
        "lastCompletionAdvanceAt": 0,
        "lastFundingAdvanceAt": 0,
        "lastTradeAdvanceAt": 0,
        "lastCompleteCount": 0,
        "lastFundingPagesFetched": 0,
        "lastTradePagesFetched": 0,
        "fundingBacklogWallets": 0,
        "tradeBacklogWallets": 0,
    }


def compute_progress_snapshot(wallets: list[str], summaries: dict) -> dict:
    complete_count = 0
    funding_backlog = 0
    trade_backlog = 0
    trade_pages = 0
    funding_pages = 0
    for wallet in wallets:
        summary = summaries.get(wallet, default_summary())
        if summary["tradeExhausted"] and summary["fundingExhausted"]:
            complete_count += 1
        if not summary["tradeExhausted"]:
            trade_backlog += 1
        if summary["tradeExhausted"] and not summary["fundingExhausted"]:
            funding_backlog += 1
        trade_pages += int(summary.get("tradePagesFetched") or 0)
        funding_pages += int(summary.get("fundingPagesFetched") or 0)
    return {
        "completeCount": complete_count,
        "tradeBacklogWallets": trade_backlog,
        "fundingBacklogWallets": funding_backlog,
        "tradePagesFetched": trade_pages,
        "fundingPagesFetched": funding_pages,
    }


def update_stall_guard(scheduler_state: dict, progress_snapshot: dict, threshold_ms: int = STALL_THRESHOLD_MS) -> dict:
    now = now_ms()
    guard = default_stall_guard()
    guard.update(scheduler_state.get("stallGuard") or {})

    if int(guard.get("lastCompletionAdvanceAt") or 0) <= 0:
        guard["lastCompletionAdvanceAt"] = now
    if int(guard.get("lastFundingAdvanceAt") or 0) <= 0:
        guard["lastFundingAdvanceAt"] = now
    if int(guard.get("lastTradeAdvanceAt") or 0) <= 0:
        guard["lastTradeAdvanceAt"] = now

    complete_count = int(progress_snapshot.get("completeCount") or 0)
    funding_pages = int(progress_snapshot.get("fundingPagesFetched") or 0)
    trade_pages = int(progress_snapshot.get("tradePagesFetched") or 0)
    funding_backlog = int(progress_snapshot.get("fundingBacklogWallets") or 0)
    trade_backlog = int(progress_snapshot.get("tradeBacklogWallets") or 0)

    if complete_count > int(guard.get("lastCompleteCount") or 0):
        guard["lastCompletionAdvanceAt"] = now
    if funding_pages > int(guard.get("lastFundingPagesFetched") or 0):
        guard["lastFundingAdvanceAt"] = now
    if trade_pages > int(guard.get("lastTradePagesFetched") or 0):
        guard["lastTradeAdvanceAt"] = now

    funding_stalled = (
        funding_backlog > 0
        and now - int(guard.get("lastFundingAdvanceAt") or 0) >= threshold_ms
        and now - int(guard.get("lastCompletionAdvanceAt") or 0) >= threshold_ms
    )
    trade_stalled = trade_backlog > 0 and now - int(guard.get("lastTradeAdvanceAt") or 0) >= threshold_ms

    previous_mode = str(guard.get("mode") or "normal")
    if funding_stalled:
        if previous_mode != "funding_rescue":
            guard["rescueEnteredAt"] = now
            guard["stalledSinceAt"] = guard.get("stalledSinceAt") or now
        guard["mode"] = "funding_rescue"
        guard["blockedOn"] = "funding_completion"
    elif trade_stalled:
        if previous_mode != "trade_rescue":
            guard["rescueEnteredAt"] = now
            guard["stalledSinceAt"] = guard.get("stalledSinceAt") or now
        guard["mode"] = "trade_rescue"
        guard["blockedOn"] = "trade_progress"
    else:
        guard["mode"] = "normal"
        guard["blockedOn"] = None
        guard["stalledSinceAt"] = None
        if previous_mode != "normal":
            guard["rescueEnteredAt"] = now

    guard["lastCompleteCount"] = complete_count
    guard["lastFundingPagesFetched"] = funding_pages
    guard["lastTradePagesFetched"] = trade_pages
    guard["fundingBacklogWallets"] = funding_backlog
    guard["tradeBacklogWallets"] = trade_backlog
    scheduler_state["stallGuard"] = guard
    return guard


def load_initial_summaries(shard_index: int, wallets: list[str], discovery_hints: dict[str, int]) -> dict:
    summaries = {}
    for wallet in wallets:
        summaries[wallet] = summarize_wallet_state(
            load_wallet_state(shard_index, wallet),
            load_wallet_record(shard_index, wallet),
            discovery_hints.get(wallet),
        )
    return summaries


def persist_wallet(shard_index: int, wallet: str, state: dict, history: dict) -> dict:
    state["last_opened_at"] = latest_opened_at_from_history(history)
    state["updatedAt"] = now_ms()
    history["updatedAt"] = now_ms()
    record = build_canonical_wallet_record(wallet, shard_index, state, history)
    write_json_atomic(wallet_state_path(shard_index, wallet), state)
    write_json_atomic(wallet_history_path(shard_index, wallet), history)
    write_json_atomic(wallet_record_path(shard_index, wallet), record)
    return record


def refresh_wallet_set(shard_index: int, shard_count: int, wallets: list[str], summaries: dict, discovery_hints: dict) -> tuple[list[str], dict]:
    next_wallets = load_wallets(shard_index, shard_count)
    current_set = set(wallets)
    next_set = set(next_wallets)
    for wallet in next_set - current_set:
        summaries[wallet] = summarize_wallet_state(
            load_wallet_state(shard_index, wallet),
            load_wallet_record(shard_index, wallet),
            discovery_hints.get(wallet),
        )
    for wallet in list(summaries.keys()):
        if wallet not in next_set:
            summaries.pop(wallet, None)
    return next_wallets, summaries


def persist_state_history(shard_index: int, wallet: str, state: dict, history: dict) -> None:
    state["last_opened_at"] = latest_opened_at_from_history(history)
    state["updatedAt"] = now_ms()
    history["updatedAt"] = now_ms()
    write_json_atomic(wallet_state_path(shard_index, wallet), state)
    write_json_atomic(wallet_history_path(shard_index, wallet), history)


def persist_state_record(shard_index: int, wallet: str, state: dict, record: dict) -> None:
    state["updatedAt"] = now_ms()
    write_json_atomic(wallet_state_path(shard_index, wallet), state)
    write_json_atomic(wallet_record_path(shard_index, wallet), record)


def persist_head_probe_record(shard_index: int, wallet: str, state: dict, probe: dict) -> dict:
    existing = load_wallet_record(shard_index, wallet) or {}
    record = dict(existing) if isinstance(existing, dict) else {}
    now = now_ms()
    last_trade = int(probe.get("lastTrade") or 0)
    last_success = int(probe.get("lastSuccessAt") or 0)

    def merge_bucket(name: str, window_ms: int | None):
        bucket = dict(record.get(name) or {})
        if last_trade > 0 and (window_ms is None or last_trade >= now - window_ms):
            bucket["lastTrade"] = max(int(bucket.get("lastTrade") or 0), last_trade)
        record[name] = bucket

    merge_bucket("all", None)
    merge_bucket("d30", 30 * 24 * 60 * 60 * 1000)
    merge_bucket("d7", 7 * 24 * 60 * 60 * 1000)
    merge_bucket("d24", 24 * 60 * 60 * 1000)

    record["wallet"] = wallet
    record["walletRecordId"] = record.get("walletRecordId") or build_wallet_record_id(wallet)
    record["shardIndex"] = shard_index
    if last_trade > 0:
        record["lastTrade"] = max(int(record.get("lastTrade") or 0), last_trade)
        record["headTradeLastSeenAt"] = last_trade
    if last_success > 0:
        record["headTradeSyncAt"] = last_success
    record["updatedAt"] = max(
        int(record.get("updatedAt") or 0),
        int(state.get("updatedAt") or 0),
        last_trade,
        last_success,
    )
    trade_pagination = dict(record.get("tradePagination") or {})
    trade_pagination["lastAttemptAt"] = max(
        int(trade_pagination.get("lastAttemptAt") or 0),
        int(probe.get("lastAttemptAt") or 0),
    ) or None
    trade_pagination["lastSuccessAt"] = max(
        int(trade_pagination.get("lastSuccessAt") or 0),
        last_success,
    ) or None
    trade_pagination["lastPageRowCount"] = int(
        probe.get("lastPageRowCount") or trade_pagination.get("lastPageRowCount") or 0
    )
    record["tradePagination"] = trade_pagination
    record["headRefresh"] = {
        "trades": {
            "lastTrade": last_trade or None,
            "lastSuccessAt": last_success or None,
            "lastAttemptAt": int(probe.get("lastAttemptAt") or 0) or None,
            "lastError": probe.get("lastError") or None,
            "lastPageRowCount": int(probe.get("lastPageRowCount") or 0),
        }
    }
    persist_state_record(shard_index, wallet, state, record)
    return record


def prepare_history_rows(existing_rows, stream_name: str):
    if stream_name == "trades":
        normalize = normalize_trade_row
        key_fn = trade_row_key
        sort_key = lambda row: (int(row["timestamp"] or 0), row["symbol"])
    else:
        normalize = normalize_funding_row
        key_fn = funding_row_key
    out = []
    seen = set()
    for row in existing_rows or []:
        normalized = normalize(row)
        key = key_fn(normalized)
        if key in seen:
            continue
        seen.add(key)
        out.append(normalized)
    return out, seen


def dedupe_and_extend(existing_rows, incoming_rows, stream_name: str, seen=None):
    if stream_name == "trades":
        normalize = normalize_trade_row
        key_fn = trade_row_key
    else:
        normalize = normalize_funding_row
        key_fn = funding_row_key

    out = existing_rows if existing_rows is not None else []
    if seen is None:
        out, seen = prepare_history_rows(out, stream_name)

    added = 0
    for row in incoming_rows or []:
        normalized = normalize(row)
        key = key_fn(normalized)
        if key in seen:
            continue
        seen.add(key)
        out.append(normalized)
        added += 1
    return out, added, seen


def append_step(stream_state: dict, step: dict) -> None:
    steps = list(stream_state.get("steps") or [])
    steps.append(step)
    if len(steps) > 512:
        steps = steps[-512:]
    stream_state["steps"] = steps


def probe_trade_head(
    wallet: str,
    limit: int,
    timeout_seconds: int,
    request_attempts: int,
    proxy_pool: ProxyPool,
    state: dict,
):
    head_state = state.setdefault("headRefresh", {}).setdefault("trades", {})
    attempt_at = now_ms()
    if int(head_state.get("nextEligibleAt") or 0) > attempt_at:
        return {
            "failed": False,
            "skipped": True,
            "lastTrade": int(head_state.get("lastTrade") or 0),
            "lastAttemptAt": attempt_at,
            "lastSuccessAt": int(head_state.get("lastSuccessAt") or 0),
            "lastPageRowCount": int(head_state.get("lastPageRowCount") or 0),
            "lastError": head_state.get("lastError"),
        }
    head_state["lastAttemptAt"] = attempt_at
    try:
        payload, entry = request_endpoint(
            wallet,
            "trades/history",
            max(1, int(limit or 1)),
            None,
            proxy_pool,
            timeout_seconds=timeout_seconds,
            attempts=request_attempts,
            prefer_direct=True,
        )
        rows = payload.get("data") or []
        latest_trade = 0
        for row in rows:
            try:
                latest_trade = max(
                    latest_trade,
                    int((row or {}).get("timestamp") or (row or {}).get("created_at") or (row or {}).get("createdAt") or 0),
                )
            except Exception:
                continue
        head_state["lastSuccessAt"] = now_ms()
        if latest_trade > 0:
            head_state["lastTrade"] = max(int(head_state.get("lastTrade") or 0), latest_trade)
        head_state["lastPageRowCount"] = len(rows)
        head_state["lastError"] = None
        head_state["retryCount"] = 0
        head_state["nextEligibleAt"] = 0
        append_step(
            state.setdefault("streams", {}).setdefault("trades", {}),
            {
                "status": "head_probe",
                "cursor": None,
                "nextCursor": payload.get("next_cursor"),
                "rowCount": len(rows),
                "addedRows": 0,
                "fetchedAt": now_ms(),
                "proxyId": entry.get("id"),
            },
        )
        return {
            "failed": False,
            "skipped": False,
            "lastTrade": int(head_state.get("lastTrade") or 0),
            "lastAttemptAt": attempt_at,
            "lastSuccessAt": int(head_state.get("lastSuccessAt") or 0),
            "lastPageRowCount": len(rows),
            "lastError": None,
        }
    except Exception as exc:
        reason = str(exc)
        head_state["retryCount"] = int(head_state.get("retryCount") or 0) + 1
        head_state["lastError"] = reason
        head_state["nextEligibleAt"] = now_ms() + backoff_seconds(head_state["retryCount"], reason) * 1000
        return {
            "failed": True,
            "skipped": False,
            "lastTrade": int(head_state.get("lastTrade") or 0),
            "lastAttemptAt": attempt_at,
            "lastSuccessAt": int(head_state.get("lastSuccessAt") or 0),
            "lastPageRowCount": int(head_state.get("lastPageRowCount") or 0),
            "lastError": reason,
        }


def load_hot_wallet_hints(shard_index: int, shard_count: int) -> dict:
    hints = {}

    def upsert(wallet: str, priority: int, activity_at: int, open_positions: int, source: str):
        normalized = str(wallet or "").strip()
        if not normalized:
            return
        next_row = {
            "wallet": normalized,
            "priority": int(priority),
            "activityAt": int(activity_at or 0),
            "openPositions": int(open_positions or 0),
            "source": str(source or "unknown"),
        }
        existing = hints.get(normalized)
        if not existing:
            hints[normalized] = next_row
            return
        existing["openPositions"] = max(
            int(existing.get("openPositions") or 0),
            int(next_row.get("openPositions") or 0),
        )
        existing["activityAt"] = max(
            int(existing.get("activityAt") or 0),
            int(next_row.get("activityAt") or 0),
        )
        if (
            int(next_row.get("priority") or 99) < int(existing.get("priority") or 99)
            or (
                int(next_row.get("priority") or 99) == int(existing.get("priority") or 99)
                and int(next_row.get("activityAt") or 0) > int(existing.get("activityAt") or 0)
            )
        ):
            existing["priority"] = int(next_row["priority"])
            existing["source"] = next_row["source"]

    for wallet, trigger_at in load_recent_wallet_triggers(
        max_age_ms=24 * 60 * 60 * 1000,
        limit=12000,
    ).items():
        if shard_index_for_wallet(wallet, shard_count) != shard_index:
            continue
        upsert(wallet, 0, int(trigger_at or 0), 0, "live_trigger_activity")

    live_wallets = load_live_wallet_universe().get("wallets") or {}
    for wallet, details in live_wallets.items():
        if shard_index_for_wallet(wallet, shard_count) != shard_index:
            continue
        open_positions = int((details or {}).get("openPositions") or 0)
        if open_positions <= 0:
            continue
        activity_at = max(
            int((details or {}).get("updatedAt") or 0),
            int((details or {}).get("liveScannedAt") or 0),
        )
        upsert(wallet, 1, activity_at, open_positions, "live_open_positions")
    recent_discovery_cutoff = now_ms() - 5 * 24 * 60 * 60 * 1000
    for wallet, last_seen_at in load_wallet_discovery_recent_seen().items():
        if shard_index_for_wallet(wallet, shard_count) != shard_index:
            continue
        if int(last_seen_at or 0) < recent_discovery_cutoff:
            continue
        upsert(wallet, 2, int(last_seen_at or 0), 0, "recent_discovery_activity")
    return hints


def collect_stream(
    wallet: str,
    shard_index: int,
    stream_name: str,
    endpoint: str,
    limit: int,
    timeout_seconds: int,
    request_attempts: int,
    proxy_pool: ProxyPool,
    state: dict,
    history: dict,
    seen_rows: set | None = None,
    page_budget: int | None = None,
    flush_interval_pages: int = 1,
):
    stream_state = ((state.get("streams") or {}).get(stream_name)) or {}
    if stream_state.get("exhausted"):
        return {"pagesCollected": 0, "exhausted": True, "failed": False}
    if int(stream_state.get("nextEligibleAt") or 0) > now_ms():
        return {"pagesCollected": 0, "exhausted": bool(stream_state.get("exhausted")), "failed": False}
    history_key = "trades" if stream_name == "trades" else "funding"
    cursor = stream_state.get("failedCursor") or stream_state.get("frontierCursor") or None
    pages_collected = 0
    pages_since_flush = 0
    while True:
        stream_state["lastAttemptAt"] = now_ms()
        try:
            payload, entry = request_endpoint(
                wallet,
                endpoint,
                limit,
                cursor,
                proxy_pool,
                timeout_seconds=timeout_seconds,
                attempts=request_attempts,
            )
            rows = payload.get("data") or []
            next_cursor = payload.get("next_cursor")
            has_more = bool(payload.get("has_more"))
            history[history_key], added, seen_rows = dedupe_and_extend(
                history.get(history_key) or [],
                rows,
                history_key,
                seen=seen_rows,
            )
            stream_state["pagesFetched"] = int(stream_state.get("pagesFetched") or 0) + 1
            stream_state["rowsFetched"] = len(history[history_key])
            stream_state["lastSuccessAt"] = now_ms()
            stream_state["frontierCursor"] = next_cursor if has_more else None
            stream_state["failedCursor"] = None
            stream_state["retryCount"] = 0
            stream_state["nextEligibleAt"] = 0
            stream_state["lastError"] = None
            stream_state["lastPageRowCount"] = len(rows)
            stream_state["exhausted"] = not has_more
            append_step(
                stream_state,
                {
                    "status": "persisted",
                    "cursor": cursor,
                    "nextCursor": next_cursor,
                    "rowCount": len(rows),
                    "addedRows": added,
                    "fetchedAt": now_ms(),
                    "proxyId": entry.get("id"),
                },
            )
            pages_collected += 1
            pages_since_flush += 1
            should_flush = pages_since_flush >= max(1, int(flush_interval_pages or 1))
            if not has_more:
                should_flush = True
            if page_budget and pages_collected >= page_budget:
                should_flush = True
            if should_flush:
                persist_state_history(shard_index, wallet, state, history)
                pages_since_flush = 0
            if not has_more:
                return {"pagesCollected": pages_collected, "exhausted": True, "failed": False}
            cursor = next_cursor
            if not cursor:
                stream_state["exhausted"] = True
                persist_state_history(shard_index, wallet, state, history)
                return {"pagesCollected": pages_collected, "exhausted": True, "failed": False}
            if page_budget and pages_collected >= page_budget:
                return {"pagesCollected": pages_collected, "exhausted": False, "failed": False}
        except Exception as exc:
            reason = str(exc)
            stream_state["retryCount"] = int(stream_state.get("retryCount") or 0) + 1
            stream_state["failedCursor"] = cursor
            stream_state["lastError"] = reason
            stream_state["nextEligibleAt"] = now_ms() + backoff_seconds(stream_state["retryCount"], reason) * 1000
            stream_state["exhausted"] = False
            append_step(
                stream_state,
                {
                    "status": "failed",
                    "cursor": cursor,
                    "nextCursor": None,
                    "rowCount": 0,
                    "addedRows": 0,
                    "fetchedAt": now_ms(),
                    "error": reason,
                },
            )
            persist_state_history(shard_index, wallet, state, history)
            return {"pagesCollected": pages_collected, "exhausted": False, "failed": True}


def process_wallet(
    wallet: str,
    shard_index: int,
    limit: int,
    timeout_seconds: int,
    request_attempts: int,
    head_history_limit: int,
    head_timeout_seconds: int,
    head_request_attempts: int,
    proxy_pool: ProxyPool,
    lane: str,
    discovery_first_seen_at: int | None,
    intake_trade_page_budget: int,
    trade_page_budget: int,
    funding_page_budget: int,
    flush_interval_pages: int,
    refresh_trade_page_budget: int,
    refresh_funding_page_budget: int,
):
    if lane == "head":
        state = load_wallet_state(shard_index, wallet)
        state["status"] = "head_refresh"
        probe = probe_trade_head(
            wallet,
            head_history_limit,
            head_timeout_seconds,
            head_request_attempts,
            proxy_pool,
            state,
        )
        record = persist_head_probe_record(shard_index, wallet, state, probe)
        return {
            "record": record,
            "summary": summarize_wallet_state(state, record, discovery_first_seen_at),
        }

    state = load_wallet_state(shard_index, wallet)
    history = load_wallet_history(shard_index, wallet)
    history["trades"], trade_seen = prepare_history_rows(history.get("trades") or [], "trades")
    history["funding"], funding_seen = prepare_history_rows(history.get("funding") or [], "funding")
    state["status"] = f"collecting_{lane}"
    persist_state_history(shard_index, wallet, state, history)

    if lane in {"intake", "trade"}:
        page_budget = intake_trade_page_budget if lane == "intake" else trade_page_budget
        collect_stream(
            wallet,
            shard_index,
            "trades",
            "trades/history",
            limit,
            timeout_seconds,
            request_attempts,
            proxy_pool,
            state,
            history,
            seen_rows=trade_seen,
            page_budget=max(1, int(page_budget or 1)),
            flush_interval_pages=flush_interval_pages,
        )
        trade_summary = summarize_wallet_state(state)
        if trade_summary["tradeExhausted"] and not trade_summary["fundingExhausted"]:
            state["status"] = "awaiting_funding"
        elif trade_summary["tradeExhausted"] and trade_summary["fundingExhausted"]:
            state["status"] = "complete"
        else:
            state["status"] = "partial"
        record = persist_wallet(shard_index, wallet, state, history)
        return {
            "record": record,
            "summary": summarize_wallet_state(state, record, discovery_first_seen_at),
        }

    if lane == "funding":
        lane_summary = summarize_wallet_state(state)
        if not lane_summary["tradeExhausted"]:
            state["status"] = "partial"
            record = persist_wallet(shard_index, wallet, state, history)
            return {
                "record": record,
                "summary": summarize_wallet_state(state, record, discovery_first_seen_at),
            }
        collect_stream(
            wallet,
            shard_index,
            "funding",
            "funding/history",
            limit,
            timeout_seconds,
            request_attempts,
            proxy_pool,
            state,
            history,
            seen_rows=funding_seen,
            page_budget=max(0, int(funding_page_budget or 0)) or None,
            flush_interval_pages=flush_interval_pages,
        )
    else:
        if lane != "refresh":
            raise RuntimeError(f"unknown_lane:{lane}")
        if summarize_wallet_state(state)["tradeExhausted"]:
            state["streams"]["trades"]["exhausted"] = False
            trade_refresh = collect_stream(
                wallet,
                shard_index,
                "trades",
                "trades/history",
                limit,
                timeout_seconds,
                request_attempts,
                proxy_pool,
                state,
                history,
                seen_rows=trade_seen,
                page_budget=max(1, int(refresh_trade_page_budget or 1)),
                flush_interval_pages=max(1, flush_interval_pages),
            )
            state["streams"]["trades"]["exhausted"] = True
            state["streams"]["trades"]["frontierCursor"] = None
            state["streams"]["trades"]["failedCursor"] = None
            if not trade_refresh.get("failed"):
                state["streams"]["trades"]["nextEligibleAt"] = 0
                state["streams"]["trades"]["retryCount"] = 0
        if summarize_wallet_state(state)["fundingExhausted"]:
            state["streams"]["funding"]["exhausted"] = False
            funding_refresh = collect_stream(
                wallet,
                shard_index,
                "funding",
                "funding/history",
                limit,
                timeout_seconds,
                request_attempts,
                proxy_pool,
                state,
                history,
                seen_rows=funding_seen,
                page_budget=max(1, int(refresh_funding_page_budget or 1)),
                flush_interval_pages=max(1, flush_interval_pages),
            )
            state["streams"]["funding"]["exhausted"] = True
            state["streams"]["funding"]["frontierCursor"] = None
            state["streams"]["funding"]["failedCursor"] = None
            if not funding_refresh.get("failed"):
                state["streams"]["funding"]["nextEligibleAt"] = 0
                state["streams"]["funding"]["retryCount"] = 0

    record = persist_wallet(shard_index, wallet, state, history)
    state["status"] = "complete" if record.get("backfillComplete") else "partial"
    record = persist_wallet(shard_index, wallet, state, history)
    return {
        "record": record,
        "summary": summarize_wallet_state(state, record, discovery_first_seen_at),
    }


def write_progress(
    shard_index: int,
    shard_count: int,
    wallets: list[str],
    summaries: dict,
    proxy_pool: ProxyPool,
    last_wallet: str | None,
    scheduler_state: dict,
    hot_wallet_hints: dict | None = None,
    head_refresh_ms: int = HOT_HEAD_REFRESH_DEFAULT_MS,
):
    complete = sum(
        1
        for wallet in wallets
        if summaries.get(wallet, default_summary())["tradeExhausted"]
        and summaries.get(wallet, default_summary())["fundingExhausted"]
    )
    partial = len(wallets) - complete
    proxy_snapshot = proxy_pool.snapshot() or {}
    hot_hints = hot_wallet_hints if isinstance(hot_wallet_hints, dict) else {}
    hot_wallets = {str(wallet).strip() for wallet in hot_hints.keys() if str(wallet).strip()}
    now = now_ms()
    hot_cutoff = now - max(60_000, int(head_refresh_ms or HOT_HEAD_REFRESH_DEFAULT_MS))
    hot_fresh = 0
    hot_stale = 0
    hot_error = 0
    hot_pending = 0
    hot_last_success_at = 0
    hot_last_trade_at = 0
    for wallet in hot_wallets:
        summary = summaries.get(wallet, default_summary())
        last_success = int(summary.get("headTradeLastSuccessAt") or 0)
        last_trade = int(summary.get("headTradeLastTrade") or 0)
        last_attempt = int(summary.get("headTradeLastAttemptAt") or 0)
        next_eligible = int(summary.get("headTradeNextEligibleAt") or 0)
        hot_last_success_at = max(hot_last_success_at, last_success)
        hot_last_trade_at = max(hot_last_trade_at, last_trade)
        if last_success > 0 and last_success >= hot_cutoff:
            hot_fresh += 1
        else:
            hot_stale += 1
        if next_eligible > now:
            hot_pending += 1
            if last_attempt > 0 and last_attempt >= last_success:
                hot_error += 1
    write_json_atomic(
        wallet_state_path(shard_index, "__progress__"),
        {
            "version": 1,
            "generatedAt": now_ms(),
            "shardIndex": shard_index,
            "shardCount": shard_count,
            "walletsTotal": len(wallets),
            "walletsComplete": complete,
            "walletsPartial": partial,
            "lastWallet": last_wallet,
            # Persist only the aggregate proxy counters here. The full entry list is
            # very large, gets rewritten every cycle, and materially slows the
            # downstream materializer/status readers without improving UI behavior.
            "proxyPool": {
                "total": int((proxy_snapshot.get("total") or 0)),
                "cooling": int((proxy_snapshot.get("cooling") or 0)),
                "active": int((proxy_snapshot.get("active") or 0)),
            },
            "queueCursor": int(scheduler_state.get("queueCursor") or 0),
            "cycle": int(scheduler_state.get("cycle") or 0),
            "stallGuard": scheduler_state.get("stallGuard") or default_stall_guard(),
            "headRefresh": {
                "hotWallets": len(hot_wallets),
                "freshWallets": hot_fresh,
                "staleWallets": hot_stale,
                "errorWallets": hot_error,
                "pendingWallets": hot_pending,
                "lastSuccessAt": hot_last_success_at or None,
                "lastTradeAt": hot_last_trade_at or None,
                "refreshMs": int(head_refresh_ms or HOT_HEAD_REFRESH_DEFAULT_MS),
            },
        },
    )


def build_ordered_wallets(wallets, summaries):
    def order_key(wallet):
        summary = summaries.get(wallet, default_summary())
        priority_ts = int(summary.get("priorityTs") or 253402300799000)
        known = 0 if priority_ts < 253402300799000 else 1
        exact = 0 if summary.get("firstTradeExact") else 1
        return (known, priority_ts, exact, wallet)

    return sorted(wallets, key=order_key)


def pick_round_robin(wallets, start_cursor, max_items, predicate, seen):
    selected = []
    total = len(wallets)
    if total <= 0 or max_items <= 0:
        return selected, start_cursor
    cursor = start_cursor % total
    scanned = 0
    while len(selected) < max_items and scanned < total:
        wallet = wallets[cursor]
        cursor = (cursor + 1) % total
        scanned += 1
        if wallet in seen:
            continue
        if predicate(wallet):
            seen.add(wallet)
            selected.append(wallet)
    return selected, cursor


def interleave_candidates(trade_wallets, funding_wallets, max_items):
    selected = []
    trade_index = 0
    funding_index = 0
    trade_total = len(trade_wallets)
    funding_total = len(funding_wallets)
    if trade_total <= 0 and funding_total <= 0:
        return selected
    if funding_total <= 0:
        return trade_wallets[:max_items]
    if trade_total <= 0:
        return funding_wallets[:max_items]

    trade_burst = max(1, round(trade_total / max(1, funding_total)))
    while len(selected) < max_items and (trade_index < trade_total or funding_index < funding_total):
        taken = 0
        while trade_index < trade_total and len(selected) < max_items and taken < trade_burst:
            selected.append(trade_wallets[trade_index])
            trade_index += 1
            taken += 1
        if funding_index < funding_total and len(selected) < max_items:
            selected.append(funding_wallets[funding_index])
            funding_index += 1
        if trade_index >= trade_total and funding_index < funding_total:
            while funding_index < funding_total and len(selected) < max_items:
                selected.append(funding_wallets[funding_index])
                funding_index += 1
        if funding_index >= funding_total and trade_index < trade_total:
            while trade_index < trade_total and len(selected) < max_items:
                selected.append(trade_wallets[trade_index])
                trade_index += 1
    return selected


def build_cycle_candidates(
    wallets,
    summaries,
    queue_cursor,
    max_tasks,
    hot_wallet_hints=None,
    prioritize_first_trade=False,
    completion_lane_pct=35,
    rescue_mode="normal",
    refresh_lane_pct=10,
    intake_lane_pct=20,
    wallet_refresh_ms=WALLET_REFRESH_DEFAULT_MS,
    head_refresh_ms=HOT_HEAD_REFRESH_DEFAULT_MS,
):
    now = now_ms()
    ordered_wallets = build_ordered_wallets(wallets, summaries)
    seen = set()
    intake_ready = []
    trade_started = []
    funding_started = []
    trade_incomplete_exists = any(not summary["tradeExhausted"] for summary in summaries.values())
    for wallet, summary in summaries.items():
        if summary["nextEligibleAt"] and summary["nextEligibleAt"] > now:
            continue
        if not summary["tradeExhausted"] and not summary["tradeStarted"]:
            discovery_ts = int(summary.get("discoveryFirstSeenAt") or 0)
            intake_ready.append(
                (
                    0 if discovery_ts > 0 else 1,
                    -discovery_ts,
                    summary["lastAttemptAt"],
                    summary["retryCount"],
                    wallet,
                )
            )
        elif not summary["tradeExhausted"] and summary["tradeStarted"]:
            trade_started.append((summary["priorityTs"], summary["lastAttemptAt"], summary["retryCount"], wallet))
        elif summary["tradeExhausted"] and not summary["fundingExhausted"] and summary["fundingStarted"]:
            funding_started.append(
                (-float(summary.get("rawVolumeUsd") or 0.0), summary["priorityTs"], summary["lastAttemptAt"], summary["retryCount"], wallet)
            )

    intake_ready.sort(key=lambda item: (item[0], item[1], item[2], item[3], item[4]))
    trade_started.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
    funding_started.sort(key=lambda item: (item[0], item[1], item[2], item[3], item[4]))

    funding_backlog_exists = bool(funding_started) or any(
        summary["tradeExhausted"] and not summary["fundingExhausted"] for summary in summaries.values()
    )
    intake_backlog_exists = bool(intake_ready)
    funding_allowed = funding_backlog_exists
    completion_pct = max(0, min(90, int(completion_lane_pct or 0)))
    intake_pct = max(0, min(50, int(intake_lane_pct or 0)))
    intake_reserve = 0
    if intake_backlog_exists:
        intake_reserve = max(1, int(round(max_tasks * (intake_pct / 100.0))))
        if rescue_mode == "funding_rescue":
            intake_reserve = min(intake_reserve, 1 if max_tasks > 1 else 0)
        elif rescue_mode == "trade_rescue":
            intake_reserve = min(intake_reserve, max(1, max_tasks // 3))
        if trade_incomplete_exists or funding_allowed:
            intake_reserve = min(intake_reserve, max(1, max_tasks - 1))
        else:
            intake_reserve = min(intake_reserve, max_tasks)
    funding_reserve = 0
    if funding_allowed:
        funding_reserve = max(2, int(round(max_tasks * (completion_pct / 100.0))))
        if prioritize_first_trade and trade_incomplete_exists:
            funding_reserve = max(2, min(funding_reserve, max_tasks // 2))
        max_funding_reserve = max_tasks - intake_reserve
        funding_reserve = min(funding_reserve, max_funding_reserve - 1) if max_funding_reserve > 1 else 0
    if rescue_mode == "funding_rescue" and funding_allowed:
        funding_reserve = max(funding_reserve, max(4, int(round(max_tasks * 0.7))))
        max_funding_reserve = max_tasks - intake_reserve
        funding_reserve = min(funding_reserve, max_funding_reserve - 1) if max_funding_reserve > 1 else 0
    if rescue_mode == "trade_rescue":
        funding_reserve = 0 if not funding_allowed else min(funding_reserve, 2)
    trade_target = max(0, max_tasks - funding_reserve - intake_reserve) if max_tasks > 0 else 0
    if trade_started and trade_target <= 0 and max_tasks > intake_reserve + funding_reserve:
        trade_target = 1

    intake_candidates = []
    trade_candidates = []
    funding_candidates = []
    head_candidates = []
    refresh_candidates = []
    for _missing_hint, _neg_discovery_ts, _last_attempt, _retry_count, wallet in intake_ready[:intake_reserve]:
        seen.add(wallet)
        intake_candidates.append(wallet)
    for _priority_ts, _last_attempt, _retry_count, wallet in trade_started[:trade_target]:
        seen.add(wallet)
        trade_candidates.append(wallet)

    def eligible_started_trade(wallet):
        summary = summaries.get(wallet, default_summary())
        return (
            not summary["tradeExhausted"]
            and summary["tradeStarted"]
            and (summary["nextEligibleAt"] or 0) <= now
        )

    extra_trade, queue_cursor = pick_round_robin(
        ordered_wallets,
        queue_cursor,
        max(0, trade_target - len(trade_candidates)),
        eligible_started_trade,
        seen,
    )
    trade_candidates.extend(extra_trade)

    if funding_allowed:
        for _neg_volume, _priority_ts, _last_attempt, _retry_count, wallet in funding_started[: funding_reserve]:
            if wallet in seen:
                continue
            seen.add(wallet)
            funding_candidates.append(wallet)

    def eligible_new_funding(wallet):
        summary = summaries.get(wallet, default_summary())
        return (
            summary["tradeExhausted"]
            and not summary["fundingExhausted"]
            and not summary["fundingStarted"]
            and (summary["nextEligibleAt"] or 0) <= now
        )

    if funding_allowed:
        funding_ready = []
        for wallet in ordered_wallets:
            if wallet in seen:
                continue
            if eligible_new_funding(wallet):
                summary = summaries.get(wallet, default_summary())
                funding_ready.append(
                    (-float(summary.get("rawVolumeUsd") or 0.0), int(summary.get("priorityTs") or 253402300799000), wallet)
                )
        funding_ready.sort(key=lambda item: (item[0], item[1], item[2]))
        for _neg_volume, _priority_ts, wallet in funding_ready[: max(0, funding_reserve - len(funding_candidates))]:
            seen.add(wallet)
            funding_candidates.append(wallet)

    if rescue_mode != "funding_rescue" and len(trade_candidates) + len(funding_candidates) < max_tasks:
        extra_intake, queue_cursor = pick_round_robin(
            ordered_wallets,
            queue_cursor,
            max_tasks - len(intake_candidates) - len(trade_candidates) - len(funding_candidates),
            lambda wallet: (
                not summaries.get(wallet, default_summary())["tradeExhausted"]
                and not summaries.get(wallet, default_summary())["tradeStarted"]
                and (summaries.get(wallet, default_summary())["nextEligibleAt"] or 0) <= now
            ),
            seen,
        )
        intake_candidates.extend(extra_intake)

    if rescue_mode != "funding_rescue" and len(intake_candidates) + len(trade_candidates) + len(funding_candidates) < max_tasks:
        extra_trade, queue_cursor = pick_round_robin(
            ordered_wallets,
            queue_cursor,
            max_tasks - len(intake_candidates) - len(trade_candidates) - len(funding_candidates),
            lambda wallet: (
                not summaries.get(wallet, default_summary())["tradeExhausted"]
                and summaries.get(wallet, default_summary())["tradeStarted"]
                and (summaries.get(wallet, default_summary())["nextEligibleAt"] or 0) <= now
            ),
            seen,
        )
        trade_candidates.extend(extra_trade)

    hot_hints = hot_wallet_hints if isinstance(hot_wallet_hints, dict) else {}
    if hot_hints:
        hot_target = min(max(1, int(round(max_tasks * 0.4))), len(hot_hints))
        if intake_backlog_exists or trade_incomplete_exists or funding_allowed:
            hot_target = min(
                hot_target,
                max(1, max_tasks // 3),
            )
        if rescue_mode == "trade_rescue":
            hot_target = min(hot_target, 1)
        hot_due_cutoff = now - max(60_000, int(head_refresh_ms or HOT_HEAD_REFRESH_DEFAULT_MS))
        hot_ready = []
        for wallet, hint in hot_hints.items():
            if wallet in seen:
                continue
            summary = summaries.get(wallet)
            if not isinstance(summary, dict):
                continue
            if int(summary.get("headTradeNextEligibleAt") or 0) > now:
                continue
            head_last_success = int(summary.get("headTradeLastSuccessAt") or 0)
            if head_last_success > hot_due_cutoff and int(hint.get("openPositions") or 0) <= 0:
                continue
            hot_ready.append(
                (
                    int(hint.get("priority") or 0),
                    0 if not summary.get("tradeExhausted") else 1,
                    -int(hint.get("openPositions") or 0),
                    -(int(hint.get("activityAt") or 0)),
                    head_last_success if head_last_success > 0 else 0,
                    wallet,
                )
            )
        hot_ready.sort(key=lambda item: (item[0], item[1], item[2], item[3], item[4], item[5]))
        for _priority, _incomplete_rank, _neg_open, _neg_activity, _head_last_success, wallet in hot_ready[:hot_target]:
            seen.add(wallet)
            head_candidates.append(wallet)

    refresh_target = 0
    if rescue_mode == "normal":
        refresh_target = max(0, int(round(max_tasks * (max(0, min(50, int(refresh_lane_pct or 0))) / 100.0))))
        refresh_target = min(refresh_target, max(0, max_tasks - len(intake_candidates) - len(trade_candidates) - len(funding_candidates)))
    if len(trade_candidates) + len(funding_candidates) == 0 and max_tasks > 0:
        refresh_target = max(1, min(max_tasks, max(1, int(round(max_tasks * 0.5)))))

    refresh_due_cutoff = now - max(5000, int(wallet_refresh_ms or WALLET_REFRESH_DEFAULT_MS))
    refresh_ready = []
    refresh_fallback = []
    for wallet in ordered_wallets:
        if wallet in seen:
            continue
        summary = summaries.get(wallet, default_summary())
        if not (summary["tradeExhausted"] and summary["fundingExhausted"]):
            continue
        last_attempt = int(summary.get("lastAttemptAt") or 0)
        last_success = int(summary.get("lastSuccessAt") or 0)
        freshness_ts = max(last_success, last_attempt)
        row = (
            0 if freshness_ts <= 0 else 1,
            freshness_ts if freshness_ts > 0 else 0,
            last_success,
            last_attempt,
            wallet,
        )
        refresh_fallback.append(row)
        if freshness_ts <= 0 or freshness_ts <= refresh_due_cutoff:
            refresh_ready.append(row)
    refresh_ready.sort(key=lambda item: (item[0], item[1], item[2], item[3], item[4]))
    if len(refresh_ready) < refresh_target:
        fallback_seen = {item[4] for item in refresh_ready}
        refresh_fallback.sort(key=lambda item: (item[0], item[1], item[2], item[3], item[4]))
        for row in refresh_fallback:
            if row[4] in fallback_seen:
                continue
            refresh_ready.append(row)
            fallback_seen.add(row[4])
            if len(refresh_ready) >= refresh_target:
                break
    for _freshness_unknown, _freshness_ts, _last_success, _last_attempt, wallet in refresh_ready[:refresh_target]:
        seen.add(wallet)
        refresh_candidates.append(wallet)

    return {
        "intake": intake_candidates,
        "trade": trade_candidates,
        "funding": funding_candidates,
        "head": head_candidates,
        "refresh": refresh_candidates,
    }, queue_cursor, {
        "intakeBacklog": len(intake_ready),
        "tradeStartedBacklog": len(trade_started),
        "fundingStartedBacklog": len(funding_started),
        "selected": len(intake_candidates) + len(trade_candidates) + len(funding_candidates) + len(head_candidates) + len(refresh_candidates),
        "tradeOnlyMode": not funding_allowed,
        "phaseMode": rescue_mode or "completion_first_backfill",
        "intakeLaneSize": len(intake_candidates),
        "completionLaneSize": len(funding_candidates),
        "tradeLaneSize": len(trade_candidates),
        "headLaneSize": len(head_candidates),
        "refreshLaneSize": len(refresh_candidates),
    }


def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--shard-count", type=int, default=DEFAULT_SHARD_COUNT)
    parser.add_argument("--proxy-file", default="")
    parser.add_argument("--wallet-concurrency", type=int, default=12)
    parser.add_argument("--active-batch-multiplier", type=int, default=8)
    parser.add_argument("--history-limit", type=int, default=240)
    parser.add_argument("--timeout-seconds", type=int, default=25)
    parser.add_argument("--request-attempts", type=int, default=2)
    parser.add_argument("--loop-sleep-seconds", type=int, default=1)
    parser.add_argument("--prioritize-first-trade", action="store_true")
    parser.add_argument("--completion-lane-pct", type=int, default=35)
    parser.add_argument("--intake-lane-pct", type=int, default=20)
    parser.add_argument("--intake-trade-page-budget", type=int, default=4)
    parser.add_argument("--trade-page-budget", type=int, default=12)
    parser.add_argument("--funding-page-budget", type=int, default=0)
    parser.add_argument("--flush-interval-pages", type=int, default=4)
    parser.add_argument("--wallet-refresh-ms", type=int, default=WALLET_REFRESH_DEFAULT_MS)
    parser.add_argument("--refresh-lane-pct", type=int, default=10)
    parser.add_argument("--refresh-trade-page-budget", type=int, default=2)
    parser.add_argument("--refresh-funding-page-budget", type=int, default=1)
    parser.add_argument("--head-history-limit", type=int, default=HEAD_HISTORY_LIMIT_DEFAULT)
    parser.add_argument("--head-timeout-seconds", type=int, default=HEAD_TIMEOUT_SECONDS_DEFAULT)
    parser.add_argument("--head-request-attempts", type=int, default=HEAD_REQUEST_ATTEMPTS_DEFAULT)
    parser.add_argument("--head-refresh-ms", type=int, default=HOT_HEAD_REFRESH_DEFAULT_MS)
    parser.add_argument("--once", action="store_true")
    return parser


def run_worker(args):
    args.shard_index = int(args.shard_index)
    args.shard_count = int(getattr(args, "shard_count", DEFAULT_SHARD_COUNT) or DEFAULT_SHARD_COUNT)
    args.wallet_concurrency = int(getattr(args, "wallet_concurrency", 12) or 12)
    args.active_batch_multiplier = int(getattr(args, "active_batch_multiplier", 8) or 8)
    args.history_limit = int(getattr(args, "history_limit", 240) or 240)
    args.timeout_seconds = int(getattr(args, "timeout_seconds", 25) or 25)
    args.request_attempts = int(getattr(args, "request_attempts", 2) or 2)
    args.loop_sleep_seconds = int(getattr(args, "loop_sleep_seconds", 1) or 1)
    args.completion_lane_pct = int(getattr(args, "completion_lane_pct", 35) or 35)
    args.intake_lane_pct = int(getattr(args, "intake_lane_pct", 20) or 20)
    args.intake_trade_page_budget = int(getattr(args, "intake_trade_page_budget", 4) or 4)
    args.trade_page_budget = int(getattr(args, "trade_page_budget", 12) or 12)
    args.funding_page_budget = int(getattr(args, "funding_page_budget", 0) or 0)
    args.flush_interval_pages = int(getattr(args, "flush_interval_pages", 4) or 4)
    args.wallet_refresh_ms = int(getattr(args, "wallet_refresh_ms", WALLET_REFRESH_DEFAULT_MS) or WALLET_REFRESH_DEFAULT_MS)
    args.refresh_lane_pct = int(getattr(args, "refresh_lane_pct", 10) or 10)
    args.refresh_trade_page_budget = int(getattr(args, "refresh_trade_page_budget", 2) or 2)
    args.refresh_funding_page_budget = int(getattr(args, "refresh_funding_page_budget", 1) or 1)
    args.head_history_limit = int(
        getattr(args, "head_history_limit", HEAD_HISTORY_LIMIT_DEFAULT)
        or HEAD_HISTORY_LIMIT_DEFAULT
    )
    args.head_timeout_seconds = int(
        getattr(args, "head_timeout_seconds", HEAD_TIMEOUT_SECONDS_DEFAULT)
        or HEAD_TIMEOUT_SECONDS_DEFAULT
    )
    args.head_request_attempts = int(
        getattr(args, "head_request_attempts", HEAD_REQUEST_ATTEMPTS_DEFAULT)
        or HEAD_REQUEST_ATTEMPTS_DEFAULT
    )
    args.head_refresh_ms = int(
        getattr(args, "head_refresh_ms", HOT_HEAD_REFRESH_DEFAULT_MS)
        or HOT_HEAD_REFRESH_DEFAULT_MS
    )
    args.prioritize_first_trade = bool(getattr(args, "prioritize_first_trade", False))
    args.once = bool(getattr(args, "once", False))

    wallets = load_wallets(args.shard_index, args.shard_count)
    proxy_pool = ProxyPool(load_proxies(args.proxy_file))
    discovery_hints = load_wallet_discovery_first_seen()
    hot_wallet_hints = load_hot_wallet_hints(args.shard_index, args.shard_count)
    summaries = load_initial_summaries(args.shard_index, wallets, discovery_hints)
    scheduler_state = load_scheduler_state(args.shard_index)
    last_wallet_refresh_at = 0

    while True:
        if now_ms() - last_wallet_refresh_at >= max(5000, int(args.wallet_refresh_ms or WALLET_REFRESH_DEFAULT_MS)):
            discovery_hints = load_wallet_discovery_first_seen()
            hot_wallet_hints = load_hot_wallet_hints(args.shard_index, args.shard_count)
            wallets, summaries = refresh_wallet_set(
                args.shard_index,
                args.shard_count,
                wallets,
                summaries,
                discovery_hints,
            )
            last_wallet_refresh_at = now_ms()
        scheduler_state["cycle"] = int(scheduler_state.get("cycle") or 0) + 1
        progress_snapshot = compute_progress_snapshot(wallets, summaries)
        stall_guard = update_stall_guard(scheduler_state, progress_snapshot)
        planning_multiplier = max(1, args.active_batch_multiplier)
        max_tasks = max(args.wallet_concurrency, args.wallet_concurrency * planning_multiplier)
        work_queues, next_cursor, cycle_meta = build_cycle_candidates(
            wallets,
            summaries,
            int(scheduler_state.get("queueCursor") or 0),
            max_tasks,
            hot_wallet_hints=hot_wallet_hints,
            prioritize_first_trade=args.prioritize_first_trade,
            completion_lane_pct=args.completion_lane_pct,
            rescue_mode=stall_guard.get("mode") or "normal",
            refresh_lane_pct=args.refresh_lane_pct,
            intake_lane_pct=args.intake_lane_pct,
            wallet_refresh_ms=args.wallet_refresh_ms,
            head_refresh_ms=args.head_refresh_ms,
        )
        scheduler_state["queueCursor"] = next_cursor
        save_scheduler_state(args.shard_index, scheduler_state)
        last_wallet = None
        intake_queue = deque(work_queues.get("intake") or [])
        trade_queue = deque(work_queues.get("trade") or [])
        funding_queue = deque(work_queues.get("funding") or [])
        head_queue = deque(work_queues.get("head") or [])
        refresh_queue = deque(work_queues.get("refresh") or [])

        if not intake_queue and not trade_queue and not funding_queue and not head_queue and not refresh_queue:
            write_progress(
                args.shard_index,
                args.shard_count,
                wallets,
                summaries,
                proxy_pool,
                last_wallet,
                scheduler_state,
                hot_wallet_hints=hot_wallet_hints,
                head_refresh_ms=args.head_refresh_ms,
            )
            complete_count = sum(
                1
                for wallet in wallets
                if summaries.get(wallet, default_summary())["tradeExhausted"]
                and summaries.get(wallet, default_summary())["fundingExhausted"]
            )
            print(
                f"[wallet-explorer-v2] shard={args.shard_index} cycle={scheduler_state['cycle']} selected=0 complete={complete_count}/{len(wallets)}",
                flush=True,
            )
            if args.once:
                return
            time.sleep(max(1, args.loop_sleep_seconds))
            continue

        submitted = 0
        completed = 0
        completion_pct = max(0, min(90, int(args.completion_lane_pct or 0)))
        trade_backlog = int(stall_guard.get("tradeBacklogWallets") or 0)
        funding_backlog = int(stall_guard.get("fundingBacklogWallets") or 0)
        funding_slot_target = 0
        head_slot_target = 0
        refresh_slot_target = 0
        intake_slot_target = 0
        if head_queue:
            head_slot_target = min(
                len(head_queue),
                max(1, int(round(args.wallet_concurrency * 0.5))),
            )
            if intake_queue:
                head_slot_target = min(
                    head_slot_target,
                    max(1, args.wallet_concurrency - 1),
                )
            elif trade_queue or funding_queue:
                head_slot_target = min(
                    head_slot_target,
                    max(1, args.wallet_concurrency - 1),
                )
        if intake_queue:
            intake_share = max(1, int(round(args.wallet_concurrency * max(0, min(50, int(args.intake_lane_pct or 0))) / 100.0)))
            intake_slot_target = min(intake_share, len(intake_queue), args.wallet_concurrency)
            if trade_queue or funding_queue:
                intake_slot_target = min(intake_slot_target, max(1, args.wallet_concurrency // 3))
            if stall_guard.get("mode") == "funding_rescue":
                intake_slot_target = min(intake_slot_target, 1 if args.wallet_concurrency > 1 else 0)
        active_concurrency = max(1, args.wallet_concurrency - intake_slot_target - head_slot_target) if (trade_queue or funding_queue) else max(0, args.wallet_concurrency - intake_slot_target - head_slot_target)
        if funding_queue:
            share = funding_backlog / max(1, trade_backlog + funding_backlog)
            desired_share = max(0.25, min(0.5, share + 0.1))
            funding_slot_target = max(1, int(round(max(1, active_concurrency) * desired_share)))
            funding_slot_target = min(funding_slot_target, active_concurrency, len(funding_queue))
        if stall_guard.get("mode") == "funding_rescue" and funding_queue:
            funding_slot_target = max(funding_slot_target, max(2, int(round(max(1, active_concurrency) * 0.65))))
            funding_slot_target = min(funding_slot_target, active_concurrency, len(funding_queue))
        trade_slot_target = max(0, active_concurrency - funding_slot_target)
        if stall_guard.get("mode") == "trade_rescue" and trade_queue:
            trade_floor = max(1, int(round(max(1, active_concurrency) * 0.8)))
            trade_slot_target = max(trade_slot_target, trade_floor)
            trade_slot_target = min(trade_slot_target, active_concurrency)
            funding_slot_target = max(1 if funding_queue else 0, active_concurrency - trade_slot_target)
        if trade_queue and trade_slot_target <= 0 and active_concurrency > 0:
            trade_slot_target = 1
            if funding_slot_target > 0:
                funding_slot_target = max(0, active_concurrency - trade_slot_target)
        if refresh_queue:
            refresh_slot_target = max(1, min(len(refresh_queue), max(1, int(round(args.wallet_concurrency * max(0, min(50, int(args.refresh_lane_pct or 0))) / 100.0)))))
            if intake_queue or head_queue or trade_queue or funding_queue:
                refresh_slot_target = min(refresh_slot_target, max(1, args.wallet_concurrency // 4))
            if head_queue:
                refresh_slot_target = min(refresh_slot_target, 0 if (trade_queue or funding_queue) else 1)
            if head_queue and len(head_queue) >= max(2, args.wallet_concurrency):
                refresh_slot_target = min(refresh_slot_target, 0 if (trade_queue or funding_queue) else 1)
            total_reserved = intake_slot_target + head_slot_target + trade_slot_target + funding_slot_target + refresh_slot_target
            if total_reserved > args.wallet_concurrency:
                overflow = total_reserved - args.wallet_concurrency
                if trade_slot_target > 1 and overflow > 0:
                    reduce_trade = min(overflow, trade_slot_target - 1)
                    trade_slot_target -= reduce_trade
                    overflow -= reduce_trade
                if funding_slot_target > (1 if funding_queue else 0) and overflow > 0:
                    reduce_funding = min(overflow, funding_slot_target - (1 if funding_queue else 0))
                    funding_slot_target -= reduce_funding
                    overflow -= reduce_funding
                if head_slot_target > (1 if head_queue else 0) and overflow > 0:
                    reduce_head = min(overflow, head_slot_target - (1 if head_queue else 0))
                    head_slot_target -= reduce_head
                    overflow -= reduce_head
                if intake_slot_target > (1 if intake_queue else 0) and overflow > 0:
                    reduce_intake = min(overflow, intake_slot_target - (1 if intake_queue else 0))
                    intake_slot_target -= reduce_intake
                    overflow -= reduce_intake
                if overflow > 0:
                    refresh_slot_target = max(0, refresh_slot_target - overflow)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.wallet_concurrency)) as executor:
            future_map = {}
            future_lane = {}
            intake_inflight = 0
            head_inflight = 0
            trade_inflight = 0
            funding_inflight = 0
            refresh_inflight = 0

            def submit_next():
                nonlocal submitted, intake_inflight, head_inflight, trade_inflight, funding_inflight, refresh_inflight
                lane = None
                wallet = None
                if refresh_queue and (refresh_inflight < refresh_slot_target and not funding_queue and not trade_queue and not intake_queue and not head_queue):
                    lane = "refresh"
                    wallet = refresh_queue.popleft()
                elif intake_queue and (intake_inflight < intake_slot_target or (not trade_queue and not funding_queue and not refresh_queue and not head_queue)):
                    lane = "intake"
                    wallet = intake_queue.popleft()
                elif head_queue and (head_inflight < head_slot_target or (not trade_queue and not funding_queue and not refresh_queue)):
                    lane = "head"
                    wallet = head_queue.popleft()
                elif funding_queue and (funding_inflight < funding_slot_target or (not trade_queue and not refresh_queue)):
                    lane = "funding"
                    wallet = funding_queue.popleft()
                elif trade_queue and (trade_inflight < trade_slot_target or (not funding_queue and not refresh_queue)):
                    lane = "trade"
                    wallet = trade_queue.popleft()
                elif refresh_queue and (refresh_inflight < refresh_slot_target or (not trade_queue and not funding_queue and not intake_queue)):
                    lane = "refresh"
                    wallet = refresh_queue.popleft()
                elif funding_queue:
                    lane = "funding"
                    wallet = funding_queue.popleft()
                elif intake_queue:
                    lane = "intake"
                    wallet = intake_queue.popleft()
                elif trade_queue:
                    lane = "trade"
                    wallet = trade_queue.popleft()
                elif refresh_queue:
                    lane = "refresh"
                    wallet = refresh_queue.popleft()
                if not wallet or not lane:
                    return False
                future = executor.submit(
                    process_wallet,
                    wallet,
                    args.shard_index,
                    args.history_limit,
                    args.timeout_seconds,
                    args.request_attempts,
                    args.head_history_limit,
                    args.head_timeout_seconds,
                    args.head_request_attempts,
                    proxy_pool,
                    lane,
                    discovery_hints.get(wallet),
                    args.intake_trade_page_budget,
                    args.trade_page_budget,
                    args.funding_page_budget,
                    args.flush_interval_pages,
                    args.refresh_trade_page_budget,
                    args.refresh_funding_page_budget,
                )
                future_map[future] = wallet
                future_lane[future] = lane
                submitted += 1
                if lane == "intake":
                    intake_inflight += 1
                elif lane == "head":
                    head_inflight += 1
                elif lane == "funding":
                    funding_inflight += 1
                elif lane == "refresh":
                    refresh_inflight += 1
                else:
                    trade_inflight += 1
                return True

            while len(future_map) < max(1, args.wallet_concurrency) and submit_next():
                pass

            last_progress_write = time.time()
            while future_map:
                done, _pending = concurrent.futures.wait(
                    list(future_map.keys()),
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in done:
                    wallet = future_map.pop(future)
                    lane = future_lane.pop(future, "trade")
                    if lane == "intake":
                        intake_inflight = max(0, intake_inflight - 1)
                    elif lane == "head":
                        head_inflight = max(0, head_inflight - 1)
                    elif lane == "funding":
                        funding_inflight = max(0, funding_inflight - 1)
                    elif lane == "refresh":
                        refresh_inflight = max(0, refresh_inflight - 1)
                    else:
                        trade_inflight = max(0, trade_inflight - 1)
                    completed += 1
                    last_wallet = wallet
                    try:
                        result = future.result()
                    except Exception:
                        result = None
                    if isinstance(result, dict) and isinstance(result.get("summary"), dict):
                        summaries[wallet] = result["summary"]
                    else:
                        summaries[wallet] = summarize_wallet_state(
                            load_wallet_state(args.shard_index, wallet),
                            load_wallet_record(args.shard_index, wallet),
                            discovery_hints.get(wallet),
                        )
                    while len(future_map) < max(1, args.wallet_concurrency) and submit_next():
                        pass
                    if time.time() - last_progress_write >= 2:
                        write_progress(
                            args.shard_index,
                            args.shard_count,
                            wallets,
                            summaries,
                            proxy_pool,
                            last_wallet,
                            scheduler_state,
                            hot_wallet_hints=hot_wallet_hints,
                            head_refresh_ms=args.head_refresh_ms,
                        )
                        last_progress_write = time.time()

        write_progress(
            args.shard_index,
            args.shard_count,
            wallets,
            summaries,
            proxy_pool,
            last_wallet,
            scheduler_state,
            hot_wallet_hints=hot_wallet_hints,
            head_refresh_ms=args.head_refresh_ms,
        )
        complete_count = sum(
            1
            for wallet in wallets
            if summaries.get(wallet, default_summary())["tradeExhausted"]
            and summaries.get(wallet, default_summary())["fundingExhausted"]
        )
        print(
            f"[wallet-explorer-v2] shard={args.shard_index} cycle={scheduler_state['cycle']} phase={cycle_meta['phaseMode']} submitted={submitted} completed={completed} selected={cycle_meta['selected']} planned_batch={max_tasks} intake_lane={cycle_meta['intakeLaneSize']} trade_lane={cycle_meta['tradeLaneSize']} completion_lane={cycle_meta['completionLaneSize']} head_lane={cycle_meta['headLaneSize']} refresh_lane={cycle_meta['refreshLaneSize']} intake_slots={intake_slot_target} head_slots={head_slot_target} trade_slots={trade_slot_target} funding_slots={funding_slot_target} refresh_slots={refresh_slot_target} intake_backlog={cycle_meta['intakeBacklog']} trade_started={cycle_meta['tradeStartedBacklog']} funding_started={cycle_meta['fundingStartedBacklog']} trade_only={str(cycle_meta['tradeOnlyMode']).lower()} stall_mode={stall_guard.get('mode')} blocked_on={stall_guard.get('blockedOn') or 'none'} complete={complete_count}/{len(wallets)}",
            flush=True,
        )
        if args.once:
            return
        time.sleep(max(1, args.loop_sleep_seconds))


def main():
    parser = build_parser()
    args = parser.parse_args()
    run_worker(args)


if __name__ == "__main__":
    main()
