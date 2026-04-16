#!/usr/bin/env python3
import argparse
import csv
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from common import (
    API_BASE,
    ProxyPool,
    aggregate_trade_metrics,
    backoff_seconds,
    build_canonical_wallet_record,
    phase1_probe_path,
    funding_row_key,
    initial_wallet_history,
    initial_wallet_state,
    isolated_wallet_dir,
    isolated_wallet_history_log_path,
    isolated_wallet_history_path,
    isolated_wallet_record_path,
    isolated_wallet_state_path,
    latest_opened_at_from_history,
    load_proxies,
    normalize_funding_row,
    normalize_trade_row,
    normalize_wallet,
    now_ms,
    read_json,
    request_endpoint,
    shard_index_for_wallet,
    trade_row_key,
    wallet_history_path,
    wallet_record_path,
    wallet_state_path,
    write_json_atomic,
)


def read_phase1_probe_state(wallet: str) -> dict | None:
    payload = read_json(phase1_probe_path(wallet), None)
    return payload if isinstance(payload, dict) else None


def write_phase1_probe_state(wallet: str, payload: dict) -> None:
    data = payload if isinstance(payload, dict) else {}
    data["wallet"] = wallet
    data["updatedAt"] = now_ms()
    write_json_atomic(phase1_probe_path(wallet), data)


def clear_phase1_probe_state(wallet: str) -> None:
    path = phase1_probe_path(wallet)
    if path.exists():
        path.unlink()


def load_focus_wallets_from_csv(path_value: str) -> set[str]:
    path_text = str(path_value or "").strip()
    if not path_text:
        return set()
    path = Path(path_text)
    if not path.exists():
        return set()
    wallets = set()
    try:
        with path.open(newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames and "wallet" in reader.fieldnames:
                for row in reader:
                    wallet = normalize_wallet((row or {}).get("wallet"))
                    if wallet:
                        wallets.add(wallet)
            else:
                handle.seek(0)
                for line in handle:
                    wallet = normalize_wallet(str(line or "").split(",")[0])
                    if wallet and wallet.lower() != "wallet":
                        wallets.add(wallet)
    except Exception:
        return set()
    return wallets


def record_has_empty_target_metrics(record: dict | None) -> bool:
    safe = record if isinstance(record, dict) else {}
    return (
        int(safe.get("tradeRowsLoaded") or safe.get("trades") or 0) <= 0
        and float(safe.get("volumeUsdRaw") or 0.0) == 0.0
        and float(safe.get("pnlUsd") or safe.get("totalPnlUsd") or 0.0) == 0.0
        and float(safe.get("feesPaidUsd") or safe.get("feesUsd") or 0.0) == 0.0
        and int(safe.get("totalWins") or safe.get("wins") or 0) <= 0
        and int(safe.get("totalLosses") or safe.get("losses") or 0) <= 0
    )


def force_metric_recovery_mode(state: dict) -> None:
    streams = state.setdefault("streams", {})
    for stream_name in ("trades", "funding"):
        stream = streams.setdefault(stream_name, {})
        stream["exhausted"] = False
        stream["frontierCursor"] = None
        stream["failedCursor"] = None
        stream["nextEligibleAt"] = 0
        stream["retryCount"] = 0
        stream["lastError"] = None
    state["status"] = "metric_recovery"


def load_local_owner_truth(wallet: str, shard_count: int) -> dict:
    shard_index = shard_index_for_wallet(wallet, shard_count)
    state = read_json(wallet_state_path(shard_index, wallet), None)
    history = read_json(wallet_history_path(shard_index, wallet), None)
    record = read_json(wallet_record_path(shard_index, wallet), None)
    return {
        "ownerShardIndex": shard_index,
        "state": state,
        "history": history,
        "record": record,
    }


def state_score(state: dict | None, history: dict | None) -> tuple:
    state = state if isinstance(state, dict) else {}
    history = history if isinstance(history, dict) else {}
    streams = state.get("streams") or {}
    trades = streams.get("trades") or {}
    funding = streams.get("funding") or {}
    trade_rows = len(history.get("trades") or [])
    funding_rows = len(history.get("funding") or [])
    return (
        1 if trades.get("exhausted") else 0,
        1 if funding.get("exhausted") else 0,
        trade_rows,
        funding_rows,
        int(trades.get("pagesFetched") or 0),
        int(funding.get("pagesFetched") or 0),
        int(state.get("updatedAt") or 0),
        int(history.get("updatedAt") or 0),
    )


def clone_or_default_state(wallet: str, shard_count: int, payload: dict | None) -> dict:
    if isinstance(payload, dict):
        out = json.loads(json.dumps(payload))
        out["wallet"] = wallet
        out["shardIndex"] = shard_index_for_wallet(wallet, shard_count)
        return out
    return initial_wallet_state(wallet, shard_index_for_wallet(wallet, shard_count))


def clone_or_default_history(wallet: str, shard_count: int, payload: dict | None) -> dict:
    if isinstance(payload, dict):
        out = json.loads(json.dumps(payload))
        out["wallet"] = wallet
        out["shardIndex"] = shard_index_for_wallet(wallet, shard_count)
        return out
    return initial_wallet_history(wallet, shard_index_for_wallet(wallet, shard_count))


def init_isolated(wallet: str, shard_count: int, local_truth: dict | None = None) -> tuple[dict, dict]:
    local_truth = local_truth if isinstance(local_truth, dict) else {}
    local_state = local_truth.get("state")
    local_history = local_truth.get("history")
    isolated_state = read_json(isolated_wallet_state_path(wallet), None)
    isolated_history = read_json(isolated_wallet_history_path(wallet), None)

    candidates = [
        (isolated_state, isolated_history),
        (local_state, local_history),
    ]
    best_state = None
    best_history = None
    best_score = None
    for state, history in candidates:
        score = state_score(state, history)
        if best_score is None or score > best_score:
            best_score = score
            best_state = state
            best_history = history

    state = clone_or_default_state(wallet, shard_count, best_state)
    history = clone_or_default_history(wallet, shard_count, best_history)
    state["status"] = "isolated_collecting"
    return state, history


def persist_isolated(wallet: str, state: dict, history: dict) -> dict:
    state["last_opened_at"] = latest_opened_at_from_history(history)
    record = build_canonical_wallet_record(wallet, state.get("shardIndex") or 0, state, history)
    write_json_atomic(isolated_wallet_state_path(wallet), state)
    write_json_atomic(isolated_wallet_history_path(wallet), history)
    write_json_atomic(isolated_wallet_record_path(wallet), record)
    log_path = isolated_wallet_history_log_path(wallet)
    if log_path.exists():
        log_path.unlink()
    return record


def persist_isolated_state_history(wallet: str, state: dict, history: dict) -> None:
    state["last_opened_at"] = latest_opened_at_from_history(history)
    state["updatedAt"] = now_ms()
    history["updatedAt"] = now_ms()
    write_json_atomic(isolated_wallet_state_path(wallet), state)
    write_json_atomic(isolated_wallet_history_path(wallet), history)


def persist_isolated_state_only(wallet: str, state: dict) -> None:
    state["updatedAt"] = now_ms()
    write_json_atomic(isolated_wallet_state_path(wallet), state)


def persist_isolated_record_only(wallet: str, state: dict, history: dict) -> dict:
    state["last_opened_at"] = latest_opened_at_from_history(history)
    state["updatedAt"] = now_ms()
    record = build_canonical_wallet_record(wallet, state.get("shardIndex") or 0, state, history)
    write_json_atomic(isolated_wallet_state_path(wallet), state)
    write_json_atomic(isolated_wallet_record_path(wallet), record)
    return record


def latest_history_timestamp(rows: list[dict], key: str = "createdAt") -> int:
    latest = 0
    for row in rows or []:
        value = int((row or {}).get(key) or 0)
        if value > latest:
            latest = value
    return latest


def build_local_history_proof(state: dict, history: dict, record: dict) -> dict:
    streams = (state or {}).get("streams") or {}
    trade_stream = streams.get("trades") or {}
    funding_stream = streams.get("funding") or {}
    trades = list((history or {}).get("trades") or [])
    funding = list((history or {}).get("funding") or [])
    trade_rows_loaded = len(trades)
    funding_rows_loaded = len(funding)
    record_trade_rows = int((record or {}).get("tradeRowsLoaded") or 0)
    record_funding_rows = int((record or {}).get("fundingRowsLoaded") or 0)
    now = now_ms()
    retry_pending = any(
        not stream.get("exhausted") and int(stream.get("nextEligibleAt") or 0) > now
        for stream in (trade_stream, funding_stream)
    ) or bool(trade_stream.get("failedCursor")) or bool(funding_stream.get("failedCursor"))
    trade_frontier_clear = not bool(trade_stream.get("frontierCursor")) and not bool(trade_stream.get("failedCursor"))
    funding_frontier_clear = not bool(funding_stream.get("frontierCursor")) and not bool(funding_stream.get("failedCursor"))
    local_complete = (
        bool(trade_stream.get("exhausted"))
        and bool(funding_stream.get("exhausted"))
        and trade_frontier_clear
        and funding_frontier_clear
        and not retry_pending
        and bool((record or {}).get("backfillComplete"))
        and record_trade_rows == trade_rows_loaded
        and record_funding_rows == funding_rows_loaded
    )
    return {
        "checkedAt": now,
        "filesPresent": {
            "state": True,
            "history": True,
            "record": True,
        },
        "tradeExhausted": bool(trade_stream.get("exhausted")),
        "fundingExhausted": bool(funding_stream.get("exhausted")),
        "tradeFrontierClear": trade_frontier_clear,
        "fundingFrontierClear": funding_frontier_clear,
        "retryClear": not retry_pending,
        "recordBackfillComplete": bool((record or {}).get("backfillComplete")),
        "tradeRowsLoaded": trade_rows_loaded,
        "fundingRowsLoaded": funding_rows_loaded,
        "recordTradeRowsLoaded": record_trade_rows,
        "recordFundingRowsLoaded": record_funding_rows,
        "tradeRowsMatch": record_trade_rows == trade_rows_loaded,
        "fundingRowsMatch": record_funding_rows == funding_rows_loaded,
        "lastTradeAt": int((record or {}).get("lastTrade") or latest_history_timestamp(trades)) or None,
        "lastFundingAt": int((record or {}).get("lastFundingAt") or latest_history_timestamp(funding)) or None,
        "complete": local_complete,
    }


def verify_head_stream(
    wallet: str,
    stream_name: str,
    stored_last_at: int,
    proxy_pool: ProxyPool,
    timeout_seconds: int,
    request_attempts: int,
) -> dict:
    endpoint = "trades/history" if stream_name == "trades" else "funding/history"
    normalize = normalize_trade_row if stream_name == "trades" else normalize_funding_row
    head_limit = max(
        1,
        int(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_HEAD_PROOF_LIMIT", "1") or 1),
    )
    try:
        payload, _entry = request_endpoint(
            wallet,
            endpoint,
            head_limit,
            None,
            proxy_pool,
            timeout_seconds=timeout_seconds,
            attempts=request_attempts,
        )
        rows = payload.get("data") or []
        normalized_rows = [normalize(row) for row in rows]
        remote_last_at = latest_history_timestamp(normalized_rows)
        if remote_last_at <= 0 and stored_last_at <= 0:
            status = "verified"
        elif remote_last_at <= 0 and stored_last_at > 0:
            status = "remote_empty"
        elif remote_last_at <= stored_last_at:
            status = "verified"
        else:
            status = "remote_newer"
        return {
            "status": status,
            "storedLastAt": stored_last_at or None,
            "remoteLastAt": remote_last_at or None,
            "hasMore": bool(payload.get("has_more")),
            "rowCount": len(normalized_rows),
            "error": None,
        }
    except Exception as exc:
        return {
            "status": "error",
            "storedLastAt": stored_last_at or None,
            "remoteLastAt": None,
            "hasMore": None,
            "rowCount": 0,
            "error": str(exc),
        }


def attach_history_proof(
    wallet: str,
    state: dict,
    history: dict,
    record: dict,
    proxy_pool: ProxyPool,
    timeout_seconds: int,
    request_attempts: int,
) -> dict:
    local_proof = build_local_history_proof(state, history, record)
    head_checked_at = now_ms()
    if local_proof["complete"]:
        # Trade-head and funding-head checks are independent. Run them together
        # so verified wallets promote faster without weakening the proof gate.
        with ThreadPoolExecutor(max_workers=2) as executor:
            trade_future = executor.submit(
                verify_head_stream,
                wallet,
                "trades",
                int(local_proof.get("lastTradeAt") or 0),
                proxy_pool,
                timeout_seconds,
                request_attempts,
            )
            funding_future = executor.submit(
                verify_head_stream,
                wallet,
                "funding",
                int(local_proof.get("lastFundingAt") or 0),
                proxy_pool,
                timeout_seconds,
                request_attempts,
            )
            trade_head = trade_future.result()
            funding_head = funding_future.result()
    else:
        trade_head = {
            "status": "skipped_local_incomplete",
            "storedLastAt": local_proof.get("lastTradeAt"),
            "remoteLastAt": None,
            "hasMore": None,
            "rowCount": 0,
            "error": None,
        }
        funding_head = {
            "status": "skipped_local_incomplete",
            "storedLastAt": local_proof.get("lastFundingAt"),
            "remoteLastAt": None,
            "hasMore": None,
            "rowCount": 0,
            "error": None,
        }
    head_complete = trade_head.get("status") == "verified" and funding_head.get("status") == "verified"
    proof_complete = bool(local_proof.get("complete")) and head_complete
    if proof_complete:
        history_proof_status = "verified_through_now"
    elif local_proof.get("complete"):
        if trade_head.get("status") == "error" or funding_head.get("status") == "error":
            history_proof_status = "head_check_error"
        elif trade_head.get("status") == "remote_newer" or funding_head.get("status") == "remote_newer":
            history_proof_status = "head_mismatch_remote_newer"
        elif trade_head.get("status") == "remote_empty" or funding_head.get("status") == "remote_empty":
            history_proof_status = "head_mismatch_remote_empty"
        else:
            history_proof_status = "local_complete_head_pending"
    else:
        history_proof_status = "backfill_incomplete"
    record["lastFundingAt"] = local_proof.get("lastFundingAt")
    record["proof"] = {
        "local": local_proof,
        "head": {
            "checkedAt": head_checked_at,
            "complete": head_complete,
            "trades": trade_head,
            "funding": funding_head,
        },
        "complete": proof_complete,
    }
    record["historyProofStatus"] = history_proof_status
    record["historyVerifiedThroughNow"] = proof_complete
    write_json_atomic(isolated_wallet_record_path(wallet), record)
    return record


def attach_phase1_metric_proof(
    wallet: str,
    state: dict,
    history: dict,
    record: dict,
    proxy_pool: ProxyPool,
    timeout_seconds: int,
    request_attempts: int,
    precomputed_trade_head: dict | None = None,
) -> dict:
    trades = history.get("trades") or []
    trade_stream = ((state or {}).get("streams") or {}).get("trades") or {}
    record_trade_rows = int((record or {}).get("tradeRowsLoaded") or 0)
    trade_rows_loaded = len(trades)
    trade_frontier_clear = not bool(trade_stream.get("frontierCursor")) and not bool(
        trade_stream.get("failedCursor")
    )
    retry_pending = (
        (not trade_stream.get("exhausted") and int(trade_stream.get("nextEligibleAt") or 0) > now_ms())
        or bool(trade_stream.get("failedCursor"))
    )
    local_complete = (
        bool(trade_stream.get("exhausted"))
        and trade_frontier_clear
        and not retry_pending
        and record_trade_rows == trade_rows_loaded
    )
    stored_last_trade = int((record or {}).get("lastTrade") or latest_history_timestamp(trades) or 0)
    trade_head = (
        precomputed_trade_head
        if isinstance(precomputed_trade_head, dict)
        else verify_head_stream(
            wallet,
            "trades",
            stored_last_trade,
            proxy_pool,
            timeout_seconds,
            request_attempts,
        )
    )
    head_complete = trade_head.get("status") == "verified"
    zero_history_result = bool(
        local_complete
        and trade_rows_loaded <= 0
        and record_trade_rows <= 0
        and stored_last_trade <= 0
        and record_has_empty_target_metrics(record)
    )
    metrics_recovered = bool(local_complete and not zero_history_result and not record_has_empty_target_metrics(record))
    verified_through_now = bool(local_complete and head_complete and metrics_recovered)
    existing_phase1 = record.get("phase1") if isinstance(record.get("phase1"), dict) else {}
    head_retry_count = int((((existing_phase1.get("head") if isinstance(existing_phase1.get("head"), dict) else {}) or {}).get("retryCount")) or 0)
    next_eligible_at = 0
    if verified_through_now:
        status = "phase1_verified_through_now"
        head_retry_count = 0
    elif zero_history_result and head_complete:
        status = "phase1_zero_history_verified"
        head_retry_count = 0
    elif zero_history_result and local_complete:
        status = "phase1_zero_history_local_complete"
        head_retry_count = 0
    elif local_complete:
        if trade_head.get("status") == "error":
            status = "phase1_head_check_error"
            head_retry_count += 1
            next_eligible_at = now_ms() + backoff_seconds(head_retry_count, str(trade_head.get("error") or "error")) * 1000
        elif trade_head.get("status") == "remote_newer":
            status = "phase1_head_mismatch_remote_newer"
            next_eligible_at = now_ms() + 15_000
        elif trade_head.get("status") == "remote_empty":
            status = "phase1_head_mismatch_remote_empty"
            next_eligible_at = now_ms() + 15_000
        else:
            status = "phase1_local_complete_head_pending"
            head_retry_count = 0
    else:
        status = "phase1_trade_backfill_incomplete"
        head_retry_count = 0
    record["nextEligibleAt"] = int(next_eligible_at or 0)
    record["phase1"] = {
        "mode": "target_metrics_trades_only",
        "targetFields": ["volume", "totalTrades", "wins", "losses", "pnl", "fees"],
        "checkedAt": now_ms(),
        "tradeRowsLoaded": trade_rows_loaded,
        "storedLastTrade": stored_last_trade or None,
        "localComplete": bool(local_complete),
        "zeroHistoryResult": bool(zero_history_result),
        "metricsRecovered": bool(metrics_recovered),
        "verifiedThroughNow": bool(verified_through_now),
        "status": status,
        "head": {
            "checkedAt": now_ms(),
            "complete": bool(head_complete),
            "retryCount": int(head_retry_count or 0),
            "nextEligibleAt": int(next_eligible_at or 0) or None,
            "trades": trade_head,
        },
    }
    write_json_atomic(isolated_wallet_record_path(wallet), record)
    return record


def phase1_prime_trade_probe(
    wallet: str,
    state: dict,
    history: dict,
    proxy_pool: ProxyPool,
    history_limit: int,
    timeout_seconds: int,
    request_attempts: int,
) -> dict | None:
    stream_state = ((state.get("streams") or {}).get("trades")) or {}
    history["trades"], seen_rows = prepare_history_rows(history.get("trades") or [], "trades")
    probe_limit = max(
        10,
        min(
            int(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_PHASE1_PROBE_LIMIT", "25") or 25),
            int(history_limit or 240),
        ),
    )
    payload, _entry = request_endpoint(
        wallet,
        "trades/history",
        probe_limit,
        None,
        proxy_pool,
        timeout_seconds=timeout_seconds,
        attempts=request_attempts,
    )
    rows = payload.get("data") or []
    normalized_rows = [normalize_trade_row(row) for row in rows]
    added_count, added_rows = append_unique_rows(history["trades"], normalized_rows, "trades", seen_rows)
    if added_rows:
        append_history_log(wallet, "trades", added_rows)
    has_more = bool(payload.get("has_more"))
    next_cursor = payload.get("next_cursor")
    checked_at = now_ms()
    stream_state["pagesFetched"] = int(stream_state.get("pagesFetched") or 0) + 1
    stream_state["rowsFetched"] = len(history["trades"])
    stream_state["lastAttemptAt"] = checked_at
    stream_state["lastSuccessAt"] = checked_at
    stream_state["frontierCursor"] = next_cursor if has_more else None
    stream_state["failedCursor"] = None
    stream_state["retryCount"] = 0
    stream_state["nextEligibleAt"] = 0
    stream_state["lastError"] = None
    stream_state["lastPageRowCount"] = len(normalized_rows)
    stream_state["exhausted"] = not has_more
    stream_state["currentLimit"] = probe_limit
    stream_state["successfulPagesAtCurrentLimit"] = 0
    append_step(
        stream_state,
        {
            "status": "persisted",
            "cursor": None,
            "nextCursor": next_cursor,
            "limit": probe_limit,
            "rowCount": len(normalized_rows),
            "addedRows": added_count,
            "fetchedAt": checked_at,
            "probe": True,
        },
    )
    return {
        "rows": normalized_rows,
        "hasMore": has_more,
        "nextCursor": next_cursor,
        "remoteLastAt": latest_history_timestamp(normalized_rows),
        "checkedAt": checked_at,
        "probeLimit": probe_limit,
    }


class RetryLater(Exception):
    pass


def finalize_phase1_zero_wallet(
    wallet: str,
    shard_count: int,
    probe: dict,
    timeout_seconds: int,
    request_attempts: int,
    proxy_pool: ProxyPool,
) -> dict:
    state = initial_wallet_state(wallet, shard_index_for_wallet(wallet, shard_count))
    history = initial_wallet_history(wallet, shard_index_for_wallet(wallet, shard_count))
    checked_at = int((probe or {}).get("checkedAt") or now_ms())
    probe_limit = int((probe or {}).get("probeLimit") or 25)
    trade_stream = ((state.get("streams") or {}).get("trades")) or {}
    trade_stream["pagesFetched"] = 1
    trade_stream["rowsFetched"] = 0
    trade_stream["lastAttemptAt"] = checked_at
    trade_stream["lastSuccessAt"] = checked_at
    trade_stream["frontierCursor"] = None
    trade_stream["failedCursor"] = None
    trade_stream["retryCount"] = 0
    trade_stream["nextEligibleAt"] = 0
    trade_stream["lastError"] = None
    trade_stream["lastPageRowCount"] = 0
    trade_stream["exhausted"] = True
    trade_stream["currentLimit"] = probe_limit
    trade_stream["successfulPagesAtCurrentLimit"] = 0
    funding_stream = ((state.get("streams") or {}).get("funding")) or {}
    funding_stream["pagesFetched"] = 0
    funding_stream["rowsFetched"] = 0
    funding_stream["lastAttemptAt"] = None
    funding_stream["lastSuccessAt"] = None
    funding_stream["frontierCursor"] = None
    funding_stream["failedCursor"] = None
    funding_stream["retryCount"] = 0
    funding_stream["nextEligibleAt"] = 0
    funding_stream["lastError"] = None
    funding_stream["lastPageRowCount"] = 0
    funding_stream["exhausted"] = False
    state["updatedAt"] = checked_at
    history["updatedAt"] = checked_at
    state["status"] = "phase1_probe_empty_observed"
    record = persist_isolated(wallet, state, history)
    record = attach_phase1_metric_proof(
        wallet,
        state,
        history,
        record,
        proxy_pool,
        timeout_seconds,
        request_attempts,
        {
            "status": "verified",
            "storedLastAt": None,
            "remoteLastAt": None,
            "hasMore": False,
            "rowCount": 0,
            "error": None,
        },
    )
    clear_phase1_probe_state(wallet)
    return record


def append_history_log(wallet: str, stream_name: str, rows: list[dict]) -> None:
    if not rows:
        return
    path = isolated_wallet_history_log_path(wallet)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as handle:
        for row in rows:
            handle.write(json.dumps({"stream": stream_name, "row": row}, separators=(",", ":")))
            handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def load_effective_isolated_history(wallet: str, fallback_history: dict | None = None) -> dict:
    history = fallback_history if isinstance(fallback_history, dict) else read_json(isolated_wallet_history_path(wallet), {}) or {}
    if not isinstance(history, dict):
        history = {}
    history.setdefault("trades", [])
    history.setdefault("funding", [])
    history["trades"], trade_seen = prepare_history_rows(history.get("trades") or [], "trades")
    history["funding"], funding_seen = prepare_history_rows(history.get("funding") or [], "funding")
    log_path = isolated_wallet_history_log_path(wallet)
    if not log_path.exists():
        return history
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
                    append_unique_rows(history["trades"], [row], "trades", trade_seen)
                elif stream_name == "funding":
                    append_unique_rows(history["funding"], [row], "funding", funding_seen)
    except FileNotFoundError:
        return history
    return history


def append_step(stream_state: dict, step: dict) -> None:
    steps = list(stream_state.get("steps") or [])
    steps.append(step)
    if len(steps) > 2048:
        steps = steps[-2048:]
    stream_state["steps"] = steps


def prepare_history_rows(existing_rows, stream_name: str):
    if stream_name == "trades":
        normalize = normalize_trade_row
        key_fn = trade_row_key
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


def append_unique_rows(existing_rows, incoming_rows, stream_name: str, seen: set):
    if stream_name == "trades":
        normalize = normalize_trade_row
        key_fn = trade_row_key
    else:
        normalize = normalize_funding_row
        key_fn = funding_row_key
    added = 0
    added_rows = []
    for row in incoming_rows or []:
        normalized = normalize(row)
        key = key_fn(normalized)
        if key in seen:
            continue
        seen.add(key)
        existing_rows.append(normalized)
        added += 1
        added_rows.append(normalized)
    return added, added_rows


def collect_stream_exhaustive(
    wallet: str,
    state: dict,
    history: dict,
    stream_name: str,
    limit: int,
    timeout_seconds: int,
    request_attempts: int,
    proxy_pool: ProxyPool,
    flush_interval_pages: int,
    record_flush_pages: int,
    refresh_page_budget: int,
):
    endpoint = "trades/history" if stream_name == "trades" else "funding/history"
    history_key = "trades" if stream_name == "trades" else "funding"
    stream_state = ((state.get("streams") or {}).get(stream_name)) or {}
    refresh_mode = False
    refresh_pages_remaining = max(1, int(refresh_page_budget or 1))
    if bool(stream_state.get("exhausted")) and not stream_state.get("failedCursor") and not stream_state.get("frontierCursor"):
        # Completed isolated wallets still need a small head refresh so recent
        # trades/funding continue to propagate into Wallet Explorer.
        refresh_mode = True
    cursor = stream_state.get("failedCursor") or stream_state.get("frontierCursor") or None
    min_limit = max(
        25,
        int(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_MIN_PAGE_LIMIT", "240") or 240),
    )
    max_limit = max(min_limit, int(limit or min_limit))
    current_limit = int(stream_state.get("currentLimit") or max_limit)
    current_limit = max(min_limit, min(max_limit, current_limit))
    growth_successes = int(stream_state.get("successfulPagesAtCurrentLimit") or 0)
    consecutive_failures = 0
    page_counter = 0
    pages_since_flush = 0
    pages_since_record = 0
    start_ms = now_ms()
    history[history_key], seen_rows = prepare_history_rows(history.get(history_key) or [], stream_name)

    while True:
        stream_state["lastAttemptAt"] = now_ms()
        try:
            payload, entry = request_endpoint(
                wallet,
                endpoint,
                current_limit,
                cursor,
                proxy_pool,
                timeout_seconds=timeout_seconds,
                attempts=request_attempts,
            )
            consecutive_failures = 0
            rows = payload.get("data") or []
            next_cursor = payload.get("next_cursor")
            has_more = bool(payload.get("has_more"))
            added_count, added_rows = append_unique_rows(history[history_key], rows, stream_name, seen_rows)
            if added_rows:
                append_history_log(wallet, stream_name, added_rows)
            if current_limit < max_limit and len(rows) >= current_limit:
                growth_successes += 1
                if growth_successes >= 3:
                    current_limit = min(max_limit, current_limit * 2)
                    growth_successes = 0
            else:
                growth_successes = 0
            page_counter += 1
            pages_since_flush += 1
            pages_since_record += 1
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
            stream_state["currentLimit"] = current_limit
            stream_state["successfulPagesAtCurrentLimit"] = growth_successes
            append_step(
                stream_state,
                {
                    "status": "persisted",
                    "cursor": cursor,
                    "nextCursor": next_cursor,
                    "limit": current_limit,
                    "rowCount": len(rows),
                    "addedRows": added_count,
                    "fetchedAt": now_ms(),
                    "proxyId": entry.get("id"),
                },
            )
            if pages_since_flush >= max(1, int(flush_interval_pages or 1)) or not has_more:
                persist_isolated_state_only(wallet, state)
                pages_since_flush = 0
            record = None
            if ((added_count > 0) and (page_counter == 1 or pages_since_record >= max(1, int(record_flush_pages or 1)))) or not has_more:
                record = persist_isolated_record_only(wallet, state, history)
                pages_since_record = 0
            elapsed_s = max(1.0, (now_ms() - start_ms) / 1000.0)
            if page_counter == 1 or page_counter % 25 == 0 or not has_more:
                print(
                    json.dumps(
                        {
                            "wallet": wallet,
                            "stream": stream_name,
                            "pagesFetched": stream_state["pagesFetched"],
                            "rowsFetched": len(history[history_key]),
                            "limit": current_limit,
                            "hasMore": has_more,
                            "frontierCursor": stream_state["frontierCursor"],
                            "rowsPerSec": round(len(history[history_key]) / elapsed_s, 2),
                            "volumeUsd": (record or {}).get("volumeUsdRaw") if history_key == "trades" else None,
                        },
                        separators=(",", ":"),
                    ),
                    flush=True,
                )
            if not has_more or not next_cursor:
                stream_state["exhausted"] = True
                stream_state["frontierCursor"] = None
                stream_state["failedCursor"] = None
                stream_state["nextEligibleAt"] = 0
                stream_state["retryCount"] = 0
                persist_isolated(wallet, state, history)
                return
            if refresh_mode:
                refresh_pages_remaining -= 1
                if refresh_pages_remaining <= 0:
                    stream_state["exhausted"] = True
                    stream_state["frontierCursor"] = None
                    stream_state["failedCursor"] = None
                    stream_state["nextEligibleAt"] = 0
                    stream_state["retryCount"] = 0
                    persist_isolated(wallet, state, history)
                    return
            cursor = next_cursor
        except Exception as exc:
            consecutive_failures += 1
            reason = str(exc)
            reason_lower = reason.lower()
            if refresh_mode:
                stream_state["lastAttemptAt"] = now_ms()
                stream_state["lastError"] = reason
                stream_state["nextEligibleAt"] = now_ms() + backoff_seconds(1, reason) * 1000
                stream_state["failedCursor"] = None
                stream_state["frontierCursor"] = None
                stream_state["retryCount"] = 1
                stream_state["exhausted"] = True
                persist_isolated_state_only(wallet, state)
                return
            prior_limit = current_limit
            if current_limit > min_limit and (
                "http_429" in reason_lower
                or "http_503" in reason_lower
                or "http_504" in reason_lower
                or "http_502" in reason_lower
                or "timeout" in reason_lower
                or "curl_failed" in reason_lower
            ):
                current_limit = max(min_limit, current_limit // 2)
                growth_successes = 0
            limit_reduced = current_limit < prior_limit
            next_retry_count = int(stream_state.get("retryCount") or 0) + 1
            if limit_reduced:
                next_retry_count = 1
            stream_state["retryCount"] = next_retry_count
            stream_state["failedCursor"] = cursor
            stream_state["lastError"] = reason
            stream_state["currentLimit"] = current_limit
            stream_state["successfulPagesAtCurrentLimit"] = growth_successes
            wait_seconds = backoff_seconds(stream_state["retryCount"], reason)
            if limit_reduced:
                wait_seconds = min(wait_seconds, 8)
            stream_state["nextEligibleAt"] = now_ms() + wait_seconds * 1000
            stream_state["exhausted"] = False
            append_step(
                stream_state,
                {
                    "status": "failed",
                    "cursor": cursor,
                    "nextCursor": None,
                    "limit": current_limit,
                    "rowCount": 0,
                    "fetchedAt": now_ms(),
                    "error": reason,
                    "waitSeconds": wait_seconds,
                },
            )
            persist_isolated_state_only(wallet, state)
            print(
                json.dumps(
                    {
                        "wallet": wallet,
                        "stream": stream_name,
                        "status": "retrying",
                        "cursor": cursor,
                        "limit": current_limit,
                        "error": reason,
                        "retryCount": stream_state["retryCount"],
                        "waitSeconds": wait_seconds,
                    },
                    separators=(",", ":"),
                ),
                flush=True,
            )
            raise RetryLater(reason)


def build_trace(wallet: str, isolated_record: dict, local_truth: dict) -> dict:
    local_record = local_truth.get("record") or {}
    local_history = local_truth.get("history") or {}
    isolated_history = load_effective_isolated_history(wallet)
    isolated_state = read_json(isolated_wallet_state_path(wallet), {}) or {}
    remote_metrics = aggregate_trade_metrics(isolated_history.get("trades") or [])
    return {
        "generatedAt": now_ms(),
        "wallet": wallet,
        "apiBase": API_BASE,
        "ownerShardIndex": local_truth.get("ownerShardIndex"),
        "local": {
            "trades": int(local_record.get("trades") or 0),
            "volumeUsdRaw": float(local_record.get("volumeUsdRaw") or 0.0),
            "feesUsd": float(local_record.get("feesUsd") or 0.0),
            "firstTrade": local_record.get("firstTrade"),
            "lastTrade": local_record.get("lastTrade"),
            "tradeRowsLoaded": int(local_record.get("tradeRowsLoaded") or len(local_history.get("trades") or [])),
            "fundingRowsLoaded": int(local_record.get("fundingRowsLoaded") or len(local_history.get("funding") or [])),
            "tradePagination": local_record.get("tradePagination"),
            "fundingPagination": local_record.get("fundingPagination"),
            "backfillComplete": bool(local_record.get("backfillComplete")),
            "validationStatus": local_record.get("validationStatus"),
        },
        "isolated": {
            "trades": int(isolated_record.get("trades") or 0),
            "volumeUsdRaw": float(isolated_record.get("volumeUsdRaw") or 0.0),
            "feesUsd": float(isolated_record.get("feesUsd") or 0.0),
            "firstTrade": isolated_record.get("firstTrade"),
            "lastTrade": isolated_record.get("lastTrade"),
            "lastFundingAt": isolated_record.get("lastFundingAt"),
            "tradeRowsLoaded": int(isolated_record.get("tradeRowsLoaded") or len(isolated_history.get("trades") or [])),
            "fundingRowsLoaded": int(isolated_record.get("fundingRowsLoaded") or len(isolated_history.get("funding") or [])),
            "tradePagination": isolated_record.get("tradePagination"),
            "fundingPagination": isolated_record.get("fundingPagination"),
            "backfillComplete": bool(isolated_record.get("backfillComplete")),
            "validationStatus": isolated_record.get("validationStatus"),
            "status": isolated_state.get("status"),
            "historyProofStatus": isolated_record.get("historyProofStatus"),
            "historyVerifiedThroughNow": bool(isolated_record.get("historyVerifiedThroughNow")),
            "proof": isolated_record.get("proof"),
        },
        "recomputedRemoteMetrics": remote_metrics,
        "comparison": {
            "tradeDelta": int(isolated_record.get("trades") or 0) - int(local_record.get("trades") or 0),
            "volumeDeltaUsd": round(float(isolated_record.get("volumeUsdRaw") or 0.0) - float(local_record.get("volumeUsdRaw") or 0.0), 2),
            "feesDeltaUsd": round(float(isolated_record.get("feesUsd") or 0.0) - float(local_record.get("feesUsd") or 0.0), 2),
            "isHeavyWallet": int(isolated_record.get("tradeRowsLoaded") or 0) >= 100000,
        },
        "paths": {
            "isolatedDir": str(isolated_wallet_dir(wallet)),
            "isolatedState": str(isolated_wallet_state_path(wallet)),
            "isolatedHistory": str(isolated_wallet_history_path(wallet)),
            "isolatedRecord": str(isolated_wallet_record_path(wallet)),
            "localState": str(wallet_state_path(local_truth.get("ownerShardIndex"), wallet)),
            "localHistory": str(wallet_history_path(local_truth.get("ownerShardIndex"), wallet)),
            "localRecord": str(wallet_record_path(local_truth.get("ownerShardIndex"), wallet)),
        },
    }


def execute_wallet(
    wallet: str,
    lane_mode: str = "auto",
    proxy_file: str = "",
    shard_count: int = 8,
    history_limit: int = 240,
    timeout_seconds: int = 25,
    request_attempts: int = 2,
    flush_interval_pages: int = 8,
    record_flush_pages: int = 24,
    refresh_page_budget: int = 2,
    proxy_pool: ProxyPool | None = None,
    emit_trace: bool = True,
) -> int:
    wallet = normalize_wallet(wallet)
    if not wallet:
        return 1
    lane_mode = str(lane_mode or "auto").strip().lower()

    local_truth = load_local_owner_truth(wallet, shard_count)
    focus_wallets = load_focus_wallets_from_csv(
        os.environ.get("PACIFICA_WALLET_EXPLORER_V2_FOCUS_COHORT_CSV", "")
    )
    isolated_record = read_json(isolated_wallet_record_path(wallet), None)
    local_record = local_truth.get("record") if isinstance(local_truth.get("record"), dict) else None
    effective_record = (
        isolated_record if isinstance(isolated_record, dict) else local_record if isinstance(local_record, dict) else {}
    )
    phase1_focus_wallet = wallet in focus_wallets
    if proxy_pool is None:
        proxy_pool = ProxyPool(load_proxies(proxy_file))
    probe_state = read_phase1_probe_state(wallet) if phase1_focus_wallet else None
    untouched_phase1_wallet = (
        phase1_focus_wallet
        and not isinstance(isolated_record, dict)
        and not isinstance(local_record, dict)
    )
    if untouched_phase1_wallet and int((probe_state or {}).get("nextEligibleAt") or 0) > now_ms():
        return 0

    phase1_trade_head = None
    preflight_probe = None
    if untouched_phase1_wallet:
        probe_state_seed = initial_wallet_state(wallet, shard_index_for_wallet(wallet, shard_count))
        probe_history_seed = initial_wallet_history(wallet, shard_index_for_wallet(wallet, shard_count))
        try:
            probe = phase1_prime_trade_probe(
                wallet,
                probe_state_seed,
                probe_history_seed,
                proxy_pool,
                history_limit,
                timeout_seconds,
                request_attempts,
            )
            if probe and not probe.get("hasMore") and not (probe.get("rows") or []):
                finalize_phase1_zero_wallet(
                    wallet,
                    shard_count,
                    probe,
                    timeout_seconds,
                    request_attempts,
                    proxy_pool,
                )
                return 0
            preflight_probe = probe
            write_phase1_probe_state(
                wallet,
                {
                    "status": "non_empty",
                    "checkedAt": int((probe or {}).get("checkedAt") or now_ms()),
                    "nextEligibleAt": 0,
                    "probeLimit": int((probe or {}).get("probeLimit") or 25),
                    "rowCount": len((probe or {}).get("rows") or []),
                    "hasMore": bool((probe or {}).get("hasMore")),
                    "remoteLastAt": int((probe or {}).get("remoteLastAt") or 0) or None,
                    "error": None,
                },
            )
            if lane_mode == "probe" and probe and probe.get("hasMore"):
                return 0
        except RetryLater as exc:
            wait_seconds = min(backoff_seconds(1, str(exc)), 30)
            write_phase1_probe_state(
                wallet,
                {
                    "status": "probe_retry",
                    "checkedAt": now_ms(),
                    "nextEligibleAt": now_ms() + wait_seconds * 1000,
                    "probeLimit": int(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_PHASE1_PROBE_LIMIT", "25") or 25),
                    "rowCount": 0,
                    "hasMore": None,
                    "remoteLastAt": None,
                    "error": str(exc),
                },
            )
            return 0
        except Exception as exc:
            wait_seconds = min(backoff_seconds(1, str(exc)), 30)
            write_phase1_probe_state(
                wallet,
                {
                    "status": "probe_retry",
                    "checkedAt": now_ms(),
                    "nextEligibleAt": now_ms() + wait_seconds * 1000,
                    "probeLimit": int(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_PHASE1_PROBE_LIMIT", "25") or 25),
                    "rowCount": 0,
                    "hasMore": None,
                    "remoteLastAt": None,
                    "error": str(exc),
                },
            )
            return 0

    state, history = init_isolated(wallet, shard_count, local_truth)
    history = load_effective_isolated_history(wallet, history)
    if phase1_focus_wallet and record_has_empty_target_metrics(effective_record):
        force_metric_recovery_mode(state)

    persist_isolated_state_only(wallet, state)
    if phase1_focus_wallet and lane_mode == "probe" and not untouched_phase1_wallet:
        return 0
    if phase1_focus_wallet:
        if lane_mode == "proof":
            preflight_probe = None
        elif preflight_probe is None:
            try:
                preflight_probe = phase1_prime_trade_probe(
                    wallet,
                    state,
                    history,
                    proxy_pool,
                    history_limit,
                    timeout_seconds,
                    request_attempts,
                )
            except RetryLater:
                return 0
            except Exception:
                preflight_probe = None
        if preflight_probe and not preflight_probe.get("hasMore"):
            remote_last_at = int(preflight_probe.get("remoteLastAt") or 0)
            phase1_trade_head = {
                "status": "verified",
                "storedLastAt": remote_last_at or None,
                "remoteLastAt": remote_last_at or None,
                "hasMore": False,
                "rowCount": len(preflight_probe.get("rows") or []),
                "error": None,
            }
        elif lane_mode != "proof":
            try:
                collect_stream_exhaustive(
                    wallet,
                    state,
                    history,
                    "trades",
                    history_limit,
                    timeout_seconds,
                    request_attempts,
                    proxy_pool,
                    flush_interval_pages,
                    record_flush_pages,
                    refresh_page_budget,
                )
            except RetryLater:
                return 0
    else:
        try:
            collect_stream_exhaustive(
                wallet,
                state,
                history,
                "trades",
                history_limit,
                timeout_seconds,
                request_attempts,
                proxy_pool,
                flush_interval_pages,
                record_flush_pages,
                refresh_page_budget,
            )
        except RetryLater:
            return 0
    if not phase1_focus_wallet:
        try:
            collect_stream_exhaustive(
                wallet,
                state,
                history,
                "funding",
                history_limit,
                timeout_seconds,
                request_attempts,
                proxy_pool,
                flush_interval_pages,
                record_flush_pages,
                refresh_page_budget,
            )
        except RetryLater:
            return 0
    state["status"] = "phase1_metrics_complete" if phase1_focus_wallet else "complete"
    isolated_record = persist_isolated(wallet, state, history)
    if phase1_focus_wallet:
        if lane_mode == "recovery" and phase1_trade_head is None:
            phase1_trade_head = {
                "status": "deferred",
                "storedLastAt": int((isolated_record or {}).get("lastTrade") or 0) or None,
                "remoteLastAt": None,
                "hasMore": None,
                "rowCount": 0,
                "error": None,
            }
        isolated_record = attach_phase1_metric_proof(
            wallet,
            state,
            history,
            isolated_record,
            proxy_pool,
            timeout_seconds,
            request_attempts,
            phase1_trade_head,
        )
        clear_phase1_probe_state(wallet)
    else:
        isolated_record = attach_history_proof(
            wallet,
            state,
            history,
            isolated_record,
            proxy_pool,
            timeout_seconds,
            request_attempts,
        )
    trace = build_trace(wallet, isolated_record, local_truth)
    write_json_atomic(isolated_wallet_dir(wallet) / "trace.json", trace)
    if emit_trace:
        print(json.dumps(trace, indent=2))
    return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--wallet", required=True)
    parser.add_argument("--lane-mode", default="auto")
    parser.add_argument("--proxy-file", required=True)
    parser.add_argument("--shard-count", type=int, default=8)
    parser.add_argument("--history-limit", type=int, default=240)
    parser.add_argument("--timeout-seconds", type=int, default=25)
    parser.add_argument("--request-attempts", type=int, default=2)
    parser.add_argument("--flush-interval-pages", type=int, default=8)
    parser.add_argument("--record-flush-pages", type=int, default=24)
    parser.add_argument("--refresh-page-budget", type=int, default=2)
    args = parser.parse_args()
    raise SystemExit(
        execute_wallet(
            wallet=args.wallet,
            lane_mode=args.lane_mode,
            proxy_file=args.proxy_file,
            shard_count=args.shard_count,
            history_limit=args.history_limit,
            timeout_seconds=args.timeout_seconds,
            request_attempts=args.request_attempts,
            flush_interval_pages=args.flush_interval_pages,
            record_flush_pages=args.record_flush_pages,
            refresh_page_budget=args.refresh_page_budget,
            emit_trace=True,
        )
    )


if __name__ == "__main__":
    main()
