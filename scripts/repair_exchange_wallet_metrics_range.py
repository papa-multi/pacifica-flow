#!/usr/bin/env python3
import argparse
import json
import multiprocessing as mp
from datetime import date, datetime, timedelta, timezone

from scripts.wallet_explorer_v2.materialize import (
    EXCHANGE_WALLET_METRIC_HISTORY_PATH,
    GLOBAL_KPI_PATH,
    WALLET_METRICS_HISTORY_DAILY_DIR,
    UI_DIR,
    V3_BUILD_SOURCE_PATH,
    add_funding_rows_to_exchange_daily,
    add_trade_rows_to_exchange_daily,
    build_active_wallet_days_from_rows,
    empty_exchange_metric_day,
    load_effective_isolated_history,
)
from scripts.wallet_explorer_v2.common import (
    DEFAULT_SHARD_COUNT,
    SHARDS_DIR,
    isolated_wallet_history_log_path,
    isolated_wallet_history_path,
    load_isolated_wallets,
    read_json,
    wallet_history_path,
    write_json_atomic,
)


def iter_dates(start_text: str, end_text: str):
    cursor = date.fromisoformat(start_text)
    end = date.fromisoformat(end_text)
    while cursor <= end:
        yield cursor.isoformat()
        cursor += timedelta(days=1)


def format_utc_day(timestamp_ms: int) -> str | None:
    if not timestamp_ms:
        return None
    return datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def load_wallet_metrics_daily_snapshot_aggregate(day: str):
    day_text = str(day or "").strip()[:10]
    if not day_text:
        return None
    snapshot_path = WALLET_METRICS_HISTORY_DAILY_DIR / f"{day_text}.json"
    snapshot = read_json(snapshot_path, None)
    if not isinstance(snapshot, dict):
        return None
    wallets = snapshot.get("wallets") if isinstance(snapshot.get("wallets"), dict) else {}
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
        volume = max(0.0, float(d24.get("volumeUsd") or 0.0))
        fees = max(
            0.0,
            float(
                d24.get("feesPaidUsd")
                if d24.get("feesPaidUsd") is not None
                else d24.get("feesUsd")
                if d24.get("feesUsd") is not None
                else row.get("feesPaidUsd")
                if row.get("feesPaidUsd") is not None
                else row.get("feesUsd")
                or 0.0
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
    }


def process_shard(args):
    shard_index, target_dates, prefer_isolated = args
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
                [
                    row
                    for row in (history.get("trades") or [])
                    if format_utc_day(int((row or {}).get("timestamp") or 0)) in target_dates
                ],
                wallet=wallet,
                active_wallet_days=active_wallet_days,
            )
            add_funding_rows_to_exchange_daily(
                daily_by_date,
                [
                    row
                    for row in (history.get("funding") or [])
                    if format_utc_day(int((row or {}).get("created_at") or 0)) in target_dates
                ],
                wallet=wallet,
                active_wallet_days=active_wallet_days,
            )
    return daily_by_date, {day: sorted(wallets) for day, wallets in active_wallet_days.items()}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--shards", type=int, default=DEFAULT_SHARD_COUNT)
    args = parser.parse_args()

    target_dates = set(iter_dates(args.start_date, args.end_date))
    prefer_isolated = {}
    for wallet in sorted(load_isolated_wallets()):
        isolated_history_exists = isolated_wallet_history_path(wallet).exists() or isolated_wallet_history_log_path(wallet).exists()
        prefer_isolated[wallet] = bool(isolated_history_exists)

    worker_count = min(args.shards, 8)
    with mp.get_context("fork").Pool(processes=worker_count) as pool:
        partials = pool.map(
            process_shard,
            [(shard_index, target_dates, prefer_isolated) for shard_index in range(args.shards)],
        )

    daily_by_date = {}
    active_wallet_days = {}
    for partial_daily, partial_active in partials:
        for day, partial_bucket in (partial_daily or {}).items():
            target_bucket = daily_by_date.setdefault(day, empty_exchange_metric_day())
            for key, value in (partial_bucket or {}).items():
                if key in {"accountsAdded", "accountsTotal", "activeWallets"}:
                    continue
                if isinstance(value, (int, float)):
                    target_bucket[key] = float(target_bucket.get(key) or 0.0) + float(value)
        for day, wallets in (partial_active or {}).items():
            active_wallet_days.setdefault(day, set()).update(wallets)

    for wallet, use_isolated in sorted(prefer_isolated.items()):
        if not use_isolated:
            continue
        history = load_effective_isolated_history(wallet)
        trades = [
            row
            for row in (history.get("trades") or [])
            if format_utc_day(int((row or {}).get("timestamp") or 0)) in target_dates
        ]
        funding = [
            row
            for row in (history.get("funding") or [])
            if format_utc_day(int((row or {}).get("created_at") or 0)) in target_dates
        ]
        add_trade_rows_to_exchange_daily(
            daily_by_date,
            trades,
            wallet=wallet,
            active_wallet_days=active_wallet_days,
        )
        add_funding_rows_to_exchange_daily(
            daily_by_date,
            funding,
            wallet=wallet,
            active_wallet_days=active_wallet_days,
        )

    compact_payload = read_json(V3_BUILD_SOURCE_PATH, {}) or {}
    compact_rows = compact_payload.get("rows") if isinstance(compact_payload, dict) else []
    rows_by_wallet = {}
    for row in compact_rows or []:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("wallet") or row.get("address") or "").strip()
        if wallet:
            rows_by_wallet[wallet] = row
    active_wallet_days_from_rows = build_active_wallet_days_from_rows(rows_by_wallet)

    payload = read_json(EXCHANGE_WALLET_METRIC_HISTORY_PATH, {}) or {}
    global_kpi_payload = read_json(GLOBAL_KPI_PATH, {}) or {}
    global_kpi_history = (
        global_kpi_payload.get("history")
        if isinstance(global_kpi_payload.get("history"), dict)
        else {}
    )
    global_kpi_daily = (
        global_kpi_history.get("dailyByDate")
        if isinstance(global_kpi_history.get("dailyByDate"), dict)
        else {}
    )
    existing_daily = payload.get("dailyByDate") or {}
    for day in sorted(target_dates):
        row = existing_daily.setdefault(day, empty_exchange_metric_day())
        computed = daily_by_date.get(day, empty_exchange_metric_day())
        snapshot_fallback = load_wallet_metrics_daily_snapshot_aggregate(day)
        if snapshot_fallback:
            if int(computed.get("trades") or 0) <= 0 and int(snapshot_fallback.get("trades") or 0) > 0:
                computed["trades"] = int(snapshot_fallback.get("trades") or 0)
            if int(computed.get("activeWallets") or 0) <= 0 and int(snapshot_fallback.get("activeWallets") or 0) > 0:
                computed["activeWallets"] = int(snapshot_fallback.get("activeWallets") or 0)
            if float(computed.get("feesUsd") or 0.0) <= 0 and float(snapshot_fallback.get("feesUsd") or 0.0) > 0:
                computed["feesUsd"] = round(float(snapshot_fallback.get("feesUsd") or 0.0), 2)
                computed["feesPaidUsd"] = round(float(snapshot_fallback.get("feesUsd") or 0.0), 2)
        for key in [
            "trades",
            "makerTrades",
            "takerTrades",
            "feesUsd",
            "feesPaidUsd",
            "liquidityPoolFeesUsd",
            "volumeUsd",
            "makerVolumeUsd",
            "takerVolumeUsd",
            "feeBearingVolumeUsd",
        ]:
            row[key] = round(float(computed.get(key) or 0.0), 2)
        row["activeWallets"] = max(
            len(active_wallet_days_from_rows.get(day) or ()),
            int(computed.get("activeWallets") or 0),
        )

    ordered_daily = {day: existing_daily[day] for day in sorted(existing_daily.keys())}
    payload["dailyByDate"] = ordered_daily
    coverage = payload.setdefault("coverage", {})
    coverage["totalTrades"] = int(sum(float((row or {}).get("trades") or 0.0) for row in ordered_daily.values()))
    coverage["totalFeesUsd"] = round(sum(float((row or {}).get("feesUsd") or 0.0) for row in ordered_daily.values()), 2)
    coverage["totalVolumeUsd"] = round(sum(float((row or {}).get("volumeUsd") or 0.0) for row in ordered_daily.values()), 2)
    payload["generatedAt"] = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    payload["source"] = {
        "mode": "wallet_explorer_v2_range_repair",
        "range": {
            "start": args.start_date,
            "end": args.end_date,
        },
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
    }
    write_json_atomic(EXCHANGE_WALLET_METRIC_HISTORY_PATH, payload)

    kpis = {
        "generatedAt": payload["generatedAt"],
        "kpis": {
            "totalAccounts": int(coverage.get("totalAccounts") or 0),
            "activeAccounts": int(coverage.get("totalAccounts") or 0),
            "totalTrades": int(coverage.get("totalTrades") or 0),
            "totalFeesUsd": float(coverage.get("totalFeesUsd") or 0.0),
        },
        "coverage": {
            "startTimeMs": None,
            "endTimeMs": payload["generatedAt"],
        },
        "source": payload["source"],
    }
    write_json_atomic(UI_DIR / "exchange_wallet_kpis.json", kpis)

    for day in sorted(target_dates):
        print(day, payload["dailyByDate"].get(day))


if __name__ == "__main__":
    main()
