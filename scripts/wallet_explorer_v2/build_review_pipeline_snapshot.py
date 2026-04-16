#!/usr/bin/env python3
import json
import gzip
from pathlib import Path

from common import UI_DIR, V2_DIR, load_wallet_discovery_first_seen, load_wallet_discovery_recent_seen, now_ms


REVIEW_DIR = V2_DIR / "review_pipeline"
STATE_PATH = REVIEW_DIR / "state.json"
V3_MANIFEST_PATH = UI_DIR.parent / "wallet_explorer_v3" / "manifest.json"
WATCH_GLOB = UI_DIR.glob("wallet_sync_watch_*.jsonl")


def read_json(path: Path, fallback=None):
    if not path.exists():
        return fallback
    try:
        with path.open() as handle:
            return json.load(handle)
    except Exception:
        return fallback


def write_json_atomic(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{now_ms()}.tmp")
    with tmp.open("w") as handle:
        json.dump(payload, handle, separators=(",", ":"))
    tmp.replace(path)


def build_status_summary_from_state(state: dict) -> dict:
    total_wallets = int(state.get("totalWallets") or 0)
    groups = state.get("groups") if isinstance(state.get("groups"), dict) else {}
    history_group = groups.get("historyCompletion") if isinstance(groups.get("historyCompletion"), dict) else {}
    deep_repair_group = groups.get("deepRepair") if isinstance(groups.get("deepRepair"), dict) else {}
    gap_group = groups.get("gapRecovery") if isinstance(groups.get("gapRecovery"), dict) else {}
    live_group = groups.get("liveMode") if isinstance(groups.get("liveMode"), dict) else {}
    history_count = int(history_group.get("count") or 0)
    deep_repair_count = int(deep_repair_group.get("count") or 0)
    gap_count = int(gap_group.get("count") or 0)
    live_count = int(live_group.get("count") or 0)
    active_history = int(history_group.get("active") or 0)
    active_gap = int(gap_group.get("active") or 0)
    active_live = int(live_group.get("active") or 0)
    tracked_lite = history_count + deep_repair_count
    tracked_full = gap_count + live_count
    tracked_total = max(total_wallets, history_count + deep_repair_count + gap_count + live_count)
    generated_at = int(state.get("generatedAt") or now_ms())
    existing = read_json(UI_DIR / "system_status_summary.json", {}) or {}
    existing_status = existing.get("status", {}) if isinstance(existing, dict) else {}
    existing_summary = existing_status.get("summary", {}) if isinstance(existing_status, dict) else {}
    wallet_progress = dict(existing_summary.get("walletExplorerProgress") or {})
    wallet_progress.update(
        {
            "discovered": total_wallets,
            "validated": live_count,
            "backfilled": tracked_total,
            "currentlyTracked": tracked_total,
            "trackedLite": tracked_lite,
            "trackedFull": tracked_full,
            "tradeHistoryComplete": tracked_full,
            "paginationCheckpointWallets": gap_count,
            "paginationFrontierWallets": history_count,
            "paginationRetryWallets": active_history,
            "paginationCompleteWallets": tracked_full,
            "tradePagesFetched": history_count + gap_count,
            "fundingPagesFetched": deep_repair_count,
            "tradePagesPending": 0,
            "fundingPagesPending": 0,
            "tradePagesFailed": 0,
            "fundingPagesFailed": 0,
            "pending": gap_count + history_count,
            "discoveryMetricType": "review_pipeline_state",
            "oldestProcessedAt": None,
            "newestProcessedAt": None,
            "discoveryWindow": None,
            "solanaDiscoveryTimeline": None,
            "walletHistoryTimeline": {
                "timelineId": "pacifica_wallet_history",
                "label": "Pacifica Wallet History",
                "currentDayLabel": "wallet_history_frontier",
                "startTimeMs": None,
                "currentTimeMs": generated_at,
                "endTimeMs": generated_at,
                "remainingMs": None,
                "percentComplete": None,
                "mode": "catch_up",
                "completed": False,
                "exactness": "derived",
                "method": "review_pipeline_state",
                "frontierWallet": None,
                "walletCounts": {
                    "activationOnly": active_live,
                    "partialHistory": active_history,
                    "recentHistoryOnly": active_gap,
                    "historyComplete": tracked_full,
                    "trackedLite": tracked_lite,
                },
            },
        }
    )
    return {
        "generatedAt": generated_at,
        "status": {
            "generatedAt": generated_at,
            "sourceCount": 1,
            "activeSourceCount": 1,
            "summary": {
                "walletExplorerProgress": wallet_progress,
            },
            "services": existing_status.get("services") if isinstance(existing_status, dict) else None,
            "startup": existing_status.get("startup") if isinstance(existing_status, dict) else None,
        },
    }


def load_rows():
    if not V3_MANIFEST_PATH.exists():
        return []
    try:
        manifest = read_json(V3_MANIFEST_PATH, {}) or {}
        rows = []
        for shard in manifest.get("shards") or []:
            shard_id = str((shard or {}).get("shardId") or "").strip()
            if not shard_id:
                continue
            shard_path = V3_MANIFEST_PATH.parent / "shards" / f"shard_{shard_id}.json.gz"
            if not shard_path.exists():
                continue
            with gzip.open(shard_path, "rt") as handle:
                payload = json.load(handle)
            rows.extend([row for row in payload.get("rows") or [] if isinstance(row, dict)])
        return rows
    except Exception:
        return []


def to_num(value, fallback=0):
    try:
        return float(value)
    except Exception:
        return fallback


def fmt_short_date(ts: int | None) -> str:
    import datetime as dt

    value = int(ts or 0)
    if value <= 0:
        return "-"
    return dt.datetime.utcfromtimestamp(value / 1000.0).strftime("%Y-%m-%d")


def load_baseline_through_at() -> int:
    latest_path = None
    for candidate in sorted(WATCH_GLOB):
        latest_path = candidate
    if latest_path and latest_path.exists():
        try:
            last = None
            with latest_path.open() as handle:
                for line in handle:
                    if line.strip():
                        last = json.loads(line)
            compact = ((last or {}).get("components") or {}).get("compactDataset") or {}
            through_at = int(compact.get("throughAt") or 0)
            if through_at > 0:
                return through_at
        except Exception:
            pass
    rows = load_rows()
    return max((int((row or {}).get("lastTrade") or 0) for row in rows), default=0)


def has_trade_metrics(row: dict) -> bool:
    safe = row if isinstance(row, dict) else {}
    return bool(
        int(safe.get("tradeRowsLoaded") or safe.get("trades") or 0) > 0
        or abs(to_num(safe.get("volumeUsdRaw"), 0.0)) > 0
        or abs(to_num(safe.get("feesPaidUsd") or safe.get("feesUsd"), 0.0)) > 0
        or abs(to_num(safe.get("pnlUsd") or safe.get("totalPnlUsd"), 0.0)) > 0
        or int(safe.get("totalWins") or 0) > 0
        or int(safe.get("totalLosses") or 0) > 0
        or int(safe.get("firstTrade") or 0) > 0
        or int(safe.get("lastTrade") or 0) > 0
    )


def build_wallet_item(wallet: str, row: dict, label: str, meta: str, remaining: str, pct: float) -> dict:
    return {
        "wallet": wallet,
        "progressPct": max(0.0, min(100.0, float(pct))),
        "summaryLabel": label,
        "metaLabel": meta,
        "remainingLabel": remaining,
        "tradeRowsLoaded": int((row or {}).get("tradeRowsLoaded") or 0),
        "volumeUsdRaw": float((row or {}).get("volumeUsdRaw") or 0.0),
        "openPositions": int((row or {}).get("openPositions") or 0),
        "updatedAt": int((row or {}).get("updatedAt") or 0) or None,
    }


def main():
    rows = load_rows()
    discovery_first_seen = load_wallet_discovery_first_seen()
    discovery_recent_seen = load_wallet_discovery_recent_seen()
    baseline_through_at = load_baseline_through_at()

    history_items = []
    deep_repair_items = []
    gap_items = []
    live_items = []

    history_count = 0
    deep_repair_count = 0
    gap_count = 0
    live_count = 0
    gap_candidate_count = 0
    zero_trade_gap_ready_count = 0

    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("wallet") or "").strip()
        if not wallet:
            continue
        first_deposit_at = int(discovery_first_seen.get(wallet) or 0)
        recent_seen_at = int(discovery_recent_seen.get(wallet) or 0)
        first_trade_at = int(row.get("firstTrade") or 0)
        last_trade_at = int(row.get("lastTrade") or 0)
        backfill_complete = bool(
            row.get("backfillComplete")
            or (
                row.get("tradeDone")
                and row.get("fundingDone")
                and not row.get("tradeHasMore")
                and not row.get("fundingHasMore")
                and not row.get("retryPending")
            )
        )
        trade_metrics = has_trade_metrics(row)
        deposit_breach = bool(
            trade_metrics and first_trade_at > 0 and first_deposit_at > 0 and first_trade_at < first_deposit_at
        )
        likely_gap = bool(
            backfill_complete
            and recent_seen_at > 0
            and max(last_trade_at, baseline_through_at) > 0
            and recent_seen_at > max(last_trade_at, baseline_through_at) + 60 * 60 * 1000
        )

        if deposit_breach:
            deep_repair_count += 1
            deep_repair_items.append(
                build_wallet_item(
                    wallet,
                    row,
                    "Trade history starts before deposit floor",
                    f"deposit {fmt_short_date(first_deposit_at)} • first trade {fmt_short_date(first_trade_at)}",
                    "needs deep repair",
                    5,
                )
            )
        elif backfill_complete:
            gap_count += 1
            if likely_gap:
                gap_candidate_count += 1
            if not trade_metrics:
                zero_trade_gap_ready_count += 1
            gap_items.append(
                build_wallet_item(
                    wallet,
                    row,
                    (
                        "No historical trades found; ready for gap closure"
                        if not trade_metrics
                        else "Historical baseline complete; ready for gap closure"
                    ),
                    (
                        f"deposit {fmt_short_date(first_deposit_at)} • no trade output"
                        if not trade_metrics
                        else f"deposit {fmt_short_date(first_deposit_at)} • first trade {fmt_short_date(first_trade_at)}"
                    ),
                    "gap recovery",
                    40 if not trade_metrics else 55,
                )
            )
        else:
            history_count += 1
            history_items.append(
                build_wallet_item(
                    wallet,
                    row,
                    "Needs historical completion",
                    f"deposit {fmt_short_date(first_deposit_at)} • last trade {fmt_short_date(last_trade_at)}",
                    "history completion",
                    0,
                )
            )

    history_items.sort(key=lambda item: int(item.get("updatedAt") or 0), reverse=True)
    deep_repair_items.sort(key=lambda item: int(item.get("updatedAt") or 0), reverse=True)
    gap_items.sort(key=lambda item: int(item.get("updatedAt") or 0), reverse=True)

    payload = {
        "version": 1,
        "generatedAt": now_ms(),
        "totalWallets": len(rows),
        "baselineThroughAt": int(baseline_through_at or 0) or None,
        "baselineThroughDate": fmt_short_date(baseline_through_at),
        "source": {
            "walletExplorerV3Path": str(V3_MANIFEST_PATH),
            "discoveryPath": str(Path("/root/pacifica-flow/data/indexer/wallet_discovery.json")),
        },
        "groups": {
            "historyCompletion": {
                "label": "History Completion",
                "phaseType": "history_completion",
                "count": history_count,
                "active": 0,
                "complete": 0,
                "progressPct": 0,
                "badgeLabel": "Step 1",
                "focusLabel": "Validate baseline history back to each wallet's first deposit, then run one extra request before the deposit boundary.",
                "summaryMetrics": [
                    {"label": "Total", "value": f"{history_count:,}", "note": "wallets queued for history validation"},
                    {"label": "Deep Repair Detected", "value": f"{deep_repair_count:,}", "note": "already breached the deposit floor"},
                    {"label": "Gap Ready", "value": f"{gap_count:,}", "note": "historically complete enough to move on"},
                    {"label": "Baseline Through", "value": fmt_short_date(baseline_through_at), "note": "current historical anchor"},
                ],
                "wallets": history_items[:12],
            },
            "deepRepair": {
                "label": "Deep Repair",
                "phaseType": "deep_history_repair",
                "count": deep_repair_count,
                "active": 0,
                "complete": 0,
                "progressPct": 0,
                "badgeLabel": "Step 2",
                "focusLabel": "Wallets where trades appear before the deposit floor. These need deeper history repair before they can continue.",
                "summaryMetrics": [
                    {"label": "Breached Wallets", "value": f"{deep_repair_count:,}", "note": "pre-detected from local first-trade vs deposit floor"},
                    {"label": "Resolved", "value": "0", "note": "not started yet"},
                ],
                "wallets": deep_repair_items[:12],
            },
            "gapRecovery": {
                "label": "Gap Recovery",
                "phaseType": "gap_recovery",
                "count": gap_count,
                "active": 0,
                "complete": 0,
                "progressPct": 0,
                "badgeLabel": "Step 3",
                "focusLabel": "After historical validation passes, close the gap from the stored baseline to now by finding overlap from page 1 backward.",
                "summaryMetrics": [
                    {"label": "Ready Now", "value": f"{gap_count:,}", "note": "wallets promoted into gap recovery"},
                    {"label": "Zero-Trade Ready", "value": f"{zero_trade_gap_ready_count:,}", "note": "no pre-gap trade output found"},
                    {"label": "Likely Active Gap", "value": f"{gap_candidate_count:,}", "note": "recent activity beyond the stored baseline"},
                ],
                "wallets": gap_items[:12],
            },
            "liveMode": {
                "label": "Live Mode",
                "phaseType": "live_mode",
                "count": live_count,
                "active": 0,
                "complete": live_count,
                "progressPct": 0,
                "badgeLabel": "Step 4",
                "focusLabel": "Only wallets with validated history and a closed head gap should be promoted into live mode.",
                "summaryMetrics": [
                    {"label": "Live Wallets", "value": f"{live_count:,}", "note": "not promoted yet"},
                    {"label": "Refresh SLA", "value": "1h", "note": "target maximum refresh age"},
                ],
                "wallets": live_items[:12],
            },
        },
    }
    write_json_atomic(STATE_PATH, payload)
    write_json_atomic(UI_DIR / "system_status_summary.json", build_status_summary_from_state(payload))
    print(json.dumps({
        "path": str(STATE_PATH),
        "totalWallets": payload["totalWallets"],
        "historyCompletion": history_count,
        "deepRepair": deep_repair_count,
        "gapRecovery": gap_count,
        "liveMode": live_count,
        "gapCandidates": gap_candidate_count,
        "baselineThroughAt": payload["baselineThroughAt"],
    }))


if __name__ == "__main__":
    main()
