#!/usr/bin/env python3
import json
import math
import hashlib
import os
import subprocess
from pathlib import Path


ROOT = Path("/root/pacifica-flow")
DATA_DIR = ROOT / "data"
INDEXER_DIR = DATA_DIR / "indexer"
SHARDS_DIR = INDEXER_DIR / "shards"
UI_DIR = DATA_DIR / "ui"
GLOBAL_KPI_PATH = DATA_DIR / "pipeline" / "global_kpi.json"
NEXTGEN_WALLET_STATE_PATH = DATA_DIR / "nextgen" / "state" / "wallet-current-state.json"
NEXTGEN_LIVE_STATUS_PATH = DATA_DIR / "nextgen" / "state" / "wallet-live-status.json"
ACTIVE_SHARDS = 8
PREVIEW_VERSION = 3
DEFAULT_PAGE_VERSION = 1
WALLET_DATASET_VERSION = 1
WALLET_DATASET_COMPACT_VERSION = 1
WALLET_TRACKING_MANIFEST_VERSION = 1
MAX_SYMBOL_BREAKDOWN = 8
WALLET_STORAGE_V3_BUILD_SCRIPT = ROOT / "scripts" / "build_wallet_storage_v3.js"
WRITE_LEGACY_DATA = str(
    os.environ.get("PACIFICA_WALLET_EXPLORER_WRITE_LEGACY_DATA", "false")
).strip().lower() == "true"
V3_BUILD_SOURCE_PATH = UI_DIR / ".build" / "wallet_dataset_compact_source.json"
NEXTGEN_STATE_CACHE = {
    "walletsMtimeMs": 0,
    "liveMtimeMs": 0,
    "wallets": {},
    "live": {},
}


def read_json(path: Path):
    if not path.exists():
        return None
    with open(path) as handle:
        return json.load(handle)


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
    wallets_payload = read_json(NEXTGEN_WALLET_STATE_PATH) or {}
    live_payload = read_json(NEXTGEN_LIVE_STATUS_PATH) or {}
    NEXTGEN_STATE_CACHE["walletsMtimeMs"] = wallets_mtime
    NEXTGEN_STATE_CACHE["liveMtimeMs"] = live_mtime
    NEXTGEN_STATE_CACHE["wallets"] = wallets_payload if isinstance(wallets_payload, dict) else {}
    NEXTGEN_STATE_CACHE["live"] = live_payload if isinstance(live_payload, dict) else {}
    return NEXTGEN_STATE_CACHE


def write_json_atomic(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as handle:
        json.dump(payload, handle, separators=(",", ":"))
    tmp.replace(path)


def build_wallet_storage_v3(source_path: Path) -> None:
    subprocess.run(
        [
            "node",
            str(WALLET_STORAGE_V3_BUILD_SCRIPT),
            f"--source={source_path}",
        ],
        check=True,
        cwd=str(ROOT),
    )


def to_num(value, fallback=0.0):
    try:
        num = float(value)
    except Exception:
        return fallback
    return num if math.isfinite(num) else fallback


def norm_wallet(value) -> str:
    return str(value or "").strip()


def row_timestamp(row: dict) -> int:
    all_bucket = row.get("all") if isinstance(row.get("all"), dict) else {}
    return max(
        int(row.get("updatedAt") or 0),
        int(row.get("createdAt") or 0),
        int(all_bucket.get("computedAt") or 0),
        int(all_bucket.get("lastTrade") or 0),
        int(row.get("lastTrade") or 0),
    )


def state_timestamp(row: dict) -> int:
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


def pagination_complete(summary=None) -> bool:
    if not isinstance(summary, dict):
        return False
    exhausted = bool(summary.get("exhausted"))
    retry_pages = int(summary.get("retryPages") or 0)
    frontier_cursor = summary.get("frontierCursor")
    return exhausted and retry_pages <= 0 and not frontier_cursor


def build_wallet_record_id(wallet: str, row: dict, state: dict) -> str:
    for source in (row, state):
        candidate = str((source or {}).get("walletRecordId") or "").strip()
        if candidate:
            return candidate
    digest = hashlib.sha1(wallet.encode("utf8")).hexdigest()[:32]
    return f"wal_{digest}"


def get_file_audit(state: dict) -> dict:
    history_audit = state.get("historyAudit") if isinstance(state.get("historyAudit"), dict) else {}
    file_audit = history_audit.get("file") if isinstance(history_audit.get("file"), dict) else {}
    return file_audit


def build_status_fields(wallet: str, row: dict, state: dict) -> dict:
    file_audit = get_file_audit(state)
    trade_pagination = state.get("tradePagination") if isinstance(state.get("tradePagination"), dict) else {}
    funding_pagination = state.get("fundingPagination") if isinstance(state.get("fundingPagination"), dict) else {}
    history_audit = state.get("historyAudit") if isinstance(state.get("historyAudit"), dict) else {}
    pending_reasons = list(history_audit.get("pendingReasons") or [])
    warnings = list(history_audit.get("warnings") or [])
    trade_done = bool(state.get("tradeDone")) or pagination_complete(trade_pagination)
    funding_done = bool(state.get("fundingDone")) or pagination_complete(funding_pagination)
    remote_verified = bool(state.get("remoteHistoryVerified")) or (trade_done and funding_done)
    history_verified = trade_done and funding_done and remote_verified and not bool(state.get("retryPending"))
    all_bucket = row.get("all") if isinstance(row.get("all"), dict) else {}
    trades = int(to_num(all_bucket.get("trades"), to_num(row.get("trades"), 0)))
    metrics_ready = trades >= 0 and row_timestamp(row) > 0
    metrics_verified = history_verified and metrics_ready
    leaderboard_eligible = metrics_verified

    if leaderboard_eligible:
        volume_status = "exact"
        volume_status_label = "Exact History Volume"
        tracking_status = "tracked_full"
        tracking_status_label = "Tracked Full"
        trade_history_status = "full_history_complete"
        trade_history_status_label = "Full History Complete"
        metrics_status = "metrics_complete"
        metrics_status_label = "Metrics Complete"
    elif trade_done or funding_done or int(state.get("tradeRowsLoaded") or 0) > 0:
        volume_status = "partial"
        volume_status_label = "Partial History Volume"
        tracking_status = "queued_backfill"
        tracking_status_label = "Queued Backfill"
        trade_history_status = "partial_trade_history"
        trade_history_status_label = "Partial Trade History"
        metrics_status = "metrics_partial"
        metrics_status_label = "Metrics Partial"
    else:
        volume_status = "blocked"
        volume_status_label = "Blocked"
        tracking_status = "tracked_lite"
        tracking_status_label = "Tracked Lite"
        trade_history_status = "activation_only"
        trade_history_status_label = "Activation Only"
        metrics_status = "metrics_pending"
        metrics_status_label = "Metrics Pending"

    if not pending_reasons:
        if not history_verified:
            pending_reasons.append("history_not_verified")
        if not metrics_verified:
            pending_reasons.append("metrics_not_verified")
        if not trade_done or not funding_done:
            pending_reasons.append("history_backfill_incomplete")
        if bool(state.get("retryPending")):
            pending_reasons.append(
                f"retry_pending:{state.get('retryReason') or 'unknown'}"
            )

    history_persisted_at = int(file_audit.get("persistedAt") or 0) or None
    trades_persisted = int(file_audit.get("tradesStored") or state.get("tradeRowsLoaded") or 0)
    funding_persisted = int(file_audit.get("fundingStored") or state.get("fundingRowsLoaded") or 0)

    return {
        "tradeDone": trade_done,
        "fundingDone": funding_done,
        "remoteHistoryVerified": remote_verified,
        "historyVerified": history_verified,
        "metricsReady": metrics_ready,
        "metricsVerified": metrics_verified,
        "leaderboardEligible": leaderboard_eligible,
        "volumeStatus": volume_status,
        "volumeStatusLabel": volume_status_label,
        "trackingStatus": tracking_status,
        "trackingStatusLabel": tracking_status_label,
        "tradeHistoryStatus": trade_history_status,
        "tradeHistoryStatusLabel": trade_history_status_label,
        "metricsStatus": metrics_status,
        "metricsStatusLabel": metrics_status_label,
        "blockerReasons": sorted(set(pending_reasons)),
        "fullBlockedReasons": sorted(set(pending_reasons)),
        "warnings": warnings,
        "historyPersistedAt": history_persisted_at,
        "tradesPersisted": trades_persisted,
        "fundingPersisted": funding_persisted,
    }


def build_prepared_row(wallet: str, row: dict, state: dict, shard_index: int) -> dict:
    all_bucket = row.get("all") if isinstance(row.get("all"), dict) else {}
    d30 = row.get("d30") if isinstance(row.get("d30"), dict) else {}
    file_audit = get_file_audit(state)
    status = build_status_fields(wallet, row, state)
    overlay = load_nextgen_state_overlay()
    nextgen_wallet = overlay["wallets"].get(wallet) if isinstance(overlay.get("wallets"), dict) else {}
    nextgen_live = overlay["live"].get(wallet) if isinstance(overlay.get("live"), dict) else {}
    nextgen_wallet = nextgen_wallet if isinstance(nextgen_wallet, dict) else {}
    nextgen_live = nextgen_live if isinstance(nextgen_live, dict) else {}
    volume_raw = to_num(all_bucket.get("volumeUsd"), to_num(row.get("volumeUsd"), 0.0))
    overlay_volume = to_num(nextgen_wallet.get("volume_usd"), 0.0)
    volume_raw = max(volume_raw, overlay_volume)
    volume_verified = volume_raw if status["historyVerified"] else None
    ranking_volume = volume_raw if status["leaderboardEligible"] else None
    wins = max(int(to_num(all_bucket.get("wins"), to_num(row.get("wins"), 0))), int(to_num(nextgen_wallet.get("total_wins"), 0)))
    losses = max(int(to_num(all_bucket.get("losses"), to_num(row.get("losses"), 0))), int(to_num(nextgen_wallet.get("total_losses"), 0)))
    trades = max(int(to_num(all_bucket.get("trades"), to_num(row.get("trades"), 0))), int(to_num(nextgen_wallet.get("trades"), 0)))
    win_rate = max(to_num(all_bucket.get("winRatePct"), to_num(row.get("winRatePct"), 0.0)), to_num(nextgen_wallet.get("win_rate"), 0.0))
    first_trade = int(all_bucket.get("firstTrade") or row.get("firstTrade") or nextgen_wallet.get("first_trade_at") or 0) or None
    last_trade = int(
        all_bucket.get("lastTrade")
        or row.get("lastTrade")
        or nextgen_wallet.get("last_trade_at")
        or 0
    ) or None
    current_activity_at = max(
        int(all_bucket.get("lastOpenedAt") or 0),
        int(row.get("lastOpenedAt") or 0),
        int(nextgen_wallet.get("last_opened_at") or 0),
        int(nextgen_live.get("last_opened_at") or 0),
    ) or None
    updated_at = max(
        row_timestamp(row),
        state_timestamp(state),
        int(state.get("discoveredAt") or 0),
        int(nextgen_wallet.get("updated_at") or 0),
        int(nextgen_live.get("updated_at") or 0),
        int(nextgen_live.get("lastLocalStateUpdateAt") or 0),
        int(current_activity_at or 0),
    ) or None
    open_positions = max(
        0,
        int(to_num(nextgen_wallet.get("open_positions"), 0)),
        int(to_num(nextgen_live.get("open_positions"), 0)),
    )
    pnl_usd = to_num(nextgen_wallet.get("realized_pnl_usd"), to_num(all_bucket.get("pnlUsd"), to_num(row.get("pnlUsd"), 0.0)))
    unrealized_pnl_usd = to_num(nextgen_wallet.get("unrealized_pnl_usd"), to_num(row.get("unrealizedPnlUsd"), 0.0))
    total_pnl_usd = pnl_usd + unrealized_pnl_usd
    exposure_usd = to_num(nextgen_wallet.get("exposure_usd"), to_num(row.get("exposureUsd"), 0.0))
    storage_refs = {
        "walletStorePath": str((row.get("storage") or {}).get("walletStorePath") or ""),
        "indexerStatePath": str((state.get("storage") or {}).get("indexerStatePath") or ""),
        "walletHistoryPath": str((row.get("storage") or {}).get("walletHistoryPath") or ""),
        "indexerShardId": str((row.get("storage") or {}).get("indexerShardId") or f"indexer_shard_{shard_index}"),
    }
    completeness = {
        "historyVerified": status["historyVerified"],
        "metricsVerified": status["metricsVerified"],
        "leaderboardEligible": status["leaderboardEligible"],
        "blockerReasons": status["blockerReasons"],
        "pendingWarnings": status["warnings"],
    }
    symbols = sorted((all_bucket.get("symbolVolumes") or {}).keys())[:24]
    symbol_breakdown = sorted(
        [
            {
                "symbol": str(symbol or "").strip().upper(),
                "volumeUsd": round(to_num(volume_usd, 0.0), 2),
            }
            for symbol, volume_usd in (all_bucket.get("symbolVolumes") or {}).items()
            if str(symbol or "").strip()
        ],
        key=lambda item: item["volumeUsd"],
        reverse=True,
    )[:MAX_SYMBOL_BREAKDOWN]

    return {
        "wallet": wallet,
        "walletRecordId": build_wallet_record_id(wallet, row, state),
        "rank": 0,
        "trades": trades,
        "volumeUsd": round(volume_raw, 2),
        "volumeUsdRaw": round(volume_raw, 2),
        "volumeUsdVerified": round(volume_verified, 2) if volume_verified is not None else None,
        "rankingVolumeUsd": round(ranking_volume, 2) if ranking_volume is not None else None,
        "volumeStatus": status["volumeStatus"],
        "volumeStatusLabel": status["volumeStatusLabel"],
        "totalWins": wins,
        "totalLosses": losses,
        "pnlUsd": round(pnl_usd, 2),
        "winRate": round(win_rate, 2),
        "firstTrade": first_trade,
        "lastTrade": last_trade,
        "lastOpenedAt": current_activity_at,
        "updatedAt": updated_at,
        "lastActivity": current_activity_at,
        "openPositions": open_positions,
        "exposureUsd": round(exposure_usd, 2),
        "unrealizedPnlUsd": round(unrealized_pnl_usd, 2),
        "totalPnlUsd": round(total_pnl_usd, 2),
        "liveActiveRank": None,
        "liveActiveScore": 0,
        "recentEvents15m": 0,
        "recentEvents1h": 0,
        "freshness": {
            "updatedAt": updated_at,
            "historyPersistedAt": status["historyPersistedAt"],
        },
        "lifecycleStage": state.get("lifecycleStage"),
        "lifecycleStatus": state.get("lifecycleStage"),
        "backfillComplete": bool(state.get("tradeDone")) and bool(state.get("fundingDone")) and bool(state.get("remoteHistoryVerified")),
        "discoveredAt": int(state.get("discoveredAt") or 0) or None,
        "discoveredBy": state.get("discoveredBy"),
        "backfillCompletedAt": int(state.get("backfillCompletedAt") or 0) or None,
        "liveTrackingSince": int(state.get("liveTrackingSince") or 0) or None,
        "liveLastScanAt": int(state.get("liveLastScanAt") or state.get("lastScannedAt") or 0) or None,
        "validationStatus": "verified" if status["historyVerified"] else "pending",
        "validationCheckedAt": status["historyPersistedAt"] or updated_at,
        "discoveryConfidence": 1,
        "depositEvidenceCount": int(state.get("depositEvidenceCount") or 0),
        "restShardId": f"indexer_shard_{shard_index}",
        "restShardIndex": shard_index,
        "historyPhase": state.get("historyPhase"),
        "tradeRowsLoaded": int(state.get("tradeRowsLoaded") or file_audit.get("tradesStored") or trades),
        "fundingRowsLoaded": int(state.get("fundingRowsLoaded") or file_audit.get("fundingStored") or 0),
        "tradeDone": status["tradeDone"],
        "fundingDone": status["fundingDone"],
        "tradeHasMore": bool(state.get("tradeHasMore")),
        "fundingHasMore": bool(state.get("fundingHasMore")),
        "tradePagination": state.get("tradePagination") if isinstance(state.get("tradePagination"), dict) else None,
        "fundingPagination": state.get("fundingPagination") if isinstance(state.get("fundingPagination"), dict) else None,
        "retryPending": bool(state.get("retryPending")),
        "retryReason": state.get("retryReason"),
        "retryQueuedAt": int(state.get("retryQueuedAt") or 0) or None,
        "forceHeadRefetch": bool(state.get("forceHeadRefetch")),
        "forceHeadRefetchReason": state.get("forceHeadRefetchReason"),
        "forceHeadRefetchQueuedAt": int(state.get("forceHeadRefetchQueuedAt") or 0) or None,
        "lastAttemptMode": state.get("lastAttemptMode"),
        "lastSuccessMode": state.get("lastSuccessMode"),
        "lastFailureMode": state.get("lastFailureMode"),
        "historyAudit": state.get("historyAudit") if isinstance(state.get("historyAudit"), dict) else None,
        "storageRefs": storage_refs,
        "metricsReady": status["metricsReady"],
        "analyticsReady": status["metricsReady"],
        "tracked": True,
        "closedPositionsObserved": 0,
        "observedPositionEvents": 0,
        "positionLifecycle": None,
        "completeness": completeness,
        "trackingStatus": status["trackingStatus"],
        "trackingStatusLabel": status["trackingStatusLabel"],
        "tradeHistoryStatus": status["tradeHistoryStatus"],
        "tradeHistoryStatusLabel": status["tradeHistoryStatusLabel"],
        "metricsStatus": status["metricsStatus"],
        "metricsStatusLabel": status["metricsStatusLabel"],
        "historyVerified": status["historyVerified"],
        "metricsVerified": status["metricsVerified"],
        "historyPersistedAt": status["historyPersistedAt"],
        "tradesPersisted": status["tradesPersisted"],
        "fundingPersisted": status["fundingPersisted"],
        "fullBlockedReasons": status["fullBlockedReasons"],
        "blockerReasons": status["blockerReasons"],
        "leaderboardEligible": status["leaderboardEligible"],
        "symbols": symbols,
        "symbolBreakdown": symbol_breakdown,
        "searchText": f"{wallet.lower()} {' '.join(symbol.lower() for symbol in symbols)}",
        "d30VolumeUsd": round(to_num(d30.get("volumeUsd"), 0.0), 2),
    }


def summarize_pagination(summary):
    if not isinstance(summary, dict):
        return None
    return {
        "fetchedPages": int(summary.get("fetchedPages") or 0),
        "pendingPages": int(summary.get("pendingPages") or 0),
        "retryPages": int(summary.get("retryPages") or 0),
        "highestKnownPage": int(summary.get("highestKnownPage") or 0) or None,
        "totalKnownPages": int(summary.get("totalKnownPages") or 0) or None,
        "frontierCursor": summary.get("frontierCursor") or None,
        "exhausted": bool(summary.get("exhausted")),
        "lastSuccessfulPage": int(summary.get("lastSuccessfulPage") or 0) or None,
        "lastAttemptedPage": int(summary.get("lastAttemptedPage") or 0) or None,
        "lastSuccessAt": int(summary.get("lastSuccessAt") or 0) or None,
        "lastFailureAt": int(summary.get("lastFailureAt") or 0) or None,
    }


def build_compact_wallet_row(row: dict) -> dict:
    completeness = row.get("completeness") if isinstance(row.get("completeness"), dict) else {}
    warnings = completeness.get("pendingWarnings") if isinstance(completeness.get("pendingWarnings"), list) else []
    blocked = row.get("fullBlockedReasons") if isinstance(row.get("fullBlockedReasons"), list) else []
    return {
        "wallet": row.get("wallet"),
        "walletRecordId": row.get("walletRecordId"),
        "trades": int(row.get("trades") or 0),
        "volumeUsdRaw": round(to_num(row.get("volumeUsdRaw"), 0.0), 2),
        "volumeUsdVerified": round(to_num(row.get("volumeUsdVerified"), 0.0), 2)
        if row.get("volumeUsdVerified") is not None
        else None,
        "rankingVolumeUsd": round(to_num(row.get("rankingVolumeUsd"), 0.0), 2)
        if row.get("rankingVolumeUsd") is not None
        else None,
        "totalWins": int(row.get("totalWins") or 0),
        "totalLosses": int(row.get("totalLosses") or 0),
        "pnlUsd": round(to_num(row.get("pnlUsd"), 0.0), 2),
        "winRate": round(to_num(row.get("winRate"), 0.0), 2),
        "firstTrade": int(row.get("firstTrade") or 0) or None,
        "lastTrade": int(row.get("lastTrade") or 0) or None,
        "updatedAt": int(row.get("updatedAt") or 0) or None,
        "lastActivity": int(row.get("lastActivity") or 0) or None,
        "openPositions": int(row.get("openPositions") or 0),
        "exposureUsd": round(to_num(row.get("exposureUsd"), 0.0), 2),
        "unrealizedPnlUsd": round(to_num(row.get("unrealizedPnlUsd"), 0.0), 2),
        "totalPnlUsd": round(to_num(row.get("totalPnlUsd"), 0.0), 2),
        "lifecycleStage": row.get("lifecycleStage"),
        "lifecycleStatus": row.get("lifecycleStatus"),
        "backfillComplete": bool(row.get("backfillComplete")),
        "discoveredAt": int(row.get("discoveredAt") or 0) or None,
        "discoveredBy": row.get("discoveredBy"),
        "backfillCompletedAt": int(row.get("backfillCompletedAt") or 0) or None,
        "liveTrackingSince": int(row.get("liveTrackingSince") or 0) or None,
        "liveLastScanAt": int(row.get("liveLastScanAt") or 0) or None,
        "validationStatus": row.get("validationStatus"),
        "validationCheckedAt": int(row.get("validationCheckedAt") or 0) or None,
        "discoveryConfidence": to_num(row.get("discoveryConfidence"), None),
        "depositEvidenceCount": int(row.get("depositEvidenceCount") or 0),
        "restShardId": row.get("restShardId"),
        "restShardIndex": int(row.get("restShardIndex") or 0) if row.get("restShardIndex") is not None else None,
        "historyPhase": row.get("historyPhase"),
        "tradeRowsLoaded": int(row.get("tradeRowsLoaded") or 0),
        "fundingRowsLoaded": int(row.get("fundingRowsLoaded") or 0),
        "tradeDone": bool(row.get("tradeDone")),
        "fundingDone": bool(row.get("fundingDone")),
        "tradeHasMore": bool(row.get("tradeHasMore")),
        "fundingHasMore": bool(row.get("fundingHasMore")),
        "remoteHistoryVerified": bool(row.get("historyVerified")),
        "tradePagination": summarize_pagination(row.get("tradePagination")),
        "fundingPagination": summarize_pagination(row.get("fundingPagination")),
        "retryPending": bool(row.get("retryPending")),
        "retryReason": row.get("retryReason"),
        "retryQueuedAt": int(row.get("retryQueuedAt") or 0) or None,
        "forceHeadRefetch": bool(row.get("forceHeadRefetch")),
        "forceHeadRefetchReason": row.get("forceHeadRefetchReason"),
        "forceHeadRefetchQueuedAt": int(row.get("forceHeadRefetchQueuedAt") or 0) or None,
        "lastAttemptMode": row.get("lastAttemptMode"),
        "lastSuccessMode": row.get("lastSuccessMode"),
        "lastFailureMode": row.get("lastFailureMode"),
        "metricsReady": bool(row.get("metricsReady")),
        "analyticsReady": bool(row.get("analyticsReady")),
        "tracked": bool(row.get("tracked")),
        "symbols": list(row.get("symbols") or []),
        "symbolBreakdown": list(row.get("symbolBreakdown") or []),
        "d30VolumeUsd": round(to_num(row.get("d30VolumeUsd"), 0.0), 2),
        "completeness": {
            "historyVerified": bool(completeness.get("historyVerified")),
            "metricsVerified": bool(completeness.get("metricsVerified")),
            "leaderboardEligible": bool(completeness.get("leaderboardEligible")),
            "trackingState": row.get("trackingStatus"),
            "trackingLabel": row.get("trackingStatusLabel"),
            "tradeHistoryState": row.get("tradeHistoryStatus"),
            "tradeHistoryLabel": row.get("tradeHistoryStatusLabel"),
            "metricsState": row.get("metricsStatus"),
            "metricsLabel": row.get("metricsStatusLabel"),
            "volumeExactness": row.get("volumeStatus"),
            "volumeExactnessLabel": row.get("volumeStatusLabel"),
            "historyPersistedAt": int(row.get("historyPersistedAt") or 0) or None,
            "tradesPersisted": int(row.get("tradesPersisted") or 0) or None,
            "fundingPersisted": int(row.get("fundingPersisted") or 0) or None,
            "pendingReasons": blocked,
            "blockerReasons": blocked,
            "fullBlockedReasons": blocked,
            "leaderboardBlockedReasons": blocked,
            "warnings": warnings,
            "pendingWarnings": warnings,
        },
    }


def query_default_page(rows: list[dict], generated_at: int, counts: dict, top_correction: dict) -> dict:
    rankable = [row for row in rows if row.get("rankingVolumeUsd") is not None]
    blocked = [row for row in rows if row.get("rankingVolumeUsd") is None]
    rankable.sort(
        key=lambda row: (
            to_num(row.get("rankingVolumeUsd"), 0.0),
            to_num(row.get("volumeUsdRaw"), 0.0),
            int(row.get("updatedAt") or 0),
            row.get("wallet") or "",
        ),
        reverse=True,
    )
    blocked.sort(
        key=lambda row: (
            to_num(row.get("volumeUsdRaw"), 0.0),
            int(row.get("updatedAt") or 0),
            row.get("wallet") or "",
        ),
        reverse=True,
    )
    ordered = rankable + blocked
    for index, row in enumerate(ordered, start=1):
        row["rank"] = index
    page_rows = ordered[:20]
    total = len(ordered)
    return {
        "generatedAt": generated_at,
        "timeframe": "all",
        "query": {
            "timeframe": "all",
            "page": 1,
            "pageSize": 20,
            "sort": "rankingVolumeUsd",
            "dir": "desc",
        },
        "sorting": {
            "key": "rankingVolumeUsd",
            "dir": "desc",
            "label": "Trusted Volume Rank",
            "shortLabel": "Trusted Rank",
            "description": "Only history-verified and metrics-verified wallets contribute to the primary rank.",
        },
        "total": total,
        "page": 1,
        "pageSize": 20,
        "pages": max(1, math.ceil(total / 20)),
        "rows": page_rows,
        "filters": {},
        "counts": counts,
        "pagination": {
            "hasPrev": False,
            "hasNext": total > 20,
        },
        "performance": {
            "datasetMode": "offline_rebuilt_snapshot",
        },
        "topCorrection": top_correction,
    }


def build_top_correction(rows: list[dict], generated_at: int) -> dict:
    raw_order = sorted(
        rows,
        key=lambda row: (to_num(row.get("volumeUsdRaw"), 0.0), int(row.get("updatedAt") or 0), row.get("wallet") or ""),
        reverse=True,
    )[:100]
    summary = {
        "total": len(raw_order),
        "backfillComplete": sum(1 for row in raw_order if row.get("tradeDone") and row.get("fundingDone") and row.get("remoteHistoryVerified")),
        "remoteHistoryVerified": sum(1 for row in raw_order if row.get("remoteHistoryVerified")),
        "historyVerified": sum(1 for row in raw_order if row.get("historyVerified")),
        "metricsVerified": sum(1 for row in raw_order if row.get("metricsVerified")),
        "leaderboardEligible": sum(1 for row in raw_order if row.get("leaderboardEligible")),
        "volumeExact": sum(1 for row in raw_order if row.get("volumeStatus") == "exact"),
        "rankBlocked": sum(1 for row in raw_order if not row.get("leaderboardEligible")),
        "forceHeadRefetch": sum(1 for row in raw_order if row.get("forceHeadRefetch")),
        "retryPending": sum(1 for row in raw_order if row.get("retryPending")),
        "tradePagesPending": sum(
            int(((row.get("tradePagination") or {}).get("pendingPages") or 0))
            for row in raw_order
        ),
        "fundingPagesPending": sum(
            int(((row.get("fundingPagination") or {}).get("pendingPages") or 0))
            for row in raw_order
        ),
    }
    wallets = []
    for index, row in enumerate(raw_order, start=1):
        wallets.append(
            {
                "rank": index,
                "wallet": row.get("wallet"),
                "walletRecordId": row.get("walletRecordId"),
                "rawVolumeUsd": round(to_num(row.get("volumeUsdRaw"), 0.0), 2),
                "trackingState": row.get("trackingStatus"),
                "tradeHistoryState": row.get("tradeHistoryStatus"),
                "metricsState": row.get("metricsStatus"),
                "leaderboardEligible": bool(row.get("leaderboardEligible")),
                "blockerReasons": row.get("fullBlockedReasons") or [],
            }
        )
    return {
        "generatedAt": generated_at,
        "cohortSize": 100,
        "summary": summary,
        "wallets": wallets,
    }


def build_summary_counts(rows: list[dict]) -> dict:
    counts = {
        "totalWallets": len(rows),
        "discoveredWallets": len(rows),
        "validatedWallets": sum(1 for row in rows if row.get("remoteHistoryVerified")),
        "backfilledWallets": sum(1 for row in rows if row.get("tradeDone") and row.get("fundingDone")),
        "trackedWallets": len(rows),
        "trackedLiteWallets": sum(1 for row in rows if row.get("trackingStatus") == "tracked_lite"),
        "trackedFullWallets": sum(1 for row in rows if row.get("trackingStatus") == "tracked_full"),
        "metricsReadyWallets": sum(1 for row in rows if row.get("metricsReady")),
        "metricsCompleteWallets": sum(1 for row in rows if row.get("metricsVerified")),
        "metricsPartialWallets": sum(1 for row in rows if row.get("metricsStatus") == "metrics_partial"),
        "metricsPendingWallets": sum(1 for row in rows if row.get("metricsStatus") == "metrics_pending"),
        "tradeHistoryCompleteWallets": sum(1 for row in rows if row.get("historyVerified")),
        "tradeHistoryPartialWallets": sum(1 for row in rows if row.get("tradeHistoryStatus") == "partial_trade_history"),
        "historyAuditPendingWallets": sum(1 for row in rows if row.get("retryPending")),
        "recentHistoryOnlyWallets": 0,
        "activationOnlyWallets": sum(1 for row in rows if row.get("tradeHistoryStatus") == "activation_only"),
        "paginationCheckpointWallets": sum(
            1
            for row in rows
            if isinstance(row.get("tradePagination"), dict) or isinstance(row.get("fundingPagination"), dict)
        ),
        "paginationFrontierWallets": sum(
            1
            for row in rows
            if ((row.get("tradePagination") or {}).get("frontierCursor"))
            or ((row.get("fundingPagination") or {}).get("frontierCursor"))
        ),
        "paginationRetryWallets": sum(
            1
            for row in rows
            if int(((row.get("tradePagination") or {}).get("retryPages") or 0)) > 0
            or int(((row.get("fundingPagination") or {}).get("retryPages") or 0)) > 0
        ),
        "paginationCompleteWallets": sum(
            1
            for row in rows
            if pagination_complete(row.get("tradePagination")) and pagination_complete(row.get("fundingPagination"))
        ),
        "tradePagesFetched": sum(int(((row.get("tradePagination") or {}).get("fetchedPages") or 0)) for row in rows),
        "fundingPagesFetched": sum(int(((row.get("fundingPagination") or {}).get("fetchedPages") or 0)) for row in rows),
        "tradePagesPending": sum(int(((row.get("tradePagination") or {}).get("pendingPages") or 0)) for row in rows),
        "fundingPagesPending": sum(int(((row.get("fundingPagination") or {}).get("pendingPages") or 0)) for row in rows),
        "tradePagesFailed": sum(int(((row.get("tradePagination") or {}).get("retryPages") or 0)) for row in rows),
        "fundingPagesFailed": sum(int(((row.get("fundingPagination") or {}).get("retryPages") or 0)) for row in rows),
        "leaderboardEligibleWallets": sum(1 for row in rows if row.get("leaderboardEligible")),
        "leaderboardBlockedWallets": sum(1 for row in rows if not row.get("leaderboardEligible")),
    }
    return counts


def load_sources():
    rows = {}
    states = {}
    duplicates = 0
    for shard_index in range(ACTIVE_SHARDS):
        shard_dir = SHARDS_DIR / f"shard_{shard_index}"
        wallet_payload = read_json(shard_dir / "wallets.json") or {}
        for wallet, row in (wallet_payload.get("wallets") or {}).items():
            wallet = norm_wallet(wallet)
            if not wallet or not isinstance(row, dict):
                continue
            existing = rows.get(wallet)
            if existing is not None:
                duplicates += 1
            if existing is None or row_timestamp(row) >= row_timestamp(existing["row"]):
                rows[wallet] = {"row": row, "shard": shard_index}
        state_payload = read_json(shard_dir / "indexer_state.json") or {}
        for wallet, state in (state_payload.get("walletStates") or {}).items():
            wallet = norm_wallet(wallet)
            if not wallet or not isinstance(state, dict):
                continue
            existing = states.get(wallet)
            if existing is None or state_timestamp(state) >= state_timestamp(existing["state"]):
                states[wallet] = {"state": state, "shard": shard_index}
    return rows, states, duplicates


def build_runtime_status(counts: dict, exchange: dict, generated_at: int) -> dict:
    progress = {
        "discovered": counts["discoveredWallets"],
        "validated": counts["validatedWallets"],
        "backfilled": counts["backfilledWallets"],
        "currentlyTracked": counts["trackedWallets"],
        "trackedLite": counts["trackedLiteWallets"],
        "trackedFull": counts["trackedFullWallets"],
        "metricsComplete": counts["metricsCompleteWallets"],
        "leaderboardEligible": counts["leaderboardEligibleWallets"],
        "tradePagesFetched": counts["tradePagesFetched"],
        "fundingPagesFetched": counts["fundingPagesFetched"],
        "tradePagesPending": counts["tradePagesPending"],
        "fundingPagesPending": counts["fundingPagesPending"],
        "tradePagesFailed": counts["tradePagesFailed"],
        "fundingPagesFailed": counts["fundingPagesFailed"],
        "completionRatio": counts["tradeHistoryCompleteWallets"] / counts["totalWallets"] if counts["totalWallets"] else 0,
        "mode": "snapshot_rebuilt",
        "updatedAt": generated_at,
    }
    return {
        "generatedAt": generated_at,
        "status": {
            "generatedAt": generated_at,
            "sourceCount": 1,
            "activeSourceCount": 1,
            "summary": {
                "walletExplorerProgress": progress,
                "exchangeOverview": exchange,
            },
            "services": [
                {"id": "ui", "name": "UI/API", "status": "ready"},
                {"id": "walletReadModel", "name": "Wallet Read Model", "status": "ready"},
            ],
            "startup": {
                "phase": "ready",
                "readyAt": generated_at,
                "steps": {},
            },
        },
    }


def main() -> int:
    source_rows, source_states, duplicates = load_sources()
    wallets = sorted(set(source_rows) | set(source_states))
    generated_at = max(
        [row_timestamp(value["row"]) for value in source_rows.values()] +
        [state_timestamp(value["state"]) for value in source_states.values()] +
        [int(GLOBAL_KPI_PATH.stat().st_mtime * 1000) if GLOBAL_KPI_PATH.exists() else 0, int(UI_DIR.stat().st_mtime * 1000) if UI_DIR.exists() else 0, 0]
    )
    prepared_rows = []
    symbol_set = set()
    partial_breakdown = {}
    for wallet in wallets:
        row_wrapper = source_rows.get(wallet) or {}
        state_wrapper = source_states.get(wallet) or {}
        shard_index = row_wrapper.get("shard", state_wrapper.get("shard", -1))
        row = dict(row_wrapper.get("row") or {"wallet": wallet})
        state = dict(state_wrapper.get("state") or {"wallet": wallet})
        prepared_row = build_prepared_row(wallet, row, state, shard_index if shard_index >= 0 else 0)
        for symbol in prepared_row.get("symbols") or []:
            symbol_set.add(symbol)
        if not prepared_row["historyVerified"]:
            reason_key = ",".join(prepared_row["fullBlockedReasons"][:3]) or "unknown"
            partial_breakdown[reason_key] = partial_breakdown.get(reason_key, 0) + 1
        prepared_rows.append(prepared_row)

    counts = build_summary_counts(prepared_rows)
    top_correction = build_top_correction(prepared_rows, generated_at)
    default_page_payload = query_default_page(prepared_rows, generated_at, counts, top_correction)
    preview_payload = {
        "version": PREVIEW_VERSION,
        "generatedAt": generated_at,
        "mode": "persisted_preview_snapshot",
        "preparedRows": prepared_rows,
        "availableSymbols": sorted(symbol_set),
        "summaryCounts": counts,
        "topCorrection": top_correction,
    }
    default_page_snapshot = {
        "version": DEFAULT_PAGE_VERSION,
        "generatedAt": generated_at,
        "sourceGeneratedAt": generated_at,
        "defaultQuery": default_page_payload["query"],
        "payload": default_page_payload,
    }

    global_kpi = read_json(GLOBAL_KPI_PATH) or {}
    market_total = to_num(global_kpi.get("totalHistoricalVolume"), 0.0)
    wallet_total_raw = sum(to_num(row.get("volumeUsdRaw"), 0.0) for row in prepared_rows)
    wallet_total_verified = sum(to_num(row.get("volumeUsdVerified"), 0.0) for row in prepared_rows)
    gap_usd = market_total - wallet_total_raw
    exchange_summary = {
        "marketTotalVolumeUsd": round(market_total, 2),
        "walletTotalRawVolumeUsd": round(wallet_total_raw, 2),
        "walletTotalVerifiedVolumeUsd": round(wallet_total_verified, 2),
        "gapUsd": round(gap_usd, 2),
        "gapReason": "wallet_history_incomplete_or_definition_gap" if abs(gap_usd) >= 0.01 else "matched",
        "walletToExchangeRatio": round((wallet_total_raw / market_total), 8) if market_total > 0 else None,
        "dailyVolumeUsd": round(to_num(global_kpi.get("dailyVolume"), 0.0), 2),
        "openInterestUsd": round(to_num(global_kpi.get("openInterestAtEnd"), 0.0), 2),
        "updatedAt": int(global_kpi.get("updatedAt") or global_kpi.get("fetchedAt") or 0) or None,
    }
    runtime_status_summary = build_runtime_status(counts, exchange_summary, generated_at)
    validation_report = {
        "generatedAt": generated_at,
        "wallets": {
            "total": counts["totalWallets"],
            "historyVerified": counts["tradeHistoryCompleteWallets"],
            "metricsVerified": counts["metricsCompleteWallets"],
            "leaderboardEligible": counts["leaderboardEligibleWallets"],
            "partial": counts["totalWallets"] - counts["tradeHistoryCompleteWallets"],
            "duplicateWalletRowsObservedDuringRead": duplicates,
            "partialReasonBreakdownTop": sorted(
                [{"reason": key, "count": value} for key, value in partial_breakdown.items()],
                key=lambda item: item["count"],
                reverse=True,
            )[:20],
        },
        "exchangeOverview": exchange_summary,
    }
    wallet_dataset = {
        "version": WALLET_DATASET_VERSION,
        "generatedAt": generated_at,
        "rows": prepared_rows,
        "counts": counts,
        "topCorrection": top_correction,
        "exchangeOverview": exchange_summary,
        "validation": validation_report,
    }
    compact_rows = [build_compact_wallet_row(row) for row in prepared_rows]
    wallet_dataset_compact = {
        "version": WALLET_DATASET_COMPACT_VERSION,
        "generatedAt": generated_at,
        "rows": compact_rows,
        "counts": counts,
        "topCorrection": top_correction,
        "exchangeOverview": exchange_summary,
        "validation": validation_report,
        "availableSymbols": sorted(symbol_set),
    }
    wallet_tracking_manifest = {
        "version": WALLET_TRACKING_MANIFEST_VERSION,
        "generatedAt": generated_at,
        "dataset": {
            "generatedAt": wallet_dataset_compact["generatedAt"],
            "rowCount": len(compact_rows),
            "counts": counts,
            "exchangeOverview": exchange_summary,
            "availableSymbols": len(symbol_set),
            "paths": {
                "compact": "wallet_explorer_v3/manifest.json",
                "defaultPage": "wallets_page1_default.json",
                "statusSummary": "system_status_summary.json",
                "validation": "validation_report.json",
            },
        },
        "defaultWalletPage": {
            "generatedAt": default_page_snapshot["generatedAt"],
            "sourceGeneratedAt": default_page_snapshot["sourceGeneratedAt"],
            "rowCount": len(default_page_payload.get("rows") or []),
            "query": default_page_payload.get("query") or {},
        },
        "statusSummary": runtime_status_summary["status"],
        "validation": {
            "generatedAt": validation_report["generatedAt"],
            "partialWallets": validation_report["wallets"]["partial"],
            "duplicateWalletRowsObservedDuringRead": validation_report["wallets"][
                "duplicateWalletRowsObservedDuringRead"
            ],
        },
    }

    write_json_atomic(UI_DIR / "wallets_preview.json", preview_payload)
    write_json_atomic(UI_DIR / "wallets_page1_default.json", default_page_snapshot)
    write_json_atomic(UI_DIR / "system_status_summary.json", runtime_status_summary)
    write_json_atomic(UI_DIR / "validation_report.json", validation_report)
    write_json_atomic(UI_DIR / "wallet_tracking_manifest.json", wallet_tracking_manifest)
    write_json_atomic(V3_BUILD_SOURCE_PATH, wallet_dataset_compact)
    build_wallet_storage_v3(V3_BUILD_SOURCE_PATH)

    print(
        json.dumps(
            {
                "generatedAt": generated_at,
                "wallets": counts["totalWallets"],
                "historyVerified": counts["tradeHistoryCompleteWallets"],
                "metricsVerified": counts["metricsCompleteWallets"],
                "leaderboardEligible": counts["leaderboardEligibleWallets"],
                "duplicates": duplicates,
                "walletTotalRawVolumeUsd": round(wallet_total_raw, 2),
                "walletTotalVerifiedVolumeUsd": round(wallet_total_verified, 2),
                "marketTotalVolumeUsd": round(market_total, 2),
                "gapUsd": round(gap_usd, 2),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
