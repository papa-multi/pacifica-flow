#!/usr/bin/env python3
import argparse
import shutil
import subprocess
from pathlib import Path

from common import (
    ARCHIVE_DIR,
    DATA_DIR,
    DEFAULT_SHARD_COUNT,
    SHARDS_DIR,
    UI_DIR,
    V2_DIR,
    build_exchange_overview,
    list_wallets_from_sources,
    now_ms,
    read_json,
    shard_addresses_path,
    shard_index_for_wallet,
    write_json_atomic,
)


LEGACY_PROCESSED_PATHS = [
    DATA_DIR / "indexer" / "shards",
    DATA_DIR / "indexer" / "wallets.json",
    DATA_DIR / "indexer" / "indexer_state.json",
    DATA_DIR / "indexer" / "wallet_history",
    UI_DIR / "wallets_page1_default.json",
    UI_DIR / "system_status_summary.json",
    UI_DIR / "wallet_tracking_manifest.json",
    UI_DIR / "validation_report.json",
]

WALLET_STORAGE_V3_BUILD_SCRIPT = Path("/root/pacifica-flow/scripts/build_wallet_storage_v3.js")
V3_BUILD_SOURCE_PATH = UI_DIR / ".build" / "wallet_dataset_compact_source.json"


def archive_legacy_processed_data(timestamp: int) -> list:
    archive_root = ARCHIVE_DIR / f"wallet_explorer_reset_{timestamp}"
    archive_root.mkdir(parents=True, exist_ok=True)
    archived = []
    for path in LEGACY_PROCESSED_PATHS:
        if not path.exists():
            continue
        destination = archive_root / path.relative_to(DATA_DIR)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if path.is_dir():
            shutil.copytree(str(path), str(destination), dirs_exist_ok=True)
        else:
            shutil.copy2(str(path), str(destination))
        archived.append({"from": str(path), "to": str(destination)})
    return archived


def build_empty_ui_payload(addresses_payload: dict) -> None:
    generated_at = now_ms()
    rows = []
    for index, item in enumerate(addresses_payload.get("wallets") or [], start=1):
        wallet = item["wallet"]
        rows.append(
            {
                "wallet": wallet,
                "walletRecordId": f"wal_placeholder_{index}",
                "rank": index,
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
        )
    overview = build_exchange_overview()
    compact = {
        "version": 2,
        "generatedAt": generated_at,
        "rows": rows,
        "counts": {
            "totalWallets": len(rows),
            "trackedWallets": len(rows),
            "trackedLiteWallets": len(rows),
            "trackedFullWallets": 0,
            "tradeHistoryCompleteWallets": 0,
            "metricsCompleteWallets": 0,
            "leaderboardEligibleWallets": 0,
            "validatedWallets": 0,
            "backfilledWallets": 0,
            "tradePagesFetched": 0,
            "fundingPagesFetched": 0,
            "tradePagesPending": len(rows),
            "fundingPagesPending": len(rows),
            "tradePagesFailed": 0,
            "fundingPagesFailed": 0,
        },
        "topCorrection": {
            "cohortTotal": min(100, len(rows)),
            "historyVerified": 0,
            "metricsVerified": 0,
            "leaderboardEligible": 0,
            "tradePagesPending": min(100, len(rows)),
            "fundingPagesPending": min(100, len(rows)),
        },
        "exchangeOverview": overview,
        "validation": {
            "generatedAt": generated_at,
            "marketTotalVolumeUsd": overview["totalHistoricalVolumeUsd"],
            "walletTotalRawVolumeUsd": 0,
            "walletTotalVerifiedVolumeUsd": 0,
            "gapUsd": round(overview["totalHistoricalVolumeUsd"], 2),
            "gapReason": "wallet_history_backfill_pending",
        },
        "availableSymbols": [],
    }
    default_payload = {
        "generatedAt": generated_at,
        "timeframe": "all",
        "query": {"q": "", "page": 1, "pageSize": 20, "sort": "rankingVolumeUsd", "dir": "desc"},
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
        "pageSize": 20,
        "pages": max(1, (len(rows) + 19) // 20),
        "rows": rows[:20],
        "counts": compact["counts"],
        "exchangeOverview": overview,
        "validation": compact["validation"],
        "availableSymbols": [],
        "warmup": {"persistedPreview": True, "liveEnrichmentPending": False},
    }
    status = {
        "generatedAt": generated_at,
        "status": {
            "generatedAt": generated_at,
            "sourceCount": 1,
            "activeSourceCount": 1,
            "summary": {
                "systemMode": "wallet_explorer_v2_reset",
                "walletExplorerProgress": {
                    "discovered": len(rows),
                    "validated": 0,
                    "backfilled": 0,
                    "currentlyTracked": len(rows),
                    "trackedLite": len(rows),
                    "trackedFull": 0,
                    "metricsComplete": 0,
                    "leaderboardEligible": 0,
                    "tradePagesFetched": 0,
                    "fundingPagesFetched": 0,
                    "tradePagesPending": len(rows),
                    "fundingPagesPending": len(rows),
                    "tradePagesFailed": 0,
                    "fundingPagesFailed": 0,
                    "completionRatio": 0,
                    "mode": "wallet_explorer_v2",
                    "updatedAt": generated_at,
                },
                "exchangeOverview": overview,
            },
            "services": [
                {"id": "walletExplorerV2", "name": "Wallet Explorer V2", "status": "starting"},
                {"id": "walletExplorerSnapshots", "name": "Wallet Explorer Snapshots", "status": "ready"},
            ],
            "startup": {"phase": "ready", "readyAt": generated_at, "steps": {}},
        },
    }
    manifest = {
        "version": 2,
        "generatedAt": generated_at,
        "dataset": {
            "generatedAt": generated_at,
            "counts": compact["counts"],
            "exchangeOverview": overview,
        },
        "files": {
            "walletExplorerV3Path": str(UI_DIR.parent / "wallet_explorer_v3" / "manifest.json"),
            "walletsPage1Default": str(UI_DIR / "wallets_page1_default.json"),
            "systemStatusSummary": str(UI_DIR / "system_status_summary.json"),
            "validationReport": str(UI_DIR / "validation_report.json"),
        },
    }
    write_json_atomic(V3_BUILD_SOURCE_PATH, compact)
    subprocess.run(
        ["node", str(WALLET_STORAGE_V3_BUILD_SCRIPT), f"--source={V3_BUILD_SOURCE_PATH}"],
        check=True,
    )
    # Preserve legacy files if they already exist; reset only refreshes the v3 store.
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
    write_json_atomic(UI_DIR / "system_status_summary.json", status)
    write_json_atomic(UI_DIR / "validation_report.json", compact["validation"])
    write_json_atomic(UI_DIR / "wallet_tracking_manifest.json", manifest)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shards", type=int, default=DEFAULT_SHARD_COUNT)
    parser.add_argument("--skip-archive", action="store_true")
    args = parser.parse_args()

    timestamp = now_ms()
    addresses_payload = list_wallets_from_sources()
    addresses_payload["version"] = 1
    addresses_payload["total"] = len(addresses_payload["wallets"])

    archived = [] if args.skip_archive else archive_legacy_processed_data(timestamp)

    V2_DIR.mkdir(parents=True, exist_ok=True)
    SHARDS_DIR.mkdir(parents=True, exist_ok=True)
    write_json_atomic(V2_DIR / "wallet_addresses.json", addresses_payload)

    shard_wallets = {index: [] for index in range(args.shards)}
    for item in addresses_payload["wallets"]:
        wallet = item["wallet"]
        shard_wallets[shard_index_for_wallet(wallet, args.shards)].append(wallet)

    for shard_index in range(args.shards):
        shard_dir = SHARDS_DIR / f"shard_{shard_index}"
        (shard_dir / "wallet_state").mkdir(parents=True, exist_ok=True)
        (shard_dir / "wallet_history").mkdir(parents=True, exist_ok=True)
        (shard_dir / "wallet_records").mkdir(parents=True, exist_ok=True)
        write_json_atomic(
            shard_addresses_path(shard_index),
            {
                "version": 1,
                "generatedAt": timestamp,
                "shardIndex": shard_index,
                "shardCount": args.shards,
                "wallets": shard_wallets[shard_index],
            },
        )
        write_json_atomic(
            shard_dir / "progress.json",
            {
                "version": 1,
                "generatedAt": timestamp,
                "updatedAt": timestamp,
                "shardIndex": shard_index,
                "shardCount": args.shards,
                "walletsTotal": len(shard_wallets[shard_index]),
                "walletsComplete": 0,
                "walletsPartial": 0,
                "lastWallet": None,
                "proxyPool": {"total": 0, "cooling": 0, "active": 0},
            },
        )

    build_empty_ui_payload(addresses_payload)
    write_json_atomic(
        V2_DIR / "reset_manifest.json",
        {
            "version": 1,
            "generatedAt": timestamp,
            "archived": archived,
            "walletsTotal": len(addresses_payload["wallets"]),
            "shards": args.shards,
        },
    )
    print(f"wallet_explorer_v2 reset complete wallets={len(addresses_payload['wallets'])} shards={args.shards}")


if __name__ == "__main__":
    main()
