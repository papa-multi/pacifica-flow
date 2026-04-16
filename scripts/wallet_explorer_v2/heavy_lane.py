#!/usr/bin/env python3
import argparse
import csv
import gzip
import json
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from common import (
    ProxyPool,
    V2_DIR,
    isolated_wallet_record_path,
    isolated_wallets_path,
    load_isolated_wallets,
    load_proxies,
    now_ms,
    phase1_probe_path,
    read_json,
    write_json_atomic,
)
from process_isolated_wallet import execute_wallet


HEAVY_STATE_PATH = Path(
    os.environ.get("PACIFICA_WALLET_EXPLORER_V2_HEAVY_STATE_PATH")
    or (V2_DIR / "heavy_lane_state.json")
)
HEAVY_CANDIDATES_PATH = Path(
    os.environ.get("PACIFICA_WALLET_EXPLORER_V2_HEAVY_CANDIDATES_PATH")
    or (V2_DIR / "heavy_lane_candidates.json")
)
V3_MANIFEST_PATH = Path("/root/pacifica-flow/data/wallet_explorer_v3/manifest.json")
PROCESS_SCRIPT_PATH = Path("/root/pacifica-flow/scripts/wallet_explorer_v2/process_isolated_wallet.py")


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
                    wallet = str((row or {}).get("wallet") or "").strip()
                    if wallet:
                        wallets.add(wallet)
            else:
                handle.seek(0)
                for line in handle:
                    wallet = str(line or "").strip()
                    if wallet and wallet.lower() != "wallet":
                        wallets.add(wallet.split(",")[0].strip())
    except Exception:
        return set()
    return wallets


def load_v3_rows():
    if not V3_MANIFEST_PATH.exists():
        return {"rows": []}, []
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
        return {"manifest": manifest, "rows": rows}, rows
    except Exception:
        return {"rows": []}, []


def overlay_record_fields(base_row, record):
    row = dict(base_row or {})
    safe = record if isinstance(record, dict) else {}
    if not safe:
        return row
    for key in (
        "wallet",
        "updatedAt",
        "backfillComplete",
        "proofHeadComplete",
        "proofLocalComplete",
        "proofClass",
        "historyProofStatus",
        "historyVerifiedThroughNow",
        "tradeRowsLoaded",
        "fundingRowsLoaded",
        "volumeUsdRaw",
        "firstTrade",
        "lastTrade",
        "feesPaidUsd",
        "pnlUsd",
        "wins",
        "losses",
        "nextEligibleAt",
        "phase1",
        "proof",
    ):
        if key in safe:
            row[key] = safe.get(key)
    row.setdefault("tradePagination", safe.get("tradePagination") if isinstance(safe.get("tradePagination"), dict) else {})
    row.setdefault("fundingPagination", safe.get("fundingPagination") if isinstance(safe.get("fundingPagination"), dict) else {})
    return row


def overlay_probe_fields(base_row, wallet: str):
    row = dict(base_row or {})
    probe = read_json(phase1_probe_path(wallet), None)
    if not isinstance(probe, dict):
        return row
    row["updatedAt"] = int(probe.get("updatedAt") or row.get("updatedAt") or 0) or row.get("updatedAt")
    row["nextEligibleAt"] = int(probe.get("nextEligibleAt") or 0) or None
    row["retryPending"] = bool(int(probe.get("nextEligibleAt") or 0) > now_ms())
    row["retryReason"] = str(probe.get("error") or row.get("retryReason") or "").strip() or None
    phase1 = row.get("phase1") if isinstance(row.get("phase1"), dict) else {}
    phase1.update(
        {
            "status": str(probe.get("status") or phase1.get("status") or "").strip().lower() or None,
            "checkedAt": int(probe.get("checkedAt") or phase1.get("checkedAt") or 0) or None,
        }
    )
    row["phase1"] = phase1
    row["hasLocalRecord"] = False
    return row


def classify_phase1_lane_mode(row: dict) -> str:
    safe = row if isinstance(row, dict) else {}
    phase1 = safe.get("phase1") if isinstance(safe.get("phase1"), dict) else {}
    phase1_status = str(phase1.get("status") or "").strip().lower()
    has_record = bool(safe.get("hasLocalRecord"))
    trade_rows = int(safe.get("tradeRowsLoaded") or safe.get("trades") or 0)
    volume_usd = float(safe.get("volumeUsdRaw") or 0.0)
    fees_usd = float(safe.get("feesPaidUsd") or safe.get("feesUsd") or 0.0)
    pnl_usd = float(safe.get("pnlUsd") or safe.get("totalPnlUsd") or 0.0)
    wins = int(safe.get("totalWins") or safe.get("wins") or 0)
    losses = int(safe.get("totalLosses") or safe.get("losses") or 0)
    local_complete = bool(phase1.get("localComplete") or safe.get("proofLocalComplete"))
    metrics_recovered = bool(
        phase1.get("metricsRecovered")
        or trade_rows > 0
        or abs(volume_usd) > 0
        or abs(fees_usd) > 0
        or abs(pnl_usd) > 0
        or wins > 0
        or losses > 0
        or phase1_status == "non_empty"
    )
    zero_history_result = bool(
        phase1.get("zeroHistoryResult")
        or local_complete
        and trade_rows <= 0
        and abs(volume_usd) <= 0
        and abs(fees_usd) <= 0
        and abs(pnl_usd) <= 0
        and wins <= 0
        and losses <= 0
        and not bool(safe.get("lastTrade"))
    )
    verified = bool(phase1.get("verifiedThroughNow") or safe.get("proofHeadComplete"))
    if verified:
        return "tracked"
    if zero_history_result:
        return "recovery"
    if phase1_status in {"phase1_head_mismatch_remote_newer", "phase1_head_mismatch_remote_empty", "phase1_trade_backfill_incomplete"}:
        return "recovery"
    if local_complete:
        return "proof"
    if metrics_recovered or has_record:
        return "recovery"
    return "probe"


def load_isolated_entries():
    payload = read_json(isolated_wallets_path(), {"wallets": []}) or {"wallets": []}
    rows = payload.get("wallets") or []
    if not isinstance(rows, list):
        rows = []
    known_wallets = set()
    normalized_rows = []
    for entry in rows:
        wallet = wallet_from_entry(entry)
        if not wallet or wallet in known_wallets:
            continue
        known_wallets.add(wallet)
        normalized_rows.append(entry if isinstance(entry, dict) else {"wallet": wallet})
    for wallet in sorted(load_isolated_wallets()):
        if wallet in known_wallets:
            continue
        known_wallets.add(wallet)
        normalized_rows.append({"wallet": wallet})
    return normalized_rows


def wallet_from_entry(entry):
    if isinstance(entry, dict):
        return str(entry.get("wallet") or "").strip()
    return str(entry or "").strip()


def load_heavy_state():
    defaults = {
        "version": 1,
        "generatedAt": now_ms(),
        "lastStartedWallet": None,
        "lastStartedAt": None,
        "lastCompletedWallet": None,
        "lastCompletedAt": None,
        "lastExitCode": None,
        "runs": 0,
        "completedRuns": 0,
        "completedLocalProofRuns": 0,
        "completedHeadProofRuns": 0,
        "completedVerifiedRuns": 0,
        "lastCompletedProofClass": None,
        "lastPromotedWallet": None,
        "lastPromotedAt": None,
        "focusPool": None,
        "focusCounts": {},
    }
    payload = read_json(HEAVY_STATE_PATH, defaults) or {}
    if not isinstance(payload, dict):
        payload = {}
    for key, value in defaults.items():
        payload.setdefault(key, value)
    return payload


def save_heavy_state(state):
    state["generatedAt"] = now_ms()
    write_json_atomic(HEAVY_STATE_PATH, state)


def read_wallet_completion_status(wallet: str) -> dict:
    record = read_json(isolated_wallet_record_path(wallet), None)
    if not isinstance(record, dict):
        return {
            "localComplete": False,
            "headComplete": False,
            "verified": False,
            "proofClass": None,
        }
    phase1 = record.get("phase1") if isinstance(record.get("phase1"), dict) else {}
    if phase1:
        phase1_local_complete = bool(phase1.get("localComplete"))
        phase1_zero_history = bool(phase1.get("zeroHistoryResult"))
        phase1_verified = bool(phase1.get("verifiedThroughNow")) and not phase1_zero_history
        phase1_status = str(phase1.get("status") or "").strip().lower() or None
        if phase1_local_complete or phase1_verified or phase1_status:
            proof_class = (
                "zero_history_result"
                if phase1_zero_history
                else "verified_through_now"
                if phase1_verified
                else "local_complete_head_pending"
                if phase1_local_complete
                else phase1_status
            )
            return {
                "localComplete": phase1_local_complete or phase1_verified,
                "headComplete": phase1_verified,
                "verified": phase1_verified,
                "proofClass": proof_class,
            }
    proof = record.get("proof") if isinstance(record.get("proof"), dict) else {}
    local = proof.get("local") if isinstance(proof.get("local"), dict) else {}
    head = proof.get("head") if isinstance(proof.get("head"), dict) else {}
    local_complete = bool(local.get("complete")) or bool(record.get("backfillComplete"))
    head_complete = bool(head.get("complete")) or bool(record.get("historyVerifiedThroughNow"))
    proof_class = str(record.get("historyProofStatus") or "").strip().lower() or None
    verified = bool(record.get("historyVerifiedThroughNow")) or proof_class == "verified_through_now"
    return {
        "localComplete": local_complete,
        "headComplete": head_complete,
        "verified": verified,
        "proofClass": proof_class,
    }


def summarize_candidate_proof_classes(candidates):
    summary = {
        "untracked": 0,
        "compact_only_unproven": 0,
        "partial_backfill": 0,
        "local_complete_head_pending": 0,
        "verified_through_now": 0,
        "other": 0,
    }
    for item in candidates or []:
        proof_class = str((item or {}).get("proofClass") or "").strip().lower()
        if proof_class in summary:
            summary[proof_class] += 1
        else:
            summary["other"] += 1
    return summary


def build_default_lane_states(parallelism: int):
    return [
        {
            "lane": lane_index,
            "wallet": None,
            "candidateClass": None,
            "startedAt": None,
            "completedAt": None,
            "exitCode": None,
            "proxyFile": None,
            "pid": None,
        }
        for lane_index in range(max(1, parallelism))
    ]


def is_backfill_complete(wallet):
    record = read_json(isolated_wallet_record_path(wallet), None)
    return bool(isinstance(record, dict) and record.get("backfillComplete"))


def is_refresh_due(wallet: str, stale_ms: int) -> bool:
    record = read_json(isolated_wallet_record_path(wallet), None)
    if not isinstance(record, dict) or not record.get("backfillComplete"):
        return False
    updated_at = int(record.get("updatedAt") or 0)
    if updated_at <= 0:
        return True
    return now_ms() - updated_at >= max(60_000, int(stale_ms or 0))


def candidate_metrics(row):
    trade_pages = int(((row.get("tradePagination") or {}).get("fetchedPages")) or 0)
    funding_pages = int(((row.get("fundingPagination") or {}).get("fetchedPages")) or 0)
    trade_rows = int(row.get("tradeRowsLoaded") or row.get("trades") or 0)
    volume_usd = float(row.get("volumeUsdRaw") or 0.0)
    return trade_pages, funding_pages, trade_rows, volume_usd


def is_heavy_candidate(row, min_pages, min_rows, min_volume_usd):
    if bool(row.get("backfillComplete")):
        return False
    trade_pages, funding_pages, trade_rows, volume_usd = candidate_metrics(row)
    return max(trade_pages, funding_pages) >= min_pages or trade_rows >= min_rows or volume_usd >= min_volume_usd


def build_heavy_candidates(rows, min_pages, min_rows, min_volume_usd, max_active):
    candidates = []
    for row in rows:
        if not is_heavy_candidate(row, min_pages, min_rows, min_volume_usd):
            continue
        trade_pages, funding_pages, trade_rows, volume_usd = candidate_metrics(row)
        candidates.append(
            {
                "wallet": str(row.get("wallet") or "").strip(),
                "score": max(trade_pages, funding_pages) * 1_000_000_000 + trade_rows * 10_000 + int(volume_usd),
                "tradePagesFetched": trade_pages,
                "fundingPagesFetched": funding_pages,
                "tradeRowsLoaded": trade_rows,
                "volumeUsdRaw": round(volume_usd, 2),
                "firstTrade": row.get("firstTrade"),
                "lastTrade": row.get("lastTrade"),
            }
        )
    candidates = [item for item in candidates if item["wallet"]]
    candidates.sort(
        key=lambda item: (
            -item["fundingPagesFetched"],
            -item["tradePagesFetched"],
            -item["tradeRowsLoaded"],
            -item["volumeUsdRaw"],
            item["wallet"],
        )
    )
    return candidates[: max(1, max_active)]


def build_tail_candidates(rows, max_active=None):
    candidates = []
    proof_priority = {
        "untracked": 4,
        "compact_only_unproven": 3,
        "partial_backfill": 2,
        "local_complete_head_pending": 1,
        "verified_through_now": 0,
    }
    for row in rows:
        if bool(row.get("backfillComplete")) and bool(row.get("proofHeadComplete")):
            continue
        trade_pages, funding_pages, trade_rows, volume_usd = candidate_metrics(row)
        wallet = str(row.get("wallet") or "").strip()
        if not wallet:
            continue
        funding_rows = int(row.get("fundingRowsLoaded") or 0)
        funding_missing = 1 if not bool((row.get("fundingPagination") or {}).get("exhausted")) else 0
        trade_missing = 1 if not bool((row.get("tradePagination") or {}).get("exhausted")) else 0
        heavy_priority = 1 if trade_rows >= 100000 or volume_usd >= 25_000_000 else 0
        metrics_missing = 1 if trade_rows <= 0 and volume_usd <= 0 else 0
        proof_class = str(row.get("proofClass") or "").strip().lower()
        live_updated_at = int(row.get("liveUpdatedAt") or row.get("liveScannedAt") or 0)
        discovery_last_seen_at = int(row.get("discoveryLastSeenAt") or 0)
        candidates.append(
            {
                "wallet": wallet,
                "score": max(trade_pages, funding_pages) * 1_000_000_000 + trade_rows * 10_000 + int(volume_usd),
                "tradePagesFetched": trade_pages,
                "fundingPagesFetched": funding_pages,
                "tradeRowsLoaded": trade_rows,
                "fundingRowsLoaded": funding_rows,
                "volumeUsdRaw": round(volume_usd, 2),
                "fundingMissing": funding_missing,
                "tradeMissing": trade_missing,
                "heavyPriority": heavy_priority,
                "metricsMissing": metrics_missing,
                "proofClass": proof_class,
                "proofLocalComplete": bool(row.get("proofLocalComplete")),
                "proofHeadComplete": bool(row.get("proofHeadComplete")),
                "proof": row.get("proof") if isinstance(row.get("proof"), dict) else None,
                "historyProofStatus": row.get("historyProofStatus"),
                "liveUpdatedAt": live_updated_at or None,
                "discoveryLastSeenAt": discovery_last_seen_at or None,
                "openPositions": int(row.get("openPositions") or 0),
                "poolName": str(row.get("poolName") or "").strip().lower() or None,
                "sourceGroup": row.get("sourceGroup"),
                "firstTrade": row.get("firstTrade"),
                "lastTrade": row.get("lastTrade"),
                "nextEligibleAt": int(row.get("nextEligibleAt") or 0) or None,
                "laneMode": classify_phase1_lane_mode(row),
                "hasLocalRecord": bool(row.get("hasLocalRecord")),
                "phase1": row.get("phase1") if isinstance(row.get("phase1"), dict) else None,
            }
        )
    candidates.sort(
        key=lambda item: (
            -item["metricsMissing"],
            -proof_priority.get(item["proofClass"], 0),
            -item["tradeRowsLoaded"],
            -item["fundingRowsLoaded"],
            -item["liveUpdatedAt"] if item["liveUpdatedAt"] is not None else 0,
            -item["discoveryLastSeenAt"] if item["discoveryLastSeenAt"] is not None else 0,
            -item["volumeUsdRaw"],
            -item["fundingPagesFetched"],
            -item["tradePagesFetched"],
            -item["fundingMissing"],
            -item["tradeMissing"],
            -item["heavyPriority"],
            item["wallet"],
        )
    )
    if max_active is None:
        return candidates
    return candidates[: max(1, max_active)]


def filter_candidates_by_pool(candidates, allowed_pools):
    allowed = {str(pool or "").strip().lower() for pool in (allowed_pools or []) if str(pool or "").strip()}
    if not allowed:
        return list(candidates or [])
    return [
        item
        for item in (candidates or [])
        if str((item or {}).get("poolName") or (item or {}).get("sourceGroup") or "").strip().lower() in allowed
    ]


def filter_candidates_by_lane_mode(candidates, lane_mode):
    mode = str(lane_mode or "").strip().lower()
    if mode in {"", "auto", "all"}:
        return list(candidates or [])
    return [
        item
        for item in (candidates or [])
        if str((item or {}).get("laneMode") or "").strip().lower() == mode
    ]


def merge_prioritized_candidates(heavy_candidates, tail_candidates, max_active):
    merged = []
    seen = set()
    for item in list(heavy_candidates or []) + list(tail_candidates or []):
        wallet = str((item or {}).get("wallet") or "").strip()
        if not wallet or wallet in seen:
            continue
        seen.add(wallet)
        merged.append(item)
        if len(merged) >= max(1, max_active):
            break
    return merged


def sync_isolated_wallets(candidates):
    existing_entries = load_isolated_entries()
    manual_entries = []
    auto_completed_entries = []
    for entry in existing_entries:
        wallet = wallet_from_entry(entry)
        if not wallet:
            continue
        if isinstance(entry, dict) and entry.get("source") == "heavy_auto":
            if is_backfill_complete(wallet):
                auto_completed_entries.append(
                    {
                        "wallet": wallet,
                        "source": "heavy_auto",
                        "status": "complete",
                        "preservedAt": now_ms(),
                    }
                )
            continue
        manual_entries.append({"wallet": wallet, "source": "manual"})

    auto_entries = [
        {
            "wallet": item["wallet"],
            "source": "heavy_auto",
            "status": "active",
            "score": item["score"],
            "tradePagesFetched": item["tradePagesFetched"],
            "fundingPagesFetched": item.get("fundingPagesFetched"),
            "tradeRowsLoaded": item["tradeRowsLoaded"],
            "volumeUsdRaw": item["volumeUsdRaw"],
            "updatedAt": now_ms(),
        }
        for item in candidates
    ]

    merged = {}
    for entry in manual_entries + auto_completed_entries + auto_entries:
        wallet = wallet_from_entry(entry)
        if wallet:
            merged[wallet] = entry
    payload = {
        "version": 2,
        "generatedAt": now_ms(),
        "wallets": list(merged.values()),
    }
    write_json_atomic(isolated_wallets_path(), payload)
    return payload


def write_candidates(payload, candidates):
    write_json_atomic(
        HEAVY_CANDIDATES_PATH,
        {
            "version": 1,
            "generatedAt": now_ms(),
            "sourceGeneratedAt": payload.get("generatedAt"),
            "count": len(candidates),
            "rows": candidates,
        },
    )


def pick_next_wallet(candidates, refresh_wallets=None, excluded_wallets=None, prefer_refresh=False):
    excluded = set(excluded_wallets or [])
    now = now_ms()
    if prefer_refresh:
        for wallet in refresh_wallets or []:
            if wallet in excluded:
                continue
            return wallet
    for item in candidates:
        wallet = str((item or {}).get("wallet") or "").strip()
        if wallet in excluded:
            continue
        if not wallet:
            continue
        next_eligible_at = int((item or {}).get("nextEligibleAt") or 0)
        if next_eligible_at > now:
            continue
        if not bool(item.get("backfillComplete")) or not bool(item.get("proofHeadComplete")):
            return wallet
    if not prefer_refresh:
        for wallet in refresh_wallets or []:
            if wallet in excluded:
                continue
            return wallet
    return None


def lane_proxy_file(proxy_dir: Path, lane_index: int) -> Path:
    return proxy_dir / f"proxies_heavy_{lane_index}.txt"


def run_wallet_in_process(wallet: str, lane_mode: str, lane_file: str, args, proxy_pool: ProxyPool | None = None) -> int:
    return int(
        execute_wallet(
            wallet=wallet,
            lane_mode=lane_mode,
            proxy_file=lane_file,
            shard_count=args.shard_count,
            history_limit=args.history_limit,
            timeout_seconds=args.timeout_seconds,
            request_attempts=args.request_attempts,
            flush_interval_pages=args.flush_interval_pages,
            record_flush_pages=args.record_flush_pages,
            refresh_page_budget=args.refresh_page_budget,
            proxy_pool=proxy_pool,
            emit_trace=False,
        )
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy-file", default="")
    parser.add_argument("--proxy-dir", default="")
    parser.add_argument("--shard-count", type=int, default=8)
    parser.add_argument("--history-limit", type=int, default=240)
    parser.add_argument("--timeout-seconds", type=int, default=25)
    parser.add_argument("--request-attempts", type=int, default=2)
    parser.add_argument("--min-pages", type=int, default=120)
    parser.add_argument("--min-rows", type=int, default=50000)
    parser.add_argument("--min-volume-usd", type=float, default=25000000.0)
    parser.add_argument("--max-active", type=int, default=4)
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--loop-sleep-seconds", type=int, default=5)
    parser.add_argument("--flush-interval-pages", type=int, default=8)
    parser.add_argument("--record-flush-pages", type=int, default=24)
    parser.add_argument("--tail-mode-threshold", type=int, default=0)
    parser.add_argument("--refresh-stale-ms", type=int, default=15 * 60 * 1000)
    parser.add_argument("--refresh-page-budget", type=int, default=2)
    parser.add_argument("--untracked-reserved-lanes", type=int, default=0)
    parser.add_argument("--lane-mode", default="auto")
    args = parser.parse_args()

    proxy_dir = Path(args.proxy_dir).resolve() if args.proxy_dir else None
    proxy_file = str(Path(args.proxy_file).resolve()) if args.proxy_file else ""
    parallelism = max(1, int(args.parallelism))
    lane_mode = str(args.lane_mode or "auto").strip().lower()
    active = {}
    executor = ThreadPoolExecutor(max_workers=parallelism) if lane_mode in {"probe", "recovery", "proof"} else None
    shared_proxy_pools = {}

    while True:
        payload, rows = load_v3_rows()
        focus_wallets = load_focus_wallets_from_csv(
            os.environ.get("PACIFICA_WALLET_EXPLORER_V2_FOCUS_COHORT_CSV", "")
        )
        if focus_wallets:
            row_map = {
                str((row or {}).get("wallet") or "").strip(): row
                for row in rows
                if str((row or {}).get("wallet") or "").strip()
            }
            rows = []
            for wallet in sorted(focus_wallets):
                base_row = row_map.get(wallet, {"wallet": wallet})
                isolated_record = read_json(isolated_wallet_record_path(wallet), None)
                row = overlay_record_fields(base_row, isolated_record)
                row["hasLocalRecord"] = isinstance(isolated_record, dict)
                if not isinstance(isolated_record, dict):
                    row = overlay_probe_fields(row, wallet)
                rows.append(row)
        incomplete_count = sum(1 for row in rows if not bool(row.get("backfillComplete")))
        tail_mode_active = args.tail_mode_threshold > 0 and incomplete_count <= args.tail_mode_threshold
        heavy_candidates = (
            []
            if focus_wallets
            else build_heavy_candidates(
                rows,
                args.min_pages,
                args.min_rows,
                args.min_volume_usd,
                args.max_active,
            )
        )
        tail_candidates = build_tail_candidates(rows, None) if tail_mode_active else []
        focus_pool = "general"
        lane_mode = str(args.lane_mode or "auto").strip().lower()
        if lane_mode not in {"", "auto", "all"}:
            focus_pool = lane_mode
            focus_tail_candidates = filter_candidates_by_lane_mode(tail_candidates, lane_mode)
            untracked_candidates = [
                item for item in tail_candidates if str((item or {}).get("laneMode") or "").strip().lower() in {"probe", "recovery"}
            ]
            catch_up_candidates = [
                item for item in tail_candidates if str((item or {}).get("laneMode") or "").strip().lower() == "proof"
            ]
        else:
            untracked_candidates = filter_candidates_by_pool(tail_candidates, {"new_intake", "untracked"})
            catch_up_candidates = filter_candidates_by_pool(tail_candidates, {"catch_up"})
            focus_tail_candidates = tail_candidates
            if tail_mode_active:
                if untracked_candidates:
                    focus_pool = "untracked"
                    focus_tail_candidates = untracked_candidates
                elif catch_up_candidates:
                    focus_pool = "catch_up"
                    focus_tail_candidates = catch_up_candidates
                else:
                    focus_pool = "tail_idle"
                    focus_tail_candidates = tail_candidates
            focus_tail_candidates = filter_candidates_by_lane_mode(focus_tail_candidates, lane_mode)
        candidates = merge_prioritized_candidates(heavy_candidates, focus_tail_candidates, args.max_active)
        heavy_wallet_set = {
            str((item or {}).get("wallet") or "").strip()
            for item in heavy_candidates
            if str((item or {}).get("wallet") or "").strip()
        }
        sync_isolated_wallets(candidates)
        write_candidates(payload, candidates)
        refresh_wallets = []
        for entry in load_isolated_entries():
            wallet = wallet_from_entry(entry)
            if not wallet:
                continue
            if is_refresh_due(wallet, args.refresh_stale_ms):
                refresh_wallets.append(wallet)
        state = load_heavy_state()
        lane_states = state.get("lanes")
        if not isinstance(lane_states, list) or len(lane_states) != parallelism:
            lane_states = build_default_lane_states(parallelism)

        finished = []
        for lane_index, proc in list(active.items()):
            if proc.get("future") is not None:
                future = proc["future"]
                if not future.done():
                    lane_states[lane_index]["wallet"] = proc["wallet"]
                    lane_states[lane_index]["candidateClass"] = proc.get("candidateClass")
                    lane_states[lane_index]["proxyFile"] = proc.get("proxyFile")
                    lane_states[lane_index]["pid"] = None
                    continue
                try:
                    result = int(future.result() or 0)
                except Exception:
                    result = 1
                finished.append((lane_index, proc, result))
                active.pop(lane_index, None)
                continue
            result = proc["process"].poll()
            if result is None:
                lane_states[lane_index]["wallet"] = proc["wallet"]
                lane_states[lane_index]["candidateClass"] = proc.get("candidateClass")
                lane_states[lane_index]["proxyFile"] = proc.get("proxyFile")
                lane_states[lane_index]["pid"] = proc["process"].pid
                continue
            finished.append((lane_index, proc, result))
            active.pop(lane_index, None)

        for lane_index, proc, result in finished:
            completion = read_wallet_completion_status(proc["wallet"])
            lane_states[lane_index]["wallet"] = proc["wallet"]
            lane_states[lane_index]["candidateClass"] = proc.get("candidateClass")
            lane_states[lane_index]["completedAt"] = now_ms()
            lane_states[lane_index]["exitCode"] = int(result)
            lane_states[lane_index]["pid"] = None
            state["lastCompletedWallet"] = proc["wallet"]
            state["lastCompletedAt"] = lane_states[lane_index]["completedAt"]
            state["lastExitCode"] = int(result)
            state["completedRuns"] = int(state.get("completedRuns") or 0) + 1
            if completion["localComplete"]:
                state["completedLocalProofRuns"] = int(
                    state.get("completedLocalProofRuns") or 0
                ) + 1
            if completion["headComplete"]:
                state["completedHeadProofRuns"] = int(
                    state.get("completedHeadProofRuns") or 0
                ) + 1
            if completion["verified"]:
                state["completedVerifiedRuns"] = int(
                    state.get("completedVerifiedRuns") or 0
                ) + 1
                state["lastPromotedWallet"] = proc["wallet"]
                state["lastPromotedAt"] = lane_states[lane_index]["completedAt"]
            state["lastCompletedProofClass"] = completion["proofClass"]

        running_wallets = {proc["wallet"] for proc in active.values()}
        reserved_heavy_lanes = 0 if focus_wallets else min(parallelism, len(heavy_wallet_set))
        for lane_index in range(parallelism):
            if lane_index in active:
                continue
            prefer_refresh = lane_index % 2 == 1
            lane_prefers_heavy = reserved_heavy_lanes > 0 and lane_index < reserved_heavy_lanes
            lane_pool_class = "general"
            if lane_prefers_heavy:
                primary_candidates = heavy_candidates
                lane_pool_class = "heavy"
            elif tail_mode_active:
                primary_candidates = focus_tail_candidates
                lane_pool_class = focus_pool
            else:
                primary_candidates = candidates
            primary_refresh_wallets = (
                [wallet for wallet in refresh_wallets if wallet in heavy_wallet_set]
                if lane_prefers_heavy
                else refresh_wallets
            )
            wallet = pick_next_wallet(
                primary_candidates,
                refresh_wallets=primary_refresh_wallets,
                excluded_wallets=running_wallets,
                prefer_refresh=prefer_refresh,
            )
            if not wallet and primary_candidates is not candidates:
                wallet = pick_next_wallet(
                    candidates,
                    refresh_wallets=refresh_wallets,
                    excluded_wallets=running_wallets,
                    prefer_refresh=prefer_refresh,
                )
            if not wallet:
                lane_states[lane_index]["wallet"] = None
                lane_states[lane_index]["candidateClass"] = None
                lane_states[lane_index]["pid"] = None
                continue
            if proxy_dir is not None:
                lane_file = lane_proxy_file(proxy_dir, lane_index)
            else:
                lane_file = Path(proxy_file)
            process = None
            future = None
            if executor is not None:
                lane_key = str(lane_file)
                proxy_pool = shared_proxy_pools.get(lane_key)
                if proxy_pool is None:
                    proxy_pool = ProxyPool(load_proxies(lane_key))
                    shared_proxy_pools[lane_key] = proxy_pool
                future = executor.submit(
                    run_wallet_in_process,
                    wallet,
                    lane_mode,
                    str(lane_file),
                    args,
                    proxy_pool,
                )
            else:
                command = [
                    "python3",
                    str(PROCESS_SCRIPT_PATH),
                    "--wallet",
                    wallet,
                    "--lane-mode",
                    lane_mode,
                    "--proxy-file",
                    str(lane_file),
                    "--shard-count",
                    str(args.shard_count),
                    "--history-limit",
                    str(args.history_limit),
                    "--timeout-seconds",
                    str(args.timeout_seconds),
                    "--request-attempts",
                    str(args.request_attempts),
                    "--flush-interval-pages",
                    str(args.flush_interval_pages),
                    "--record-flush-pages",
                    str(args.record_flush_pages),
                    "--refresh-page-budget",
                    str(args.refresh_page_budget),
                ]
                process = subprocess.Popen(command)
            candidate_class = "heavy" if wallet in heavy_wallet_set else lane_pool_class if tail_mode_active else "general"
            active[lane_index] = {
                "wallet": wallet,
                "process": process,
                "future": future,
                "proxyFile": str(lane_file),
                "candidateClass": candidate_class,
            }
            running_wallets.add(wallet)
            if wallet in refresh_wallets:
                refresh_wallets = [item for item in refresh_wallets if item != wallet]
            lane_states[lane_index]["wallet"] = wallet
            lane_states[lane_index]["candidateClass"] = candidate_class
            lane_states[lane_index]["startedAt"] = now_ms()
            lane_states[lane_index]["completedAt"] = None
            lane_states[lane_index]["exitCode"] = None
            lane_states[lane_index]["proxyFile"] = str(lane_file)
            lane_states[lane_index]["pid"] = process.pid if process is not None else None
            state["lastStartedWallet"] = wallet
            state["lastStartedAt"] = lane_states[lane_index]["startedAt"]
            state["runs"] = int(state.get("runs") or 0) + 1

        state["lanes"] = lane_states
        state["tailModeActive"] = tail_mode_active
        state["incompleteCount"] = incomplete_count
        state["candidateCount"] = len(candidates)
        state["heavyCandidateCount"] = len(heavy_candidates)
        state["tailCandidateCount"] = len(tail_candidates)
        state["focusPool"] = focus_pool
        state["laneMode"] = lane_mode
        state["focusCounts"] = {
            "untracked": len(untracked_candidates),
            "catchUp": len(catch_up_candidates),
            "heavy": len(heavy_candidates),
            "activeTail": len(focus_tail_candidates),
        }
        state["candidateProofClassCounts"] = summarize_candidate_proof_classes(candidates)
        state["candidateScope"] = "focus_cohort" if focus_wallets else "global"
        state["focusCohortCount"] = len(focus_wallets)
        state["activeWallets"] = len(active)
        state["activeCount"] = len(active)
        state["activeWalletList"] = [
            {
                "lane": lane_index,
                "wallet": proc["wallet"],
                "candidateClass": proc.get("candidateClass"),
                "pid": int(proc["process"].pid or 0) if proc.get("process") is not None else None,
            }
            for lane_index, proc in sorted(active.items())
        ]
        state["activeCandidates"] = list(state["activeWalletList"])
        save_heavy_state(state)
        time.sleep(max(1, args.loop_sleep_seconds))


if __name__ == "__main__":
    main()
