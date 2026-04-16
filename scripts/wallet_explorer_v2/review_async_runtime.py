#!/usr/bin/env python3
import argparse
import asyncio
import gzip
import json
import os
import sqlite3
import sys
import threading
import uuid
from collections import deque
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from common import (
    UI_DIR,
    V2_DIR,
    build_canonical_wallet_record,
    latest_opened_at_from_history,
    load_proxies,
    load_wallet_discovery_first_seen,
    load_wallet_discovery_recent_seen,
    normalize_wallet,
    now_ms,
    read_json,
    trade_row_key,
    write_json_atomic,
)
from phase1_async_runtime import AsyncPacificaClient, AsyncProxyScheduler, backoff_ms, validate_proxy_subset
from process_isolated_wallet import (
    append_unique_rows,
    init_isolated,
    latest_history_timestamp,
    load_local_owner_truth,
    prepare_history_rows,
)


V3_MANIFEST_PATH = UI_DIR.parent / "wallet_explorer_v3" / "manifest.json"
REVIEW_DIR = V2_DIR / "review_pipeline"
DEFAULT_DB_PATH = str(REVIEW_DIR / "runtime.sqlite3")
STATE_PATH = REVIEW_DIR / "state.json"
STATUS_PATH = REVIEW_DIR / "wallet_status.json"

STAGE_HISTORY = "history"
STAGE_DEEP_REPAIR = "deep_repair"
STAGE_GAP = "gap"
STAGE_LIVE_TRANSITION = "live_transition"
STAGE_LIVE = "live"
STAGE_ZERO_VERIFIED = "zero_verified"
HISTORY_HEAVY_THRESHOLD = 10_000
MIN_TRANSITION_WINDOW_MS = 2 * 60 * 1000
MAX_TRANSITION_WINDOW_MS = 15 * 60 * 1000

PRIORITY_BY_STAGE = {
    STAGE_HISTORY: 100,
    STAGE_DEEP_REPAIR: 90,
    STAGE_GAP: 60,
    STAGE_LIVE_TRANSITION: 40,
    STAGE_LIVE: 20,
    STAGE_ZERO_VERIFIED: 10,
}


def to_num(value, fallback=0.0):
    try:
        out = float(value)
    except Exception:
        return fallback
    return out


def fmt_short_date(ts: int | None) -> str:
    import datetime as dt

    value = int(ts or 0)
    if value <= 0:
        return "-"
    return dt.datetime.fromtimestamp(value / 1000.0, dt.UTC).strftime("%Y-%m-%d")


def compact_rows() -> list[dict]:
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
        return [row for row in rows if normalize_wallet(row.get("wallet"))]
    except Exception:
        return []


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


def backfill_complete(row: dict) -> bool:
    safe = row if isinstance(row, dict) else {}
    return bool(
        safe.get("backfillComplete")
        or (
            safe.get("tradeDone")
            and safe.get("fundingDone")
            and not safe.get("tradeHasMore")
            and not safe.get("fundingHasMore")
            and not safe.get("retryPending")
        )
    )


def baseline_through_at() -> int:
    rows = compact_rows()
    return max((int((row or {}).get("lastTrade") or 0) for row in rows), default=0)


def bounded_transition_window_ms(journey_started_at: int, now: int, args) -> int:
    raw_duration = max(0, int(now) - max(0, int(journey_started_at or 0)))
    min_window = max(
        60_000,
        int(getattr(args, "min_transition_window_ms", MIN_TRANSITION_WINDOW_MS) or MIN_TRANSITION_WINDOW_MS),
    )
    max_window = max(
        min_window,
        int(getattr(args, "max_transition_window_ms", MAX_TRANSITION_WINDOW_MS) or MAX_TRANSITION_WINDOW_MS),
    )
    return max(min_window, min(raw_duration, max_window))


class SQLiteReviewStore:
    def __init__(self, path: str):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        with self._lock:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA busy_timeout=30000")
            self.conn.execute("PRAGMA wal_autocheckpoint=1000")
            self.conn.execute("PRAGMA journal_size_limit=134217728")
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS wallets (
                    wallet TEXT PRIMARY KEY,
                    stage TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 0,
                    next_eligible_at INTEGER NOT NULL DEFAULT 0,
                    claim_token TEXT,
                    claim_expires_at INTEGER NOT NULL DEFAULT 0,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    updated_at INTEGER NOT NULL DEFAULT 0,
                    journey_started_at INTEGER NOT NULL DEFAULT 0,
                    stage_started_at INTEGER NOT NULL DEFAULT 0,
                    first_deposit_at INTEGER NOT NULL DEFAULT 0,
                    recent_seen_at INTEGER NOT NULL DEFAULT 0,
                    baseline_through_at INTEGER NOT NULL DEFAULT 0,
                    trade_rows_loaded INTEGER NOT NULL DEFAULT 0,
                    funding_rows_loaded INTEGER NOT NULL DEFAULT 0,
                    first_trade_at INTEGER NOT NULL DEFAULT 0,
                    last_trade_at INTEGER NOT NULL DEFAULT 0,
                    volume_usd REAL NOT NULL DEFAULT 0,
                    backfill_complete INTEGER NOT NULL DEFAULT 0,
                    deposit_breach INTEGER NOT NULL DEFAULT 0,
                    zero_trade INTEGER NOT NULL DEFAULT 0,
                    history_complete INTEGER NOT NULL DEFAULT 0,
                    history_floor_verified_at INTEGER NOT NULL DEFAULT 0,
                    gap_closed INTEGER NOT NULL DEFAULT 0,
                    gap_target_head_at INTEGER NOT NULL DEFAULT 0,
                    gap_target_started_at INTEGER NOT NULL DEFAULT 0,
                    gap_closed_through_at INTEGER NOT NULL DEFAULT 0,
                    verified_through_at INTEGER NOT NULL DEFAULT 0,
                    last_head_check_at INTEGER NOT NULL DEFAULT 0,
                    transition_started_at INTEGER NOT NULL DEFAULT 0,
                    transition_window_ms INTEGER NOT NULL DEFAULT 0,
                    transition_complete INTEGER NOT NULL DEFAULT 0,
                    live_transition_until INTEGER NOT NULL DEFAULT 0,
                    live_since INTEGER NOT NULL DEFAULT 0,
                    source_record_path TEXT
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_review_wallets_stage_ready ON wallets(stage, next_eligible_at, priority, updated_at)"
            )
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_review_wallets_claim ON wallets(claim_expires_at)")
            existing_columns = {
                str(row["name"])
                for row in self.conn.execute("PRAGMA table_info(wallets)").fetchall()
            }
            for name, definition in (
                ("history_complete", "INTEGER NOT NULL DEFAULT 0"),
                ("history_floor_verified_at", "INTEGER NOT NULL DEFAULT 0"),
                ("gap_target_head_at", "INTEGER NOT NULL DEFAULT 0"),
                ("gap_target_started_at", "INTEGER NOT NULL DEFAULT 0"),
                ("gap_closed_through_at", "INTEGER NOT NULL DEFAULT 0"),
                ("verified_through_at", "INTEGER NOT NULL DEFAULT 0"),
                ("last_head_check_at", "INTEGER NOT NULL DEFAULT 0"),
                ("transition_started_at", "INTEGER NOT NULL DEFAULT 0"),
                ("transition_window_ms", "INTEGER NOT NULL DEFAULT 0"),
                ("transition_complete", "INTEGER NOT NULL DEFAULT 0"),
                ("live_since", "INTEGER NOT NULL DEFAULT 0"),
            ):
                if name not in existing_columns:
                    self.conn.execute(f"ALTER TABLE wallets ADD COLUMN {name} {definition}")

    def close(self):
        with self._lock:
            self.conn.close()

    def seed_wallets(self, seed_rows: list[dict]):
        now = now_ms()
        with self._lock:
            for row in seed_rows:
                wallet = normalize_wallet(row.get("wallet"))
                if not wallet:
                    continue
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO wallets (
                        wallet, stage, priority, next_eligible_at, updated_at,
                        journey_started_at, stage_started_at, first_deposit_at, recent_seen_at,
                        baseline_through_at, trade_rows_loaded, funding_rows_loaded,
                        first_trade_at, last_trade_at, volume_usd, backfill_complete,
                        deposit_breach, zero_trade, history_complete, history_floor_verified_at,
                        gap_closed, gap_target_head_at, gap_target_started_at, gap_closed_through_at,
                        verified_through_at, last_head_check_at, transition_started_at, transition_window_ms,
                        transition_complete, live_transition_until, live_since, source_record_path
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        wallet,
                        row["stage"],
                        PRIORITY_BY_STAGE[row["stage"]],
                        0,
                        now,
                        now,
                        now,
                        int(row.get("firstDepositAt") or 0),
                        int(row.get("recentSeenAt") or 0),
                        int(row.get("baselineThroughAt") or 0),
                        int(row.get("tradeRowsLoaded") or 0),
                        int(row.get("fundingRowsLoaded") or 0),
                        int(row.get("firstTradeAt") or 0),
                        int(row.get("lastTradeAt") or 0),
                        float(row.get("volumeUsd") or 0.0),
                        1 if row.get("backfillComplete") else 0,
                        1 if row.get("depositBreach") else 0,
                        1 if row.get("zeroTrade") else 0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        row.get("sourceRecordPath"),
                    ),
                )
                self.conn.execute(
                    """
                    UPDATE wallets
                    SET first_deposit_at = ?,
                        recent_seen_at = ?,
                        baseline_through_at = ?,
                        trade_rows_loaded = ?,
                        funding_rows_loaded = ?,
                        first_trade_at = ?,
                        last_trade_at = ?,
                        volume_usd = ?,
                        backfill_complete = ?,
                        deposit_breach = ?,
                        zero_trade = ?,
                        stage = CASE
                            WHEN COALESCE(first_deposit_at, 0) = 0 AND ? > 0 THEN ?
                            ELSE stage
                        END,
                        priority = CASE
                            WHEN COALESCE(first_deposit_at, 0) = 0 AND ? > 0 THEN ?
                            ELSE priority
                        END,
                        stage_started_at = CASE
                            WHEN COALESCE(first_deposit_at, 0) = 0 AND ? > 0 THEN ?
                            ELSE stage_started_at
                        END,
                        history_complete = CASE
                            WHEN COALESCE(first_deposit_at, 0) = 0 AND ? > 0 THEN 0
                            ELSE history_complete
                        END,
                        history_floor_verified_at = CASE
                            WHEN COALESCE(first_deposit_at, 0) = 0 AND ? > 0 THEN 0
                            ELSE history_floor_verified_at
                        END,
                        gap_closed = CASE
                            WHEN COALESCE(first_deposit_at, 0) = 0 AND ? > 0 THEN 0
                            ELSE gap_closed
                        END,
                        gap_target_head_at = CASE
                            WHEN COALESCE(first_deposit_at, 0) = 0 AND ? > 0 THEN 0
                            ELSE gap_target_head_at
                        END,
                        gap_target_started_at = CASE
                            WHEN COALESCE(first_deposit_at, 0) = 0 AND ? > 0 THEN 0
                            ELSE gap_target_started_at
                        END,
                        gap_closed_through_at = CASE
                            WHEN COALESCE(first_deposit_at, 0) = 0 AND ? > 0 THEN 0
                            ELSE gap_closed_through_at
                        END,
                        verified_through_at = CASE
                            WHEN COALESCE(first_deposit_at, 0) = 0 AND ? > 0 THEN 0
                            ELSE verified_through_at
                        END,
                        last_head_check_at = CASE
                            WHEN COALESCE(first_deposit_at, 0) = 0 AND ? > 0 THEN 0
                            ELSE last_head_check_at
                        END,
                        transition_started_at = CASE
                            WHEN COALESCE(first_deposit_at, 0) = 0 AND ? > 0 THEN 0
                            ELSE transition_started_at
                        END,
                        transition_window_ms = CASE
                            WHEN COALESCE(first_deposit_at, 0) = 0 AND ? > 0 THEN 0
                            ELSE transition_window_ms
                        END,
                        transition_complete = CASE
                            WHEN COALESCE(first_deposit_at, 0) = 0 AND ? > 0 THEN 0
                            ELSE transition_complete
                        END,
                        live_transition_until = CASE
                            WHEN COALESCE(first_deposit_at, 0) = 0 AND ? > 0 THEN 0
                            ELSE live_transition_until
                        END,
                        live_since = CASE
                            WHEN COALESCE(first_deposit_at, 0) = 0 AND ? > 0 THEN 0
                            ELSE live_since
                        END,
                        source_record_path = ?,
                        updated_at = ?
                    WHERE wallet = ?
                    """,
                    (
                        int(row.get("firstDepositAt") or 0),
                        int(row.get("recentSeenAt") or 0),
                        int(row.get("baselineThroughAt") or 0),
                        int(row.get("tradeRowsLoaded") or 0),
                        int(row.get("fundingRowsLoaded") or 0),
                        int(row.get("firstTradeAt") or 0),
                        int(row.get("lastTradeAt") or 0),
                        float(row.get("volumeUsd") or 0.0),
                        1 if row.get("backfillComplete") else 0,
                        1 if row.get("depositBreach") else 0,
                        1 if row.get("zeroTrade") else 0,
                        int(row.get("firstDepositAt") or 0),
                        row["stage"],
                        int(row.get("firstDepositAt") or 0),
                        PRIORITY_BY_STAGE[row["stage"]],
                        int(row.get("firstDepositAt") or 0),
                        now,
                        int(row.get("firstDepositAt") or 0),
                        int(row.get("firstDepositAt") or 0),
                        int(row.get("firstDepositAt") or 0),
                        int(row.get("firstDepositAt") or 0),
                        int(row.get("firstDepositAt") or 0),
                        int(row.get("firstDepositAt") or 0),
                        int(row.get("firstDepositAt") or 0),
                        int(row.get("firstDepositAt") or 0),
                        int(row.get("firstDepositAt") or 0),
                        int(row.get("firstDepositAt") or 0),
                        int(row.get("firstDepositAt") or 0),
                        int(row.get("firstDepositAt") or 0),
                        int(row.get("firstDepositAt") or 0),
                        row.get("sourceRecordPath"),
                        now,
                        wallet,
                    ),
                )

    def _claim_order_sql(self) -> str:
        return f"""
            ORDER BY
              priority DESC,
              CASE
                WHEN stage = '{STAGE_HISTORY}' AND zero_trade = 0 AND last_trade_at >= ? AND trade_rows_loaded <= 1000 THEN 0
                WHEN stage = '{STAGE_HISTORY}' AND zero_trade = 0 AND trade_rows_loaded BETWEEN 1 AND 100 THEN 1
                WHEN stage = '{STAGE_HISTORY}' AND zero_trade = 0 AND trade_rows_loaded BETWEEN 101 AND 1000 THEN 2
                WHEN stage = '{STAGE_HISTORY}' AND zero_trade = 0 AND last_trade_at >= ? AND trade_rows_loaded <= 10000 THEN 3
                WHEN stage = '{STAGE_HISTORY}' AND zero_trade = 0 AND trade_rows_loaded BETWEEN 1001 AND 10000 THEN 4
                WHEN stage = '{STAGE_HISTORY}' AND zero_trade = 0 THEN 5
                WHEN stage = '{STAGE_HISTORY}' AND zero_trade = 1 AND recent_seen_at >= ? THEN 6
                WHEN stage = '{STAGE_HISTORY}' THEN 7
                WHEN stage = '{STAGE_DEEP_REPAIR}' AND trade_rows_loaded <= 10000 THEN 0
                WHEN stage = '{STAGE_DEEP_REPAIR}' THEN 1
                ELSE 0
              END ASC,
              next_eligible_at ASC,
              stage_started_at ASC,
              updated_at ASC,
              wallet ASC
        """

    def claim_batch(self, stages: list[str], worker_id: str, lease_ms: int, batch_size: int, lane_mode: str | None = None) -> list[sqlite3.Row]:
        now = now_ms()
        recent_cutoff = now - (7 * 24 * 60 * 60 * 1000)
        placeholders = ",".join("?" for _ in stages)
        lease_until = now + max(10_000, int(lease_ms or 120_000))
        requested = max(1, int(batch_size or 1))
        if lane_mode == "live":
            requested = 1
        extra_where = ""
        extra_params: list[object] = []
        if lane_mode == "history":
            extra_where = f"""
              AND (
                stage != '{STAGE_HISTORY}'
                OR trade_rows_loaded <= {HISTORY_HEAVY_THRESHOLD}
                OR last_trade_at >= ?
                OR recent_seen_at >= ?
              )
            """
            extra_params.extend([recent_cutoff, recent_cutoff])
        elif lane_mode == "history_heavy":
            extra_where = f"""
              AND (
                stage = '{STAGE_DEEP_REPAIR}'
                OR (
                  stage = '{STAGE_HISTORY}'
                  AND trade_rows_loaded > {HISTORY_HEAVY_THRESHOLD}
                  AND COALESCE(last_trade_at, 0) < ?
                  AND COALESCE(recent_seen_at, 0) < ?
                )
              )
            """
            extra_params.extend([recent_cutoff, recent_cutoff])
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self.conn.execute(
                    f"""
                    SELECT wallet FROM wallets
                    WHERE stage IN ({placeholders})
                      AND next_eligible_at <= ?
                      AND (claim_expires_at IS NULL OR claim_expires_at <= ?)
                      {extra_where}
                    {self._claim_order_sql()}
                    LIMIT ?
                    """,
                    [*stages, now, now, *extra_params, recent_cutoff, recent_cutoff, recent_cutoff, requested],
                ).fetchall()
                if not rows:
                    self.conn.execute("COMMIT")
                    return []
                wallets = [str(row["wallet"]) for row in rows]
                claimed = []
                for wallet in wallets:
                    token = f"{worker_id}:{uuid.uuid4().hex}"
                    cursor = self.conn.execute(
                        """
                        UPDATE wallets
                        SET claim_token = ?, claim_expires_at = ?, updated_at = ?
                        WHERE wallet = ?
                          AND next_eligible_at <= ?
                          AND (claim_expires_at IS NULL OR claim_expires_at <= ?)
                        """,
                        (token, lease_until, now, wallet, now, now),
                    )
                    if int(cursor.rowcount or 0) > 0:
                        claimed.append(wallet)
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
            if not claimed:
                return []
            claimed_placeholders = ",".join("?" for _ in claimed)
            full_rows = self.conn.execute(
                f"SELECT * FROM wallets WHERE wallet IN ({claimed_placeholders})",
                claimed,
            ).fetchall()
            row_map = {str(row["wallet"]): row for row in full_rows}
            return [row_map[wallet] for wallet in claimed if wallet in row_map]

    def claim_next(self, stages: list[str], worker_id: str, lease_ms: int) -> sqlite3.Row | None:
        rows = self.claim_batch(stages, worker_id, lease_ms, 1)
        if not rows:
            return None
        return rows[0]

    def release_wallet(self, wallet: str, **updates):
        if not updates:
            updates = {}
        current = None
        if "stage" in updates:
            with self._lock:
                current = self.conn.execute("SELECT stage FROM wallets WHERE wallet = ?", (wallet,)).fetchone()
        merged = {
            "claim_token": None,
            "claim_expires_at": 0,
            "updated_at": now_ms(),
        }
        merged.update(updates)
        if "stage" in updates and current is not None and str(current["stage"]) != str(updates["stage"]):
            merged.setdefault("stage_started_at", now_ms())
            merged.setdefault("priority", PRIORITY_BY_STAGE.get(str(updates["stage"]), 0))
        columns = []
        values = []
        for key, value in merged.items():
            columns.append(f"{key} = ?")
            values.append(value)
        values.append(wallet)
        with self._lock:
            self.conn.execute(f"UPDATE wallets SET {', '.join(columns)} WHERE wallet = ?", values)

    def counts(self) -> dict[str, int]:
        with self._lock:
            rows = self.conn.execute("SELECT stage, COUNT(*) AS count FROM wallets GROUP BY stage").fetchall()
        counts = {str(row["stage"]): int(row["count"]) for row in rows}
        for stage in (STAGE_HISTORY, STAGE_DEEP_REPAIR, STAGE_GAP, STAGE_LIVE_TRANSITION, STAGE_LIVE, STAGE_ZERO_VERIFIED):
            counts.setdefault(stage, 0)
        return counts

    def active_claim_counts(self) -> dict[str, int]:
        now = now_ms()
        with self._lock:
            rows = self.conn.execute(
                "SELECT stage, COUNT(*) AS count FROM wallets WHERE claim_expires_at > ? GROUP BY stage",
                (now,),
            ).fetchall()
        counts = {str(row["stage"]): int(row["count"]) for row in rows}
        return counts

    def cleanup_expired_claims(self) -> int:
        now = now_ms()
        with self._lock:
            cursor = self.conn.execute(
                """
                UPDATE wallets
                SET claim_token = NULL, claim_expires_at = 0
                WHERE claim_expires_at > 0
                  AND claim_expires_at <= ?
                  AND claim_token IS NOT NULL
                """,
                (now,),
            )
        return int(cursor.rowcount or 0)

    def error_counts(self, limit: int = 10) -> list[dict]:
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT COALESCE(last_error, '<null>') AS error, COUNT(*) AS count
                FROM wallets
                GROUP BY error
                ORDER BY count DESC, error ASC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        return [{"error": str(row["error"]), "count": int(row["count"])} for row in rows]

    def recent_update_counts(self) -> dict[str, int]:
        now = now_ms()
        windows = {
            "1m": now - 60_000,
            "5m": now - 300_000,
            "15m": now - 900_000,
            "60m": now - 3_600_000,
        }
        out = {}
        with self._lock:
            for label, cutoff in windows.items():
                out[label] = int(
                    self.conn.execute(
                        "SELECT COUNT(*) FROM wallets WHERE updated_at >= ?",
                        (cutoff,),
                    ).fetchone()[0]
                )
        return out

    def sample_wallets(self, stage: str, limit: int = 12) -> list[dict]:
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT wallet, stage, updated_at, first_deposit_at, recent_seen_at, baseline_through_at,
                       trade_rows_loaded, funding_rows_loaded, first_trade_at, last_trade_at,
                       volume_usd, backfill_complete, deposit_breach, zero_trade,
                       history_complete, history_floor_verified_at, gap_closed, gap_target_head_at,
                       gap_target_started_at, gap_closed_through_at, verified_through_at, last_head_check_at,
                       transition_started_at, transition_window_ms, transition_complete,
                       live_transition_until, live_since, last_error
                FROM wallets
                WHERE stage = ?
                ORDER BY updated_at DESC, wallet ASC
                LIMIT ?
                """,
                (stage, max(1, int(limit))),
            ).fetchall()
        return [dict(row) for row in rows]

    def export_wallet_status_rows(self, limit: int = 5000) -> list[dict]:
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT wallet, stage, first_deposit_at, trade_rows_loaded, funding_rows_loaded,
                       first_trade_at, last_trade_at, zero_trade, history_complete, gap_closed,
                       verified_through_at, last_head_check_at, transition_complete,
                       live_transition_until, live_since, updated_at
                FROM wallets
                WHERE verified_through_at > 0
                   OR stage IN (?, ?, ?, ?)
                ORDER BY verified_through_at DESC, updated_at DESC, wallet ASC
                LIMIT ?
                """,
                (
                    STAGE_GAP,
                    STAGE_LIVE_TRANSITION,
                    STAGE_LIVE,
                    STAGE_ZERO_VERIFIED,
                    max(1, int(limit)),
                ),
            ).fetchall()
        return [dict(row) for row in rows]


class ReviewAsyncRuntime:
    def __init__(self, args):
        self.args = args
        self.worker_id = f"{args.lane_mode}:{os.getpid()}"
        self.store = SQLiteReviewStore(args.db_path)
        self.discovery_first_seen = load_wallet_discovery_first_seen()
        self.discovery_recent_seen = load_wallet_discovery_recent_seen()
        self.baseline_at = baseline_through_at()
        proxy_entries = []
        proxy_dir = Path(args.proxy_dir)
        if proxy_dir.exists():
            for path in sorted(proxy_dir.glob("*.txt")):
                proxy_entries.extend(load_proxies(str(path)))
        if str(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_ASYNC_VALIDATE_PROXIES", "true")).strip().lower() not in {"0", "false", "no"}:
            proxy_entries = validate_proxy_subset(proxy_entries, args.timeout_seconds)
        self.scheduler = AsyncProxyScheduler(proxy_entries, args.parallelism)
        self.client = AsyncPacificaClient(self.scheduler, args.timeout_seconds, args.db_path)
        self.active = {}
        self.active_lock = asyncio.Lock()

    def classify_seed_row(self, row: dict) -> dict:
        wallet = normalize_wallet(row.get("wallet"))
        first_deposit_at = int(self.discovery_first_seen.get(wallet) or 0)
        recent_seen_at = int(self.discovery_recent_seen.get(wallet) or 0)
        first_trade_at = int(row.get("firstTrade") or 0)
        last_trade_at = int(row.get("lastTrade") or 0)
        trade_metrics = has_trade_metrics(row)
        complete = backfill_complete(row)
        breach = bool(trade_metrics and first_trade_at > 0 and first_deposit_at > 0 and first_trade_at < first_deposit_at)
        if first_deposit_at <= 0:
            stage = STAGE_HISTORY
        elif breach:
            stage = STAGE_DEEP_REPAIR
        else:
            stage = STAGE_HISTORY
        source_record_path = (
            str((Path("/root/pacifica-flow/data/wallet_explorer_v2/isolated") / wallet / "wallet_record.json"))
            if wallet
            else None
        )
        return {
            "wallet": wallet,
            "stage": stage,
            "firstDepositAt": first_deposit_at,
            "recentSeenAt": recent_seen_at,
            "baselineThroughAt": self.baseline_at,
            "tradeRowsLoaded": int(row.get("tradeRowsLoaded") or 0),
            "fundingRowsLoaded": int(row.get("fundingRowsLoaded") or 0),
            "firstTradeAt": first_trade_at,
            "lastTradeAt": last_trade_at,
            "volumeUsd": float(row.get("volumeUsdRaw") or 0.0),
            "backfillComplete": complete,
            "depositBreach": breach,
            "zeroTrade": not trade_metrics,
            "sourceRecordPath": source_record_path,
        }

    def seed(self):
        rows = [self.classify_seed_row(row) for row in compact_rows()]
        self.store.seed_wallets(rows)

    async def set_active(self, worker_name: str, wallet: str | None, stage: str | None):
        async with self.active_lock:
            self.active[worker_name] = {
                "wallet": wallet,
                "stage": stage,
                "updatedAt": now_ms(),
            }

    def load_wallet_context(self, wallet: str):
        local_truth = load_local_owner_truth(wallet, self.args.shard_count)
        state, history = init_isolated(wallet, self.args.shard_count, local_truth)
        history["trades"], trade_seen = prepare_history_rows(history.get("trades") or [], "trades")
        history["funding"], _funding_seen = prepare_history_rows(history.get("funding") or [], "funding")
        record_path = Path("/root/pacifica-flow/data/wallet_explorer_v2/isolated") / wallet / "wallet_record.json"
        record = read_json(record_path, {}) or {}
        if not isinstance(record, dict) or not record:
            record = build_canonical_wallet_record(wallet, int(state.get("shardIndex") or 0), state, history)
        return state, history, record, trade_seen

    def persist_context(self, wallet: str, state: dict, history: dict):
        state["last_opened_at"] = latest_opened_at_from_history(history)
        record = build_canonical_wallet_record(wallet, int(state.get("shardIndex") or 0), state, history)
        base_dir = Path("/root/pacifica-flow/data/wallet_explorer_v2/isolated") / wallet
        base_dir.mkdir(parents=True, exist_ok=True)
        write_json_atomic(base_dir / "wallet_state.json", state)
        write_json_atomic(base_dir / "wallet_history.json", history)
        write_json_atomic(base_dir / "wallet_record.json", record)
        return record

    async def fetch_trade_page(self, wallet: str, limit: int, cursor: str | None = None):
        payload, _entry = await self.client.request_history(wallet, "trades/history", limit, cursor)
        rows = payload.get("data") or []
        normalized_rows, _ = prepare_history_rows(rows, "trades")
        return {
            "rows": normalized_rows,
            "hasMore": bool(payload.get("has_more")),
            "nextCursor": payload.get("next_cursor"),
            "remoteLastAt": latest_history_timestamp(normalized_rows, "timestamp"),
        }

    @staticmethod
    def oldest_trade_at(rows: list[dict]) -> int:
        out = 0
        oldest = None
        for row in rows or []:
            ts = int((row or {}).get("timestamp") or 0)
            if ts <= 0:
                continue
            if oldest is None or ts < oldest:
                oldest = ts
        return int(oldest or 0)

    def history_fetch_plan(self, row: sqlite3.Row) -> tuple[int, int, int]:
        loaded = max(0, int(row["trade_rows_loaded"] or 0))
        zero_trade = bool(int(row["zero_trade"] or 0)) and loaded <= 0
        recent_cutoff = now_ms() - (7 * 24 * 60 * 60 * 1000)
        recent_trade = int(row["last_trade_at"] or row["recent_seen_at"] or 0) >= recent_cutoff
        limit = max(1, int(self.args.history_limit))
        page_budget = max(1, int(self.args.page_budget))
        extra_checks = max(1, int(self.args.extra_history_checks))
        if zero_trade:
            return 1, 1, 1
        if loaded > 50000:
            limit = min(max(limit, 40), 60)
            page_budget = min(page_budget, 2)
        elif loaded > 20000:
            limit = min(max(limit, 50), 80)
            page_budget = min(max(2, page_budget), 3)
        elif loaded > 10000:
            limit = min(max(limit, 60), 100)
            page_budget = min(max(3, page_budget), 4)
        elif loaded > 1000:
            limit = max(limit, 100)
            page_budget = max(page_budget, 10)
        elif loaded > 100:
            limit = max(limit, 100)
            page_budget = max(page_budget, 14)
        else:
            limit = max(limit, 120)
            page_budget = max(page_budget, 20)
        if recent_trade:
            if loaded <= 1000:
                limit = max(limit, 120)
                page_budget = max(page_budget, 24)
            elif loaded <= 10000:
                page_budget = max(page_budget, 12)
        return limit, page_budget, extra_checks

    def deep_repair_fetch_plan(self, row: sqlite3.Row) -> tuple[int, int]:
        loaded = max(0, int(row["trade_rows_loaded"] or 0))
        limit = max(1, int(self.args.history_limit))
        page_budget = max(1, int(self.args.deep_repair_page_budget))
        if loaded > 50000:
            return min(max(limit, 60), 80), min(max(page_budget, 2), 3)
        if loaded > 10000:
            return min(max(limit, 80), 120), min(max(page_budget, 3), 4)
        if loaded > 1000:
            return max(limit, 100), max(page_budget, 6)
        return max(limit, 120), max(page_budget, 8)

    def gap_fetch_plan(self, row: sqlite3.Row) -> tuple[int, int]:
        loaded = max(0, int(row["trade_rows_loaded"] or 0))
        zero_trade = bool(int(row["zero_trade"] or 0)) and loaded <= 0
        recent_cutoff = now_ms() - (7 * 24 * 60 * 60 * 1000)
        recent_trade = int(row["last_trade_at"] or row["recent_seen_at"] or 0) >= recent_cutoff
        limit = max(1, int(self.args.gap_limit))
        page_budget = max(1, int(self.args.gap_page_budget))
        if zero_trade:
            return 1, 1
        if loaded > 50000:
            limit = min(max(limit, 40), 60)
            page_budget = min(max(page_budget, 2), 3)
        elif loaded > 10000:
            limit = min(max(limit, 60), 100)
            page_budget = min(max(page_budget, 4), 6)
        elif loaded > 1000 or recent_trade:
            limit = max(limit, 120)
            page_budget = max(page_budget, 16)
        elif loaded > 100:
            limit = max(limit, 80)
            page_budget = max(page_budget, 14)
        else:
            limit = max(limit, 60)
            page_budget = max(page_budget, 16)
        return limit, page_budget

    @staticmethod
    def revisit_delay_ms_for_history(loaded: int) -> int:
        loaded = max(0, int(loaded or 0))
        if loaded > 50000:
            return 10 * 60 * 1000
        if loaded > 20000:
            return 5 * 60 * 1000
        if loaded > 10000:
            return 2 * 60 * 1000
        if loaded > 1000:
            return 30 * 1000
        return 0

    @staticmethod
    def revisit_delay_ms_for_gap(loaded: int) -> int:
        loaded = max(0, int(loaded or 0))
        if loaded > 50000:
            return 5 * 60 * 1000
        if loaded > 10000:
            return 90 * 1000
        if loaded > 1000:
            return 20 * 1000
        return 0

    async def handle_history(self, row: sqlite3.Row):
        wallet = str(row["wallet"])
        first_deposit_at = int(row["first_deposit_at"] or 0)
        if first_deposit_at <= 0:
            self.store.release_wallet(
                wallet,
                stage=STAGE_HISTORY,
                next_eligible_at=now_ms() + max(5 * 60 * 1000, int(self.args.gap_retry_ms)),
                attempt_count=int(row["attempt_count"] or 0) + 1,
                last_error="missing_first_deposit_at",
            )
            return
        state, history, record, trade_seen = self.load_wallet_context(wallet)
        trade_stream = ((state.get("streams") or {}).get("trades")) or {}
        pages_fetched = int(trade_stream.get("pagesFetched") or 0)
        frontier = trade_stream.get("frontierCursor")
        exhausted = bool(trade_stream.get("exhausted"))
        pages = 0
        current_limit, current_page_budget, extra_checks = self.history_fetch_plan(row)

        while pages < int(current_page_budget):
            oldest_at = self.oldest_trade_at(history.get("trades") or [])
            reached_floor = bool(first_deposit_at > 0 and oldest_at > 0 and oldest_at <= first_deposit_at)
            if exhausted or reached_floor:
                break
            if not frontier and pages_fetched > 0 and history.get("trades"):
                exhausted = True
                break
            result = await self.fetch_trade_page(wallet, current_limit, frontier if frontier else None)
            append_unique_rows(history["trades"], result["rows"], "trades", trade_seen)
            pages += 1
            pages_fetched += 1
            frontier = result["nextCursor"] if result["hasMore"] else None
            exhausted = not result["hasMore"]
            trade_stream["pagesFetched"] = pages_fetched
            trade_stream["rowsFetched"] = len(history.get("trades") or [])
            trade_stream["frontierCursor"] = frontier
            trade_stream["exhausted"] = exhausted
            trade_stream["lastSuccessAt"] = now_ms()
            trade_stream["lastAttemptAt"] = now_ms()
            trade_stream["lastError"] = None
            trade_stream["failedCursor"] = None
            trade_stream["nextEligibleAt"] = 0

        oldest_at = self.oldest_trade_at(history.get("trades") or [])
        reached_floor = bool(first_deposit_at > 0 and oldest_at > 0 and oldest_at <= first_deposit_at)
        breach = False
        if reached_floor and not exhausted:
            for _ in range(extra_checks):
                if not frontier:
                    break
                result = await self.fetch_trade_page(wallet, current_limit, frontier)
                append_unique_rows(history["trades"], result["rows"], "trades", trade_seen)
                pages_fetched += 1
                frontier = result["nextCursor"] if result["hasMore"] else None
                exhausted = not result["hasMore"]
                trade_stream["pagesFetched"] = pages_fetched
                trade_stream["rowsFetched"] = len(history.get("trades") or [])
                trade_stream["frontierCursor"] = frontier
                trade_stream["exhausted"] = exhausted
                if result["rows"]:
                    breach = True
                if exhausted:
                    break

        state["updatedAt"] = now_ms()
        history["updatedAt"] = now_ms()
        record = self.persist_context(wallet, state, history)
        trade_rows_loaded = int(record.get("tradeRowsLoaded") or 0)
        last_trade_at = int(record.get("lastTrade") or 0)
        if breach:
            self.store.release_wallet(
                wallet,
                stage=STAGE_DEEP_REPAIR,
                next_eligible_at=0,
                trade_rows_loaded=trade_rows_loaded,
                funding_rows_loaded=int(record.get("fundingRowsLoaded") or 0),
                first_trade_at=int(record.get("firstTrade") or 0),
                last_trade_at=last_trade_at,
                volume_usd=float(record.get("volumeUsdRaw") or 0.0),
                backfill_complete=0,
                deposit_breach=1,
                history_complete=0,
                history_floor_verified_at=0,
                zero_trade=1 if trade_rows_loaded <= 0 else 0,
                last_error=None,
            )
            return
        if exhausted or reached_floor:
            verified_at = now_ms()
            self.store.release_wallet(
                wallet,
                stage=STAGE_GAP,
                next_eligible_at=0,
                trade_rows_loaded=trade_rows_loaded,
                funding_rows_loaded=int(record.get("fundingRowsLoaded") or 0),
                first_trade_at=int(record.get("firstTrade") or 0),
                last_trade_at=last_trade_at,
                volume_usd=float(record.get("volumeUsdRaw") or 0.0),
                backfill_complete=1 if exhausted or reached_floor else 0,
                deposit_breach=0,
                history_complete=1,
                history_floor_verified_at=verified_at,
                zero_trade=1 if trade_rows_loaded <= 0 else 0,
                last_error=None,
            )
            return
        self.store.release_wallet(
            wallet,
            stage=STAGE_HISTORY,
            next_eligible_at=now_ms() + self.revisit_delay_ms_for_history(trade_rows_loaded),
            trade_rows_loaded=trade_rows_loaded,
            funding_rows_loaded=int(record.get("fundingRowsLoaded") or 0),
            first_trade_at=int(record.get("firstTrade") or 0),
            last_trade_at=last_trade_at,
            volume_usd=float(record.get("volumeUsdRaw") or 0.0),
            backfill_complete=0,
            deposit_breach=0,
            history_complete=0,
            history_floor_verified_at=0,
            zero_trade=1 if trade_rows_loaded <= 0 else 0,
            last_error=None,
        )

    async def handle_deep_repair(self, row: sqlite3.Row):
        wallet = str(row["wallet"])
        state, history, record, trade_seen = self.load_wallet_context(wallet)
        trade_stream = ((state.get("streams") or {}).get("trades")) or {}
        frontier = trade_stream.get("frontierCursor")
        exhausted = bool(trade_stream.get("exhausted"))
        pages_fetched = int(trade_stream.get("pagesFetched") or 0)
        current_limit, current_page_budget = self.deep_repair_fetch_plan(row)
        pages = 0

        while pages < int(current_page_budget):
            if exhausted:
                break
            if not frontier and pages_fetched > 0 and history.get("trades"):
                exhausted = True
                break
            result = await self.fetch_trade_page(wallet, current_limit, frontier if frontier else None)
            append_unique_rows(history["trades"], result["rows"], "trades", trade_seen)
            pages += 1
            pages_fetched += 1
            frontier = result["nextCursor"] if result["hasMore"] else None
            exhausted = not result["hasMore"]
            trade_stream["pagesFetched"] = pages_fetched
            trade_stream["rowsFetched"] = len(history.get("trades") or [])
            trade_stream["frontierCursor"] = frontier
            trade_stream["exhausted"] = exhausted
            trade_stream["lastSuccessAt"] = now_ms()
            trade_stream["lastAttemptAt"] = now_ms()
            trade_stream["lastError"] = None

        state["updatedAt"] = now_ms()
        history["updatedAt"] = now_ms()
        record = self.persist_context(wallet, state, history)
        if exhausted:
            verified_at = now_ms()
            self.store.release_wallet(
                wallet,
                stage=STAGE_GAP,
                next_eligible_at=0,
                trade_rows_loaded=int(record.get("tradeRowsLoaded") or 0),
                funding_rows_loaded=int(record.get("fundingRowsLoaded") or 0),
                first_trade_at=int(record.get("firstTrade") or 0),
                last_trade_at=int(record.get("lastTrade") or 0),
                volume_usd=float(record.get("volumeUsdRaw") or 0.0),
                backfill_complete=1,
                deposit_breach=0,
                history_complete=1,
                history_floor_verified_at=verified_at,
                zero_trade=1 if int(record.get("tradeRowsLoaded") or 0) <= 0 else 0,
                last_error=None,
            )
            return
        self.store.release_wallet(
            wallet,
            stage=STAGE_DEEP_REPAIR,
            next_eligible_at=now_ms() + self.revisit_delay_ms_for_history(int(record.get("tradeRowsLoaded") or 0)),
            trade_rows_loaded=int(record.get("tradeRowsLoaded") or 0),
            funding_rows_loaded=int(record.get("fundingRowsLoaded") or 0),
            first_trade_at=int(record.get("firstTrade") or 0),
            last_trade_at=int(record.get("lastTrade") or 0),
            volume_usd=float(record.get("volumeUsdRaw") or 0.0),
            backfill_complete=0,
            deposit_breach=1,
            history_complete=0,
            history_floor_verified_at=0,
            zero_trade=1 if int(record.get("tradeRowsLoaded") or 0) <= 0 else 0,
            last_error=None,
        )

    async def handle_gap(self, row: sqlite3.Row):
        wallet = str(row["wallet"])
        if int(row["first_deposit_at"] or 0) <= 0 or not bool(int(row["history_complete"] or 0)):
            self.store.release_wallet(
                wallet,
                stage=STAGE_HISTORY,
                next_eligible_at=0,
                gap_closed=0,
                gap_target_head_at=0,
                gap_target_started_at=0,
                gap_closed_through_at=0,
                verified_through_at=0,
                last_head_check_at=0,
                transition_started_at=0,
                transition_window_ms=0,
                transition_complete=0,
                live_transition_until=0,
                live_since=0,
                last_error=None,
            )
            return
        baseline_at = int(row["baseline_through_at"] or self.baseline_at)
        state, history, record, trade_seen = self.load_wallet_context(wallet)
        local_last_trade = int(record.get("lastTrade") or 0)
        local_has_trades = int(record.get("tradeRowsLoaded") or 0) > 0
        pages = 0
        cursor = None
        overlap_found = False
        boundary_reached = False
        remote_head_at = int(row["gap_target_head_at"] or 0)
        gap_started_at = int(row["gap_target_started_at"] or 0)
        current_limit, current_page_budget = self.gap_fetch_plan(row)

        while pages < int(current_page_budget):
            result = await self.fetch_trade_page(wallet, current_limit, cursor)
            if pages == 0 and remote_head_at <= 0:
                remote_head_at = int(result.get("remoteLastAt") or 0)
                gap_started_at = now_ms()
            rows = result["rows"]
            if not rows:
                overlap_found = True
                break
            if any(trade_row_key(item) in trade_seen for item in rows):
                overlap_found = True
            append_unique_rows(history["trades"], rows, "trades", trade_seen)
            pages += 1
            oldest_remote_at = self.oldest_trade_at(rows)
            if not local_has_trades and baseline_at > 0 and oldest_remote_at > 0 and oldest_remote_at <= baseline_at:
                boundary_reached = True
            if overlap_found or boundary_reached:
                break
            if not result["hasMore"]:
                if not local_has_trades:
                    boundary_reached = True
                break
            cursor = result["nextCursor"]

        state["updatedAt"] = now_ms()
        history["updatedAt"] = now_ms()
        record = self.persist_context(wallet, state, history)
        if overlap_found or boundary_reached:
            now = now_ms()
            zero_trade = int(record.get("tradeRowsLoaded") or 0) <= 0
            if zero_trade:
                self.store.release_wallet(
                    wallet,
                    stage=STAGE_ZERO_VERIFIED,
                    next_eligible_at=0,
                    trade_rows_loaded=int(record.get("tradeRowsLoaded") or 0),
                    funding_rows_loaded=int(record.get("fundingRowsLoaded") or 0),
                    first_trade_at=int(record.get("firstTrade") or 0),
                    last_trade_at=int(record.get("lastTrade") or 0),
                    volume_usd=float(record.get("volumeUsdRaw") or 0.0),
                    backfill_complete=1,
                    deposit_breach=0,
                    zero_trade=1,
                    gap_closed=1,
                    gap_target_head_at=remote_head_at,
                    gap_target_started_at=gap_started_at or now,
                    gap_closed_through_at=now,
                    verified_through_at=now,
                    last_head_check_at=now,
                    transition_started_at=0,
                    transition_window_ms=0,
                    transition_complete=1,
                    live_transition_until=0,
                    live_since=0,
                    last_error=None,
                )
                return
            journey_started_at = int(row["journey_started_at"] or now)
            transition_duration = bounded_transition_window_ms(journey_started_at, now, self.args)
            self.store.release_wallet(
                wallet,
                stage=STAGE_LIVE_TRANSITION,
                next_eligible_at=now + max(60_000, int(self.args.transition_poll_ms)),
                trade_rows_loaded=int(record.get("tradeRowsLoaded") or 0),
                funding_rows_loaded=int(record.get("fundingRowsLoaded") or 0),
                first_trade_at=int(record.get("firstTrade") or 0),
                last_trade_at=int(record.get("lastTrade") or 0),
                volume_usd=float(record.get("volumeUsdRaw") or 0.0),
                backfill_complete=1,
                deposit_breach=0,
                zero_trade=0,
                gap_closed=1,
                gap_target_head_at=remote_head_at,
                gap_target_started_at=gap_started_at or now,
                gap_closed_through_at=now,
                verified_through_at=now,
                last_head_check_at=now,
                transition_started_at=now,
                transition_window_ms=transition_duration,
                transition_complete=0,
                live_transition_until=now + transition_duration,
                last_error=None,
            )
            return
        self.store.release_wallet(
            wallet,
            stage=STAGE_GAP,
            next_eligible_at=now_ms() + max(
                self.revisit_delay_ms_for_gap(int(record.get("tradeRowsLoaded") or 0)),
                15_000,
            ),
            trade_rows_loaded=int(record.get("tradeRowsLoaded") or 0),
            funding_rows_loaded=int(record.get("fundingRowsLoaded") or 0),
            first_trade_at=int(record.get("firstTrade") or 0),
            last_trade_at=int(record.get("lastTrade") or 0),
            volume_usd=float(record.get("volumeUsdRaw") or 0.0),
            backfill_complete=1,
            deposit_breach=0,
            zero_trade=1 if int(record.get("tradeRowsLoaded") or 0) <= 0 else 0,
            gap_closed=0,
            gap_target_head_at=remote_head_at,
            gap_target_started_at=gap_started_at or now_ms(),
            last_error=None,
        )

    async def handle_live(self, row: sqlite3.Row):
        wallet = str(row["wallet"])
        stage = str(row["stage"])
        now = now_ms()
        if int(row["first_deposit_at"] or 0) <= 0 or not bool(int(row["history_complete"] or 0)):
            self.store.release_wallet(
                wallet,
                stage=STAGE_HISTORY,
                next_eligible_at=0,
                gap_closed=0,
                gap_target_head_at=0,
                gap_target_started_at=0,
                gap_closed_through_at=0,
                verified_through_at=0,
                last_head_check_at=0,
                transition_started_at=0,
                transition_window_ms=0,
                transition_complete=0,
                live_transition_until=0,
                live_since=0,
                last_error=None,
            )
            return
        if stage == STAGE_LIVE_TRANSITION:
            transition_started_at = int(row["transition_started_at"] or 0)
            transition_window_ms = bounded_transition_window_ms(
                int(row["journey_started_at"] or transition_started_at or now),
                now,
                self.args,
            )
            clamped_transition_until = (
                transition_started_at + transition_window_ms if transition_started_at > 0 else 0
            )
            stored_transition_until = int(row["live_transition_until"] or 0)
            transition_until = min(
                value for value in (stored_transition_until, clamped_transition_until) if value > 0
            ) if (stored_transition_until > 0 or clamped_transition_until > 0) else 0
            recent_head_check_at = max(
                int(row["last_head_check_at"] or 0),
                int(row["verified_through_at"] or 0),
            )
            head_checked_since_transition = (
                recent_head_check_at > 0
                and transition_started_at > 0
                and recent_head_check_at >= transition_started_at
            )
            if (
                transition_until > 0
                and now >= transition_until
                and head_checked_since_transition
            ):
                self.store.release_wallet(
                    wallet,
                    stage=STAGE_LIVE,
                    next_eligible_at=now + max(5 * 60 * 1000, int(self.args.live_poll_ms)),
                    live_since=now,
                    gap_closed=1,
                    gap_closed_through_at=max(int(row["gap_closed_through_at"] or 0), recent_head_check_at, now),
                    verified_through_at=max(recent_head_check_at, now),
                    last_head_check_at=max(recent_head_check_at, now),
                    transition_complete=1,
                    transition_window_ms=transition_window_ms,
                    live_transition_until=transition_until,
                    last_error=None,
                )
                return
        local_last_trade = int(row["last_trade_at"] or 0)
        result = await self.fetch_trade_page(wallet, 1, None)
        remote_last = int(result.get("remoteLastAt") or 0)
        now = now_ms()
        if remote_last > local_last_trade:
            self.store.release_wallet(
                wallet,
                stage=STAGE_GAP,
                next_eligible_at=0,
                gap_closed=0,
                gap_target_head_at=remote_last,
                gap_target_started_at=now,
                gap_closed_through_at=0,
                verified_through_at=0,
                last_head_check_at=now,
                transition_started_at=0,
                transition_window_ms=0,
                transition_complete=0,
                live_transition_until=0,
                live_since=0,
                last_error=None,
            )
            return
        if stage == STAGE_LIVE_TRANSITION:
            transition_started_at = int(row["transition_started_at"] or 0)
            transition_window_ms = bounded_transition_window_ms(
                int(row["journey_started_at"] or transition_started_at or now),
                now,
                self.args,
            )
            clamped_transition_until = (
                transition_started_at + transition_window_ms if transition_started_at > 0 else 0
            )
            stored_transition_until = int(row["live_transition_until"] or 0)
            transition_until = min(
                value for value in (stored_transition_until, clamped_transition_until) if value > 0
            ) if (stored_transition_until > 0 or clamped_transition_until > 0) else 0
            if transition_until > 0 and now >= transition_until:
                self.store.release_wallet(
                    wallet,
                    stage=STAGE_LIVE,
                    next_eligible_at=now + max(5 * 60 * 1000, int(self.args.live_poll_ms)),
                    live_since=now,
                    gap_closed=1,
                    gap_closed_through_at=now,
                    verified_through_at=now,
                    last_head_check_at=now,
                    transition_complete=1,
                    last_error=None,
                )
                return
            self.store.release_wallet(
                wallet,
                stage=STAGE_LIVE_TRANSITION,
                next_eligible_at=now + max(60_000, int(self.args.transition_poll_ms)),
                gap_closed=1,
                gap_closed_through_at=now,
                verified_through_at=now,
                last_head_check_at=now,
                transition_window_ms=transition_window_ms,
                live_transition_until=transition_until,
                last_error=None,
            )
            return
        self.store.release_wallet(
            wallet,
            stage=STAGE_LIVE,
            next_eligible_at=now + max(5 * 60 * 1000, int(self.args.live_poll_ms)),
            gap_closed=1,
            gap_closed_through_at=now,
            verified_through_at=now,
            last_head_check_at=now,
            transition_complete=1,
            last_error=None,
        )

    async def handle_wallet(self, row: sqlite3.Row):
        stage = str(row["stage"])
        if stage == STAGE_HISTORY:
            await self.handle_history(row)
        elif stage == STAGE_DEEP_REPAIR:
            await self.handle_deep_repair(row)
        elif stage == STAGE_GAP:
            await self.handle_gap(row)
        elif stage in {STAGE_LIVE_TRANSITION, STAGE_LIVE}:
            await self.handle_live(row)
        else:
            self.store.release_wallet(str(row["wallet"]), stage=stage)

    async def worker_loop(self, index: int):
        worker_name = f"{self.args.lane_mode}:{index}"
        claim_buffer: deque[sqlite3.Row] = deque()
        stage_map = {
            "history": [STAGE_HISTORY, STAGE_DEEP_REPAIR],
            "history_heavy": [STAGE_DEEP_REPAIR, STAGE_HISTORY],
            "deep_repair": [STAGE_DEEP_REPAIR, STAGE_HISTORY],
            "gap": [STAGE_GAP, STAGE_HISTORY],
            "live": [STAGE_LIVE_TRANSITION, STAGE_LIVE],
            "auto": [STAGE_GAP, STAGE_HISTORY, STAGE_DEEP_REPAIR, STAGE_LIVE_TRANSITION, STAGE_LIVE],
        }
        stages = stage_map.get(self.args.lane_mode, [STAGE_GAP])
        while True:
            claimed = None
            if claim_buffer:
                claimed = claim_buffer.popleft()
            else:
                try:
                    for stage in stages:
                        batch = self.store.claim_batch(
                            [stage],
                            worker_name,
                            self.args.lease_ms,
                            self.args.claim_batch_size,
                            self.args.lane_mode,
                        )
                        if batch:
                            claimed = batch[0]
                            for extra in batch[1:]:
                                claim_buffer.append(extra)
                            break
                except Exception as exc:
                    print(
                        f"[review-runtime] worker={worker_name} claim_batch_failed error={exc}",
                        file=sys.stderr,
                        flush=True,
                    )
                    await self.set_active(worker_name, None, None)
                    await asyncio.sleep(max(0.25, float(self.args.loop_sleep_seconds)))
                    continue
            if claimed is None:
                await self.set_active(worker_name, None, None)
                await asyncio.sleep(max(0.1, float(self.args.loop_sleep_seconds)))
                continue
            wallet = str(claimed["wallet"])
            await self.set_active(worker_name, wallet, str(claimed["stage"]))
            try:
                await self.handle_wallet(claimed)
            except Exception as exc:
                retry_count = int(claimed["attempt_count"] or 0) + 1
                self.store.release_wallet(
                    wallet,
                    stage=str(claimed["stage"]),
                    attempt_count=retry_count,
                    next_eligible_at=now_ms() + backoff_ms(retry_count, str(exc)),
                    last_error=str(exc),
                )
            finally:
                await self.set_active(worker_name, None, None)

    def stage_summary(self, stage: str, sample_rows: list[dict], counts: dict[str, int]) -> dict:
        if stage == STAGE_HISTORY:
            label = "History Completion"
            focus = "Validate baseline history backward to first deposit, then issue 1-3 extra requests before the deposit boundary."
            count = counts.get(STAGE_HISTORY, 0)
        elif stage == STAGE_DEEP_REPAIR:
            label = "Deep Repair"
            focus = "Wallets where trades continue before the deposit floor. Continue backward pagination until the older history is resolved."
            count = counts.get(STAGE_DEEP_REPAIR, 0)
        elif stage == STAGE_GAP:
            label = "Gap Recovery"
            focus = "Fetch newest pages from page 1 backward until overlap with the stored baseline is proven."
            count = counts.get(STAGE_GAP, 0)
        else:
            label = "Live Mode"
            focus = "Live transition and permanent live polling. Any newer head reopens the wallet into Gap."
            count = counts.get(STAGE_LIVE, 0) + counts.get(STAGE_LIVE_TRANSITION, 0)
        wallets = []
        for row in sample_rows:
            verified_at = int(row.get("verified_through_at") or 0)
            head_checked_at = int(row.get("last_head_check_at") or 0)
            wallets.append(
                {
                    "wallet": row["wallet"],
                    "progressPct": 0,
                    "summaryLabel": label,
                    "metaLabel": (
                        f"deposit {fmt_short_date(int(row.get('first_deposit_at') or 0))} • "
                        f"last trade {fmt_short_date(int(row.get('last_trade_at') or 0))} • "
                        f"verified {fmt_short_date(verified_at)} • "
                        f"head {fmt_short_date(head_checked_at)}"
                    ),
                    "remainingLabel": stage,
                    "tradeRowsLoaded": int(row.get("trade_rows_loaded") or 0),
                    "volumeUsdRaw": float(row.get("volume_usd") or 0.0),
                    "openPositions": 0,
                    "updatedAt": int(row.get("updated_at") or 0) or None,
                }
            )
        return {
            "label": label,
            "phaseType": stage,
            "count": count,
            "active": 0,
            "complete": 0,
            "progressPct": 0,
            "badgeLabel": label,
            "focusLabel": focus,
            "summaryMetrics": [],
            "wallets": wallets[:12],
        }

    async def publish_loop(self):
        while True:
            self.store.cleanup_expired_claims()
            counts = self.store.counts()
            active_claims = self.store.active_claim_counts()
            history_group = self.stage_summary(STAGE_HISTORY, self.store.sample_wallets(STAGE_HISTORY), counts)
            history_group["active"] = active_claims.get(STAGE_HISTORY, 0)
            history_group["summaryMetrics"] = [
                {"label": "Queued", "value": f"{counts.get(STAGE_HISTORY, 0):,}", "note": "wallets in history validation"},
                {"label": "Deep Repair", "value": f"{counts.get(STAGE_DEEP_REPAIR, 0):,}", "note": "wallets promoted for older-history repair"},
                {"label": "Baseline Through", "value": fmt_short_date(self.baseline_at), "note": "historical anchor"},
            ]
            deep_group = self.stage_summary(STAGE_DEEP_REPAIR, self.store.sample_wallets(STAGE_DEEP_REPAIR), counts)
            deep_group["active"] = active_claims.get(STAGE_DEEP_REPAIR, 0)
            deep_group["summaryMetrics"] = [
                {"label": "Queued", "value": f"{counts.get(STAGE_DEEP_REPAIR, 0):,}", "note": "wallets in deep repair"},
                {"label": "Gap Ready", "value": f"{counts.get(STAGE_GAP, 0):,}", "note": "ready for gap recovery"},
            ]
            gap_group = self.stage_summary(STAGE_GAP, self.store.sample_wallets(STAGE_GAP), counts)
            gap_group["active"] = active_claims.get(STAGE_GAP, 0)
            gap_group["summaryMetrics"] = [
                {"label": "Queued", "value": f"{counts.get(STAGE_GAP, 0):,}", "note": "wallets in gap recovery"},
                {"label": "Live Transition", "value": f"{counts.get(STAGE_LIVE_TRANSITION, 0):,}", "note": "closing transition gaps"},
                {"label": "Live", "value": f"{counts.get(STAGE_LIVE, 0):,}", "note": "already in live mode"},
                {"label": "Verified Zero", "value": f"{counts.get(STAGE_ZERO_VERIFIED, 0):,}", "note": "verified current with zero trades"},
            ]
            live_group = self.stage_summary(
                STAGE_LIVE,
                self.store.sample_wallets(STAGE_LIVE_TRANSITION) + self.store.sample_wallets(STAGE_LIVE),
                counts,
            )
            live_group["active"] = active_claims.get(STAGE_LIVE_TRANSITION, 0) + active_claims.get(STAGE_LIVE, 0)
            live_group["summaryMetrics"] = [
                {"label": "Transition", "value": f"{counts.get(STAGE_LIVE_TRANSITION, 0):,}", "note": "temporary live catch-up"},
                {"label": "Permanent Live", "value": f"{counts.get(STAGE_LIVE, 0):,}", "note": "steady-state live tracking"},
                {"label": "Verified Zero", "value": f"{counts.get(STAGE_ZERO_VERIFIED, 0):,}", "note": "kept out of live mode by design"},
                {"label": "Refresh SLA", "value": "1h", "note": "target maximum refresh age"},
            ]
            payload = {
                "version": 2,
                "generatedAt": now_ms(),
                "totalWallets": sum(counts.values()),
                "verifiedZeroTradeWallets": counts.get(STAGE_ZERO_VERIFIED, 0),
                "stageCounts": counts,
                "activeClaims": active_claims,
                "errors": self.store.error_counts(),
                "recentUpdates": self.store.recent_update_counts(),
                "baselineThroughAt": int(self.baseline_at or 0) or None,
                "baselineThroughDate": fmt_short_date(self.baseline_at),
                "source": {
                "walletExplorerV3Path": str(V3_MANIFEST_PATH),
                    "runtimeDbPath": str(self.args.db_path),
                },
                "groups": {
                    "historyCompletion": history_group,
                    "deepRepair": deep_group,
                    "gapRecovery": gap_group,
                    "liveMode": live_group,
                },
            }
            write_json_atomic(STATE_PATH, payload)
            status_rows = self.store.export_wallet_status_rows()
            write_json_atomic(
                STATUS_PATH,
                {
                    "version": 1,
                    "generatedAt": payload["generatedAt"],
                    "rows": status_rows,
                },
            )
            await asyncio.sleep(max(2.0, float(self.args.publish_interval_seconds)))

    async def run(self):
        self.seed()
        publishers = [asyncio.create_task(self.publish_loop())]
        workers = [
            asyncio.create_task(self.worker_loop(index))
            for index in range(max(1, int(self.args.parallelism)))
        ]
        await asyncio.gather(*publishers, *workers)


def build_parser():
    parser = argparse.ArgumentParser(description="Run the wallet review runtime.")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--proxy-dir", default=str(V2_DIR / "review_pipeline" / "proxy_shards"))
    parser.add_argument("--shard-count", type=int, default=8)
    parser.add_argument("--timeout-seconds", type=int, default=20)
    parser.add_argument("--parallelism", type=int, default=8)
    parser.add_argument("--lane-mode", default="auto")
    parser.add_argument("--lease-ms", type=int, default=120000)
    parser.add_argument("--loop-sleep-seconds", type=float, default=0.1)
    parser.add_argument("--claim-batch-size", type=int, default=4)
    parser.add_argument("--history-limit", type=int, default=60)
    parser.add_argument("--gap-limit", type=int, default=25)
    parser.add_argument("--page-budget", type=int, default=2)
    parser.add_argument("--deep-repair-page-budget", type=int, default=4)
    parser.add_argument("--gap-page-budget", type=int, default=4)
    parser.add_argument("--extra-history-checks", type=int, default=2)
    parser.add_argument("--gap-retry-ms", type=int, default=300000)
    parser.add_argument("--transition-poll-ms", type=int, default=300000)
    parser.add_argument("--live-poll-ms", type=int, default=3600000)
    parser.add_argument("--min-transition-window-ms", type=int, default=MIN_TRANSITION_WINDOW_MS)
    parser.add_argument("--max-transition-window-ms", type=int, default=MAX_TRANSITION_WINDOW_MS)
    parser.add_argument("--publish-interval-seconds", type=float, default=5.0)
    return parser


def main():
    args = build_parser().parse_args()
    runtime = ReviewAsyncRuntime(args)
    asyncio.run(runtime.run())


if __name__ == "__main__":
    main()
