#!/usr/bin/env python3
import argparse
import asyncio
import csv
import json
import os
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterable

import requests

from common import (
    API_BASE,
    build_canonical_wallet_record,
    get_http_session,
    initial_wallet_history,
    initial_wallet_state,
    latest_opened_at_from_history,
    load_proxies,
    normalize_trade_row,
    normalize_wallet,
    now_ms,
    shard_index_for_wallet,
    write_json_atomic,
)
from process_isolated_wallet import (
    append_unique_rows,
    init_isolated,
    latest_history_timestamp,
    load_local_owner_truth,
    prepare_history_rows,
    record_has_empty_target_metrics,
)


PHASE1_TARGET_FIELDS = ["volume", "totalTrades", "wins", "losses", "pnl", "fees"]
TRACKED_STAGES = {"tracked", "zero"}
FINAL_STAGES = {"tracked", "zero"}
DEFAULT_DB_PATH = "/root/pacifica-flow/data/wallet_explorer_v2/phase1/async_runtime.sqlite3"


def to_num(value, fallback=0.0):
    try:
        number = float(value)
    except Exception:
        return fallback
    return number


def backoff_ms(retry_count: int, reason: str) -> int:
    lower = str(reason or "").lower()
    if "429" in lower:
        return min(120_000, 8_000 * max(1, retry_count))
    if "504" in lower or "502" in lower:
        return min(90_000, 6_000 * max(1, retry_count))
    if "timeout" in lower:
        return min(180_000, 15_000 * max(1, retry_count))
    return min(120_000, 10_000 * max(1, retry_count))


def has_recovered_metrics(record: dict | None) -> bool:
    safe = record if isinstance(record, dict) else {}
    if not safe:
        return False
    return not record_has_empty_target_metrics(safe)


def load_focus_wallets_from_csv(path_value: str) -> list[str]:
    path_text = str(path_value or "").strip()
    if not path_text:
        return []
    path = Path(path_text)
    if not path.exists():
        return []
    wallets = []
    try:
        with path.open(newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames and "wallet" in reader.fieldnames:
                for row in reader:
                    wallet = normalize_wallet((row or {}).get("wallet"))
                    if wallet:
                        wallets.append(wallet)
            else:
                handle.seek(0)
                for line in handle:
                    wallet = normalize_wallet(str(line or "").split(",")[0])
                    if wallet and wallet.lower() != "wallet":
                        wallets.append(wallet)
    except Exception:
        return []
    seen = set()
    out = []
    for wallet in wallets:
        if wallet in seen:
            continue
        seen.add(wallet)
        out.append(wallet)
    return out


def validate_proxy_subset(proxies: list[str], timeout_seconds: int, max_workers: int = 64) -> list[str]:
    unique = []
    seen = set()
    for proxy in proxies or []:
        text = str(proxy or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        unique.append(text)
    if not unique:
        return []
    connect_timeout = min(max(1, int(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_ASYNC_CONNECT_TIMEOUT_SECONDS", "3") or 3)), max(1, int(timeout_seconds or 8)))
    read_timeout = min(max(connect_timeout, 4), max(4, int(timeout_seconds or 8)))
    probe_url = f"{API_BASE}/markets"

    def check(proxy: str) -> tuple[str, bool]:
        try:
            session = get_http_session(proxy)
            response = session.get(
                probe_url,
                timeout=(connect_timeout, read_timeout),
                allow_redirects=True,
            )
            status = int(response.status_code or 0)
            return proxy, status in {200, 400, 401, 403, 404}
        except requests.RequestException:
            return proxy, False
        except Exception:
            return proxy, False

    healthy = []
    with ThreadPoolExecutor(max_workers=max(8, min(max_workers, len(unique)))) as executor:
        for proxy, ok in executor.map(check, unique):
            if ok:
                healthy.append(proxy)
    return healthy


def estimate_direct_request_credits(endpoint: str, limit: int) -> int:
    endpoint_text = str(endpoint or "").strip().lower()
    request_limit = max(1, int(limit or 1))
    if "funding" in endpoint_text:
        if request_limit <= 1:
            return 3
        if request_limit <= 25:
            return 4
        if request_limit <= 120:
            return 6
        return 8
    if request_limit <= 1:
        return 3
    if request_limit <= 25:
        return 4
    if request_limit <= 120:
        return 6
    return 8


def estimate_proxy_request_spacing_ms(endpoint: str, limit: int) -> int:
    endpoint_text = str(endpoint or "").strip().lower()
    request_limit = max(1, int(limit or 1))
    spacing_scale = max(
        0.25,
        min(2.0, float(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_ASYNC_PROXY_SPACING_SCALE", "1.0") or 1.0)),
    )
    if "funding" in endpoint_text:
        if request_limit <= 1:
            return max(500, int(1800 * spacing_scale))
        if request_limit <= 25:
            return max(500, int(2800 * spacing_scale))
        if request_limit <= 120:
            return max(750, int(4500 * spacing_scale))
        return max(1000, int(6500 * spacing_scale))
    if request_limit <= 1:
        return max(500, int(1500 * spacing_scale))
    if request_limit <= 25:
        return max(500, int(2400 * spacing_scale))
    if request_limit <= 120:
        return max(750, int(4000 * spacing_scale))
    return max(1000, int(6000 * spacing_scale))


class SQLiteCreditRateLimiter:
    def __init__(self, db_path: str, capacity: int, window_ms: int, bucket: str = "direct"):
        self.db_path = str(db_path)
        self.capacity = max(1, int(capacity or 1))
        self.window_ms = max(1000, int(window_ms or 60000))
        self.bucket = str(bucket or "direct")
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None, timeout=30)
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
                CREATE TABLE IF NOT EXISTS rate_limit_events (
                    bucket TEXT NOT NULL,
                    at_ms INTEGER NOT NULL,
                    credits INTEGER NOT NULL
                )
                """
            )
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rate_limit_events_bucket_at ON rate_limit_events(bucket, at_ms)")

    async def acquire(self, credits: int):
        needed = max(1, int(credits or 1))
        while True:
            wait_ms = 0
            with self._lock:
                now = now_ms()
                window_start = now - self.window_ms
                self.conn.execute("BEGIN IMMEDIATE")
                try:
                    self.conn.execute(
                        "DELETE FROM rate_limit_events WHERE at_ms < ?",
                        (window_start - self.window_ms,),
                    )
                    used = int(
                        (
                            self.conn.execute(
                                "SELECT COALESCE(SUM(credits), 0) FROM rate_limit_events WHERE bucket = ? AND at_ms > ?",
                                (self.bucket, window_start),
                            ).fetchone()[0]
                        )
                        or 0
                    )
                    if used + needed <= self.capacity:
                        self.conn.execute(
                            "INSERT INTO rate_limit_events(bucket, at_ms, credits) VALUES (?, ?, ?)",
                            (self.bucket, now, needed),
                        )
                        self.conn.execute("COMMIT")
                        return
                    oldest_row = self.conn.execute(
                        "SELECT MIN(at_ms) FROM rate_limit_events WHERE bucket = ? AND at_ms > ?",
                        (self.bucket, window_start),
                    ).fetchone()
                    oldest_at = int(oldest_row[0] or now)
                    wait_ms = max(250, (oldest_at + self.window_ms) - now + 50)
                    self.conn.execute("COMMIT")
                except Exception:
                    self.conn.execute("ROLLBACK")
                    raise
            await asyncio.sleep(wait_ms / 1000.0)


class AsyncProxyScheduler:
    def __init__(self, proxies: Iterable[str], executor_workers: int):
        raw = []
        seen = set()
        for proxy in proxies or []:
            text = str(proxy or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            raw.append(text)
        if not raw:
            raw = [None]
        self.entries = [
            {
                "id": idx,
                "proxy": proxy,
                "cooldown_until": 0,
                "inflight": 0,
                "successes": 0,
                "failures": 0,
                "last_error": None,
                "disabled_until": 0,
                "next_ready_at": 0,
            }
            for idx, proxy in enumerate(raw)
        ]
        self._lock = asyncio.Lock()
        self._executor = ThreadPoolExecutor(max_workers=max(8, int(executor_workers or 8)))
        self._max_inflight_per_proxy = max(
            1,
            int(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_ASYNC_PROXY_MAX_INFLIGHT", "1") or 1),
        )

    @property
    def executor(self):
        return self._executor

    async def acquire(self, wallet: str | None = None, endpoint: str | None = None, limit: int | None = None) -> dict:
        wallet_text = str(wallet or "")
        start_index = shard_index_for_wallet(wallet_text, len(self.entries)) if wallet_text else 0
        request_spacing_ms = estimate_proxy_request_spacing_ms(str(endpoint or ""), int(limit or 1))
        while True:
            async with self._lock:
                now = now_ms()
                chosen = None
                for offset in range(len(self.entries)):
                    entry = self.entries[(start_index + offset) % len(self.entries)]
                    if int(entry["disabled_until"] or 0) > now:
                        continue
                    if int(entry["cooldown_until"] or 0) > now:
                        continue
                    if int(entry["next_ready_at"] or 0) > now:
                        continue
                    if int(entry["inflight"] or 0) >= self._max_inflight_per_proxy:
                        continue
                    if chosen is None or (
                        (int(entry["inflight"] or 0), -int(entry["successes"] or 0), int(entry["failures"] or 0))
                        < (int(chosen["inflight"] or 0), -int(chosen["successes"] or 0), int(chosen["failures"] or 0))
                    ):
                        chosen = entry
                        if entry["inflight"] <= 0:
                            break
                if chosen is not None:
                    chosen["inflight"] += 1
                    if chosen.get("proxy"):
                        chosen["next_ready_at"] = max(int(chosen["next_ready_at"] or 0), now + request_spacing_ms)
                    return chosen
                next_ready_at = min(
                    max(
                        int(entry["cooldown_until"] or 0),
                        int(entry["disabled_until"] or 0),
                        int(entry["next_ready_at"] or 0),
                        now,
                    )
                    for entry in self.entries
                )
            await asyncio.sleep(max(0.05, min(1.0, (next_ready_at - now_ms()) / 1000.0)))

    async def mark_success(self, entry: dict):
        async with self._lock:
            entry["inflight"] = max(0, int(entry["inflight"] or 0) - 1)
            entry["cooldown_until"] = 0
            entry["successes"] = int(entry["successes"] or 0) + 1
            entry["last_error"] = None
            entry["disabled_until"] = 0

    async def mark_failure(self, entry: dict, reason: str, retry_count: int = 1):
        async with self._lock:
            entry["inflight"] = max(0, int(entry["inflight"] or 0) - 1)
            entry["failures"] = int(entry["failures"] or 0) + 1
            entry["last_error"] = str(reason or "")
            now = now_ms()
            base_cooldown = now + backoff_ms(retry_count, reason)
            entry["cooldown_until"] = max(int(entry["cooldown_until"] or 0), base_cooldown)
            lower = str(reason or "").lower()
            if "429" in lower:
                entry["next_ready_at"] = max(int(entry["next_ready_at"] or 0), now + 60_000)
            elif "504" in lower or "timeout" in lower:
                entry["next_ready_at"] = max(int(entry["next_ready_at"] or 0), now + 15_000)
            hard_proxy_failure = any(
                token in lower
                for token in (
                    "proxyerror",
                    "network is unreachable",
                    "failed to establish a new connection",
                    "connecttimeout",
                    "connection to ",
                )
            )
            if hard_proxy_failure and int(entry["successes"] or 0) <= 0:
                if int(entry["failures"] or 0) >= 2:
                    entry["disabled_until"] = max(int(entry["disabled_until"] or 0), now + 15 * 60 * 1000)
                if int(entry["failures"] or 0) >= 4:
                    entry["disabled_until"] = max(int(entry["disabled_until"] or 0), now + 6 * 60 * 60 * 1000)

    async def snapshot(self) -> dict:
        async with self._lock:
            now = now_ms()
            return {
                "total": len(self.entries),
                "active": sum(1 for entry in self.entries if int(entry["inflight"] or 0) > 0),
                "cooling": sum(1 for entry in self.entries if int(entry["cooldown_until"] or 0) > now),
                "disabled": sum(1 for entry in self.entries if int(entry["disabled_until"] or 0) > now),
                "entries": [
                    {
                        "id": int(entry["id"]),
                        "proxy": entry["proxy"],
                        "inflight": int(entry["inflight"] or 0),
                        "successes": int(entry["successes"] or 0),
                        "failures": int(entry["failures"] or 0),
                        "cooldownUntil": int(entry["cooldown_until"] or 0),
                        "disabledUntil": int(entry["disabled_until"] or 0),
                        "nextReadyAt": int(entry["next_ready_at"] or 0),
                        "lastError": entry["last_error"],
                    }
                    for entry in self.entries
                ],
            }


class AsyncPacificaClient:
    def __init__(self, scheduler: AsyncProxyScheduler, timeout_seconds: int = 20, db_path: str = DEFAULT_DB_PATH):
        self.scheduler = scheduler
        self.timeout_seconds = max(1, int(timeout_seconds or 20))
        self.connect_timeout_seconds = min(self.timeout_seconds, max(1, int(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_ASYNC_CONNECT_TIMEOUT_SECONDS", "3") or 3)))
        direct_capacity = int(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_ASYNC_DIRECT_CREDIT_CAPACITY", "80") or 80)
        direct_window_ms = int(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_ASYNC_DIRECT_CREDIT_WINDOW_MS", "60000") or 60000)
        self.direct_rate_limiter = SQLiteCreditRateLimiter(db_path, direct_capacity, direct_window_ms, "phase1_direct")

    def _request_json_sync(self, url: str, proxy: str | None):
        session = get_http_session(proxy)
        response = session.get(
            url,
            timeout=(self.connect_timeout_seconds, self.timeout_seconds),
            allow_redirects=True,
        )
        status = int(response.status_code or 0)
        if status == 429:
            raise RuntimeError("http_429")
        if status < 200 or status >= 300:
            raise RuntimeError(f"http_{status}")
        try:
            return response.json()
        except Exception as exc:
            raise RuntimeError(f"invalid_json:{exc}") from exc

    async def request_history(self, wallet: str, endpoint: str, limit: int, cursor: str | None = None):
        params = {"account": wallet, "limit": str(max(1, int(limit or 1)))}
        if cursor:
            params["cursor"] = cursor
        query = "&".join(f"{key}={value}" for key, value in params.items())
        url = f"{API_BASE}/{endpoint}?{query}"
        entry = await self.scheduler.acquire(wallet, endpoint, limit)
        if not entry.get("proxy"):
            await self.direct_rate_limiter.acquire(estimate_direct_request_credits(endpoint, limit))
        loop = asyncio.get_running_loop()
        try:
            payload = await loop.run_in_executor(
                self.scheduler.executor,
                self._request_json_sync,
                url,
                entry.get("proxy"),
            )
            await self.scheduler.mark_success(entry)
            return payload, entry
        except Exception as exc:
            await self.scheduler.mark_failure(entry, str(exc), 1)
            raise


class SQLiteStateStore:
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
                    trade_rows_loaded INTEGER NOT NULL DEFAULT 0,
                    funding_rows_loaded INTEGER NOT NULL DEFAULT 0,
                    last_trade_at INTEGER NOT NULL DEFAULT 0,
                    volume_usd REAL NOT NULL DEFAULT 0,
                    pnl_usd REAL NOT NULL DEFAULT 0,
                    wins INTEGER NOT NULL DEFAULT 0,
                    losses INTEGER NOT NULL DEFAULT 0,
                    fees_usd REAL NOT NULL DEFAULT 0,
                    local_complete INTEGER NOT NULL DEFAULT 0,
                    metrics_recovered INTEGER NOT NULL DEFAULT 0,
                    head_complete INTEGER NOT NULL DEFAULT 0,
                    verified INTEGER NOT NULL DEFAULT 0,
                    zero_history_result INTEGER NOT NULL DEFAULT 0,
                    status TEXT,
                    proof_class TEXT,
                    source_record_path TEXT
                )
                """
            )
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_wallets_stage_ready ON wallets(stage, next_eligible_at, priority, updated_at)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_wallets_claim ON wallets(claim_expires_at)")

    def close(self):
        with self._lock:
            self.conn.close()

    def seed_wallets(self, wallets: list[str], classifier):
        now = now_ms()
        rows = []
        for wallet in wallets:
            stage_info = classifier(wallet)
            rows.append(
                (
                    wallet,
                    stage_info["stage"],
                    int(stage_info.get("priority") or 0),
                    int(stage_info.get("nextEligibleAt") or 0),
                    now,
                    int(stage_info.get("tradeRowsLoaded") or 0),
                    int(stage_info.get("fundingRowsLoaded") or 0),
                    int(stage_info.get("lastTradeAt") or 0),
                    float(stage_info.get("volumeUsd") or 0.0),
                    float(stage_info.get("pnlUsd") or 0.0),
                    int(stage_info.get("wins") or 0),
                    int(stage_info.get("losses") or 0),
                    float(stage_info.get("feesUsd") or 0.0),
                    1 if stage_info.get("localComplete") else 0,
                    1 if stage_info.get("metricsRecovered") else 0,
                    1 if stage_info.get("headComplete") else 0,
                    1 if stage_info.get("verified") else 0,
                    1 if stage_info.get("zeroHistoryResult") else 0,
                    stage_info.get("status"),
                    stage_info.get("proofClass"),
                    stage_info.get("sourceRecordPath"),
                )
            )
        with self._lock:
            self.conn.executemany(
                """
                INSERT INTO wallets (
                    wallet, stage, priority, next_eligible_at, updated_at,
                    trade_rows_loaded, funding_rows_loaded, last_trade_at,
                    volume_usd, pnl_usd, wins, losses, fees_usd,
                    local_complete, metrics_recovered, head_complete, verified,
                    zero_history_result, status, proof_class, source_record_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(wallet) DO UPDATE SET
                    stage=excluded.stage,
                    priority=excluded.priority,
                    next_eligible_at=excluded.next_eligible_at,
                    updated_at=excluded.updated_at,
                    trade_rows_loaded=excluded.trade_rows_loaded,
                    funding_rows_loaded=excluded.funding_rows_loaded,
                    last_trade_at=excluded.last_trade_at,
                    volume_usd=excluded.volume_usd,
                    pnl_usd=excluded.pnl_usd,
                    wins=excluded.wins,
                    losses=excluded.losses,
                    fees_usd=excluded.fees_usd,
                    local_complete=excluded.local_complete,
                    metrics_recovered=excluded.metrics_recovered,
                    head_complete=excluded.head_complete,
                    verified=excluded.verified,
                    zero_history_result=excluded.zero_history_result,
                    status=excluded.status,
                    proof_class=excluded.proof_class,
                    source_record_path=excluded.source_record_path,
                    claim_token=NULL,
                    claim_expires_at=0
                """,
                rows,
            )

    def claim_next(self, stages: list[str], worker_id: str, lease_ms: int) -> sqlite3.Row | None:
        now = now_ms()
        placeholders = ",".join("?" for _ in stages)
        with self._lock:
            cursor = self.conn.execute(
                f"""
                SELECT wallet FROM wallets
                WHERE stage IN ({placeholders})
                  AND next_eligible_at <= ?
                  AND (claim_expires_at IS NULL OR claim_expires_at <= ?)
                ORDER BY priority DESC, next_eligible_at ASC, updated_at ASC, wallet ASC
                LIMIT 1
                """,
                [*stages, now, now],
            )
            row = cursor.fetchone()
            if row is None:
                return None
            wallet = row["wallet"]
            token = f"{worker_id}:{uuid.uuid4().hex}"
            self.conn.execute(
                """
                UPDATE wallets
                SET claim_token = ?, claim_expires_at = ?, updated_at = ?
                WHERE wallet = ?
                """,
                (token, now + max(10_000, int(lease_ms or 120_000)), now, wallet),
            )
            return self.conn.execute("SELECT * FROM wallets WHERE wallet = ?", (wallet,)).fetchone()

    def update_wallet(self, wallet: str, **updates):
        if not updates:
            return
        columns = []
        values = []
        for key, value in updates.items():
            columns.append(f"{key} = ?")
            values.append(value)
        values.append(wallet)
        with self._lock:
            self.conn.execute(f"UPDATE wallets SET {', '.join(columns)} WHERE wallet = ?", values)

    def release_wallet(self, wallet: str, **updates):
        merged = {"claim_token": None, "claim_expires_at": 0, "updated_at": now_ms()}
        merged.update(updates)
        self.update_wallet(wallet, **merged)

    def counts(self) -> dict:
        with self._lock:
            rows = self.conn.execute(
                "SELECT stage, COUNT(*) AS count FROM wallets GROUP BY stage"
            ).fetchall()
        counts = {row["stage"]: int(row["count"]) for row in rows}
        counts.setdefault("probe", 0)
        counts.setdefault("recovery", 0)
        counts.setdefault("proof", 0)
        counts.setdefault("tracked", 0)
        counts.setdefault("audit", 0)
        counts.setdefault("zero", 0)
        return counts

    def next_candidates(self, limit: int = 50) -> list[dict]:
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT wallet, stage, priority, next_eligible_at, trade_rows_loaded, funding_rows_loaded,
                       volume_usd, verified, zero_history_result, status, proof_class
                FROM wallets
                ORDER BY
                    CASE stage
                        WHEN 'probe' THEN 0
                        WHEN 'recovery' THEN 1
                        WHEN 'proof' THEN 2
                        WHEN 'audit' THEN 3
                        WHEN 'zero' THEN 4
                        WHEN 'tracked' THEN 5
                        ELSE 6
                    END,
                    next_eligible_at ASC,
                    priority DESC,
                    wallet ASC
                LIMIT ?
                """,
                (max(1, int(limit or 50)),),
            ).fetchall()
        return [dict(row) for row in rows]


class Phase1AsyncRuntime:
    def __init__(self, args):
        self.args = args
        self.worker_id = f"{args.lane_mode}:{os.getpid()}"
        self.store = SQLiteStateStore(args.db_path)
        self.runs = 0
        self.completed_runs = 0
        self.completed_verified_runs = 0
        self.last_completed_wallet = None
        self.last_completed_at = None
        self.last_promoted_wallet = None
        self.last_promoted_at = None
        self.completed_local_proof_runs = 0
        self.completed_head_proof_runs = 0
        self.last_started_wallet = None
        self.last_started_at = None
        self.last_exit_code = None
        self.active = {}
        self.active_lock = asyncio.Lock()
        self.focus_wallets = load_focus_wallets_from_csv(args.focus_cohort_csv)
        proxy_entries = []
        proxy_dir = Path(args.proxy_dir)
        if proxy_dir.exists():
            for path in sorted(proxy_dir.glob("*.txt")):
                proxy_entries.extend(load_proxies(str(path)))
        if str(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_ASYNC_VALIDATE_PROXIES", "true")).strip().lower() not in {"0", "false", "no"}:
            proxy_entries = validate_proxy_subset(proxy_entries, args.timeout_seconds)
        self.scheduler = AsyncProxyScheduler(proxy_entries, args.parallelism)
        self.client = AsyncPacificaClient(self.scheduler, args.timeout_seconds, args.db_path)
        direct_only = len([entry for entry in self.scheduler.entries if entry.get("proxy")]) <= 0
        direct_parallelism_cap = max(
            1,
            int(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_ASYNC_DIRECT_PARALLELISM", "8") or 8),
        )
        self.effective_parallelism = (
            min(max(1, int(args.parallelism)), direct_parallelism_cap) if direct_only else max(1, int(args.parallelism))
        )

    def classify_wallet(self, wallet: str) -> dict:
        record_path = None
        isolated_record_path = Path("/root/pacifica-flow/data/wallet_explorer_v2/isolated") / wallet / "wallet_record.json"
        isolated_record = None
        if isolated_record_path.exists():
            record_path = str(isolated_record_path)
            with isolated_record_path.open() as handle:
                isolated_record = json.load(handle)
        safe = isolated_record if isinstance(isolated_record, dict) else {}
        phase1 = safe.get("phase1") if isinstance(safe.get("phase1"), dict) else {}
        metrics_present = has_recovered_metrics(safe)
        local_complete = bool(phase1.get("localComplete"))
        verified = bool(phase1.get("verifiedThroughNow")) and metrics_present
        empty_probe_confirmations = max(
            0,
            int(phase1.get("emptyProbeConfirmations") or (1 if phase1.get("zeroHistoryResult") else 0) or 0),
        )
        status = str(phase1.get("status") or safe.get("historyProofStatus") or "").strip().lower() or None
        zero_history = bool(
            phase1.get("zeroHistoryResult")
            or (
                local_complete
                and not metrics_present
                and int(safe.get("tradeRowsLoaded") or 0) <= 0
                and not int(safe.get("lastTrade") or 0)
            )
        )
        explicit_zero_history_statuses = {
            "phase1_probe_empty_observed",
            "phase1_zero_history_local_complete",
        }
        if verified:
            stage = "tracked"
        elif zero_history and status in explicit_zero_history_statuses:
            stage = "zero"
        elif zero_history:
            stage = "probe"
        elif local_complete and metrics_present:
            stage = "proof"
        elif safe:
            stage = "recovery"
        else:
            stage = "probe"
        priority = {
            "probe": 100,
            "recovery": 80,
            "proof": 60,
            "audit": 20,
            "zero": 15,
            "tracked": 10,
        }[stage]
        return {
            "stage": stage,
            "priority": priority,
            "nextEligibleAt": int(phase1.get("head", {}).get("nextEligibleAt") or safe.get("nextEligibleAt") or 0),
            "tradeRowsLoaded": int(safe.get("tradeRowsLoaded") or 0),
            "fundingRowsLoaded": int(safe.get("fundingRowsLoaded") or 0),
            "lastTradeAt": int(safe.get("lastTrade") or 0),
            "volumeUsd": float(safe.get("volumeUsdRaw") or 0.0),
            "pnlUsd": float(safe.get("pnlUsd") or 0.0),
            "wins": int(safe.get("totalWins") or 0),
            "losses": int(safe.get("totalLosses") or 0),
            "feesUsd": float(safe.get("feesPaidUsd") or safe.get("feesUsd") or 0.0),
            "localComplete": local_complete,
            "metricsRecovered": metrics_present,
            "headComplete": verified,
            "verified": verified,
            "zeroHistoryResult": zero_history,
            "emptyProbeConfirmations": empty_probe_confirmations,
            "status": status,
            "proofClass": "verified_through_now" if verified else "zero_history_result" if zero_history else "local_complete_head_pending" if local_complete else stage,
            "sourceRecordPath": record_path,
        }

    def seed(self):
        self.store.seed_wallets(self.focus_wallets, self.classify_wallet)

    async def set_active(self, worker_name: str, wallet: str | None, stage: str | None = None):
        async with self.active_lock:
            self.active[worker_name] = {
                "wallet": wallet,
                "stage": stage,
                "updatedAt": now_ms(),
            }

    def load_wallet_state(self, wallet: str):
        local_truth = load_local_owner_truth(wallet, self.args.shard_count)
        state, history = init_isolated(wallet, self.args.shard_count, local_truth)
        history["trades"], _ = prepare_history_rows(history.get("trades") or [], "trades")
        history["funding"], _ = prepare_history_rows(history.get("funding") or [], "funding")
        return state, history

    def write_wallet_state(self, wallet: str, state: dict, history: dict, phase1: dict):
        state["last_opened_at"] = latest_opened_at_from_history(history)
        record = build_canonical_wallet_record(wallet, int(state.get("shardIndex") or 0), state, history)
        existing = {}
        record_path = Path("/root/pacifica-flow/data/wallet_explorer_v2/isolated") / wallet / "wallet_record.json"
        if record_path.exists():
            try:
                with record_path.open() as handle:
                    existing = json.load(handle)
            except Exception:
                existing = {}
        record["phase1"] = {
            **(existing.get("phase1") if isinstance(existing.get("phase1"), dict) else {}),
            **phase1,
        }
        isolated_dir = record_path.parent
        isolated_dir.mkdir(parents=True, exist_ok=True)
        write_json_atomic(isolated_dir / "wallet_state.json", state)
        write_json_atomic(isolated_dir / "wallet_history.json", history)
        write_json_atomic(record_path, record)
        return record

    def build_phase1_payload(self, stage_status: str, state: dict, history: dict, record: dict, head: dict | None = None, zero_history: bool = False):
        head = head if isinstance(head, dict) else {
            "status": "pending",
            "storedLastAt": int((record or {}).get("lastTrade") or 0) or None,
            "remoteLastAt": None,
            "hasMore": None,
            "rowCount": 0,
            "error": None,
        }
        metrics_present = has_recovered_metrics(record)
        local_complete = bool(((state.get("streams") or {}).get("trades") or {}).get("exhausted")) and int((record or {}).get("tradeRowsLoaded") or 0) == len(history.get("trades") or [])
        verified = bool(head.get("status") == "verified" and metrics_present and not zero_history)
        checked_at = now_ms()
        return {
            "mode": "target_metrics_trades_only",
            "targetFields": PHASE1_TARGET_FIELDS,
            "checkedAt": checked_at,
            "tradeRowsLoaded": int((record or {}).get("tradeRowsLoaded") or len(history.get("trades") or [])),
            "storedLastTrade": int((record or {}).get("lastTrade") or 0) or None,
            "localComplete": local_complete,
            "zeroHistoryResult": bool(zero_history),
            "metricsRecovered": bool(metrics_present and not zero_history),
            "verifiedThroughNow": bool(verified),
            "status": stage_status,
            "head": {
                "checkedAt": checked_at,
                "complete": bool(head.get("status") == "verified"),
                "retryCount": 0,
                "nextEligibleAt": None,
                "trades": head,
            },
        }

    def load_existing_phase1(self, wallet: str) -> dict:
        record_path = Path("/root/pacifica-flow/data/wallet_explorer_v2/isolated") / wallet / "wallet_record.json"
        if not record_path.exists():
            return {}
        try:
            loaded = json.loads(record_path.read_text())
        except Exception:
            return {}
        phase1 = loaded.get("phase1")
        return phase1 if isinstance(phase1, dict) else {}

    async def fetch_trade_page(self, wallet: str, limit: int, cursor: str | None = None):
        payload, _entry = await self.client.request_history(wallet, "trades/history", limit, cursor)
        rows = [normalize_trade_row(row) for row in (payload.get("data") or [])]
        return {
            "rows": rows,
            "hasMore": bool(payload.get("has_more")),
            "nextCursor": payload.get("next_cursor"),
            "remoteLastAt": latest_history_timestamp(rows, "timestamp"),
        }

    async def handle_probe(self, wallet: str):
        state, history = self.load_wallet_state(wallet)
        existing_phase1 = self.load_existing_phase1(wallet)
        probe_limit = max(1, int(self.args.probe_limit))
        result = await self.fetch_trade_page(wallet, probe_limit, None)
        trade_seen = set()
        history["trades"], trade_seen = prepare_history_rows(history.get("trades") or [], "trades")
        append_unique_rows(history["trades"], result["rows"], "trades", trade_seen)
        trade_stream = ((state.get("streams") or {}).get("trades")) or {}
        trade_stream["pagesFetched"] = int(trade_stream.get("pagesFetched") or 0) + 1
        trade_stream["rowsFetched"] = len(history["trades"])
        trade_stream["lastAttemptAt"] = now_ms()
        trade_stream["lastSuccessAt"] = now_ms()
        trade_stream["frontierCursor"] = result["nextCursor"] if result["hasMore"] else None
        trade_stream["failedCursor"] = None
        trade_stream["lastError"] = None
        trade_stream["retryCount"] = 0
        trade_stream["nextEligibleAt"] = 0
        trade_stream["exhausted"] = not result["hasMore"]
        state["status"] = "probe_complete"
        empty_probe_confirmations = (
            max(0, int(existing_phase1.get("emptyProbeConfirmations") or 0)) + 1
            if (not result["rows"] and not result["hasMore"])
            else 0
        )
        phase1_payload = self.build_phase1_payload(
            "phase1_probe_non_empty" if result["rows"] else "phase1_probe_empty_observed",
            state,
            history,
            build_canonical_wallet_record(wallet, int(state.get("shardIndex") or 0), state, history),
            {
                "status": "verified" if not result["hasMore"] else "deferred",
                "storedLastAt": int(result["remoteLastAt"] or 0) or None,
                "remoteLastAt": int(result["remoteLastAt"] or 0) or None,
                "hasMore": result["hasMore"],
                "rowCount": len(result["rows"]),
                "error": None,
            },
            zero_history=not result["rows"] and not result["hasMore"],
        )
        phase1_payload["emptyProbeConfirmations"] = empty_probe_confirmations
        record = self.write_wallet_state(wallet, state, history, phase1_payload)
        if not result["rows"] and not result["hasMore"]:
            self.completed_local_proof_runs += 1
            self.store.release_wallet(
                wallet,
                stage="zero" if empty_probe_confirmations >= 2 else "probe",
                priority=15 if empty_probe_confirmations >= 2 else 30,
                next_eligible_at=0 if empty_probe_confirmations >= 2 else now_ms() + 5 * 60 * 1000,
                zero_history_result=1,
                local_complete=1,
                metrics_recovered=0,
                head_complete=0,
                verified=0,
                status="phase1_probe_empty_observed",
                proof_class="zero_history_result",
                trade_rows_loaded=0,
                last_trade_at=0,
                volume_usd=0.0,
                pnl_usd=0.0,
                wins=0,
                losses=0,
                fees_usd=0.0,
            )
            return
        next_stage = "proof" if not result["hasMore"] and has_recovered_metrics(record) else "recovery"
        self.store.release_wallet(
            wallet,
            stage=next_stage,
            priority=80 if next_stage == "recovery" else 60,
            zero_history_result=0,
            local_complete=1 if next_stage == "proof" else 0,
            metrics_recovered=1 if has_recovered_metrics(record) else 0,
            trade_rows_loaded=int(record.get("tradeRowsLoaded") or 0),
            funding_rows_loaded=int(record.get("fundingRowsLoaded") or 0),
            last_trade_at=int(record.get("lastTrade") or 0),
            volume_usd=float(record.get("volumeUsdRaw") or 0.0),
            pnl_usd=float(record.get("pnlUsd") or 0.0),
            wins=int(record.get("totalWins") or 0),
            losses=int(record.get("totalLosses") or 0),
            fees_usd=float(record.get("feesPaidUsd") or record.get("feesUsd") or 0.0),
            status="phase1_probe_non_empty",
            proof_class=next_stage,
        )

    async def handle_recovery(self, wallet: str):
        state, history = self.load_wallet_state(wallet)
        existing_phase1 = self.load_existing_phase1(wallet)
        trade_stream = ((state.get("streams") or {}).get("trades")) or {}
        cursor = trade_stream.get("frontierCursor")
        trade_seen = set()
        history["trades"], trade_seen = prepare_history_rows(history.get("trades") or [], "trades")
        if not cursor and history["trades"]:
            trade_stream["exhausted"] = True
        page_budget = max(1, int(self.args.page_budget))
        history_limit = max(1, int(self.args.history_limit))
        current_limit = max(25, min(history_limit, int(trade_stream.get("currentLimit") or min(history_limit, 120))))
        pages = 0
        while pages < page_budget and not bool(trade_stream.get("exhausted")):
            result = await self.fetch_trade_page(wallet, current_limit, trade_stream.get("frontierCursor"))
            append_unique_rows(history["trades"], result["rows"], "trades", trade_seen)
            pages += 1
            trade_stream["pagesFetched"] = int(trade_stream.get("pagesFetched") or 0) + 1
            trade_stream["rowsFetched"] = len(history["trades"])
            trade_stream["lastAttemptAt"] = now_ms()
            trade_stream["lastSuccessAt"] = now_ms()
            trade_stream["frontierCursor"] = result["nextCursor"] if result["hasMore"] else None
            trade_stream["failedCursor"] = None
            trade_stream["lastError"] = None
            trade_stream["retryCount"] = 0
            trade_stream["nextEligibleAt"] = 0
            trade_stream["exhausted"] = not result["hasMore"]
            trade_stream["currentLimit"] = current_limit
            if not result["hasMore"]:
                break
        state["status"] = "recovery_complete" if bool(trade_stream.get("exhausted")) else "recovery_in_progress"
        record = self.write_wallet_state(
            wallet,
            state,
            history,
            self.build_phase1_payload(
                "phase1_local_complete_head_pending" if bool(trade_stream.get("exhausted")) and has_recovered_metrics(build_canonical_wallet_record(wallet, int(state.get("shardIndex") or 0), state, history)) else "phase1_trade_backfill_incomplete",
                state,
                history,
                build_canonical_wallet_record(wallet, int(state.get("shardIndex") or 0), state, history),
                {
                    "status": "deferred",
                    "storedLastAt": int(latest_history_timestamp(history.get("trades") or [], "timestamp") or 0) or None,
                    "remoteLastAt": None,
                    "hasMore": None,
                    "rowCount": 0,
                    "error": None,
                },
                zero_history=bool(trade_stream.get("exhausted")) and not has_recovered_metrics(build_canonical_wallet_record(wallet, int(state.get("shardIndex") or 0), state, history)),
            ),
        )
        if bool(trade_stream.get("exhausted")) and not has_recovered_metrics(record):
            self.completed_local_proof_runs += 1
            empty_probe_confirmations = max(2, int(existing_phase1.get("emptyProbeConfirmations") or 0) + 1)
            self.store.release_wallet(
                wallet,
                stage="zero" if empty_probe_confirmations >= 2 else "probe",
                priority=15 if empty_probe_confirmations >= 2 else 30,
                next_eligible_at=0 if empty_probe_confirmations >= 2 else now_ms() + 5 * 60 * 1000,
                zero_history_result=1,
                local_complete=1,
                metrics_recovered=0,
                status="phase1_zero_history_local_complete",
                proof_class="zero_history_result",
                trade_rows_loaded=int(record.get("tradeRowsLoaded") or 0),
                funding_rows_loaded=int(record.get("fundingRowsLoaded") or 0),
                last_trade_at=int(record.get("lastTrade") or 0),
                volume_usd=float(record.get("volumeUsdRaw") or 0.0),
                pnl_usd=float(record.get("pnlUsd") or 0.0),
                wins=int(record.get("totalWins") or 0),
                losses=int(record.get("totalLosses") or 0),
                fees_usd=float(record.get("feesPaidUsd") or record.get("feesUsd") or 0.0),
            )
            return
        next_stage = "proof" if bool(trade_stream.get("exhausted")) else "recovery"
        self.store.release_wallet(
            wallet,
            stage=next_stage,
            priority=60 if next_stage == "proof" else 80,
            zero_history_result=0,
            local_complete=1 if next_stage == "proof" else 0,
            metrics_recovered=1 if has_recovered_metrics(record) else 0,
            status="phase1_local_complete_head_pending" if next_stage == "proof" else "phase1_trade_backfill_incomplete",
            proof_class=next_stage,
            trade_rows_loaded=int(record.get("tradeRowsLoaded") or 0),
            funding_rows_loaded=int(record.get("fundingRowsLoaded") or 0),
            last_trade_at=int(record.get("lastTrade") or 0),
            volume_usd=float(record.get("volumeUsdRaw") or 0.0),
            pnl_usd=float(record.get("pnlUsd") or 0.0),
            wins=int(record.get("totalWins") or 0),
            losses=int(record.get("totalLosses") or 0),
            fees_usd=float(record.get("feesPaidUsd") or record.get("feesUsd") or 0.0),
            next_eligible_at=0,
        )

    async def handle_proof(self, wallet: str):
        state, history = self.load_wallet_state(wallet)
        existing_phase1 = self.load_existing_phase1(wallet)
        record_path = Path("/root/pacifica-flow/data/wallet_explorer_v2/isolated") / wallet / "wallet_record.json"
        record = {}
        if record_path.exists():
            with record_path.open() as handle:
                record = json.load(handle)
        if not has_recovered_metrics(record):
            phase1 = self.build_phase1_payload(
                "phase1_zero_history_local_complete",
                state,
                history,
                record,
                {
                    "status": "skipped_zero_history",
                    "storedLastAt": int(record.get("lastTrade") or 0) or None,
                    "remoteLastAt": None,
                    "hasMore": None,
                    "rowCount": 0,
                    "error": None,
                },
                zero_history=True,
            )
            self.write_wallet_state(wallet, state, history, phase1)
            zero_confirmations = max(0, int(existing_phase1.get("emptyProbeConfirmations") or 0))
            self.store.release_wallet(
                wallet,
                stage="zero" if zero_confirmations >= 2 else "probe",
                priority=15 if zero_confirmations >= 2 else 30,
                next_eligible_at=0 if zero_confirmations >= 2 else now_ms() + 5 * 60 * 1000,
                zero_history_result=1,
                local_complete=1,
                metrics_recovered=0,
                verified=0,
                status=phase1["status"],
                proof_class="zero_history_result",
            )
            return
        result = await self.fetch_trade_page(wallet, 1, None)
        stored_last = int(record.get("lastTrade") or 0)
        remote_last = int(result.get("remoteLastAt") or 0)
        if remote_last <= 0 and stored_last <= 0:
            head = "remote_empty"
        elif remote_last <= stored_last:
            head = "verified"
        else:
            head = "remote_newer"
        phase1 = self.build_phase1_payload(
            "phase1_verified_through_now" if head == "verified" else "phase1_head_mismatch_remote_newer",
            state,
            history,
            record,
            {
                "status": head,
                "storedLastAt": stored_last or None,
                "remoteLastAt": remote_last or None,
                "hasMore": bool(result.get("hasMore")),
                "rowCount": len(result.get("rows") or []),
                "error": None,
            },
            zero_history=False,
        )
        record = self.write_wallet_state(wallet, state, history, phase1)
        if head == "verified":
            self.completed_head_proof_runs += 1
            self.store.release_wallet(
                wallet,
                stage="tracked",
                priority=10,
                local_complete=1,
                metrics_recovered=1,
                head_complete=1,
                verified=1,
                zero_history_result=0,
                status="phase1_verified_through_now",
                proof_class="verified_through_now",
                trade_rows_loaded=int(record.get("tradeRowsLoaded") or 0),
                funding_rows_loaded=int(record.get("fundingRowsLoaded") or 0),
                last_trade_at=int(record.get("lastTrade") or 0),
                volume_usd=float(record.get("volumeUsdRaw") or 0.0),
                pnl_usd=float(record.get("pnlUsd") or 0.0),
                wins=int(record.get("totalWins") or 0),
                losses=int(record.get("totalLosses") or 0),
                fees_usd=float(record.get("feesPaidUsd") or record.get("feesUsd") or 0.0),
            )
            self.completed_verified_runs += 1
            self.last_promoted_wallet = wallet
            self.last_promoted_at = now_ms()
        else:
            self.store.release_wallet(
                wallet,
                stage="recovery",
                priority=80,
                local_complete=1,
                metrics_recovered=1,
                head_complete=0,
                verified=0,
                zero_history_result=0,
                status="phase1_head_mismatch_remote_newer",
                proof_class="local_complete_head_pending",
                next_eligible_at=0,
            )

    async def handle_wallet(self, wallet_row: sqlite3.Row):
        wallet = str(wallet_row["wallet"]).strip()
        stage = str(wallet_row["stage"]).strip().lower()
        self.runs += 1
        self.last_started_wallet = wallet
        self.last_started_at = now_ms()
        if stage == "probe":
            await self.handle_probe(wallet)
        elif stage == "audit":
            await self.handle_probe(wallet)
        elif stage == "recovery":
            await self.handle_recovery(wallet)
        elif stage == "proof":
            await self.handle_proof(wallet)
        else:
            self.store.release_wallet(wallet, stage=stage)
        self.completed_runs += 1
        self.last_completed_wallet = wallet
        self.last_completed_at = now_ms()
        self.last_exit_code = 0

    async def worker_loop(self, index: int):
        worker_name = f"{self.args.lane_mode}:{index}"
        stage_map = {
            "probe": ["probe", "audit"],
            "recovery": ["recovery", "probe", "audit"],
            "proof": ["proof", "recovery", "probe", "audit"],
            "auto": ["proof", "recovery", "probe", "audit"],
        }
        stages = stage_map.get(self.args.lane_mode, ["probe"])
        while True:
            claimed = None
            for stage in stages:
                claimed = self.store.claim_next([stage], worker_name, self.args.lease_ms)
                if claimed is not None:
                    break
            if claimed is None:
                await self.set_active(worker_name, None, None)
                await asyncio.sleep(max(0.1, float(self.args.loop_sleep_seconds)))
                continue
            wallet = str(claimed["wallet"]).strip()
            await self.set_active(worker_name, wallet, str(claimed["stage"]))
            try:
                await self.handle_wallet(claimed)
            except Exception as exc:
                retry_count = int(claimed["attempt_count"] or 0) + 1
                retry_stage = "probe" if str(claimed["stage"]).strip().lower() == "audit" else str(claimed["stage"])
                self.store.release_wallet(
                    wallet,
                    stage=retry_stage,
                    attempt_count=retry_count,
                    next_eligible_at=now_ms() + backoff_ms(retry_count, str(exc)),
                    last_error=str(exc),
                    status=f"error:{type(exc).__name__}",
                )
            finally:
                await self.set_active(worker_name, None, None)

    async def publish_loop(self):
        state_path = Path(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_HEAVY_STATE_PATH") or DEFAULT_DB_PATH.replace(".sqlite3", ".state.json"))
        candidates_path = Path(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_HEAVY_CANDIDATES_PATH") or DEFAULT_DB_PATH.replace(".sqlite3", ".candidates.json"))
        while True:
            counts = self.store.counts()
            proxy_snapshot = await self.scheduler.snapshot()
            async with self.active_lock:
                lane_states_by_name = {
                    name: {
                        "wallet": item.get("wallet"),
                        "candidateClass": item.get("stage"),
                        "updatedAt": item.get("updatedAt"),
                    }
                    for name, item in sorted(self.active.items())
                }
            lanes = []
            active_wallets = []
            for index in range(self.effective_parallelism):
                worker_name = f"{self.args.lane_mode}:{index}"
                item = lane_states_by_name.get(worker_name) or {}
                wallet = item.get("wallet")
                candidate_class = item.get("candidateClass")
                updated_at = item.get("updatedAt")
                lane = {
                    "lane": index,
                    "wallet": wallet,
                    "candidateClass": candidate_class,
                    "startedAt": updated_at if wallet else None,
                    "completedAt": None,
                    "exitCode": None,
                    "proxyFile": None,
                    "pid": os.getpid(),
                }
                lanes.append(lane)
                if wallet:
                    active_wallets.append(lane)
            candidate_rows = []
            candidate_proof_class_counts = {
                "untracked": 0,
                "compact_only_unproven": 0,
                "partial_backfill": 0,
                "local_complete_head_pending": 0,
                "verified_through_now": 0,
                "zero_history_result": 0,
                "other": 0,
            }
            for row in self.store.next_candidates(100):
                stage = str(row.get("stage") or "").strip().lower()
                proof_class = str(row.get("proof_class") or "").strip().lower()
                candidate_class = (
                    "untracked"
                    if stage == "probe"
                    else "partial_backfill"
                    if stage == "recovery"
                    else "local_complete_head_pending"
                    if stage == "proof"
                    else "zero_history_result"
                    if stage in {"audit", "zero"}
                    else "verified_through_now"
                    if stage == "tracked"
                    else "other"
                )
                if candidate_class not in candidate_proof_class_counts:
                    candidate_proof_class_counts["other"] += 1
                else:
                    candidate_proof_class_counts[candidate_class] += 1
                candidate_rows.append(
                    {
                        "wallet": row.get("wallet"),
                        "candidateClass": candidate_class,
                        "proofClass": proof_class or candidate_class,
                        "stage": stage,
                        "priority": row.get("priority"),
                        "nextEligibleAt": row.get("next_eligible_at"),
                        "tradeRowsLoaded": row.get("trade_rows_loaded"),
                        "fundingRowsLoaded": row.get("funding_rows_loaded"),
                        "volumeUsd": row.get("volume_usd"),
                        "verified": bool(row.get("verified")),
                        "zeroHistoryResult": bool(row.get("zero_history_result")),
                        "status": row.get("status"),
                    }
                )
            focus_counts = {
                "untracked": int((counts.get("probe") or 0) + (counts.get("audit") or 0)),
                "catchUp": int((counts.get("recovery") or 0) + (counts.get("proof") or 0)),
                "zero": int(counts.get("zero") or 0),
                "heavy": 0,
                "activeTail": 0,
            }
            payload = {
                "version": 1,
                "instanceKey": f"{self.args.lane_mode}_{self.args.instance_index}",
                "instanceId": self.args.instance_index,
                "laneMode": self.args.lane_mode,
                "generatedAt": now_ms(),
                "lastStartedWallet": self.last_started_wallet,
                "lastStartedAt": self.last_started_at,
                "lastExitCode": self.last_exit_code,
                "runs": self.runs,
                "completedRuns": self.completed_runs,
                "completedLocalProofRuns": self.completed_local_proof_runs,
                "completedHeadProofRuns": self.completed_head_proof_runs,
                "completedVerifiedRuns": self.completed_verified_runs,
                "lastCompletedWallet": self.last_completed_wallet,
                "lastCompletedAt": self.last_completed_at,
                "lastPromotedWallet": self.last_promoted_wallet,
                "lastPromotedAt": self.last_promoted_at,
                "lastCompletedProofClass": None,
                "focusPool": self.args.lane_mode,
                "focusCounts": focus_counts,
                "counts": counts,
                "candidateCount": sum(int(value or 0) for value in counts.values()),
                "heavyCandidateCount": 0,
                "tailCandidateCount": int(counts.get("audit") or 0),
                "candidateProofClassCounts": candidate_proof_class_counts,
                "candidateScope": "focus_cohort",
                "focusCohortCount": len(self.focus_wallets),
                "activeWallets": len(active_wallets),
                "activeCount": len(active_wallets),
                "activeWalletList": active_wallets,
                "activeCandidates": active_wallets,
                "tailModeActive": self.args.lane_mode in {"probe", "recovery", "proof"},
                "incompleteCount": int((counts.get("probe") or 0) + (counts.get("recovery") or 0) + (counts.get("proof") or 0)),
                "proxyPool": proxy_snapshot,
                "lanes": lanes,
            }
            write_json_atomic(state_path, payload)
            write_json_atomic(
                candidates_path,
                {
                    "version": 1,
                    "generatedAt": now_ms(),
                    "sourceGeneratedAt": now_ms(),
                    "count": len(candidate_rows),
                    "rows": candidate_rows,
                },
            )
            await asyncio.sleep(2.0)

    async def run(self):
        self.seed()
        tasks = [asyncio.create_task(self.publish_loop())]
        for index in range(self.effective_parallelism):
            tasks.append(asyncio.create_task(self.worker_loop(index)))
        await asyncio.gather(*tasks)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--instance-index", type=int, default=int(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_PENDING_INSTANCE_INDEX", "0") or 0))
    parser.add_argument("--proxy-dir", required=True)
    parser.add_argument("--shard-count", type=int, default=8)
    parser.add_argument("--history-limit", type=int, default=240)
    parser.add_argument("--timeout-seconds", type=int, default=20)
    parser.add_argument("--request-attempts", type=int, default=2)
    parser.add_argument("--max-active", type=int, default=32)
    parser.add_argument("--parallelism", type=int, default=32)
    parser.add_argument("--untracked-reserved-lanes", type=int, default=0)
    parser.add_argument("--loop-sleep-seconds", type=float, default=0.5)
    parser.add_argument("--flush-interval-pages", type=int, default=8)
    parser.add_argument("--record-flush-pages", type=int, default=24)
    parser.add_argument("--tail-mode-threshold", type=int, default=100000)
    parser.add_argument("--lane-mode", default="auto")
    parser.add_argument("--db-path", default=os.environ.get("PACIFICA_WALLET_EXPLORER_V2_ASYNC_DB_PATH", DEFAULT_DB_PATH))
    parser.add_argument("--focus-cohort-csv", default=os.environ.get("PACIFICA_WALLET_EXPLORER_V2_FOCUS_COHORT_CSV", ""))
    parser.add_argument("--probe-limit", type=int, default=int(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_PHASE1_PROBE_LIMIT", "25") or 25))
    parser.add_argument("--page-budget", type=int, default=int(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_ASYNC_PAGE_BUDGET", "4") or 4))
    parser.add_argument("--lease-ms", type=int, default=int(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_ASYNC_LEASE_MS", "120000") or 120000))
    return parser.parse_args()


def main():
    args = parse_args()
    runtime = Phase1AsyncRuntime(args)
    asyncio.run(runtime.run())


if __name__ == "__main__":
    main()
