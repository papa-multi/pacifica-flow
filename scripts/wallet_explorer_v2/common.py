#!/usr/bin/env python3
import hashlib
import json
import math
import os
import gzip
import subprocess
import threading
import time
import urllib.parse
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter


ROOT = Path("/root/pacifica-flow")
DATA_DIR = ROOT / "data"
INDEXER_DIR = DATA_DIR / "indexer"
LIVE_POSITIONS_DIR = DATA_DIR / "live_positions"
PIPELINE_DIR = DATA_DIR / "pipeline"
UI_DIR = DATA_DIR / "ui"
V2_DIR = DATA_DIR / "wallet_explorer_v2"
PHASE1_DIR = V2_DIR / "phase1"
SHARDS_DIR = V2_DIR / "shards"
ISOLATED_DIR = V2_DIR / "isolated"
ARCHIVE_DIR = DATA_DIR / "archive"
LIVE_WALLET_UNIVERSE_PATH = LIVE_POSITIONS_DIR / "live_wallet_universe.json"
WALLET_ACTIVITY_TRIGGERS_PATH = LIVE_POSITIONS_DIR / "wallet_activity_triggers.ndjson"
PUBLIC_ENDPOINT_DETAIL_CACHE_PATH = DATA_DIR / "wallet_explorer_v3" / "public_endpoint_wallet_details.json"
API_BASE = "https://api.pacifica.fi/api/v1"
DEFAULT_SHARD_COUNT = 8
HTTP_SESSION_CACHE = {}
HTTP_SESSION_LOCK = threading.Lock()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_json(path: Path, fallback=None):
    if not path.exists():
        return fallback
    try:
        with open(path) as handle:
            return json.load(handle)
    except Exception:
        return fallback


def write_json_atomic(path: Path, payload) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with open(tmp, "w") as handle:
        json.dump(payload, handle, separators=(",", ":"))
    tmp.replace(path)


def now_ms() -> int:
    return int(time.time() * 1000)


def normalize_wallet(value) -> str | None:
    text = str(value or "").strip()
    return text or None


def stable_shard_hash(value: str) -> int:
    text = str(value or "")
    hash_value = 5381
    for char in text:
        hash_value = ((hash_value << 5) + hash_value + ord(char)) & 0xFFFFFFFF
    return hash_value


def shard_index_for_wallet(wallet: str, shard_count: int = DEFAULT_SHARD_COUNT) -> int:
    return stable_shard_hash(f"wallet:{normalize_wallet(wallet) or ''}") % max(1, int(shard_count))


def wallet_state_path(shard_index: int, wallet: str) -> Path:
    return SHARDS_DIR / f"shard_{shard_index}" / "wallet_state" / f"{wallet}.json"


def wallet_history_path(shard_index: int, wallet: str) -> Path:
    return SHARDS_DIR / f"shard_{shard_index}" / "wallet_history" / f"{wallet}.json"


def wallet_record_path(shard_index: int, wallet: str) -> Path:
    return SHARDS_DIR / f"shard_{shard_index}" / "wallet_records" / f"{wallet}.json"


def shard_addresses_path(shard_index: int) -> Path:
    return SHARDS_DIR / f"shard_{shard_index}" / "addresses.json"


def isolated_wallets_path() -> Path:
    override = os.environ.get("PACIFICA_WALLET_EXPLORER_V2_ISOLATED_WALLETS_PATH")
    return Path(override) if override else (V2_DIR / "isolated_wallets.json")


def phase_state_path() -> Path:
    return V2_DIR / "phase_state.json"


def exact_first_trade_queue_path() -> Path:
    return V2_DIR / "wallet_exact_first_trade_queue.json"


def isolated_wallet_dir(wallet: str) -> Path:
    return ISOLATED_DIR / str(wallet)


def isolated_wallet_state_path(wallet: str) -> Path:
    return isolated_wallet_dir(wallet) / "wallet_state.json"


def isolated_wallet_history_path(wallet: str) -> Path:
    return isolated_wallet_dir(wallet) / "wallet_history.json"


def isolated_wallet_history_log_path(wallet: str) -> Path:
    return isolated_wallet_dir(wallet) / "wallet_history_log.ndjson"


def isolated_wallet_record_path(wallet: str) -> Path:
    return isolated_wallet_dir(wallet) / "wallet_record.json"


def phase1_probe_path(wallet: str) -> Path:
    return PHASE1_DIR / "probe_state" / f"{wallet}.json"


def load_isolated_wallets() -> set[str]:
    payload = read_json(isolated_wallets_path(), {"wallets": []}) or {"wallets": []}
    out = set()
    for item in payload.get("wallets") or []:
        if isinstance(item, dict):
            wallet = normalize_wallet(item.get("wallet"))
        else:
            wallet = normalize_wallet(item)
        if wallet:
            out.add(wallet)
    # The manifest can lag the actual isolated history store. Always union it
    # with the on-disk isolated wallet directories so recent isolated histories
    # are not silently excluded from downstream materialization.
    try:
        for entry in ISOLATED_DIR.iterdir():
            if not entry.is_dir():
                continue
            wallet = normalize_wallet(entry.name)
            if wallet:
                out.add(wallet)
    except FileNotFoundError:
        pass
    return out


def load_wallet_discovery_first_seen() -> dict[str, int]:
    payload = read_json(INDEXER_DIR / "wallet_discovery.json", {}) or {}
    wallets = payload.get("wallets") or {}
    out = {}
    if isinstance(wallets, dict):
      items = wallets.items()
    else:
      items = []
    for wallet, details in items:
        normalized = normalize_wallet(wallet)
        if not normalized or not isinstance(details, dict):
            continue
        ts = int(
            details.get("firstSeenBlockTimeMs")
            or details.get("firstSeenAt")
            or details.get("lastSeenBlockTimeMs")
            or details.get("lastSeenAt")
            or 0
        )
        if ts > 0:
            out[normalized] = ts
    fallback_payload = read_json(PUBLIC_ENDPOINT_DETAIL_CACHE_PATH, {}) or {}
    fallback_wallets = fallback_payload.get("wallets") or {}
    if isinstance(fallback_wallets, dict):
        for wallet, details in fallback_wallets.items():
            normalized = normalize_wallet(wallet)
            if not normalized or normalized in out or not isinstance(details, dict):
                continue
            ts = int(details.get("firstSeenAt") or details.get("lastSeenAt") or 0)
            if ts > 0:
                out[normalized] = ts
    return out


def load_wallet_discovery_recent_seen() -> dict[str, int]:
    payload = read_json(INDEXER_DIR / "wallet_discovery.json", {}) or {}
    wallets = payload.get("wallets") or {}
    out = {}
    if isinstance(wallets, dict):
        items = wallets.items()
    else:
        items = []
    for wallet, details in items:
        normalized = normalize_wallet(wallet)
        if not normalized or not isinstance(details, dict):
            continue
        ts = int(
            details.get("lastSeenBlockTimeMs")
            or details.get("lastSeenAt")
            or details.get("updatedAt")
            or details.get("firstSeenBlockTimeMs")
            or details.get("firstSeenAt")
            or 0
        )
        if ts > 0:
            out[normalized] = ts
    fallback_payload = read_json(PUBLIC_ENDPOINT_DETAIL_CACHE_PATH, {}) or {}
    fallback_wallets = fallback_payload.get("wallets") or {}
    if isinstance(fallback_wallets, dict):
        for wallet, details in fallback_wallets.items():
            normalized = normalize_wallet(wallet)
            if not normalized or normalized in out or not isinstance(details, dict):
                continue
            ts = int(details.get("lastSeenAt") or details.get("firstSeenAt") or 0)
            if ts > 0:
                out[normalized] = ts
    return out


def load_live_wallet_universe() -> dict:
    payload = read_json(LIVE_WALLET_UNIVERSE_PATH, {}) or {}
    rows = payload.get("wallets") or []
    out = {}
    generated_at = int(payload.get("generatedAt") or 0)
    if not isinstance(rows, list):
        rows = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        wallet = normalize_wallet(item.get("wallet"))
        if not wallet:
            continue
        updated_at = int(item.get("updatedAt") or item.get("liveScannedAt") or generated_at or 0)
        live_scanned_at = int(item.get("liveScannedAt") or updated_at or 0)
        out[wallet] = {
            "wallet": wallet,
            "updatedAt": updated_at,
            "liveScannedAt": live_scanned_at,
            "openPositions": int(item.get("openPositions") or 0),
            "sources": list(item.get("sources") or []),
        }
    canonical_last_trade_at = int(all_bucket.get("lastTrade") or 0) or None
    canonical_activity_at = canonical_last_trade_at or canonical_last_opened_at
    return {
        "generatedAt": generated_at,
        "wallets": out,
    }


def read_tail_lines(path: Path, max_lines: int = 5000, max_bytes: int = 4 * 1024 * 1024) -> list[str]:
    if not path.exists():
        return []
    try:
        with open(path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            if size <= 0:
                return []
            chunk_size = 256 * 1024
            remaining = size
            buffer = b""
            line_count = 0
            while remaining > 0 and len(buffer) < max_bytes and line_count <= max_lines:
                read_size = min(chunk_size, remaining)
                remaining -= read_size
                handle.seek(remaining)
                buffer = handle.read(read_size) + buffer
                line_count = buffer.count(b"\n")
            return [
                raw.decode("utf-8", "ignore")
                for raw in buffer.splitlines()[-max_lines:]
                if raw.strip()
            ]
    except Exception:
        return []


def load_recent_wallet_triggers(max_age_ms: int = 24 * 60 * 60 * 1000, limit: int = 5000) -> dict[str, int]:
    cutoff = now_ms() - max(60_000, int(max_age_ms or 0))
    out = {}
    for line in reversed(read_tail_lines(WALLET_ACTIVITY_TRIGGERS_PATH, max_lines=max(100, int(limit or 0)))):
        try:
            payload = json.loads(line)
        except Exception:
            continue
        wallet = normalize_wallet((payload or {}).get("wallet"))
        if not wallet:
            continue
        ts = int((payload or {}).get("at") or (payload or {}).get("timestamp") or 0)
        if ts <= 0:
            continue
        if ts < cutoff:
            break
        if ts > int(out.get(wallet) or 0):
            out[wallet] = ts
    return out


def default_phase_state() -> dict:
    return {
        "version": 1,
        "mode": "discover_exact_first_trade",
        "generatedAt": now_ms(),
        "exactFirstTradeWallets": 0,
        "totalWallets": 0,
        "remainingWallets": 0,
        "queuePath": str(exact_first_trade_queue_path()),
    }


def load_phase_state() -> dict:
    payload = read_json(phase_state_path(), default_phase_state()) or default_phase_state()
    if not isinstance(payload, dict):
        return default_phase_state()
    mode = str(payload.get("mode") or "").strip()
    if mode not in {"discover_exact_first_trade", "oldest_first_backfill"}:
        payload["mode"] = "discover_exact_first_trade"
    payload.setdefault("version", 1)
    payload.setdefault("generatedAt", now_ms())
    payload.setdefault("exactFirstTradeWallets", 0)
    payload.setdefault("totalWallets", 0)
    payload.setdefault("remainingWallets", 0)
    payload.setdefault("queuePath", str(exact_first_trade_queue_path()))
    return payload


def list_wallets_from_sources() -> dict:
    wallets = {}

    def add(wallet, source):
        normalized = normalize_wallet(wallet)
        if not normalized:
            return
        entry = wallets.setdefault(normalized, {"wallet": normalized, "sources": set()})
        entry["sources"].add(source)

    seeds_path = ROOT / "config" / "wallet-seeds.txt"
    if seeds_path.exists():
        for line in seeds_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                add(line, "wallet_seeds")

    deposit_path = INDEXER_DIR / "deposit_wallets.json"
    deposit_payload = read_json(deposit_path, {})
    for wallet in deposit_payload.get("wallets") or []:
        add(wallet, "deposit_wallets")

    discovery_path = INDEXER_DIR / "wallet_discovery.json"
    discovery_payload = read_json(discovery_path, {})
    discovery_wallets = discovery_payload.get("wallets") or {}
    if isinstance(discovery_wallets, dict):
        for wallet in discovery_wallets.keys():
            add(wallet, "wallet_discovery")
    elif isinstance(discovery_wallets, list):
        for item in discovery_wallets:
            if isinstance(item, dict):
                add(item.get("wallet") or item.get("address"), "wallet_discovery")
            else:
                add(item, "wallet_discovery")

    v3_manifest_path = DATA_DIR.parent / "wallet_explorer_v3" / "manifest.json"
    if v3_manifest_path.exists():
        try:
            manifest = read_json(v3_manifest_path, {}) or {}
            shard_dir = v3_manifest_path.parent / "shards"
            for shard in manifest.get("shards") or []:
                shard_id = str((shard or {}).get("shardId") or "").strip()
                if not shard_id:
                    continue
                shard_path = shard_dir / f"shard_{shard_id}.json.gz"
                if not shard_path.exists():
                    continue
                with gzip.open(shard_path, "rt") as handle:
                    payload = json.load(handle)
                for row in payload.get("rows") or []:
                    if isinstance(row, dict):
                        add(row.get("wallet"), "wallet_explorer_v3")
        except Exception:
            pass

    return {
        "generatedAt": now_ms(),
        "wallets": [
            {
                "wallet": wallet,
                "sources": sorted(entry["sources"]),
            }
            for wallet, entry in sorted(wallets.items())
        ],
    }


def build_wallet_record_id(wallet: str) -> str:
    digest = hashlib.sha1(str(wallet).encode("utf8")).hexdigest()[:32]
    return f"wal_{digest}"


def to_num(value, fallback=0.0):
    try:
        number = float(value)
    except Exception:
        return fallback
    return number if math.isfinite(number) else fallback


def normalize_trade_row(row) -> dict:
    return {
        "historyId": (row or {}).get("history_id")
        if (row or {}).get("history_id") is not None
        else (row or {}).get("historyId"),
        "orderId": (row or {}).get("order_id")
        if (row or {}).get("order_id") is not None
        else (row or {}).get("orderId"),
        "clientOrderId": (row or {}).get("client_order_id")
        if (row or {}).get("client_order_id") is not None
        else (row or {}).get("clientOrderId"),
        "symbol": str((row or {}).get("symbol") or "").upper(),
        "side": str((row or {}).get("side") or "").lower(),
        "amount": str((row or {}).get("amount") if (row or {}).get("amount") is not None else "0"),
        "price": str((row or {}).get("price") if (row or {}).get("price") is not None else "0"),
        "entryPrice": str(
            (row or {}).get("entry_price")
            if (row or {}).get("entry_price") is not None
            else (row or {}).get("entryPrice")
            if (row or {}).get("entryPrice") is not None
            else "0"
        ),
        "fee": str((row or {}).get("fee") if (row or {}).get("fee") is not None else "0"),
        "liquidityPoolFee": str(
            (row or {}).get("liquidity_pool_fee")
            if (row or {}).get("liquidity_pool_fee") is not None
            else (row or {}).get("liquidityPoolFee")
            if (row or {}).get("liquidityPoolFee") is not None
            else (row or {}).get("lp_fee")
            if (row or {}).get("lp_fee") is not None
            else (row or {}).get("supply_side_fee")
            if (row or {}).get("supply_side_fee") is not None
            else "0"
        ),
        "pnl": str((row or {}).get("pnl") if (row or {}).get("pnl") is not None else "0"),
        "eventType": str(
            (row or {}).get("event_type")
            if (row or {}).get("event_type") is not None
            else (row or {}).get("eventType")
            if (row or {}).get("eventType") is not None
            else ""
        ).lower(),
        "cause": str((row or {}).get("cause") or "").lower(),
        "timestamp": int((row or {}).get("timestamp") or (row or {}).get("created_at") or (row or {}).get("createdAt") or 0),
    }


def normalize_funding_row(row) -> dict:
    return {
        "symbol": str((row or {}).get("symbol") or "").upper(),
        "payout": str((row or {}).get("payout") if (row or {}).get("payout") is not None else "0"),
        "created_at": int((row or {}).get("created_at") or (row or {}).get("createdAt") or 0),
    }


def classify_trade_execution(row) -> str:
    safe = normalize_trade_row(row)
    event_type = str(safe.get("eventType") or "").strip().lower()
    fee = to_num(safe.get("fee"), 0.0)
    if "maker" in event_type:
        return "maker"
    if "taker" in event_type:
        return "taker"
    if fee > 0:
        return "taker"
    if fee < 0:
        return "maker"
    return "maker"


def trade_row_key(row: dict) -> str:
    safe = normalize_trade_row(row)
    history_id = safe.get("historyId")
    if history_id not in (None, ""):
        return f"history:{history_id}"
    return "|".join(
        [
            str(safe["timestamp"]),
            str(safe.get("orderId") or ""),
            safe["symbol"],
            safe.get("side") or "",
            safe["amount"],
            safe["price"],
            safe["fee"],
            safe["pnl"],
            safe.get("eventType") or "",
            safe.get("cause") or "",
        ]
    )


def funding_row_key(row: dict) -> str:
    safe = normalize_funding_row(row)
    return "|".join([str(safe["created_at"]), safe["symbol"], safe["payout"]])


def aggregate_trade_metrics(rows, since_ts=None) -> dict:
    volume_usd = 0.0
    maker_volume_usd = 0.0
    taker_volume_usd = 0.0
    fee_bearing_volume_usd = 0.0
    fees_paid_usd = 0.0
    liquidity_pool_fees_usd = 0.0
    fee_rebates_usd = 0.0
    net_fees_usd = 0.0
    pnl_usd = 0.0
    maker_trades = 0
    taker_trades = 0
    wins = 0
    losses = 0
    first_trade = None
    last_trade = None
    first_opened_at = None
    last_opened_at = None
    symbol_volumes = {}
    selected = []
    for original in rows or []:
        row = normalize_trade_row(original)
        timestamp = int(row["timestamp"] or 0)
        if since_ts is not None and (timestamp <= 0 or timestamp < since_ts):
            continue
        amount = abs(to_num(row["amount"], 0.0))
        price = to_num(row["price"], 0.0)
        fee = to_num(row["fee"], 0.0)
        liquidity_pool_fee = max(0.0, to_num(row.get("liquidityPoolFee"), 0.0))
        pnl = to_num(row["pnl"], 0.0)
        notional = amount * price
        execution_kind = classify_trade_execution(row)
        volume_usd += notional
        if execution_kind == "taker":
            taker_volume_usd += notional
            taker_trades += 1
        else:
            maker_volume_usd += notional
            maker_trades += 1
        if fee > 0:
            fees_paid_usd += fee
        elif fee < 0:
            fee_rebates_usd += abs(fee)
        if fee > 0 or liquidity_pool_fee > 0:
            fee_bearing_volume_usd += notional
        liquidity_pool_fees_usd += liquidity_pool_fee
        net_fees_usd += fee
        pnl_usd += pnl
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
        if timestamp > 0:
            first_trade = timestamp if first_trade is None else min(first_trade, timestamp)
            last_trade = timestamp if last_trade is None else max(last_trade, timestamp)
            if str(row.get("side") or "").strip().lower().startswith("open_"):
                first_opened_at = timestamp if first_opened_at is None else min(first_opened_at, timestamp)
                last_opened_at = timestamp if last_opened_at is None else max(last_opened_at, timestamp)
        if row["symbol"]:
            symbol_volumes[row["symbol"]] = symbol_volumes.get(row["symbol"], 0.0) + notional
        selected.append(row)
    total_decisions = wins + losses
    total_fees_usd = fees_paid_usd + liquidity_pool_fees_usd
    return {
        "computedAt": now_ms(),
        "trades": len(selected),
        "volumeUsd": round(volume_usd, 2),
        "makerTrades": maker_trades,
        "takerTrades": taker_trades,
        "makerVolumeUsd": round(maker_volume_usd, 2),
        "takerVolumeUsd": round(taker_volume_usd, 2),
        "feeBearingVolumeUsd": round(fee_bearing_volume_usd, 2),
        "makerSharePct": round((maker_volume_usd / volume_usd) * 100, 2) if volume_usd > 0 else 0.0,
        "takerSharePct": round((taker_volume_usd / volume_usd) * 100, 2) if volume_usd > 0 else 0.0,
        "feesUsd": round(total_fees_usd, 2),
        "feesPaidUsd": round(fees_paid_usd, 2),
        "liquidityPoolFeesUsd": round(liquidity_pool_fees_usd, 2),
        "feeRebatesUsd": round(fee_rebates_usd, 2),
        "netFeesUsd": round(net_fees_usd, 2),
        "effectiveFeeRateBps": round((total_fees_usd / volume_usd) * 10000, 4) if volume_usd > 0 else 0.0,
        "feeBearingRateBps": round((total_fees_usd / fee_bearing_volume_usd) * 10000, 4)
        if fee_bearing_volume_usd > 0
        else 0.0,
        "revenueUsd": round(fees_paid_usd, 2),
        "pnlUsd": round(pnl_usd, 2),
        "wins": wins,
        "losses": losses,
        "winRatePct": round((wins / total_decisions) * 100, 2) if total_decisions > 0 else 0.0,
        "firstTrade": first_trade,
        "lastTrade": last_trade,
        "firstOpenedAt": first_opened_at,
        "lastOpenedAt": last_opened_at,
        "symbolVolumes": {k: round(v, 2) for k, v in sorted(symbol_volumes.items())},
    }


def latest_opened_at_from_trades(rows) -> int | None:
    latest = 0
    for original in rows or []:
        row = normalize_trade_row(original)
        side = str(row.get("side") or "").strip().lower()
        if not side.startswith("open_"):
            continue
        timestamp = int(row.get("timestamp") or 0)
        if timestamp > latest:
            latest = timestamp
    return latest or None


def latest_opened_at_from_history(history: dict | None) -> int | None:
    payload = history if isinstance(history, dict) else {}
    return latest_opened_at_from_trades(payload.get("trades") or [])


def build_bucket_metrics(trades, window_days: int | None):
    if window_days is None:
        return aggregate_trade_metrics(trades, None)
    cutoff = now_ms() - window_days * 24 * 60 * 60 * 1000
    return aggregate_trade_metrics(trades, cutoff)


def summarize_stream(stream_state: dict, history_stream: list) -> dict:
    state = stream_state or {}
    failed_pages = 1 if state.get("failedCursor") else 0
    pending_pages = 0 if state.get("exhausted") else 1
    return {
        "fetchedPages": int(state.get("pagesFetched") or 0),
        "failedPages": failed_pages,
        "pendingPages": pending_pages,
        "frontierCursor": state.get("frontierCursor"),
        "failedCursor": state.get("failedCursor"),
        "exhausted": bool(state.get("exhausted")),
        "rowsFetched": len(history_stream or []),
        "lastSuccessAt": state.get("lastSuccessAt"),
        "lastAttemptAt": state.get("lastAttemptAt"),
        "retryCount": int(state.get("retryCount") or 0),
        "lastError": state.get("lastError"),
        "nextEligibleAt": state.get("nextEligibleAt"),
        "lastPageRowCount": int(state.get("lastPageRowCount") or 0),
    }


def build_canonical_wallet_record(wallet: str, shard_index: int, state: dict, history: dict) -> dict:
    trades = [normalize_trade_row(row) for row in (history.get("trades") or [])]
    funding = [normalize_funding_row(row) for row in (history.get("funding") or [])]
    trade_stream = ((state or {}).get("streams") or {}).get("trades") or {}
    funding_stream = ((state or {}).get("streams") or {}).get("funding") or {}
    trade_pagination = summarize_stream(trade_stream, trades)
    funding_pagination = summarize_stream(funding_stream, funding)
    all_bucket = build_bucket_metrics(trades, None)
    d30 = build_bucket_metrics(trades, 30)
    d7 = build_bucket_metrics(trades, 7)
    d24 = build_bucket_metrics(trades, 1)
    head_refresh = (((state or {}).get("headRefresh") or {}).get("trades")) or {}
    head_last_trade = int(head_refresh.get("lastTrade") or 0)
    head_last_success_at = int(head_refresh.get("lastSuccessAt") or 0)
    now = now_ms()
    if head_last_trade > int(all_bucket.get("lastTrade") or 0):
        all_bucket["lastTrade"] = head_last_trade
    if head_last_trade > 0 and head_last_trade >= now - 30 * 24 * 60 * 60 * 1000:
        d30["lastTrade"] = max(int(d30.get("lastTrade") or 0), head_last_trade)
    if head_last_trade > 0 and head_last_trade >= now - 7 * 24 * 60 * 60 * 1000:
        d7["lastTrade"] = max(int(d7.get("lastTrade") or 0), head_last_trade)
    if head_last_trade > 0 and head_last_trade >= now - 24 * 60 * 60 * 1000:
        d24["lastTrade"] = max(int(d24.get("lastTrade") or 0), head_last_trade)
    retry_reasons = [stream.get("lastError") for stream in (trade_stream, funding_stream) if stream.get("lastError")]
    retry_pending = any(not stream.get("exhausted") and int(stream.get("nextEligibleAt") or 0) > now_ms() for stream in (trade_stream, funding_stream)) or bool(trade_stream.get("failedCursor")) or bool(funding_stream.get("failedCursor"))
    history_verified = bool(trade_stream.get("exhausted")) and bool(funding_stream.get("exhausted")) and not retry_pending
    trade_rows_loaded = len(trades)
    funding_rows_loaded = len(funding)
    last_funding_at = max((int((row or {}).get("createdAt") or 0) for row in funding), default=0)
    proof_local = {
        "checkedAt": now,
        "filesPresent": {
            "state": True,
            "history": True,
            "record": True,
        },
        "tradeExhausted": bool(trade_stream.get("exhausted")),
        "fundingExhausted": bool(funding_stream.get("exhausted")),
        "tradeFrontierClear": not bool(trade_stream.get("frontierCursor")) and not bool(trade_stream.get("failedCursor")),
        "fundingFrontierClear": not bool(funding_stream.get("frontierCursor")) and not bool(funding_stream.get("failedCursor")),
        "retryClear": not retry_pending,
        "recordBackfillComplete": history_verified,
        "tradeRowsLoaded": trade_rows_loaded,
        "fundingRowsLoaded": funding_rows_loaded,
        "recordTradeRowsLoaded": trade_rows_loaded,
        "recordFundingRowsLoaded": funding_rows_loaded,
        "tradeRowsMatch": True,
        "fundingRowsMatch": True,
        "lastTradeAt": int(all_bucket.get("lastTrade") or 0) or None,
        "lastFundingAt": last_funding_at or None,
        "complete": history_verified,
    }
    blocker_reasons = []
    if not trade_stream.get("exhausted"):
        blocker_reasons.append("trade_history_incomplete")
    if not funding_stream.get("exhausted"):
        blocker_reasons.append("funding_history_incomplete")
    if retry_pending:
        blocker_reasons.append(f"retry_pending:{retry_reasons[0] if retry_reasons else 'unknown'}")
    metrics_verified = history_verified
    leaderboard_eligible = metrics_verified
    volume_status = "exact" if leaderboard_eligible else "partial" if trades else "blocked"
    volume_status_label = {
        "exact": "Exact History Volume",
        "partial": "Partial History Volume",
        "blocked": "Blocked",
    }[volume_status]
    available_symbols = sorted([symbol for symbol in all_bucket.get("symbolVolumes", {}).keys() if symbol])
    canonical_last_opened_at = (
        int(all_bucket.get("lastOpenedAt") or 0)
        or int((state or {}).get("last_opened_at") or 0)
        or latest_opened_at_from_history(history)
        or None
    )
    updated_at = max(
        int((state or {}).get("updatedAt") or 0),
        int(history.get("updatedAt") or 0),
        int(all_bucket.get("computedAt") or 0),
        int(all_bucket.get("lastTrade") or 0),
        head_last_success_at,
    )
    return {
        "wallet": wallet,
        "walletRecordId": build_wallet_record_id(wallet),
        "shardIndex": shard_index,
        "updatedAt": updated_at,
        "trades": all_bucket["trades"],
        "volumeUsd": all_bucket["volumeUsd"],
        "volumeUsdRaw": all_bucket["volumeUsd"],
        "volumeUsdVerified": all_bucket["volumeUsd"] if history_verified else None,
        "rankingVolumeUsd": all_bucket["volumeUsd"] if leaderboard_eligible else None,
        "feesUsd": all_bucket["feesUsd"],
        "feesPaidUsd": all_bucket["feesPaidUsd"],
        "liquidityPoolFeesUsd": all_bucket["liquidityPoolFeesUsd"],
        "feeRebatesUsd": all_bucket["feeRebatesUsd"],
        "netFeesUsd": all_bucket["netFeesUsd"],
        "revenueUsd": all_bucket["revenueUsd"],
        "pnlUsd": all_bucket["pnlUsd"],
        "totalWins": all_bucket["wins"],
        "totalLosses": all_bucket["losses"],
        "winRate": all_bucket["winRatePct"],
        "firstTrade": all_bucket["firstTrade"],
        "lastTrade": all_bucket["lastTrade"],
        "lastOpenedAt": canonical_last_opened_at,
        "lastActivity": canonical_activity_at,
        "lastActivityAt": canonical_activity_at,
        "lastActiveAt": canonical_activity_at,
        "lastFundingAt": last_funding_at or None,
        "headTradeLastSeenAt": head_last_trade or None,
        "headTradeSyncAt": head_last_success_at or None,
        "openPositions": 0,
        "exposureUsd": 0,
        "unrealizedPnlUsd": 0,
        "totalPnlUsd": all_bucket["pnlUsd"],
        "tradeDone": bool(trade_stream.get("exhausted")),
        "fundingDone": bool(funding_stream.get("exhausted")),
        "tradeHasMore": not bool(trade_stream.get("exhausted")),
        "fundingHasMore": not bool(funding_stream.get("exhausted")),
        "tradeRowsLoaded": trade_rows_loaded,
        "fundingRowsLoaded": funding_rows_loaded,
        "tradePagination": trade_pagination,
        "fundingPagination": funding_pagination,
        "retryPending": retry_pending,
        "retryReason": retry_reasons[0] if retry_reasons else None,
        "metricsReady": True,
        "backfillComplete": history_verified,
        "validationStatus": "verified" if history_verified else "partial",
        "historyPhase": "complete" if history_verified else "backfill",
        "historyProofStatus": "local_complete_head_pending" if history_verified else "backfill_incomplete",
        "historyVerifiedThroughNow": False,
        "all": all_bucket,
        "d30": d30,
        "d7": d7,
        "d24": d24,
        "symbols": available_symbols[:24],
        "symbolBreakdown": [
            {"symbol": symbol, "volumeUsd": volume}
            for symbol, volume in sorted(
                all_bucket.get("symbolVolumes", {}).items(),
                key=lambda item: item[1],
                reverse=True,
            )[:8]
        ],
        "completeness": {
            "historyVerified": history_verified,
            "metricsVerified": metrics_verified,
            "leaderboardEligible": leaderboard_eligible,
            "trackingState": "tracked_full" if history_verified else "queued_backfill" if trades or funding else "tracked_lite",
            "tradeHistoryState": "full_history_complete" if history_verified else "partial_trade_history" if trades else "activation_only",
            "metricsState": "metrics_complete" if metrics_verified else "metrics_partial" if trades else "metrics_pending",
            "volumeExactness": volume_status,
            "volumeExactnessLabel": volume_status_label,
            "fullBlockedReasons": blocker_reasons,
            "warnings": [],
            "historyPersistedAt": history.get("updatedAt"),
        },
        "sources": {
            "historyPath": str(wallet_history_path(shard_index, wallet)),
            "statePath": str(wallet_state_path(shard_index, wallet)),
            "recordPath": str(wallet_record_path(shard_index, wallet)),
        },
        "proof": {
            "local": proof_local,
            "head": {
                "checkedAt": None,
                "complete": False,
                "trades": {"status": "pending", "storedLastAt": int(all_bucket.get("lastTrade") or 0) or None},
                "funding": {"status": "pending", "storedLastAt": last_funding_at or None},
            },
            "complete": False,
        },
        "headRefresh": {
            "trades": {
                "lastTrade": head_last_trade or None,
                "lastSuccessAt": head_last_success_at or None,
                "lastAttemptAt": int(head_refresh.get("lastAttemptAt") or 0) or None,
                "lastError": head_refresh.get("lastError") or None,
                "lastPageRowCount": int(head_refresh.get("lastPageRowCount") or 0),
            },
        },
    }


def initial_stream_state() -> dict:
    return {
        "frontierCursor": None,
        "failedCursor": None,
        "exhausted": False,
        "pagesFetched": 0,
        "rowsFetched": 0,
        "lastAttemptAt": None,
        "lastSuccessAt": None,
        "retryCount": 0,
        "nextEligibleAt": 0,
        "lastError": None,
        "lastPageRowCount": 0,
        "steps": [],
    }


def initial_wallet_state(wallet: str, shard_index: int) -> dict:
    timestamp = now_ms()
    return {
        "version": 2,
        "wallet": wallet,
        "shardIndex": shard_index,
        "createdAt": timestamp,
        "updatedAt": timestamp,
        "status": "pending",
        "streams": {
            "trades": initial_stream_state(),
            "funding": initial_stream_state(),
        },
        "headRefresh": {
            "trades": {
                "lastAttemptAt": None,
                "lastSuccessAt": None,
                "lastTrade": None,
                "lastError": None,
                "retryCount": 0,
                "nextEligibleAt": 0,
                "lastPageRowCount": 0,
            }
        },
    }


def initial_wallet_history(wallet: str, shard_index: int) -> dict:
    timestamp = now_ms()
    return {
        "version": 2,
        "wallet": wallet,
        "shardIndex": shard_index,
        "createdAt": timestamp,
        "updatedAt": timestamp,
        "trades": [],
        "funding": [],
    }


def load_proxies(proxy_file: str | None) -> list:
    if not proxy_file:
        return []
    path = Path(proxy_file)
    if not path.exists():
        return []
    return [
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


class ProxyPool:
    def __init__(self, proxies=None):
        raw = list(proxies or [])
        self.entries = [
            {
                "id": index,
                "proxy": proxy,
                "cooldownUntil": 0.0,
                "failures": 0,
                "successes": 0,
            }
            for index, proxy in enumerate(raw)
        ]
        if not self.entries:
            self.entries = [{"id": 0, "proxy": None, "cooldownUntil": 0.0, "failures": 0, "successes": 0}]
        self.lock = threading.Lock()
        self.cursor = 0

    def acquire(self):
        while True:
            with self.lock:
                now = time.time()
                for offset in range(len(self.entries)):
                    index = (self.cursor + offset) % len(self.entries)
                    entry = self.entries[index]
                    if entry["cooldownUntil"] <= now:
                        self.cursor = (index + 1) % len(self.entries)
                        return entry
                sleep_for = min(max(0.1, min(entry["cooldownUntil"] - now for entry in self.entries)), 1.0)
            time.sleep(sleep_for)

    def mark_success(self, entry):
        with self.lock:
            entry["successes"] += 1
            entry["cooldownUntil"] = 0.0

    def mark_failure(self, entry, reason: str):
        reason_text = str(reason or "").lower()
        retry_after = 60 if "429" in reason_text else 25 if "timeout" in reason_text else 12
        with self.lock:
            entry["failures"] += 1
            entry["cooldownUntil"] = max(entry["cooldownUntil"], time.time() + retry_after)

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "total": len(self.entries),
                "cooling": sum(1 for entry in self.entries if entry["cooldownUntil"] > time.time()),
                "active": len(self.entries),
                "entries": [
                    {
                        "id": entry["id"],
                        "proxy": entry["proxy"],
                        "successes": entry["successes"],
                        "failures": entry["failures"],
                        "cooldownUntil": entry["cooldownUntil"],
                    }
                    for entry in self.entries
                ],
            }


def fetch_json_via_curl(url: str, proxy: str | None, timeout_seconds: int = 20):
    marker = f"__PF_STATUS__{int(time.time() * 1000)}"
    command = [
        "curl",
        "--silent",
        "--show-error",
        "--location",
        "--max-time",
        str(max(1, int(timeout_seconds))),
    ]
    if proxy:
        command.extend(["--proxy", str(proxy)])
    command.extend(["--write-out", f"\\n{marker}:%{{http_code}}", url])
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"curl_failed:{(result.stderr or result.stdout or 'unknown').strip()[:240]}")
    output = result.stdout or ""
    token = f"\n{marker}:"
    index = output.rfind(token)
    if index < 0:
        raise RuntimeError("curl_missing_status_marker")
    body = output[:index]
    status = int((output[index + 1 :].split(":", 1)[1] or "0").strip() or "0")
    if status == 429:
        raise RuntimeError("http_429")
    if status < 200 or status >= 300:
        raise RuntimeError(f"http_{status}")
    try:
        return json.loads(body)
    except Exception as exc:
        raise RuntimeError(f"invalid_json:{exc}") from exc


def normalize_proxy_url(proxy: str | None) -> str | None:
    text = str(proxy or "").strip()
    if not text:
        return None
    if "://" not in text:
        text = f"http://{text}"
    return text


def get_http_session(proxy: str | None):
    key = normalize_proxy_url(proxy) or "__direct__"
    with HTTP_SESSION_LOCK:
        session = HTTP_SESSION_CACHE.get(key)
        if session is not None:
            return session
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=16, pool_maxsize=64, max_retries=0, pool_block=False)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.trust_env = False
        session.headers.update({"accept": "application/json"})
        if key != "__direct__":
            session.proxies.update({"http": key, "https": key})
        HTTP_SESSION_CACHE[key] = session
        return session


def fetch_json_via_http(url: str, proxy: str | None, timeout_seconds: int = 20):
    session = get_http_session(proxy)
    try:
        response = session.get(
            url,
            timeout=max(1, int(timeout_seconds)),
            allow_redirects=True,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"http_client_error:{str(exc)[:240]}") from exc
    status = int(response.status_code or 0)
    if status == 429:
        raise RuntimeError("http_429")
    if status < 200 or status >= 300:
        raise RuntimeError(f"http_{status}")
    try:
        return response.json()
    except Exception as exc:
        raise RuntimeError(f"invalid_json:{exc}") from exc


def backoff_seconds(retry_count: int, reason: str) -> int:
    lower = str(reason or "").lower()
    if "429" in lower:
        return min(60, 8 * max(1, retry_count))
    if "http_504" in lower or "http_502" in lower:
        return min(45, 6 * max(1, retry_count))
    if "timeout" in lower:
        return min(240, 20 * max(1, retry_count))
    return min(180, 10 * max(1, retry_count))


def build_exchange_overview() -> dict:
    payload = read_json(PIPELINE_DIR / "global_kpi.json", {}) or {}
    volume_meta = payload.get("volumeMeta") if isinstance(payload.get("volumeMeta"), dict) else {}
    total_historical_volume = to_num(
        payload.get("totalHistoricalVolume"),
        to_num(volume_meta.get("cumulativeVolume"), 0.0),
    )
    return {
        "updatedAt": int(payload.get("updatedAt") or payload.get("fetchedAt") or 0) or now_ms(),
        "totalHistoricalVolumeUsd": round(total_historical_volume, 2),
        "dailyVolumeUsd": round(to_num(payload.get("dailyVolume"), 0.0), 2),
        "volumeMethod": payload.get("volumeMethod"),
        "trackingStartDate": volume_meta.get("trackingStartDate"),
        "backfillComplete": bool(volume_meta.get("backfillComplete")),
    }


def request_endpoint(
    wallet: str,
    endpoint: str,
    limit: int,
    cursor: str | None,
    proxy_pool: ProxyPool,
    timeout_seconds: int = 20,
    attempts: int = 3,
    prefer_direct: bool = False,
):
    params = {"account": wallet, "limit": str(limit)}
    if cursor:
        params["cursor"] = cursor
    url = f"{API_BASE}/{endpoint}?{urllib.parse.urlencode(params)}"
    transport = str(os.environ.get("PACIFICA_WALLET_EXPLORER_V2_HTTP_TRANSPORT", "requests")).strip().lower()
    fetch_fn = fetch_json_via_http if transport != "curl" else fetch_json_via_curl
    last_error = None
    if prefer_direct:
        try:
            payload = fetch_fn(url, None, timeout_seconds)
            return payload, {"id": "direct_pref", "proxy": None}
        except Exception as exc:
            last_error = str(exc)
    for _attempt in range(attempts):
        entry = proxy_pool.acquire()
        try:
            payload = fetch_fn(url, entry.get("proxy"), timeout_seconds)
            proxy_pool.mark_success(entry)
            return payload, entry
        except Exception as exc:
            last_error = str(exc)
            proxy_pool.mark_failure(entry, last_error)
            if "429" in last_error.lower():
                break
    # Fall back to a direct request after proxy exhaustion so refresh lanes do
    # not freeze when the proxy pool is degraded but the upstream endpoint is healthy.
    if last_error and "429" in last_error.lower():
        raise RuntimeError(last_error or "request_failed")
    try:
        payload = fetch_fn(url, None, timeout_seconds)
        return payload, {"id": "direct", "proxy": None}
    except Exception as exc:
        last_error = str(exc)
    raise RuntimeError(last_error or "request_failed")
