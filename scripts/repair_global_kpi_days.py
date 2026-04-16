#!/usr/bin/env python3
import argparse
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests


API_BASE = "https://api.pacifica.fi/api/v1"
GLOBAL_KPI_PATH = Path("/root/pacifica-flow/data/pipeline/global_kpi.json")
REQUEST_TIMEOUT = 45
MIN_REQUEST_GAP_SEC = 0.5


def write_json_atomic(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as handle:
        json.dump(payload, handle, separators=(",", ":"), ensure_ascii=True)
    tmp.replace(path)


def iter_dates(start_text: str, end_text: str):
    cursor = date.fromisoformat(start_text)
    end = date.fromisoformat(end_text)
    while cursor <= end:
        yield cursor
        cursor += timedelta(days=1)


def get_json(session: requests.Session, path: str, params=None, retries: int = 8):
    delay = 1.0
    last_error = None
    for _ in range(retries):
        try:
            response = session.get(f"{API_BASE}{path}", params=params or {}, timeout=REQUEST_TIMEOUT)
            if response.status_code == 429:
                last_error = RuntimeError(f"429 {path}")
                time.sleep(delay)
                delay = min(delay * 2, 8.0)
                continue
            response.raise_for_status()
            return response.json()
        except Exception as error:
            last_error = error
            time.sleep(delay)
            delay = min(delay * 2, 8.0)
    raise last_error or RuntimeError(f"request_failed {path}")


def fetch_markets(session: requests.Session):
    payload = get_json(session, "/info")
    rows = payload.get("data") if isinstance(payload, dict) else None
    markets = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        created_at = int(row.get("created_at") or 0)
        markets.append({"symbol": symbol, "created_at": created_at})
    return markets


def extract_exact_day_volume(rows, start_ms: int):
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        try:
            row_start = int(row.get("t") or 0)
        except Exception:
            row_start = 0
        if row_start != start_ms:
            continue
        try:
            v = float(row.get("v") or 0.0)
            c = float(row.get("c") or 0.0)
        except Exception:
            continue
        if v < 0 or c < 0:
            continue
        return round((v * c) / 2.0, 8)
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()

    session = requests.Session()
    markets = fetch_markets(session)

    with open(GLOBAL_KPI_PATH) as handle:
        payload = json.load(handle)

    history = payload.setdefault("history", {})
    daily_by_date = history.setdefault("dailyByDate", {})

    last_request_at = 0.0
    repaired = {}

    for day in iter_dates(args.start_date, args.end_date):
        day_text = day.isoformat()
        start_ms = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000)
        day_end_ms = start_ms + 86400000 - 1
        query_end_ms = start_ms + 86400000 * 2
        symbol_volumes = {}
        errors = []
        failed_symbols = []
        eligible = [row["symbol"] for row in markets if int(row.get("created_at") or 0) <= day_end_ms]

        for symbol in eligible:
            elapsed = time.monotonic() - last_request_at
            if elapsed < MIN_REQUEST_GAP_SEC:
                time.sleep(MIN_REQUEST_GAP_SEC - elapsed)
            last_request_at = time.monotonic()
            try:
                data = get_json(
                    session,
                    "/kline",
                    params={
                        "symbol": symbol,
                        "interval": "1d",
                        "start_time": start_ms,
                        "end_time": query_end_ms,
                    },
                )
                rows = data.get("data") if isinstance(data, dict) else None
                symbol_volume = extract_exact_day_volume(rows, start_ms)
                if symbol_volume is None:
                    continue
                symbol_volumes[symbol] = symbol_volume
            except Exception as error:
                errors.append(f"{symbol}:{error}")
                failed_symbols.append(symbol)

        if failed_symbols:
            time.sleep(12.0)
            retry_errors = []
            retry_failed = []
            for symbol in failed_symbols:
                elapsed = time.monotonic() - last_request_at
                if elapsed < MIN_REQUEST_GAP_SEC:
                    time.sleep(MIN_REQUEST_GAP_SEC - elapsed)
                last_request_at = time.monotonic()
                try:
                    data = get_json(
                        session,
                        "/kline",
                        params={
                            "symbol": symbol,
                            "interval": "1d",
                            "start_time": start_ms,
                            "end_time": query_end_ms,
                        },
                    )
                    rows = data.get("data") if isinstance(data, dict) else None
                    symbol_volume = extract_exact_day_volume(rows, start_ms)
                    if symbol_volume is None:
                        retry_failed.append(symbol)
                        continue
                    symbol_volumes[symbol] = symbol_volume
                except Exception as error:
                    retry_errors.append(f"{symbol}:{error}")
                    retry_failed.append(symbol)
            failed_symbols = retry_failed
            errors.extend(retry_errors)

        if failed_symbols:
            raise RuntimeError(f"{day_text} repair incomplete: {errors[:8]}")

        daily_volume = round(sum(symbol_volumes.values()), 8)
        row = daily_by_date.get(day_text) if isinstance(daily_by_date.get(day_text), dict) else {}
        row.update(
            {
                "dailyVolume": daily_volume,
                "symbolVolumes": symbol_volumes,
                "symbolCount": len(eligible),
                "okSymbols": len(symbol_volumes),
                "failedSymbols": 0,
                "missingSymbols": 0,
                "sampleErrors": [],
                "updatedAt": int(time.time() * 1000),
            }
        )
        row.pop("refreshBatchSize", None)
        daily_by_date[day_text] = row
        repaired[day_text] = row

    payload["generatedAt"] = int(time.time() * 1000)
    write_json_atomic(GLOBAL_KPI_PATH, payload)
    print(json.dumps(repaired, indent=2))


if __name__ == "__main__":
    main()
