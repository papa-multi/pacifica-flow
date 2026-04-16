#!/usr/bin/env python3
import argparse
import json
import threading
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


ROOT = Path("/root/pacifica-flow")
RUNTIME_DB = ROOT / "data" / "wallet_explorer_v2" / "review_pipeline" / "runtime.sqlite3"
DEFAULT_OUTPUT = ROOT / "data" / "ui" / ".build" / "zero_verified_volume_recheck.json"
BASE_URL = "https://app.pacifica.fi/api/v1/portfolio/volume"


def load_zero_verified_wallets():
    con = sqlite3.connect(str(RUNTIME_DB))
    try:
        cur = con.cursor()
        return [row[0] for row in cur.execute("select wallet from wallets where stage='zero_verified' order by wallet")]
    finally:
        con.close()


def load_wallets_from_file(path_text):
    payload = json.loads(Path(path_text).read_text())
    if isinstance(payload, dict):
        if isinstance(payload.get("wallets"), list):
            return [str(item).strip() for item in payload.get("wallets") if str(item).strip()]
        if isinstance(payload.get("failedWallets"), list):
            return [str(item.get("wallet") or "").strip() for item in payload.get("failedWallets") if str(item.get("wallet") or "").strip()]
    if isinstance(payload, list):
        return [str(item).strip() for item in payload if str(item).strip()]
    return []


_RATE_LOCK = threading.Lock()
_NEXT_ALLOWED_AT = 0.0


def maybe_wait(min_interval_sec):
    global _NEXT_ALLOWED_AT
    wait_for = 0.0
    with _RATE_LOCK:
        now = time.monotonic()
        if _NEXT_ALLOWED_AT > now:
            wait_for = _NEXT_ALLOWED_AT - now
            now = _NEXT_ALLOWED_AT
        if min_interval_sec > 0:
            _NEXT_ALLOWED_AT = now + min_interval_sec
    if wait_for > 0:
        time.sleep(wait_for)


def fetch_volume(wallet, timeout, min_interval_sec=0.0, retries=0):
    url = f"{BASE_URL}?account={urllib.parse.quote(wallet)}&_ts={int(time.time() * 1000)}"
    attempt = 0
    last_error = None
    while attempt <= max(0, retries):
        maybe_wait(min_interval_sec)
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "User-Agent": "pacifica-flow-zero-verified-recheck/1.0",
            },
        )
        started_at = int(time.time() * 1000)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
            data = payload.get("data") if isinstance(payload, dict) else {}
            volume_raw = data.get("volume_all_time") if isinstance(data, dict) else 0
            volume = float(volume_raw or 0.0)
            return {
                "wallet": wallet,
                "ok": True,
                "volumeAllTime": volume,
                "isZero": abs(volume) <= 0,
                "fetchedAt": int(time.time() * 1000),
                "latencyMs": int(time.time() * 1000) - started_at,
                "attempts": attempt + 1,
                "payload": payload,
            }
        except urllib.error.HTTPError as error:
            body = ""
            try:
                body = error.read().decode("utf-8", errors="replace")
            except Exception:
                body = ""
            last_error = {
                "wallet": wallet,
                "ok": False,
                "error": f"http_{error.code}",
                "body": body[:400],
                "fetchedAt": int(time.time() * 1000),
                "latencyMs": int(time.time() * 1000) - started_at,
                "attempts": attempt + 1,
            }
            if error.code == 429 and attempt < retries:
                time.sleep(min(10, 1 + attempt * 2))
                attempt += 1
                continue
            return last_error
        except Exception as error:
            last_error = {
                "wallet": wallet,
                "ok": False,
                "error": str(error),
                "fetchedAt": int(time.time() * 1000),
                "latencyMs": int(time.time() * 1000) - started_at,
                "attempts": attempt + 1,
            }
            if attempt < retries:
                time.sleep(min(5, 1 + attempt))
                attempt += 1
                continue
            return last_error
    return last_error or {"wallet": wallet, "ok": False, "error": "unknown"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=24)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--wallet-file")
    parser.add_argument("--min-interval-sec", type=float, default=0.0)
    parser.add_argument("--retries", type=int, default=0)
    args = parser.parse_args()

    wallets = load_wallets_from_file(args.wallet_file) if args.wallet_file else load_zero_verified_wallets()
    results = []
    started_at = int(time.time() * 1000)

    with ThreadPoolExecutor(max_workers=max(1, int(args.workers or 1))) as executor:
        futures = {
            executor.submit(fetch_volume, wallet, args.timeout, float(args.min_interval_sec or 0.0), int(args.retries or 0)): wallet
            for wallet in wallets
        }
        for future in as_completed(futures):
            results.append(future.result())

    ok_results = [item for item in results if item.get("ok")]
    failed_results = [item for item in results if not item.get("ok")]
    zero_results = [item for item in ok_results if item.get("isZero")]
    non_zero_results = [item for item in ok_results if not item.get("isZero")]

    payload = {
        "generatedAt": int(time.time() * 1000),
        "startedAt": started_at,
        "source": BASE_URL,
        "walletCount": len(wallets),
        "okCount": len(ok_results),
        "failedCount": len(failed_results),
        "zeroCount": len(zero_results),
        "nonZeroCount": len(non_zero_results),
        "nonZeroWallets": [
            {
                "wallet": item["wallet"],
                "volumeAllTime": item["volumeAllTime"],
                "latencyMs": item["latencyMs"],
            }
            for item in sorted(non_zero_results, key=lambda item: (-item["volumeAllTime"], item["wallet"]))
        ],
        "failedWallets": failed_results,
        "results": results,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2))

    print(json.dumps(
        {
            "walletCount": payload["walletCount"],
            "okCount": payload["okCount"],
            "failedCount": payload["failedCount"],
            "zeroCount": payload["zeroCount"],
            "nonZeroCount": payload["nonZeroCount"],
            "output": str(output_path),
            "topNonZero": payload["nonZeroWallets"][:10],
        },
        indent=2,
    ))


if __name__ == "__main__":
    main()
