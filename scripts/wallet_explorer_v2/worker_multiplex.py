#!/usr/bin/env python3
import argparse
import os
import threading
import traceback
from pathlib import Path
from types import SimpleNamespace

from common import DEFAULT_SHARD_COUNT
from worker import run_worker


def parse_shards(raw: str) -> list[int]:
    values = []
    seen = set()
    for part in str(raw or "").split(","):
        token = part.strip()
        if not token:
            continue
        value = int(token)
        if value in seen:
            continue
        seen.add(value)
        values.append(value)
    if not values:
        raise argparse.ArgumentTypeError("at least one shard index is required")
    return values


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shards", required=True, type=parse_shards)
    parser.add_argument("--shard-count", type=int, default=DEFAULT_SHARD_COUNT)
    parser.add_argument("--proxy-dir", default="")
    parser.add_argument("--wallet-concurrency", type=int, default=12)
    parser.add_argument("--active-batch-multiplier", type=int, default=8)
    parser.add_argument("--history-limit", type=int, default=240)
    parser.add_argument("--timeout-seconds", type=int, default=25)
    parser.add_argument("--request-attempts", type=int, default=2)
    parser.add_argument("--loop-sleep-seconds", type=int, default=1)
    parser.add_argument("--prioritize-first-trade", action="store_true")
    parser.add_argument("--completion-lane-pct", type=int, default=35)
    parser.add_argument("--intake-lane-pct", type=int, default=20)
    parser.add_argument("--intake-trade-page-budget", type=int, default=4)
    parser.add_argument("--trade-page-budget", type=int, default=12)
    parser.add_argument("--funding-page-budget", type=int, default=0)
    parser.add_argument("--flush-interval-pages", type=int, default=4)
    parser.add_argument("--wallet-refresh-ms", type=int, default=30 * 1000)
    parser.add_argument("--refresh-lane-pct", type=int, default=10)
    parser.add_argument("--refresh-trade-page-budget", type=int, default=2)
    parser.add_argument("--refresh-funding-page-budget", type=int, default=1)
    parser.add_argument("--head-history-limit", type=int, default=3)
    parser.add_argument("--head-timeout-seconds", type=int, default=12)
    parser.add_argument("--head-request-attempts", type=int, default=3)
    parser.add_argument("--head-refresh-ms", type=int, default=5 * 60 * 1000)
    parser.add_argument("--once", action="store_true")
    return parser


def build_shard_args(args, shard_index: int) -> SimpleNamespace:
    proxy_dir = Path(args.proxy_dir or "").expanduser()
    proxy_file = proxy_dir / f"proxies_{shard_index}.txt"
    return SimpleNamespace(
        shard_index=shard_index,
        shard_count=args.shard_count,
        proxy_file=str(proxy_file),
        wallet_concurrency=args.wallet_concurrency,
        active_batch_multiplier=args.active_batch_multiplier,
        history_limit=args.history_limit,
        timeout_seconds=args.timeout_seconds,
        request_attempts=args.request_attempts,
        loop_sleep_seconds=args.loop_sleep_seconds,
        prioritize_first_trade=bool(args.prioritize_first_trade),
        completion_lane_pct=args.completion_lane_pct,
        intake_lane_pct=args.intake_lane_pct,
        intake_trade_page_budget=args.intake_trade_page_budget,
        trade_page_budget=args.trade_page_budget,
        funding_page_budget=args.funding_page_budget,
        flush_interval_pages=args.flush_interval_pages,
        wallet_refresh_ms=args.wallet_refresh_ms,
        refresh_lane_pct=args.refresh_lane_pct,
        refresh_trade_page_budget=args.refresh_trade_page_budget,
        refresh_funding_page_budget=args.refresh_funding_page_budget,
        head_history_limit=args.head_history_limit,
        head_timeout_seconds=args.head_timeout_seconds,
        head_request_attempts=args.head_request_attempts,
        head_refresh_ms=args.head_refresh_ms,
        once=bool(args.once),
    )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.shard_count = int(args.shard_count or DEFAULT_SHARD_COUNT)
    for shard_index in args.shards:
        if shard_index < 0 or shard_index >= args.shard_count:
            raise SystemExit(f"invalid shard index {shard_index} for shard_count={args.shard_count}")

    threads = []

    def run_shard(shard_index: int) -> None:
        shard_args = build_shard_args(args, shard_index)
        try:
            run_worker(shard_args)
            if not args.once:
                raise RuntimeError(f"worker thread for shard {shard_index} exited unexpectedly")
        except BaseException:
            traceback.print_exc()
            os._exit(1)

    for shard_index in args.shards:
        thread = threading.Thread(
            target=run_shard,
            name=f"wallet-explorer-v2-shard-{shard_index}",
            args=(shard_index,),
            daemon=False,
        )
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
