#!/usr/bin/env python3
import json
from pathlib import Path


def read_json(path: Path):
    with open(path) as handle:
        return json.load(handle)


def rows_from_store(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if isinstance(payload.get("wallets"), dict):
            return list(payload["wallets"].values())
        if isinstance(payload.get("wallets"), list):
            return payload["wallets"]
        if isinstance(payload.get("rows"), list):
            return payload["rows"]
    return []


def summarize_pagination(raw):
    if not isinstance(raw, dict):
        return {
            "exhausted": False,
            "retryPages": 0,
            "pendingPages": 0,
            "frontierCursor": None,
            "fetchedPages": 0,
            "totalKnownPages": None,
        }
    return {
        "exhausted": bool(raw.get("exhausted")),
        "retryPages": int(raw.get("retryPages") or 0),
        "pendingPages": int(raw.get("pendingPages") or 0),
        "frontierCursor": raw.get("frontierCursor"),
        "fetchedPages": int(raw.get("fetchedPages") or 0),
        "totalKnownPages": None
        if raw.get("totalKnownPages") is None
        else int(raw.get("totalKnownPages") or 0),
    }


def pagination_complete(summary):
    return bool(
        summary
        and summary["exhausted"]
        and summary["retryPages"] <= 0
        and summary["pendingPages"] <= 0
        and not summary["frontierCursor"]
    )


def main() -> int:
    base = Path("/root/pacifica-flow/data/indexer/shards")
    wallet_rows = []
    states = {}
    shard_of = {}

    for shard in sorted(path for path in base.iterdir() if path.is_dir()):
        wallet_path = shard / "wallets.json"
        state_path = shard / "indexer_state.json"
        if wallet_path.exists():
            for row in rows_from_store(read_json(wallet_path)):
                wallet = str((row or {}).get("wallet") or "").strip()
                if not wallet:
                    continue
                wallet_rows.append(row)
                shard_of[wallet] = shard.name
        if state_path.exists():
            payload = read_json(state_path)
            for wallet, row in (payload.get("walletStates") or {}).items():
                states[wallet] = row

    wallet_rows.sort(
        key=lambda row: float((((row.get("all") or {}).get("volumeUsd")) or 0)),
        reverse=True,
    )

    top = []
    stale_both_complete = 0
    for wallet, row in states.items():
        trade_pagination = summarize_pagination(
            row.get("tradePagination")
            or (((row.get("historyAudit") or {}).get("pagination") or {}).get("trades"))
        )
        funding_pagination = summarize_pagination(
            row.get("fundingPagination")
            or (((row.get("historyAudit") or {}).get("pagination") or {}).get("funding"))
        )
        both_complete = pagination_complete(trade_pagination) and pagination_complete(
            funding_pagination
        )
        mismatch = both_complete and (
            not row.get("tradeDone")
            or not row.get("fundingDone")
            or not row.get("remoteHistoryVerified")
            or row.get("tradeCursor")
            or row.get("fundingCursor")
            or row.get("tradeHasMore")
            or row.get("fundingHasMore")
        )
        if mismatch:
            stale_both_complete += 1

    raw_total_volume = 0.0
    for row in wallet_rows:
        raw_total_volume += float((((row.get("all") or {}).get("volumeUsd")) or 0))

    for rank, row in enumerate(wallet_rows[:10], 1):
        wallet = row["wallet"]
        state = states.get(wallet, {})
        trade_pagination = summarize_pagination(
            state.get("tradePagination")
            or (((state.get("historyAudit") or {}).get("pagination") or {}).get("trades"))
        )
        funding_pagination = summarize_pagination(
            state.get("fundingPagination")
            or (((state.get("historyAudit") or {}).get("pagination") or {}).get("funding"))
        )
        history = None
        history_path = base / shard_of[wallet] / "wallet_history" / f"{wallet}.json"
        if history_path.exists():
            payload = read_json(history_path)
            history = {
                "tradesStored": len(payload.get("trades") or []),
                "fundingStored": len(payload.get("funding") or []),
                "tradeExhausted": bool(
                    ((((payload.get("audit") or {}).get("pagination") or {}).get("trades")) or {}).get(
                        "exhausted"
                    )
                ),
                "fundingExhausted": bool(
                    ((((payload.get("audit") or {}).get("pagination") or {}).get("funding")) or {}).get(
                        "exhausted"
                    )
                ),
            }
        top.append(
            {
                "rank": rank,
                "wallet": wallet,
                "shard": shard_of[wallet],
                "volumeUsd": round(float((((row.get("all") or {}).get("volumeUsd")) or 0)), 2),
                "trades": int((((row.get("all") or {}).get("trades")) or 0)),
                "state": {
                    "tradeDone": bool(state.get("tradeDone")),
                    "fundingDone": bool(state.get("fundingDone")),
                    "remoteHistoryVerified": bool(state.get("remoteHistoryVerified")),
                    "tradeHasMore": bool(state.get("tradeHasMore")),
                    "fundingHasMore": bool(state.get("fundingHasMore")),
                    "tradeCursor": state.get("tradeCursor"),
                    "fundingCursor": state.get("fundingCursor"),
                    "historyPhase": state.get("historyPhase"),
                    "lifecycleStage": state.get("lifecycleStage"),
                    "retryPending": bool(state.get("retryPending")),
                    "retryReason": state.get("retryReason"),
                    "forceHeadRefetch": bool(state.get("forceHeadRefetch")),
                },
                "pagination": {
                    "trades": trade_pagination,
                    "funding": funding_pagination,
                    "bothComplete": pagination_complete(trade_pagination)
                    and pagination_complete(funding_pagination),
                },
                "history": history,
            }
        )

    print(
        json.dumps(
            {
                "walletCount": len(wallet_rows),
                "rawVolumeUsd": round(raw_total_volume, 2),
                "staleBothComplete": stale_both_complete,
                "top10": top,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
