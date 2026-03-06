# Phase 1 / Core Slice

## Included

- Read-only best-in-class single-wallet dashboard.
- REST snapshot bootstrap across market + account + histories:
  - `/info`, `/info/prices`
  - `/account`, `/account/settings`
  - `/positions`, `/orders`, `/orders/history`
  - `/trades/history`, `/funding/history`
  - `/portfolio`, `/account/balance/history`
  - `/funding_rate/history` per focus symbol
  - `/book`, `/kline`, `/kline/mark` per focus symbol
- WS deltas:
  - `prices`, `bbo`, `book`, `trades`, `candle`, `mark_price_candle`
  - `account_info`, `account_positions`, `account_trades`, `account_order_updates`
  - `account_margin`, `account_leverage`
- Internal API for dashboard + filtered histories + timeline.
- Replay-safe append-only event storage and deterministic rebuild on restart.

## Excluded (later phases)

- Signed trading actions
- Full NFT deployment execution on-chain
- Automated allowlist/reveal workers
