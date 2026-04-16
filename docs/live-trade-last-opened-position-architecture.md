# Live Trade: Last Opened Position Architecture

## Goal

Track newly opened positions across the tracked wallet universe and expose a prepared `lastOpenedPosition` read model to Live Trade.

The product requirement is:

- no `Last Seen` as the main wallet signal
- use `Last Opened Position`
- preserve full current open-position state
- detect new opens quickly
- keep backend/UI reads cheap
- minimize load on the backup server

## Core Design

The system is split into four lanes.

### 1. Canonical Wallet Universe

Source:

- tracked wallet corpus from `wallet_dataset_compact.json`
- wallet performance store

Purpose:

- one canonical wallet list
- one consistent shard assignment
- no duplicate polling across workers

Rules:

- shard by stable wallet hash
- one shard owns one wallet at a time
- all live state is keyed by wallet and position key

### 2. Detection Lane

Purpose:

- cheaply detect that a wallet may have opened a new position

Method:

- poll `/account`
- use `positions_count`
- compare against last known account state

Why this is cheap:

- much smaller payload than `/positions`
- enough to detect `0 -> >0` or `n -> n+1`

Recommended proxy budget:

- dedicate about `200` proxies total to this lane
- with `4` shards, start with `50` detector proxies per shard

Scheduling:

- hot wallets first
- then wallets with active open positions
- then recent-trigger wallets
- then warm/high-value wallets
- then full background sweep

### 3. Materializer Lane

Purpose:

- fetch full position details only when needed

Trigger conditions:

- detector lane reports `positions_count > known_count`
- hot WS lane reports `account_positions` change
- wallet is already open and due for reconcile
- gap repair / confirmation is required

Method:

- call `/positions`
- normalize rows
- compare against stored position fingerprint
- emit `position_opened`, `position_size_changed`, `position_closed`

This lane should be narrower than the detector lane and should not scan the full wallet universe blindly.

### 4. Hot WS Lane

Purpose:

- maintain fast updates for wallets that already look active

Subscriptions:

- `account_positions`
- `account_trades`
- `account_order_updates`

Promotion rules:

- wallet currently has open positions
- wallet just triggered a new open candidate
- wallet just traded / received an order update

Demotion rules:

- no WS activity for the configured inactivity window
- no open positions and no recent triggers

## State Model

Per wallet, store:

- `accountPositionsCount`
- `materializedPositionsCount`
- `state`
  - `closed`
  - `hinted_open`
  - `materialized_open`
  - `uncertain`
- `lastAccountAt`
- `lastPositionsAt`
- `lastStateChangeAt`
- `pendingBootstrap`

Per position, store:

- wallet
- position key
- symbol
- side
- entry
- size
- position USD
- unrealized PnL
- current open flag
- current opened at
- first seen at
- last seen at

Per wallet, also store a prepared `lastOpenedPosition` summary:

- `lastOpenedPositionAt`
- `lastOpenedPositionSymbol`
- `lastOpenedPositionSide`
- `lastOpenedPositionDirection`
- `lastOpenedPositionEntry`
- `lastOpenedPositionSize`
- `lastOpenedPositionUsd`
- `lastOpenedPositionPnlUsd`
- `lastOpenedPositionObservedAt`
- `lastOpenedPositionUpdatedAt`
- `lastOpenedPositionSource`
- `lastOpenedPositionConfidence`

## Change Detection

The change detector compares:

- previous materialized positions by `wallet + positionKey`
- new normalized positions by `wallet + positionKey`

Rules:

- key appears now but not before: `position_opened`
- key exists in both and size changed: `position_size_changed`
- key disappears after close confirmation: `position_closed`

Important anti-false-positive rules:

- do not treat snapshot gaps as new opens
- do not close immediately on one empty response if the account lane still indicates open positions
- require confirmation for close when the wallet was recently open

## Event Pipeline

Persist normalized events with details:

- wallet
- symbol
- side / direction
- position key
- size
- entry
- mark
- position USD
- unrealized PnL
- opened at
- observed at
- updated at
- source
- confidence

The event log is append-only for recent history and feeds:

- Live Trade `New Live Opens`
- wallet drawer detail
- observability metrics

## UI Read Model

The UI should read a prepared dataset, not diff raw positions itself.

Required read models:

- current open positions
- recent open events
- wallet performance rows
- wallet-level `lastOpenedPosition`

That keeps refreshes cheap and avoids repeated heavy merges in the browser.

## Minimal Load on the Other Server

Primary rule:

- keep detection, diffing, event generation, and UI read models on the main server

Backup server role:

- egress / proxy assist only
- optional overflow collector only when local detector health falls below threshold
- no primary UI read path

This keeps the other server out of the hot product path and limits it to network help instead of owning the live model.

## Recommended Resource Split

For a practical first production configuration:

- `200` proxies total for detection lane
- small on-demand materializer pool for `/positions`
- WS hot lane reserved for currently open / newly triggered wallets

This is more efficient than brute-force `/positions` polling across all wallets.

## Operational Metrics

Track and alert on:

- wallet coverage percent
- detector lag
- materialization lag
- trigger-to-open-event latency
- proxy health
- timeout clients
- stale wallets
- gap wallet count
- newly opened positions per minute

## Reuse for Wallet Tracking

The same model should power wallet tracking:

- one wallet universe
- one position state store
- one event log
- separate UI read models

That avoids having Wallet Explorer, Wallet Performance, and Live Trade drift apart.
