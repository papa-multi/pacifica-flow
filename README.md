# PacificaFlow Scaffold

PacificaFlow is a Pacifica-native project scaffold with two core principles:

- Live, replay-safe market/account analytics behind a stable internal API.
- Self-serve marketplace UX: users can create collections and configure mint launches from UI without manual backend/admin setup.

## Run

```bash
npm install
npm start
```

Open:

```bash
http://localhost:3200
```

Live trades page:

```bash
http://localhost:3200/live-trade/
```

Low-RPC mode (safer when you do not have a paid RPC key):

```bash
npm run start:onchain-lite
```

This mode runs on `http://localhost:3400` by default with conservative RPC pacing.

Split workers (recommended for scale):

```bash
# Terminal 1: wallet history indexer (no onchain discovery)
PORT=3201 npm run worker:wallet-indexer

# Terminal 2: on-chain discovery worker
PORT=3202 npm run worker:onchain-discovery

# Terminal 3: global KPI worker (DefiLlama source of truth)
PORT=3203 npm run worker:global-kpi

# Terminal 4: UI + API service (reads checkpoints, no background indexing)
PORT=3200 npm run service:ui-api
```

## Systemd Services (Auto-start + Auto-restart)

This repo includes systemd units so you do not need multiple terminals.

Service layout:

- `pacifica-wallet-indexer.service`
  - full wallet history indexing (no artificial trade/history cap)
  - multi-egress enabled (uses all healthy proxies/IPs in `working_proxies.txt`)
- `pacifica-onchain-discovery.service`
  - Solana on-chain discovery + deposit-wallet registry updates
- `pacifica-global-kpi.service`
  - global KPI/volume worker using DefiLlama source-of-truth logic from `/api/v1/info/prices`
  - writes snapshot to `data/pipeline/global_kpi.json`
- `pacifica-ui-api.service`
  - UI + internal API (reads checkpoints/snapshots, does not run indexer loops)

Port binding note:

- systemd units force fixed ports in `ExecStart` (`3201/3202/3203/3200`), so a `PORT=...` value inside `config/runtime.env` will not override worker ports.

Grouped target:

- `pacifica-flow.target` (starts/stops all 4 services together)

Install and start:

```bash
cd /root/pacifica-flow
sudo bash ./scripts/systemd/install.sh --now
```

Or via npm wrappers:

```bash
cd /root/pacifica-flow
sudo npm run systemd:install:now
```

Control:

```bash
# all services
sudo systemctl start pacifica-flow.target
sudo systemctl stop pacifica-flow.target
sudo systemctl restart pacifica-flow.target
sudo systemctl status pacifica-flow.target

# individual services
sudo systemctl status pacifica-wallet-indexer.service
sudo systemctl restart pacifica-wallet-indexer.service
sudo systemctl status pacifica-onchain-discovery.service
sudo systemctl status pacifica-global-kpi.service
sudo systemctl status pacifica-ui-api.service
```

Logs:

```bash
# follow all services
sudo journalctl -f -u pacifica-flow.target

# per service
sudo journalctl -f -u pacifica-wallet-indexer.service
sudo journalctl -f -u pacifica-onchain-discovery.service
sudo journalctl -f -u pacifica-global-kpi.service
sudo journalctl -f -u pacifica-ui-api.service
```

Quick status helper:

```bash
cd /root/pacifica-flow
npm run systemd:status
```

Uninstall units:

```bash
cd /root/pacifica-flow
sudo bash ./scripts/systemd/uninstall.sh
```

## Environment Variables

- `PORT` (default: `3200`)
- `HOST` (default: `0.0.0.0`)
- `PACIFICA_SERVICE` (`all|api|ui|live`, default: `all`)
- `PACIFICA_API_BASE` (default: `https://api.pacifica.fi/api/v1`)
- `PACIFICA_WS_URL` (default: `wss://ws.pacifica.fi/ws`)
- `PACIFICA_API_CONFIG_KEY` (optional, sent as `PF-API-KEY`)
- `PACIFICA_API_RPM_CAP` (default: `250`, direct-client Pacifica REST cap)
- `PACIFICA_RATE_LIMIT_WINDOW_MS` (default: `60000`)
- `PACIFICA_API_USAGE_LOG_INTERVAL_MS` (default: `15000`)
- `PACIFICA_MULTI_EGRESS_ENABLED` (default: `true`; enables proxy/IP pool for indexer REST scans)
- `PACIFICA_MULTI_EGRESS_PROXY_FILE` (default: `./data/indexer/working_proxies.txt`)
- `PACIFICA_MULTI_EGRESS_MAX_PROXIES` (default: `130`)
- `PACIFICA_MULTI_EGRESS_RPM_CAP_PER_IP` (default: `250`)
- `PACIFICA_MULTI_EGRESS_INCLUDE_DIRECT` (default: `true`)
- `PACIFICA_MULTI_EGRESS_TRANSPORT` (`curl|fetch`, default: `curl`)
- `PACIFICA_MULTI_EGRESS_MATCH_WORKERS` (default: `true`; raises scan worker count to client-pool size)
- `SOLANA_RPC_URL` (default: `https://api.mainnet-beta.solana.com`)
- `SOLANA_RPC_RATE_LIMIT_CAPACITY` (default: `3`)
- `SOLANA_RPC_RATE_LIMIT_WINDOW_MS` (default: `1000`)
- `SOLANA_RPC_GLOBAL_BUCKET_CAPACITY` (default: `1.5`)
- `SOLANA_RPC_GLOBAL_REFILL_PER_SEC` (default: `0.8`)
- `SOLANA_RPC_SIG_REFILL_PER_SEC` (default: `0.35`)
- `SOLANA_RPC_TX_REFILL_PER_SEC` (default: `0.5`)
- `SOLANA_RPC_SIG_MAX_CONCURRENCY` (default: `1`)
- `SOLANA_RPC_TX_MAX_CONCURRENCY` (default: `2`)
- `SOLANA_RPC_SIG_INITIAL_CONCURRENCY` (default: `1`)
- `SOLANA_RPC_TX_INITIAL_CONCURRENCY` (default: `1`)
- `SOLANA_RPC_429_BASE_DELAY_MS` (default: `2500`)
- `SOLANA_RPC_429_MAX_DELAY_MS` (default: `120000`)
- `SOLANA_RPC_RAMP_QUIET_MS` (default: `180000`)
- `SOLANA_RPC_RAMP_STEP_MS` (default: `60000`)
- `PACIFICA_ACCOUNT` (optional; enables account snapshots/streams)
- `PACIFICA_DATA_DIR` (default: `./data`)
- `PACIFICA_WS_ENABLED` (default: `true`)
- `PACIFICA_SNAPSHOT_ENABLED` (default: `true`; set `false` on pure indexer workers)
- `PACIFICA_GLOBAL_KPI_ENABLED` (default: `true`)
- `PACIFICA_GLOBAL_KPI_VOLUME_METHOD` (`defillama_compat|prices_rolling_24h`, default: `defillama_compat`)
- `PACIFICA_GLOBAL_KPI_REFRESH_MS` (default: `300000` with `defillama_compat`, `15000` with `prices_rolling_24h`)
- `PACIFICA_GLOBAL_KPI_PATH` (default: `./data/pipeline/global_kpi.json`)
- `PACIFICA_INDEXER_ENABLED` (default: `true`)
- `PACIFICA_INDEXER_RUNNER_ENABLED` (default: `true`; when `false`, loads indexer state but does not run scan/discovery loops)
- `PACIFICA_ONCHAIN_ENABLED` (default: `true`)
- `PACIFICA_ONCHAIN_RUNNER_ENABLED` (default: `false`; standalone on-chain discovery loop)
- `PACIFICA_PROGRAM_IDS` (CSV program IDs; default includes `PCFA5iYgmqK6MqPhWNKg7Yv7auX7VZ4Cx7T1eJyrAMH`)
- `PACIFICA_ONCHAIN_MODE` (`backfill|live`, default: `backfill`)
- `PACIFICA_ONCHAIN_START_TIME_MS` (default: `1757376000000` = 2025-09-09 UTC)
- `PACIFICA_ONCHAIN_END_TIME_MS` (optional)
- `PACIFICA_ONCHAIN_SCAN_PAGES_PER_CYCLE` (default: `1`)
- `PACIFICA_ONCHAIN_SIGNATURE_PAGE_LIMIT` (default: `20`)
- `PACIFICA_ONCHAIN_TX_CONCURRENCY` (default: `1`)
- `PACIFICA_ONCHAIN_MAX_TX_PER_CYCLE` (default: `12`)
- `PACIFICA_ONCHAIN_VALIDATE_BATCH` (default: `20`)
- `PACIFICA_ONCHAIN_DISCOVERY_TYPE` (`deposit_only|all_interactions`, default: `deposit_only`)
- `PACIFICA_DEPOSIT_VAULTS` (CSV, default Pacifica vault)
- `PACIFICA_ONCHAIN_CHECKPOINT_INTERVAL_MS` (default: `3000`)
- `PACIFICA_ONCHAIN_LOG_INTERVAL_MS` (default: `30000`)
- `PACIFICA_ONCHAIN_LOG_FORMAT` (`kv|json`, default: `kv`)
- `PACIFICA_ONCHAIN_RUNNER_INTERVAL_MS` (default: `5000`)
- `PACIFICA_ONCHAIN_EXCLUDE_ADDRESSES` (optional CSV explicit exclusions)
- `PACIFICA_WALLET_SEEDS` (comma-separated fallback wallets)
- `PACIFICA_WALLET_SOURCE_FILE` (default: `./config/wallet-seeds.txt`)
- `PACIFICA_WALLET_SOURCE_URL` (optional URL that returns wallet list JSON)
- `PACIFICA_DEPOSIT_WALLETS_PATH` (default: `./data/indexer/deposit_wallets.json`)
- `PACIFICA_INDEXER_SCAN_INTERVAL_MS` (default: `30000`)
- `PACIFICA_INDEXER_DISCOVERY_INTERVAL_MS` (default: `120000`)
- `PACIFICA_INDEXER_BATCH_SIZE` (default: `40`)
- `PACIFICA_INDEXER_MAX_PAGES_PER_WALLET` (default: `3`)
- `PACIFICA_INDEXER_FULL_HISTORY_PER_WALLET` (default: `true`; when true, keep paging until `has_more=false`)
- `PACIFICA_INDEXER_TRADES_PAGE_LIMIT` (default: `400`)
- `PACIFICA_INDEXER_FUNDING_PAGE_LIMIT` (default: `400`)
- `PACIFICA_INDEXER_WALLET_SCAN_CONCURRENCY` (default: `131`)
- `PACIFICA_INDEXER_LIVE_WALLETS_PER_SCAN` (default: `10`; wallets polled in live-tracking group per scan cycle)
- `PACIFICA_INDEXER_LIVE_MAX_PAGES_PER_WALLET` (default: `1`; head pages fetched per live wallet each cycle)
- `PACIFICA_INDEXER_CACHE_ENTRIES_PER_ENDPOINT` (default: `500`)
- `PACIFICA_INDEXER_BACKLOG_MODE_ENABLED` (default: `true`)
- `PACIFICA_INDEXER_BACKLOG_WALLETS_THRESHOLD` (default: `2000`)
- `PACIFICA_INDEXER_BACKLOG_AVG_WAIT_MS_THRESHOLD` (default: `2700000` = 45m)
- `PACIFICA_INDEXER_BACKLOG_DISCOVER_EVERY_CYCLES` (default: `8`)
- `PACIFICA_INDEXER_BACKLOG_REFILL_BATCH` (default: `INDEXER_BATCH_SIZE * 8`)
- `PACIFICA_INDEXER_SCAN_RAMP_QUIET_MS` (default: `180000`)
- `PACIFICA_INDEXER_SCAN_RAMP_STEP_MS` (default: `60000`)
- `PACIFICA_INDEXER_DISCOVERY_ONLY` (default: `false`)

## Phase 1 Scope in This Scaffold

- Read-only premium dashboard + exchange-wide Wallet Explorer indexer.
- Replay-safe append-only event log (`data/pipeline/events/events.ndjson`).
- Snapshot projections for deterministic restart rebuilds (`data/pipeline/snapshots/*.json`).
- Internal API layer consumed by UI.
- Optional tracked wallet/account can be set from UI or via config (`PACIFICA_ACCOUNT`) for detailed account channels.
- Continuous wallet indexer scans discovered wallets and maintains global wallet rankings.
- Backlog mode/backpressure: when pending backlog or wait-time thresholds are exceeded, on-chain discovery is slowed/paused and scan priority is shifted to pending/failed wallets.
- Data surfaces include:
  - account overview + risk indicators
  - open positions, open orders, order updates
  - order/trade/funding/balance/portfolio history
  - symbol allocations, performance metrics, market microstructure
  - deterministic activity timeline
- Creator Studio API/UI scaffold for self-serve collection + mint-launch planning.

Feature parity tracking reference:

- `docs/reya-feature-parity-checklist.md`

## Explicit Constraint

`account_orders` websocket is not used. Open orders are bootstrapped from REST `/orders` and real-time deltas come from `account_order_updates` WS.

## Global Wallet Discovery (On-chain + Source)

Pacifica public market streams do not expose wallet addresses directly. This scaffold now supports full Solana on-chain discovery:

- crawl signatures for each Pacifica program via `getSignaturesForAddress`
- fetch each tx via `getTransaction(..., jsonParsed)`
- extract candidate user wallets (fee payer + signers + parsed instruction owners/authorities + token owners)
- persist append-only raw logs:
  - `data/indexer/raw_signatures.ndjson`
  - `data/indexer/raw_transactions.ndjson`
- persist deterministic checkpoints and discovered-wallet evidence:
  - `data/indexer/onchain_state.json`
  - `data/indexer/wallet_discovery.json`
- optional Pacifica REST validation (`/api/v1/account?account=<wallet>`) with states `pending|confirmed|rejected`

Default mode is `deposit_only`, so discovered wallets are wallets that deposited into Pacifica vault(s).

Per-wallet indexing is resume-safe:

- Persistent `tradeCursor` / `fundingCursor`
- Persistent `tradeDone` / `fundingDone`
- Per-wallet cursor-page cache to avoid re-fetching previously processed cursor pages
- Aggressive dedupe by `history_id` (with deterministic fallback key when `history_id` is absent)

Depositor attribution priority (strict):

1. transfer `authority`/`owner`/`multisigAuthority`
2. owner of the SPL token `source` account (from pre/post token balances)
3. `feePayer` fallback only when 1 and 2 are missing **and** there is exactly one signer

External discovery sources are still supported and merged:

- `PACIFICA_WALLET_SOURCE_URL`
- `PACIFICA_WALLET_SOURCE_FILE`
- `PACIFICA_WALLET_SEEDS`

Useful operational endpoints:

- `GET /api/indexer/status`
- `GET /api/progress/overview` (single endpoint for wallet completion/backlog + onchain + egress usage)
- `GET /api/kpi/global` (latest global KPI snapshot persisted by global KPI worker)
- `POST /api/kpi/global/refresh` (manual refresh; only when global KPI worker enabled in that process)
- `POST /api/indexer/reset` (`{"preserveKnownWallets":true,"resetWalletStore":true,"clearHistoryFiles":true}`)
- `GET /api/indexer/diagnostics?page=1&pageSize=100&status=pending|partial|indexed|failed|pending_backfill|backfilling|fully_indexed|live_tracking&lifecycle=pending_backfill|backfilling|fully_indexed|live_tracking&q=<wallet_substring>`
- `POST /api/indexer/discover`
- `POST /api/indexer/scan`
- `POST /api/indexer/wallets` (`{"wallets": ["..."]}`)
- `GET /api/indexer/onchain/status`
- `POST /api/indexer/onchain/step` (`{"pages":3,"validateLimit":50}`)
- `POST /api/indexer/onchain/mode` (`{"mode":"backfill"|"live"}`)
- `POST /api/indexer/onchain/window` (`{"startTimeMs":1757376000000,"endTimeMs":null}`)
- `POST /api/indexer/onchain/reset` (`{"resetWallets":false}`)
- `GET /api/indexer/onchain/wallets?confirmed=1&confidenceMin=0.8&page=1&pageSize=50`
- `GET /api/indexer/onchain/deposit_wallets?page=1&pageSize=200`
- `GET /api/indexer/onchain/deposit_wallet_evidence?wallet=<ADDRESS>&page=1&pageSize=100`
- `GET /api/live-trades` (live trades/positions/wallet-performance payload for `/live-trade/`)
- `GET /api/defillama/v2` (returns `{ dailyVolume, openInterestAtEnd }` from `/api/v1/info/prices`)

Exchange overview volume source of truth:

- `GET /api/exchange/overview` now sets `kpis.totalVolumeUsd` from the same DefiLlama logic:
  - `dailyVolume = sum(Number(volume_24h))` from `GET /api/v1/info/prices`
- `volumeRank` is also sourced from `GET /api/v1/info/prices` and ranked by `volume_24h` (desc), with:
  - `symbol`
  - `volume_24h_usd`
  - `open_interest_usd` (`open_interest * mark`)

Runtime logs now include:

- current slot/block-time reached
- estimated remaining slots/time to target start boundary
- progress percentage

Wallet lifecycle model (explicit in backend + UI):

1. `discovered`
2. `pending_backfill`
3. `backfilling`
4. `fully_indexed` (transient marker when backfill completes)
5. `live_tracking` (wallet removed from historical queue, polled for new activity only)

Backfill completion condition (source of truth):

- `tradeDone=true`
- `fundingDone=true`
- `tradeCursor=null`
- `fundingCursor=null`
- completion observed on a successful scan (`lastSuccessAt` set)

## Deposit Registry Audit

Run sample-based correctness audits against stored raw transactions:

```bash
npm run audit:deposits -- --sample 50 --seed 42
```

Optional output file:

```bash
npm run audit:deposits -- --sample 100 --out ./data/indexer/deposit_audit_report.json
```

The audit verifies sampled detections still satisfy:

- transaction includes Pacifica program,
- token transfer destination is one of configured vaults,
- depositor attribution matches extraction rules.

If your existing `wallet_discovery.json` was created before evidence fields were added, enrich it from raw transactions:

```bash
npm run enrich:deposit-evidence
```

Preview into a separate file:

```bash
npm run enrich:deposit-evidence -- --out /tmp/wallet_discovery.enriched.json
```
