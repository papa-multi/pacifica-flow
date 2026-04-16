# PacificaFlow

<p align="center">
  <img src="pacifica/bg4.webp" alt="PacificaFlow hero" width="100%" />
</p>

<p align="center">
  Pacifica-native analytics, wallet intelligence, copy trading, and workflow control.
</p>

<p align="center">
  <strong>Live data.</strong> <strong>Operational UI.</strong> <strong>Private-by-default runtime.</strong>
</p>

## What it does

- Wallet explorer and wallet performance
- Live positions, funding, and alerting
- Copy trading and execution controls
- Telegram copilot for wallet tracking

## API surface

<details>
<summary>Public routes and upstream endpoints used by the project</summary>

### Public UI routes

- `/`
- `/live-trade/`
- `/wallets-explorer/`
- `/wallet-performance/`
- `/copy-trading/`
- `/copy-trading/settings`
- `/settings/`
- `/social-trade/`

### Internal API routes

- Auth:
  - `/api/auth/access`
  - `/api/auth/twitter/status`
  - `/api/auth/twitter/start`
  - `/api/auth/twitter/callback`
  - `/api/auth/me`
  - `/api/auth/settings`
  - `/api/auth/profile`
  - `/api/auth/api-keys`
  - `/api/auth/signup`
  - `/api/auth/signup/verify`
  - `/api/auth/signup/resend`
  - `/api/auth/signin`
  - `/api/auth/signin/verify`
  - `/api/auth/mfa/setup`
  - `/api/auth/mfa/verify`
  - `/api/auth/mfa/disable`
  - `/api/auth/mfa/backup-codes`
  - `/api/auth/signout`
- Wallet explorer and system:
  - `/api/wallets`
  - `/api/wallets/profile`
  - `/api/wallets/sync-health`
  - `/api/system/status`
  - `/api/exchange/overview`
  - `/api/exchange/metric-series`
- Wallet performance:
  - `/api/wallet-performance`
  - `/api/wallet-performance/wallet/:wallet`
- Live trade:
  - `/api/live-trades`
  - `/api/live-trades/metrics`
  - `/api/live-trades/stream`
  - `/api/live-trades/wallet/:wallet`
  - `/api/live-trades/wallet-first`
  - `/api/live-trades/wallet-first/metrics`
  - `/api/live-trades/wallet-first/stream`
  - `/api/live-trades/wallet-first/positions`
- Copy trading:
  - `/api/copy/status`
  - `/api/copy/settings`
  - `/api/copy/risk`
  - `/api/copy/leaders`
  - `/api/copy/leaders/:id`
  - `/api/copy/leaders/:id/activity`
  - `/api/copy/enable`
  - `/api/copy/disable`
  - `/api/copy/pause`
  - `/api/copy/stop-keep`
  - `/api/copy/stop-close-all`
  - `/api/copy/manual-override`
  - `/api/copy/preview`
  - `/api/copy/run`
  - `/api/copy/activity`
  - `/api/copy/execution-log`
  - `/api/copy/positions`
  - `/api/copy/reconciliation`
  - `/api/copy/stream`
- Copy Trading Workspace:
  - `/api/copy-trading/balance`
  - `/api/copy-trading/positions`
  - `/api/copy-trading/leaders`
  - `/api/copy-trading/execution`
  - `/api/copy-trading/execution/run`
  - `/api/copy-trading/execution/preview`
  - `/api/copy-trading/leaders/:wallet`
  - `/api/copy-trading/leaders/:wallet/activity`
- Nextgen/read-model helpers:
  - `/api/nextgen/*`

### Pacifica upstream endpoints used by the backend

- `/api/v1/info`
- `/api/v1/info/prices`
- `/api/v1/account`
- `/api/v1/account/settings`
- `/api/v1/account/api_keys`
- `/api/v1/account/balance/history`
- `/api/v1/positions`
- `/api/v1/orders`
- `/api/v1/orders/history`
- `/api/v1/trades/history`
- `/api/v1/funding/history`
- `/api/v1/funding_rate/history`
- `/api/v1/portfolio`
- `/api/v1/portfolio/volume`
- `/api/v1/book`
- `/api/v1/kline`
- `/api/v1/kline/mark`
- `/api/v1/trades`

</details>

## Run

```bash
npm install
npm start
```

Open:

```bash
http://localhost:3200
```

## Services

- `pacifica-ui-api.service` for the UI/API shell
- `pacifica-wallet-indexer.service` for wallet history
- `pacifica-live-positions@.service` for live wallet-first shards
- `pacifica-global-kpi.service` for exchange-wide metrics
- `pacifica-telegram-copilot-bot.service` for Telegram wallet tracking

## Configuration

Use `config/runtime.example.env` as the template.

- Copy it to `config/runtime.env`
- Keep `config/runtime.env` local and out of git
- Store API keys, bot tokens, and secrets only in local runtime files or deployment env vars

## Public release notes

The repository is safe to publish as long as local runtime files stay untracked.
