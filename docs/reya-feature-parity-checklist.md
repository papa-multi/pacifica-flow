# ReyaFlow -> PacificaFlow Feature Parity Checklist

This checklist uses `reya-flow` as a **product-scope reference only**.  
All PacificaFlow data sources remain Pacifica APIs + Solana on-chain indexing.

## Reference Inventory (from `reya-flow`)

- Global Exchange Overview page
  - timeframe toggle (`All`, `24h`, `30d`)
  - KPI cards (volume, fees, trades, accounts, revenue)
  - volume rank / top markets
  - sync/live/rebuild status badges
- Wallet Explorer page
  - searchable ranked table
  - sorting, filtering, pagination
  - inspect action
- Wallet Profile detail
  - wallet-level KPI summary
  - symbol breakdown
  - first/last trade, win rate, pnl, fees, volume
- Live trades module
  - stream host, market tabs, wallet performance tables
- Deterministic wallet pipeline
  - identity/events/state/metrics/api/rebuild layers
  - dedupe + replay rebuild progress
- API/docs/ops surfaces
  - health/stats/series/prices
  - rebuild status + controls
  - alerting/copy-trade ecosystem pages

## PacificaFlow Parity Status

## Legend
- `DONE` = implemented and wired
- `PARTIAL` = implemented but missing pieces
- `TODO` = not implemented yet

| Feature | PacificaFlow Status | Notes |
|---|---|---|
| Exchange Overview page (premium UI) | `DONE` | `public/index.html` + `public/app.js` exchange tab |
| Timeframe toggle (`All/24h/30d`) | `DONE` | Used in `/api/exchange/overview` |
| KPI cards (revenue/accounts/trades/volume/fees) | `DONE` | Derived from indexed wallets + pipeline state |
| Volume rank table (live) | `DONE` | Exchange tab VOLUME RANK table |
| Wallet Explorer ranked table | `DONE` | Search + page-size + pagination + inspect |
| Wallet Profile panel | `DONE` | `/api/wallets/profile` summary + symbol breakdown |
| Internal API boundary (UI never calls Pacifica directly) | `DONE` | UI reads only local `/api/*` |
| Deposit-wallet canonical discovery (on-chain) | `DONE` | `deposit_only` mode + vault/program condition |
| Deposit evidence per wallet (signature/slot/block/vault/ix meta) | `DONE` | Stored in `wallet_discovery.json` |
| Sample-based deposit audit tooling | `DONE` | `npm run audit:deposits` |
| Global REST throttle cap (`<= 250 req/min`) | `DONE` | Shared limiter + usage logs |
| Live trades dedicated page parity (Reya `/live-trade`) | `TODO` | Need dedicated page + richer streaming UI |
| Advanced wallet filters/sorts parity | `PARTIAL` | Basic search/pagination exists; advanced filters pending |
| Rebuild control panel parity in UI | `PARTIAL` | Backend replay exists; full UI controls pending |
| Persona/share cards parity | `TODO` | Not yet implemented |
| Alerts/Copy-trade product surfaces parity | `TODO` | Not yet implemented |
| Docs/changelog UI parity | `PARTIAL` | Core docs exist in `docs/`; public docs UI pending |

## Implementation Worklist (Next)

1. Build Pacifica Live Trades page (real-time tabs + tables + stream health).
2. Add advanced Wallet Explorer filters/sorting/export.
3. Add rebuild dashboard card (progress %, ETA, controls).
4. Add share/profile pages (wallet persona snapshots).
5. Add alerts module surface (phase-gated, optional bot integration).

