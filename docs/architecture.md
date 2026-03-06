# Architecture Overview

## Modules

- `src/services/transport`
  - `rest_client`, `ws_client`, `signer`, `rate_limit_guard`, `retry_policy`, `clock_sync`
- `src/services/pipeline`
  - `identity`, `events`, `state`, `metrics`, `api`, `rebuild`, `index`
- `src/components`
  - `general-data` (health + dashboard APIs)
  - `wallet-tracking` (account/positions/orders/trades APIs)
  - `live-trades-host` (Pacifica websocket orchestration)
  - `creator-studio` (self-serve collection + mint setup APIs)

## Persistence

- Append-only event log: `data/pipeline/events/events.ndjson`
- Event index + dedupe: `data/pipeline/events/index.json`
- Snapshot projections:
  - `data/pipeline/snapshots/state.json`
  - `data/pipeline/snapshots/metrics.json`
  - `data/pipeline/identity.json`

## Deterministic Recovery

On startup:

1. Load snapshots and indexes.
2. Replay event log in order of `eventId`.
3. Recompute metrics.
4. Resume live ingestion.

