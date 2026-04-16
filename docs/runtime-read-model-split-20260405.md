## Runtime Split: Main Host vs Proxy Host

### Goal

Keep the main host responsive for UI/API and read models, while moving proxy-bound collection off the hot path.

### Main host responsibilities

- Serve `pacifica-ui-api.service`
- Read and cache prepared live-position shards
- Build lightweight read models for:
  - Live Trade
  - Wallet Explorer
  - Wallet Performance
  - Token Analysis
- Run low-frequency maintenance timers only

### Proxy host responsibilities

- Run live-position census workers
- Run live-position materializer workers
- Talk to the proxy pool
- Produce canonical shard snapshots under `data/live_positions/`

### Sync path

- Main host pulls remote live shards with a single rsync session
- Pull cadence: every `10s`
- Single-session pull is used to avoid opening many SSH sessions and to reduce remote load

### Why this split

- Proxies were not healthy from the main host
- Local proxy workers were competing with the API for CPU and disk
- The API host was rebuilding and scanning too much state locally
- Prepared remote shards are cheaper to consume than polling live locally

### Main host defaults

- Local live-position workers: disabled
- Local wallet phase1 workers: disabled
- API uses external shards
- API memory budget raised to `4 GiB`
- High-frequency local maintenance reduced:
  - remote live pull: `10s`
  - trade-store rebuild: `15m`
  - wallet materializer: `20m`
  - pending remote sync: `10m`
  - pending state sync: `2m`

### Operational policy

- Use the main host for serving and caching
- Use the proxy host for network-bound collection
- Prefer prepared state over request-time recomputation
- Avoid colocating proxy collectors with the hot UI/API node
