# Pacifica On-Chain Wallet Discovery

This project now supports canonical wallet discovery from Solana chain data when no exchange endpoint lists all wallets.

Default discovery mode: `deposit_only` (extracts wallets that deposit into Pacifica vault).

## Program IDs

Default main Pacifica program ID (from `pacifica-python-sdk/rest/deposit.py`):

- `PCFA5iYgmqK6MqPhWNKg7Yv7auX7VZ4Cx7T1eJyrAMH`

Configure one or multiple programs via:

- `PACIFICA_PROGRAM_IDS=ID1,ID2,...`

## RPC Methods Used

1. `getSignaturesForAddress(programId, { before, limit, commitment })`
2. `getTransaction(signature, { encoding: "jsonParsed", commitment })`
3. `getAccountInfo` (reserved for future account classification checks)

## Public-RPC Stability Controls

The scanner is configured for slow-but-steady progress on free mainnet RPC:

- strict global token-bucket + per-method token-buckets
- adaptive per-method concurrency (auto cut on 429, slow ramp-up after quiet period)
- 429-specific exponential backoff + jitter (high base and high max)
- small pages + limited tx processing per cycle
- in-memory response cache for signature/transaction lookups
- persistent pending-transaction retry queue

## Backfill Window

- Default start: `2025-09-09T00:00:00Z` (`1757376000000`)
- Configurable via `PACIFICA_ONCHAIN_START_TIME_MS`
- Optional end bound via `PACIFICA_ONCHAIN_END_TIME_MS`

For each program ID, scanner walks backwards using `before` cursor until block time is older than start window.

## Checkpoint + Resume

Checkpoint file: `data/indexer/onchain_state.json`

Contains:

- per-program `before` cursor
- per-program progress counters
- done flags
- mode (`backfill|live`)
- bounded recent-signature ring buffer (de-dupe for overlap/restarts)
- pending transaction retry queue (for tx fetches that hit 429/temporary failures)

Resume behavior:

- Restart server: scanner resumes from persisted `before` cursors
- `POST /api/indexer/onchain/reset` resets cursors (and optionally wallet discovery table)

## Raw Data Layers

Append-only raw capture:

- `data/indexer/raw_signatures.ndjson`
- `data/indexer/raw_transactions.ndjson`

Discovered wallet evidence:

- `data/indexer/wallet_discovery.json`

## Wallet Extraction Rules

In `deposit_only` mode, a row is counted only when:

- transaction touches at least one configured Pacifica program ID, and
- parsed SPL token `transfer`/`transferChecked` destination equals one configured deposit vault.

Depositor attribution priority:

1. transfer instruction `authority` / `owner` / `multisigAuthority`
2. owner of transfer `source` token account (derived from pre/post token balances)
3. fee payer fallback only when 1 and 2 are both missing **and** tx has exactly one signer

Exclusions:

- known system/program addresses (system, token program, ATA, compute budget, etc.)
- runtime program IDs found in transaction instructions
- configured Pacifica program IDs

Each wallet stores:

- first/last seen (time, signature)
- confidence max + cumulative score
- evidence/source counters
- per-program counts
- validation state
- deposit evidence rows (bounded ring buffer) with:
  - `signature`, `slot`, `blockTimeMs`, `vault`
  - `source`, `mint`, `authority`, `sourceOwner`, `selectedBy`
  - `instructionKind`, `instructionIndex`, `parentInstructionIndex`, `innerInstructionIndex`

In `deposit_only` mode, wallet is extracted only when a parsed token transfer has destination in `PACIFICA_DEPOSIT_VAULTS` and tx touches Pacifica program.

## Pacifica REST Validation

Optional validation call:

- `GET /api/v1/account?account=<wallet>`

Validation states in wallet table:

- `pending`
- `confirmed`
- `rejected`

Retryable API failures (429/5xx) remain `pending` for later re-check.

## Live Mode

After backfill:

1. Switch mode: `POST /api/indexer/onchain/mode {"mode":"live"}`
2. Continue periodic `onchain/step` calls (or discovery timer)
3. New signatures are de-duped via recent-signature buffer

## APIs

- `GET /api/indexer/onchain/status`
- `POST /api/indexer/onchain/step`
- `POST /api/indexer/onchain/mode`
- `POST /api/indexer/onchain/window`
- `POST /api/indexer/onchain/reset`
- `GET /api/indexer/onchain/wallets`
- `GET /api/indexer/onchain/deposit_wallets`
- `GET /api/indexer/onchain/deposit_wallet_evidence?wallet=<WALLET>`

`GET /api/indexer/onchain/status` includes runtime metrics:

- current/global RPS and per-method RPS
- per-method concurrency and backoff remaining
- recent 429 rate
- pending transaction count
- progress estimate (`pct`) for backfill window
- current oldest/newest scanned slot and block-time
- estimated remaining slots/time to the configured start boundary

## Verification / Audit

Use the sample-based audit utility:

```bash
npm run audit:deposits -- --sample 50 --seed 42
```

The audit cross-checks sampled wallet evidence against `raw_transactions.ndjson` by re-running extraction rules and verifying:

- Pacifica program condition,
- vault transfer condition,
- depositor wallet attribution strategy (`authority`/`source_owner`/strict fee payer fallback).

If historical wallet records were created before evidence storage existed, enrich them:

```bash
npm run enrich:deposit-evidence
```

## Notes on Throughput

Without a paid RPC:

- use lower request rates (`SOLANA_RPC_RATE_LIMIT_CAPACITY`)
- lower tx concurrency (`PACIFICA_ONCHAIN_TX_CONCURRENCY`)
- expect slower backfill and occasional retry bursts

Recommended conservative values:

- `PACIFICA_ONCHAIN_SIGNATURE_PAGE_LIMIT=20`
- `PACIFICA_ONCHAIN_TX_CONCURRENCY=1`
- `PACIFICA_ONCHAIN_MAX_TX_PER_CYCLE=12`

For production-scale backfill, use paid RPC with archival guarantees.
