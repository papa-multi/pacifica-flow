# Repo State Sync

This repo can publish resumable runtime state to a dedicated Git branch without polluting the main code branch.

## Branch model

- code stays on your normal working branch
- runtime backups go to a separate branch: `state-sync`
- the backup branch is checked out into a separate worktree:
  - `/root/pacifica-flow-state-sync`

That prevents hourly state commits from touching the active code working tree.

## What is mirrored directly

The hourly sync copies the current resumable state for:

- `data/nextgen/source-state.json`
- `data/nextgen/events/cursors.json`
- `data/nextgen/state/`
- `data/live_positions/`
- `data/pipeline/global_kpi.json`
- `data/pipeline/events/index.json`
- `data/pipeline/snapshots/`
- `data/pipeline/identity.json`
- `data/indexer/indexer_state.json`
- `data/indexer/wallets.json`
- `data/indexer/wallet_discovery.json`
- `data/indexer/wallet_discovery_summary.json`
- `data/indexer/onchain_state.json`
- `data/indexer/deposit_wallets.json`
- `data/indexer/working_proxies.txt`
- `data/wallet_explorer_v2/review_pipeline/`
- selected UI read-model files under `data/ui/`

These files are restored directly on another server.

## What is archived as immutable chunks

Large append-only logs are not recommitted as one giant file every hour.
They are stored as fixed-size chunks in the `state-sync` branch:

- `data/nextgen/events/events.ndjson`
- `data/pipeline/events/events.ndjson`
- `data/indexer/raw_transactions.ndjson`
- `data/indexer/raw_signatures.ndjson`

That preserves history while keeping each hourly sync incremental.

## Commands

Create a snapshot and push it:

```bash
node scripts/repo_state_sync.js snapshot
```

Restore the latest saved state from the backup branch:

```bash
node scripts/repo_state_sync.js restore --fetch
```

Restore and start the stack:

```bash
bash scripts/systemd/resume_from_repo_state.sh --fetch --start
```

Show latest sync status:

```bash
node scripts/repo_state_sync.js status
```

## systemd

Installed timer:

- `pacifica-repo-state-sync.timer`

It runs hourly and calls:

- `scripts/systemd/run_repo_state_sync.sh`

## GitHub authentication

The push step needs working Git credentials for the repo remote.

Current remote:

- `git@github.com:papa-multi/pacifica-flow.git`

So the server needs either:

- a valid SSH deploy key/account key that can push to that repo
- or the remote changed to an HTTPS URL with a token-based credential helper

Without that, hourly snapshots can still be created locally, but the final `git push` step will fail.

## Important note

This design preserves the canonical resumable state and append-only journals that the system needs to continue from the last saved point.
It intentionally does not try to commit every giant rebuildable cache or local archive directory into GitHub on every run.
