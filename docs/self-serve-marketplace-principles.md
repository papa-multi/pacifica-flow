# Self-Serve Marketplace Principle

PacificaFlow marketplace flows are designed so users can launch and manage NFT collections directly from the UI, without manual engineer/admin backend operations.

## Product Rule

The core product must work as:

`Connect wallet -> Create collection -> Upload assets -> Configure mint -> Launch`

## Required Behavior

- UI-first collection creation with validation and guided steps.
- UI-first mint setup and launch planning with review/preview before confirmation.
- User-owned execution model:
  - user signs transactions client-side,
  - backend provides orchestration/indexing/metadata automation,
  - no manual per-launch backend work.
- Admin tooling is optional for safety/moderation and never required for normal launch flow.

## Implementation Implications

- Backend APIs should create launch plans/intents and return unsigned actions for user signing.
- Metadata hosting, allowlist verification, and indexing should run as automatic services.
- All creator actions should be represented as immutable events in append-only logs for audit and replay.
