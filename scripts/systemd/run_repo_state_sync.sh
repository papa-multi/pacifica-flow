#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

exec /usr/bin/env node "${ROOT_DIR}/scripts/repo_state_sync.js" snapshot
