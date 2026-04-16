#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SHARD_INDEX="${1:?missing shard index}"
SHARD_COUNT="${2:?missing shard count}"

exec "${ROOT_DIR}/scripts/systemd/run_live_positions_shard.sh" "${SHARD_INDEX}" "${SHARD_COUNT}" "account_census"
