#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

FETCH="false"
START="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fetch)
      FETCH="true"
      shift
      ;;
    --start)
      START="true"
      shift
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

ARGS=("restore")
if [[ "${FETCH}" == "true" ]]; then
  ARGS+=("--fetch")
fi
if [[ "${START}" == "true" ]]; then
  ARGS+=("--start")
fi

exec /usr/bin/env node "${ROOT_DIR}/scripts/repo_state_sync.js" "${ARGS[@]}"
