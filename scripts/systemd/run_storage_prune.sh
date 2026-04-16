#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DATA_DIR="${ROOT_DIR}/data"
TMP_AGE_MINUTES="${PACIFICA_STORAGE_TMP_AGE_MINUTES:-360}"
ARCHIVE_AGE_MINUTES="${PACIFICA_STORAGE_ARCHIVE_AGE_MINUTES:-60}"

bytes_removed=0
files_removed=0
bytes_compressed=0
files_compressed=0

add_bytes_removed() {
  bytes_removed=$((bytes_removed + $1))
  files_removed=$((files_removed + 1))
}

add_bytes_compressed() {
  bytes_compressed=$((bytes_compressed + $1))
  files_compressed=$((files_compressed + 1))
}

if [[ -d "${DATA_DIR}" ]]; then
  while IFS= read -r -d '' file; do
    size=$(stat -c '%s' "${file}" 2>/dev/null || echo 0)
    rm -f "${file}" || true
    add_bytes_removed "${size}"
  done < <(
    find "${DATA_DIR}" -type f \( -name '*.tmp' -o -name '*.tmp.*' \) -mmin +"${TMP_AGE_MINUTES}" -print0 2>/dev/null
  )
fi

RESET_ARCHIVE_DIR="${DATA_DIR}/reset_archive"
if [[ -d "${RESET_ARCHIVE_DIR}" ]]; then
  while IFS= read -r -d '' file; do
    if [[ -f "${file}.gz" ]]; then
      continue
    fi
    size=$(stat -c '%s' "${file}" 2>/dev/null || echo 0)
    gzip -9 -n "${file}"
    add_bytes_compressed "${size}"
  done < <(
    find "${RESET_ARCHIVE_DIR}" -type f \
      \( -name '*.json' -o -name '*.ndjson' \) \
      -mmin +"${ARCHIVE_AGE_MINUTES}" \
      -print0 2>/dev/null
  )
fi

echo "storage_prune removed_files=${files_removed} removed_bytes=${bytes_removed} compressed_files=${files_compressed} compressed_input_bytes=${bytes_compressed}"
