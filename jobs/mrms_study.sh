#!/usr/bin/env bash
# Scheduler independent; pass additional dataset_jobs arguments after this script.
set -euo pipefail
script_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
python_bin="${ECORE_PYTHON:-python}"
: "${ECORE_DESTINATION:?Set ECORE_DESTINATION to durable archive storage}"
: "${ECORE_SCRATCH:?Set ECORE_SCRATCH to writable fast local scratch}"
: "${ECORE_INDEX_PATH:?Set ECORE_INDEX_PATH to a Linux-local database path}"
exec "$python_bin" "$script_root/scripts/dataset_jobs.py" fetch mrms \
  --phase all --start 2021-01-01 --end 2026-07-01 \
  --destination "$ECORE_DESTINATION" --scratch "$ECORE_SCRATCH" \
  --index-path "$ECORE_INDEX_PATH" --output "${ECORE_RUN_OUTPUT:-results/study-mrms}" \
  --monthly-writers "${ECORE_MONTH_WRITERS:-4}" --workers "${ECORE_DOWNLOADS:-8}" \
  --decode-workers "${ECORE_DECODERS:-1}" --max-hours "${ECORE_MAX_HOURS:-12}" "$@"
