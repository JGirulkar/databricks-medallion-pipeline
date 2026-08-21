#!/usr/bin/env bash
# Bronze CE E2E — thin wrapper around scripts/bronze_e2e.py
#
# Quick rerun (default — no deploy, no bootstrap):
#   ./scripts/run-bronze-e2e-ce.sh
#
# After code changes:
#   ./scripts/run-bronze-e2e-ce.sh --deploy
#
# First-time / DDL changes:
#   ./scripts/run-bronze-e2e-ce.sh --deploy --bootstrap
#
# Verify an existing batch only:
#   ./scripts/run-bronze-e2e-ce.sh --verify-only 20260821T095201Z
#
# Agent rule: run THIS script only — do not re-implement orchestration inline.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export DATABRICKS_CONFIG_PROFILE="${DATABRICKS_CONFIG_PROFILE:-de-assessment-ce}"
export DATABRICKS_HOST="${DATABRICKS_HOST:-https://dbc-06f970f4-0f19.cloud.databricks.com}"

CATALOG="de_assessment"
DEPLOY=0
BOOTSTRAP=0
VERIFY_ONLY=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --deploy) DEPLOY=1; shift ;;
    --bootstrap) BOOTSTRAP=1; shift ;;
    --verify-only)
      VERIFY_ONLY="${2:?--verify-only requires batch_id}"
      shift 2
      ;;
    --catalog)
      CATALOG="${2:?--catalog requires value}"
      shift 2
      ;;
    --help|-h)
      sed -n '2,16p' "$0"
      exit 0
      ;;
    --*) echo "Unknown flag: $1" >&2; exit 2 ;;
    *) CATALOG="$1"; shift ;;
  esac
done

PY=(python3 "${REPO_ROOT}/scripts/bronze_e2e.py")

if [[ -n "${VERIFY_ONLY}" ]]; then
  exec "${PY[@]}" verify --catalog "${CATALOG}" --batch-id "${VERIFY_ONLY}"
fi

ARGS=(run --catalog "${CATALOG}")
[[ "${DEPLOY}" -eq 1 ]] && ARGS+=(--deploy)
[[ "${BOOTSTRAP}" -eq 1 ]] && ARGS+=(--bootstrap)

exec "${PY[@]}" "${ARGS[@]}"
