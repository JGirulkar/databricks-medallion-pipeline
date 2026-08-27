#!/usr/bin/env bash
# Verify a bronze E2E batch on CE (SQL warehouse — no Spark job submit).
# Prefer: ./scripts/run-bronze-e2e-ce.sh --verify-only BATCH_ID
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export DATABRICKS_CONFIG_PROFILE="${DATABRICKS_CONFIG_PROFILE:-de-assessment-ce}"
export DATABRICKS_HOST="${DATABRICKS_HOST:-https://dbc-06f970f4-0f19.cloud.databricks.com}"

CATALOG="${1:-de_assessment}"
BATCH_ID="${2:?usage: run-bronze-e2e-verify-ce.sh [catalog] BATCH_ID}"

exec python3 "${REPO_ROOT}/scripts/bronze_e2e.py" verify \
  --catalog "${CATALOG}" \
  --batch-id "${BATCH_ID}"
