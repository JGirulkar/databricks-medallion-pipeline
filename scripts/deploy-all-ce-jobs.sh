#!/usr/bin/env bash
# Deploy ALL assessment CE jobs in one pass — update in place (preserves run history).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export DATABRICKS_CONFIG_PROFILE="${DATABRICKS_CONFIG_PROFILE:-de-assessment-ce}"
export DATABRICKS_HOST="${DATABRICKS_HOST:-https://dbc-06f970f4-0f19.cloud.databricks.com}"

CATALOG="${1:-de_assessment}"
PROFILE="${DATABRICKS_CONFIG_PROFILE}"
USER_EMAIL="$(databricks current-user me --profile "${PROFILE}" -o json \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["userName"])')"

BRONZE_WS="/Workspace/Users/${USER_EMAIL}/de-medallion-assessment/bronze"
DATA_GEN_WS="/Workspace/Users/${USER_EMAIL}/de-medallion-assessment/data_generation"
BRONZE_SRC="${REPO_ROOT}/databricks/jobs/bronze/src"
DATA_GEN_SRC="${REPO_ROOT}/databricks/jobs/data_generation/src"

echo "==> Upload data generation sources to ${DATA_GEN_WS}"
databricks workspace mkdirs "${DATA_GEN_WS}" --profile "${PROFILE}" 2>/dev/null || true
databricks workspace import-dir "${DATA_GEN_SRC}" "${DATA_GEN_WS}" --overwrite --profile "${PROFILE}"

echo "==> Upload bronze sources to ${BRONZE_WS}"
databricks workspace mkdirs "${BRONZE_WS}" --profile "${PROFILE}" 2>/dev/null || true
databricks workspace import-dir "${BRONZE_SRC}" "${BRONZE_WS}" --overwrite --profile "${PROFILE}"

echo "==> Upsert all CE jobs (catalog=${CATALOG}) — job_ids preserved on update"
export CATALOG PROFILE BRONZE_WS DATA_GEN_WS
python3 "${REPO_ROOT}/scripts/ce_job_registry.py"
