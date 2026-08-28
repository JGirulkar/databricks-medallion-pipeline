#!/usr/bin/env bash
# Deploy ALL assessment CE jobs in one pass — upsert every job (preserves job_ids + run history).
# Never delete/recreate jobs; always use this script instead of per-layer deploy wrappers.
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
SILVER_WS="/Workspace/Users/${USER_EMAIL}/de-medallion-assessment/silver"
GOLD_WS="/Workspace/Users/${USER_EMAIL}/de-medallion-assessment/gold"
BRONZE_SRC="${REPO_ROOT}/databricks/jobs/bronze/src"
DATA_GEN_SRC="${REPO_ROOT}/databricks/jobs/data_generation/src"
SILVER_SRC="${REPO_ROOT}/databricks/jobs/silver/src"
GOLD_SRC="${REPO_ROOT}/databricks/jobs/gold/src"

echo "==> Upload data generation sources to ${DATA_GEN_WS}"
databricks workspace mkdirs "${DATA_GEN_WS}" --profile "${PROFILE}" 2>/dev/null || true
databricks workspace import-dir "${DATA_GEN_SRC}" "${DATA_GEN_WS}" --overwrite --profile "${PROFILE}"

echo "==> Upload bronze sources to ${BRONZE_WS}"
databricks workspace mkdirs "${BRONZE_WS}" --profile "${PROFILE}" 2>/dev/null || true
databricks workspace import-dir "${BRONZE_SRC}" "${BRONZE_WS}" --overwrite --profile "${PROFILE}"

echo "==> Upload silver sources to ${SILVER_WS}"
databricks workspace mkdirs "${SILVER_WS}" --profile "${PROFILE}" 2>/dev/null || true
databricks workspace import-dir "${SILVER_SRC}" "${SILVER_WS}" --overwrite --profile "${PROFILE}"

echo "==> Upload gold sources to ${GOLD_WS}"
databricks workspace mkdirs "${GOLD_WS}" --profile "${PROFILE}" 2>/dev/null || true
databricks workspace import-dir "${GOLD_SRC}" "${GOLD_WS}" --overwrite --profile "${PROFILE}"

echo "==> Upsert all CE jobs (catalog=${CATALOG}) — job_ids preserved on update"
export CATALOG PROFILE BRONZE_WS DATA_GEN_WS SILVER_WS GOLD_WS
python3 "${REPO_ROOT}/scripts/ce_job_registry.py"
