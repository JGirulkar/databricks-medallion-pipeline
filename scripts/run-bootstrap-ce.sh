#!/usr/bin/env bash
# Run bronze bootstrap via registered job (deploy jobs first with deploy-all-ce-jobs.sh).
set -euo pipefail

export DATABRICKS_CONFIG_PROFILE="${DATABRICKS_CONFIG_PROFILE:-de-assessment-ce}"
export DATABRICKS_HOST="${DATABRICKS_HOST:-https://dbc-06f970f4-0f19.cloud.databricks.com}"
PROFILE="${DATABRICKS_CONFIG_PROFILE}"
JOB_NAME="de_assessment_bronze_bootstrap"

JOB_ID="$(databricks jobs list --profile "${PROFILE}" -o json \
  | python3 -c "
import json,sys
data=json.load(sys.stdin)
jobs=data if isinstance(data,list) else data.get('jobs',[])
for j in jobs:
    if j.get('settings',{}).get('name')=='${JOB_NAME}':
        print(j['job_id']); break
")"

if [[ -z "${JOB_ID}" ]]; then
  echo "Job ${JOB_NAME} not found. Run ./scripts/deploy-all-ce-jobs.sh first." >&2
  exit 1
fi

echo "==> Run ${JOB_NAME} (job_id=${JOB_ID})"
databricks jobs run-now "${JOB_ID}" --profile "${PROFILE}" --timeout 30m
