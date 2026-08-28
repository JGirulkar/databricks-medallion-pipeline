#!/usr/bin/env bash
# Deploy (upsert) and publish the sales overview dashboard, mirroring the job
# deploy: idempotent by display name, safe to re-run, keeps the dashboard URL
# stable. Always re-passes --dataset-catalog/--dataset-schema on update (an
# update REPLACES the serialized dashboard; omitting them breaks every
# bare-table query).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export DATABRICKS_CONFIG_PROFILE="${DATABRICKS_CONFIG_PROFILE:-de-assessment-ce}"
PROFILE="${DATABRICKS_CONFIG_PROFILE}"

CATALOG="${1:-de_assessment}"
SCHEMA="gold"
DISPLAY_NAME="Sales Overview (${CATALOG})"
JSON_FILE="${REPO_ROOT}/databricks/dashboards/sales_overview.lvdash.json"

USER_EMAIL="$(databricks current-user me --profile "${PROFILE}" -o json \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["userName"])')"
PARENT="/Workspace/Users/${USER_EMAIL}/de-medallion-assessment/dashboards"

WAREHOUSE_ID="${WAREHOUSE_ID:-$(databricks warehouses list --profile "${PROFILE}" -o json \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["id"])')}"

echo "==> Regenerate dashboard_queries.sql from the JSON"
python3 "${REPO_ROOT}/scripts/gen_dashboard_queries.py"

echo "==> Render qualified table names (CLI has no --dataset-catalog flag)"
# The committed JSON stays portable (bare table names). This CLI version
# predates the dataset-catalog/schema flags, so the deploy renders
# catalog.schema onto each FROM before upload — same substitution idea as
# the gold job's {silver}/{gold} placeholders. The structural unit test
# guarantees every FROM target is a bare, known gold table.
RENDERED="$(mktemp)"
python3 - "${JSON_FILE}" "${CATALOG}" "${SCHEMA}" "${RENDERED}" <<'PY'
import json, sys

src, catalog, schema, out = sys.argv[1:5]
spec = json.load(open(src, encoding="utf-8"))
tables = ("sales_by_product", "revenue_by_customer", "daily_weekly_trends", "customer_segmentation")
for ds in spec["datasets"]:
    ds["queryLines"] = [
        next((line.replace(f"FROM {t}", f"FROM {catalog}.{schema}.{t}") for t in tables if f"FROM {t}" in line), line)
        for line in ds["queryLines"]
    ]
json.dump(spec, open(out, "w", encoding="utf-8"))
PY

echo "==> Upsert '${DISPLAY_NAME}' (warehouse ${WAREHOUSE_ID})"
databricks workspace mkdirs "${PARENT}" --profile "${PROFILE}" 2>/dev/null || true

EXISTING_ID="$(databricks lakeview list --profile "${PROFILE}" -o json \
  | python3 -c "import json,sys; ds=[d for d in json.load(sys.stdin) if d.get('display_name')=='${DISPLAY_NAME}']; print(ds[0]['dashboard_id'] if ds else '')")"

if [[ -z "${EXISTING_ID}" ]]; then
  DASHBOARD_ID=$(databricks lakeview create \
    --display-name "${DISPLAY_NAME}" \
    --warehouse-id "${WAREHOUSE_ID}" \
    --serialized-dashboard "$(cat "${RENDERED}")" \
    --json "{\"parent_path\": \"${PARENT}\"}" \
    --profile "${PROFILE}" -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)["dashboard_id"])')
  echo "  created dashboard_id=${DASHBOARD_ID}"
else
  DASHBOARD_ID="${EXISTING_ID}"
  databricks lakeview update "${DASHBOARD_ID}" \
    --serialized-dashboard "$(cat "${RENDERED}")" \
    --profile "${PROFILE}" -o json > /dev/null
  echo "  updated dashboard_id=${DASHBOARD_ID}"
fi

echo "==> Publish"
databricks lakeview publish "${DASHBOARD_ID}" --warehouse-id "${WAREHOUSE_ID}" --profile "${PROFILE}" -o json > /dev/null
databricks lakeview get-published "${DASHBOARD_ID}" --profile "${PROFILE}" -o json \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print("  published:", d.get("display_name"), "embed_credentials=", d.get("embed_credentials"))'
HOST="${DATABRICKS_HOST:-https://dbc-06f970f4-0f19.cloud.databricks.com}"
echo "  URL: ${HOST}/dashboardsv3/${DASHBOARD_ID}/published"
