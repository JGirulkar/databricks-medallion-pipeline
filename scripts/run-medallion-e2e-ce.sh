#!/usr/bin/env bash
# Bronze + Silver CE E2E — individual ingests, silver table-update triggers.
#
#   ./scripts/run-medallion-e2e-ce.sh --deploy
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export DATABRICKS_CONFIG_PROFILE="${DATABRICKS_CONFIG_PROFILE:-de-assessment-ce}"
export DATABRICKS_HOST="${DATABRICKS_HOST:-https://dbc-06f970f4-0f19.cloud.databricks.com}"

CATALOG="de_assessment"
DEPLOY=0
FORCE_BRONZE_BOOTSTRAP=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --deploy) DEPLOY=1; shift ;;
    --force-bronze-bootstrap) FORCE_BRONZE_BOOTSTRAP=1; shift ;;
    --catalog)
      CATALOG="${2:?--catalog requires value}"
      shift 2
      ;;
    --help|-h)
      sed -n '2,5p' "$0"
      exit 0
      ;;
    --*) echo "Unknown flag: $1" >&2; exit 2 ;;
    *) CATALOG="$1"; shift ;;
  esac
done

ARGS=(run --catalog "${CATALOG}")
[[ "${DEPLOY}" -eq 1 ]] && ARGS+=(--deploy)
[[ "${FORCE_BRONZE_BOOTSTRAP}" -eq 1 ]] && ARGS+=(--force-bronze-bootstrap)

exec python3 "${REPO_ROOT}/scripts/medallion_e2e.py" "${ARGS[@]}"
