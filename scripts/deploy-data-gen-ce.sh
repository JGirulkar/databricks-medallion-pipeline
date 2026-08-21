#!/usr/bin/env bash
# Deprecated wrapper — always deploy ALL jobs together to preserve run history.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "NOTE: deploy-data-gen-ce.sh delegates to deploy-all-ce-jobs.sh (all jobs, no delete)."
exec "${SCRIPT_DIR}/deploy-all-ce-jobs.sh" "${1:-de_assessment}"
