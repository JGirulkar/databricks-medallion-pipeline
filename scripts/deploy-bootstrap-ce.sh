#!/usr/bin/env bash
# Lean CE bootstrap — no bundle/Terraform.
# Prefer deploy-all-ce-jobs.sh + run-bootstrap-ce.sh for registered jobs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"${SCRIPT_DIR}/deploy-bronze-jobs-ce.sh" "${1:-de_assessment}"
"${SCRIPT_DIR}/run-bootstrap-ce.sh"
