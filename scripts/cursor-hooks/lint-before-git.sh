#!/usr/bin/env bash
set -euo pipefail
input=$(cat)
command=$(echo "${input}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('command',''))" 2>/dev/null || echo "")
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LINT_SCRIPT="${ROOT_DIR}/scripts/lint.sh"
if [[ "${command}" =~ git[[:space:]]+commit ]]; then
  if [[ -x "${LINT_SCRIPT}" ]] && ! "${LINT_SCRIPT}" --staged 2>&1; then
    echo '{"permission": "ask", "user_message": "Lint failed. Fix before commit."}'
    exit 0
  fi
elif [[ "${command}" =~ git[[:space:]]+push ]]; then
  if [[ -x "${LINT_SCRIPT}" ]] && ! "${LINT_SCRIPT}" 2>&1; then
    echo '{"permission": "ask", "user_message": "Lint failed. Fix before push."}'
    exit 0
  fi
fi
echo '{"permission": "allow"}'
