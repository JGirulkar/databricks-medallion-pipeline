#!/usr/bin/env bash
set -euo pipefail
input=$(cat)
command=$(echo "${input}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('command',''))" 2>/dev/null || echo "")
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LINT_SCRIPT="${ROOT_DIR}/scripts/lint.sh"

check_private_paths() {
  local staged
  staged="$(git -C "${ROOT_DIR}" diff --cached --name-only)"
  if echo "${staged}" | grep -Eq '^(\.private/|\.cursor/private-notes/|ai-prompts/capture/sessions/|ai-prompts/capture/INDEX\.md$)'; then
    echo '{"permission":"ask","user_message":"Blocked: private/raw capture files are staged (.private/, .cursor/private-notes/, ai-prompts/capture/sessions/, ai-prompts/capture/INDEX.md). Unstage them before commit/push."}'
    return 1
  fi
  return 0
}

if [[ "${command}" =~ git[[:space:]]+commit ]]; then
  if ! check_private_paths; then
    exit 0
  fi
  if [[ -x "${LINT_SCRIPT}" ]] && ! "${LINT_SCRIPT}" --staged >/dev/null 2>&1; then
    echo '{"permission": "ask", "user_message": "Lint failed. Fix before commit."}'
    exit 0
  fi
elif [[ "${command}" =~ git[[:space:]]+push ]]; then
  if ! check_private_paths; then
    exit 0
  fi
  if [[ -x "${LINT_SCRIPT}" ]] && ! "${LINT_SCRIPT}" >/dev/null 2>&1; then
    echo '{"permission": "ask", "user_message": "Lint failed. Fix before push."}'
    exit 0
  fi
fi
echo '{"permission": "allow"}'
