#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG="${ROOT_DIR}/ai-prompts/.session-capture.log"
echo "$(date -Iseconds) session ended — update ai-prompts/ if needed" >> "${LOG}"
echo '{"permission": "allow"}'
