#!/usr/bin/env bash
# Stop hook entrypoint — writes session draft to ai-prompts/capture/sessions/
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec python3 "${ROOT_DIR}/scripts/cursor-hooks/session_capture.py" stop
