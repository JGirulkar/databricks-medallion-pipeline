#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

STAGED=false
FIX=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --staged) STAGED=true; shift ;;
    --fix) FIX=true; shift ;;
    *) shift ;;
  esac
done

RUFF_ARGS=(check)
if $FIX; then RUFF_ARGS+=(--fix); fi
if $STAGED; then RUFF_ARGS+=(--diff); fi

echo "==> ruff"
if command -v ruff >/dev/null 2>&1; then
  ruff "${RUFF_ARGS[@]}" databricks/ scripts/ || exit 1
else
  echo "WARN: ruff not installed"
fi

echo "==> bundle validate"
if command -v databricks >/dev/null 2>&1; then
  source scripts/env.sh
  (cd databricks/bundle && databricks bundle validate -t dev) || exit 1
else
  echo "WARN: databricks CLI not installed — see docs/SETUP.md"
fi

echo "Lint OK"
