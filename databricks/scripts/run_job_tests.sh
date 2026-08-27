#!/usr/bin/env bash
# Assessment test runner for pipeline jobs.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "${ROOT}/.." && pwd)"
source "${REPO_ROOT}/scripts/env.sh" 2>/dev/null || true

cd "${ROOT}"

FORBID_SKIPS=false
MARKERS=""
JOB=""
LIST=false
ALL=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --forbid-skips) FORBID_SKIPS=true; shift ;;
    -m) MARKERS="$2"; shift 2 ;;
    --list) LIST=true; shift ;;
    --all) ALL=true; shift ;;
    -h|--help)
      echo "Usage: run_job_tests.sh [job] [-m markers] [--forbid-skips] [--list] [--all]"
      exit 0 ;;
    *) JOB="$1"; shift ;;
  esac
done

if $LIST; then
  echo "data_generation bronze silver gold"
  exit 0
fi

PYTEST_ARGS=(-q)
if [[ -n "${MARKERS}" ]]; then
  PYTEST_ARGS+=(-m "${MARKERS}")
fi
if $FORBID_SKIPS; then
  PYTEST_ARGS+=(-ra)
fi

run_job() {
  local job="$1"
  local dir="${ROOT}/jobs/${job}"
  if [[ ! -d "${dir}/tests" ]]; then
    echo "No tests for ${job}"
    return 0
  fi
  echo "==> ${job}"
  local out status=0
  out="$(cd "${dir}" && uv run --no-sync python -m pytest tests/ "${PYTEST_ARGS[@]}" 2>&1)" || status=$?
  echo "${out}"
  if [[ ${status} -ne 0 ]]; then
    return "${status}"
  fi
  # A skipped unit/spark test is a defect, not a pass (see test-strategy.md).
  if $FORBID_SKIPS && grep -Eq '[0-9]+ skipped' <<<"${out}"; then
    echo "ERROR: ${job} reported skipped tests and --forbid-skips is set" >&2
    return 1
  fi
  return 0
}

if $ALL; then
  for j in data_generation bronze silver gold; do
    run_job "$j" || exit 1
  done
elif [[ -n "${JOB}" ]]; then
  run_job "${JOB}"
else
  run_job data_generation
fi
