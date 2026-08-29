# Task Breakdown

## Phase A — Env

- [x] Repo at `~/Desktop/Projects/databricks-medallion-pipeline/` (standalone assessment project)
- [x] Rules, skills, hooks, MCP config
- [x] docs/SETUP.md + deploy-strategy.md
- [x] Superpowers + Databricks plugins installed
- [x] GitHub workflows: validate + deploy-ce (manual)
- [x] uv venv: `source scripts/env.sh && cd databricks && uv sync --all-packages --all-groups --no-group cluster`
- [x] User: `databricks auth login --profile de-assessment-ce`
- [x] User: JDK 21 install — local Spark tests run
- [x] User: `gh auth login` add **JGirulkar** (no logout) → push via `github-assessment` skill
- [ ] User: GitHub secrets `DATABRICKS_HOST` + `DATABRICKS_TOKEN`

## Phase B — Implementation

- [x] data_generation job + tests (seed + delta modes, 17 tests)
- [x] bronze jobs (append-only, guarded; 62 tests)
- [x] silver DQ jobs (3 outcomes, orphan healing, parallel; 61 tests)
- [x] CE deploy + two-delivery end-to-end run green (all invariants from the tables)
- [x] gold aggregations (4 tables; filter _is_orphan/_is_deleted; derived segment_type)
- [x] dashboard SQL + guide

## Phase C — Submission

- [ ] acceptance-criteria all [x]
- [x] reflection.md
- [x] push from the personal GitHub account
