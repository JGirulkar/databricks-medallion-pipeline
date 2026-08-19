# Task Breakdown

## Phase A — Env

- [x] Repo at `~/Desktop/Projects/databricks-medallion-pipeline/` (sibling of Intelo)
- [x] Rules, skills, hooks, MCP config
- [x] docs/SETUP.md + deploy-strategy.md
- [x] Superpowers + Databricks plugins installed
- [x] GitHub workflows: validate + deploy-ce (manual)
- [x] uv venv: `source scripts/env.sh && cd databricks && uv sync --all-packages --all-groups --no-group cluster`
- [ ] User: `databricks auth login --profile de-assessment-ce`
- [ ] User: JDK 21 install (`sudo apt install openjdk-21-jdk`) — needed for local Spark tests
- [ ] User: `gh auth login` add **JGirulkar** (no logout) → push via `github-assessment` skill
- [ ] User: GitHub secrets `DATABRICKS_HOST` + `DATABRICKS_TOKEN`

## Phase B — Implementation

- [ ] data_generation job + tests
- [ ] bronze jobs
- [ ] silver DQ jobs
- [ ] gold aggregations
- [ ] dashboard SQL + guide
- [ ] CE bundle deploy smoke

## Phase C — Submission

- [ ] acceptance-criteria all [x]
- [ ] reflection.md
- [ ] gh push (ttn email)
