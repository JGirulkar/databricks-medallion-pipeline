# Databricks Medallion Pipeline — DE C1 Assessment

E-commerce medallion architecture pipeline: sample data → Bronze → Silver (DQ) → Gold → Dashboard.

## Quick start

```bash
source scripts/env.sh
cd databricks && uv sync --all-packages --all-groups --no-group cluster
./databricks/scripts/run_job_tests.sh --list
databricks bundle validate -t dev
```

See [docs/SETUP.md](docs/SETUP.md) for environment setup and [docs/deploy-strategy.md](docs/deploy-strategy.md) for CI vs local deploy.

## Structure

- `databricks/jobs/` — pipeline jobs per layer
- `databricks/bundle/` — Databricks Asset Bundle (`de-assessment-ce`)
- `ai-prompts/` — AI prompt history (assessment evidence)
- `cursor-workflow/` — Cursor context and spec
- `.cursor/` — rules, skills, hooks (assessment-scoped)

## Isolation

Uses Databricks profile **`de-assessment-ce`** only. Do not use other workspaces or credentials.
