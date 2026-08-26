# Databricks Medallion Pipeline — DE C1 Assessment

E-commerce medallion architecture pipeline: sample data → Bronze → Silver (DQ) → Gold → Dashboard.

## Quick start

All commands run from the repository root.

```bash
# 1. Environment (profile de-assessment-ce, JDK for local Spark)
source scripts/env.sh

# 2. Python dependencies
(cd databricks && uv sync --all-packages --all-groups --no-group cluster)

# 3. Tests — unit and local Spark tiers
bash databricks/scripts/run_job_tests.sh --all --forbid-skips

# 4. Deploy the ten assessment jobs to Databricks CE
bash scripts/deploy-all-ce-jobs.sh

# 5. End-to-end run: generate data, ingest, conform, verify against the tables
bash scripts/run-medallion-e2e-ce.sh
```

Steps 1–3 need no Databricks connection. Steps 4–5 require the
`de-assessment-ce` profile — see [docs/SETUP.md](docs/SETUP.md) and
[docs/AUTH.md](docs/AUTH.md).

The seed CSVs in [`data/`](data/) are committed, so the data model and its
intentional quality issues can be inspected without running anything —
see [database/seed-data-notes.md](database/seed-data-notes.md).

## Structure

- `databricks/jobs/` — pipeline jobs per layer
- `data/` — committed seed CSVs with intentional quality issues
- `database/` — schema and seed-data notes
- `ai-prompts/` — AI prompt history (assessment evidence)
- `cursor-workflow/` — Cursor context and spec
- `.cursor/` — rules, skills, hooks (assessment-scoped)

## Isolation

Uses Databricks profile **`de-assessment-ce`** only. Do not use other workspaces or credentials.
