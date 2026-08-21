---
name: deploy-ce-job
description: Deploy and run Databricks jobs on CE using de-assessment-ce profile. Prefer lean workspace upload + jobs submit; use bundle direct engine when CLI is patched.
---

# Deploy CE Job

Assessment-only. Profile **`de-assessment-ce`**.

## Prerequisites

```bash
export DATABRICKS_CONFIG_PROFILE=de-assessment-ce
export DATABRICKS_HOST=https://dbc-06f970f4-0f19.cloud.databricks.com
databricks auth profiles   # de-assessment-ce must be Valid
```

Do **not** rely on `source scripts/env.sh` in agent shells if GitHub token export is blocked — set the two `DATABRICKS_*` vars above instead.

## Lean path (recommended now) — no Terraform

Use for CE when CLI v0.261.x (Terraform GPG key expired):

```bash
# Upload code + upsert ALL jobs in one pass (preserves job_id + run history)
./scripts/deploy-all-ce-jobs.sh de_assessment

# Legacy wrappers (same as deploy-all-ce-jobs.sh):
# ./scripts/deploy-bronze-jobs-ce.sh de_assessment
# ./scripts/deploy-data-gen-ce.sh de_assessment

# Run bootstrap once (idempotent)
./scripts/run-bootstrap-ce.sh

# Or both:
./scripts/deploy-bootstrap-ce.sh de_assessment
```

**Important:** Do not deploy bronze or data-gen jobs separately with delete+recreate — that drops run history. Always use `deploy-all-ce-jobs.sh`, which calls `jobs update` when a job already exists.

Jobs registered:
- `de_assessment_data_generation`
- `de_assessment_bronze_products` / `customers` / `orders` (schedules PAUSED)
- `de_assessment_bronze_ingest_all`

# Register data generation job (serverless) + upload code
./scripts/deploy-all-ce-jobs.sh de_assessment

# Generate CSVs into volumes, then ingest
databricks jobs run-now <data_gen_job_id> --profile de-assessment-ce --timeout 60m
databricks jobs run-now <ingest_all_job_id> --profile de-assessment-ce --timeout 60m

## Bundle path (when CLI upgraded)

CLI **≥0.279** supports `bundle.engine: direct` (no Terraform). CLI **≥0.290.2** fixes the April 2026 Terraform GPG issue if you stay on terraform engine.

```yaml
# databricks/bundle/databricks.yml
bundle:
  name: de-medallion-assessment
  engine: direct
```

```bash
cd databricks/bundle
databricks bundle deploy -t dev --profile de-assessment-ce
databricks bundle run job_bronze_bootstrap -t dev --profile de-assessment-ce
```

Upgrade CLI when ready:

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

## Rules

- Profile **de-assessment-ce** only
- Bootstrap before ingest; ingest waits for CSVs from data generator
- Document run IDs in `ai-prompts/04-bronze-layer.md` (or use `bronze-e2e-ce` skill — JSON block has run IDs)

## Bronze E2E

Use **`bronze-e2e-ce`** skill — one command:

```bash
./scripts/run-bronze-e2e-ce.sh              # quick rerun
./scripts/run-bronze-e2e-ce.sh --deploy     # after code changes
```

## Poll

```
/loop 2m Check databricks jobs run status for bootstrap. Summarize errors or confirm success.
```
