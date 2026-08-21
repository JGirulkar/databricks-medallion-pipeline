---
name: bronze-e2e-ce
description: Run bronze layer CE E2E (deploy, data gen, ingest, verify). Use when redeploying bronze, validating ingest on CE, or rerunning timestamped batch E2E — never inline orchestration.
---

# Bronze E2E on CE

Assessment-only. Profile **`de-assessment-ce`**.

## Agent rule (minimize tokens)

1. **Read this skill** — do not rewrite poll/deploy/verify logic in chat or inline Python.
2. **Run one script** — parse the `=== E2E JSON ===` block from stdout.
3. **Report** — batch_id, run IDs, `passed`, manifest `rows_written`; append P-entry to `ai-prompts/04-bronze-layer.md` only if substantive.
4. **On failure** — show `errors` from JSON + filtered log lines already printed by the script; do not re-query CE unless JSON is missing.

## Prerequisites

```bash
export DATABRICKS_CONFIG_PROFILE=de-assessment-ce
export DATABRICKS_HOST=https://dbc-06f970f4-0f19.cloud.databricks.com
databricks auth profiles   # de-assessment-ce must be Valid
```

## Commands (pick one)

| Scenario | Command |
|----------|---------|
| **Quick rerun** (default) | `./scripts/run-bronze-e2e-ce.sh` |
| **After code change** | `./scripts/run-bronze-e2e-ce.sh --deploy` |
| **First time / bootstrap DDL** | `./scripts/run-bronze-e2e-ce.sh --deploy --bootstrap` |
| **Verify existing batch** | `./scripts/run-bronze-e2e-ce.sh --verify-only YYYYMMDDTHHMMSSZ` |

Implementation: `scripts/bronze_e2e.py` (orchestration + SQL verify via warehouse `3579c90d6618d56d`).

## What the E2E does

1. Optional **`deploy-all-ce-jobs.sh`** (upsert — preserves job history)
2. Optional **bootstrap** (`de_assessment_bronze_bootstrap`)
3. **Data gen** → timestamped CSVs `{entity}_{batch_id}.csv`
4. **Orders** — file-arrival trigger (120s wait, UNPAUSED)
5. **Products + customers** — manual `run-now`
6. **Verify** — manifest `rows_written` > 0 + batch row counts in bronze

## Success criteria (`passed: true` in JSON)

| Entity | Batch rows in bronze | Manifest |
|--------|----------------------|----------|
| products | 500 | `rows_written` > 0, `files_processed` = 1, `rows_rescued` ≈ 0 |
| customers | 10010 | same |
| orders | 100020 | same; file archived to `orders/processed/` |

CSV requires `header=true` on ingest and nullable Int64 id columns in data gen (null FKs must not write as `2869.0` floats).

## JSON output (parse this)

```
=== E2E JSON ===
{
  "batch_id": "20260821T095201Z",
  "runs": { "data_gen": "...", "orders": "...", ... },
  "manifest": { "products": { "rows_written": 501, ... }, ... },
  "bronze_batch_rows": { "products": 501, ... },
  "passed": true,
  "errors": []
}
```

## Stable job names (IDs preserved on upsert deploy)

- `de_assessment_data_generation`
- `de_assessment_bronze_bootstrap`
- `de_assessment_bronze_products` / `customers` / `orders` (products/customers schedules PAUSED)
- `de_assessment_bronze_ingest_all` (manual smoke only — **not** used in E2E)

## Do not

- Run ad-hoc `databricks jobs submit` loops for E2E when this script exists
- Call E2E success when `passed: false` or manifest `rows_written=0`
- Delete/recreate jobs (drops run history) — use `deploy-all-ce-jobs.sh` only
- Use non-assessment profiles

## Related

- Deploy detail: `deploy-ce-job` skill
- Local tests before deploy: `medallion-pipeline-local-test` skill
- Layer sign-off: `layer-completion` skill
