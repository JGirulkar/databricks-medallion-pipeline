---
name: medallion-pipeline-local-test
description: Run local pytest for databricks jobs — unit/spark/cluster tiers. Use after editing jobs or DQ logic.
---

# Medallion Pipeline Local Test

Adapted from Intelo conventions-databricks-testing (assessment scope).

## Tiers

| Marker | When |
|--------|------|
| `unit` | Pure Python DQ logic, no JVM |
| `spark` | Local Spark + Delta transforms |
| `cluster` | CE smoke only — never to hide skips |

## Commands

```bash
source scripts/env.sh
./databricks/scripts/run_job_tests.sh data_generation
./databricks/scripts/run_job_tests.sh silver -m "unit or spark" --forbid-skips
./databricks/scripts/run_job_tests.sh --all
```

## DQ parametrization

Assert detection of intentional issues:
- 50 NULL emails, 10 duplicate customer_ids
- 100 NULL customer_id, 200 NULL product_id orders
- 50 orphan customer_id, 30 orphan product_id
- 20 duplicate order_ids

Skipped `unit` or `spark` test = defect.
