# Test Strategy

## Tiers

- `unit` — DQ logic, config (no JVM)
- `spark` — local Spark + Delta
- `cluster` — CE smoke only

## Commands

```bash
./databricks/scripts/run_job_tests.sh --all
./databricks/scripts/run_job_tests.sh silver --forbid-skips
```

## Gate

Skipped unit/spark = defect. Document CE limitations honestly.
