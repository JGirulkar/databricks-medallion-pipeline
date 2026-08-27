---
name: layer-completion
description: Verify a medallion layer is complete before commit. Use when finishing data_generation, bronze, silver, gold, or dashboard layer.
---

# Layer Completion

Adapted from senior phase-completion skill.

## Checklist

1. **Scope** — only current layer; DoD from `acceptance-criteria.md`
2. **Lint** — `./scripts/lint.sh`
3. **Tests** — `./databricks/scripts/run_job_tests.sh <layer>`
4. **Deploy** — `bash scripts/deploy-all-ce-jobs.sh` (if jobs changed)
5. **CE run** — REQUIRED: `bash scripts/run-medallion-e2e-ce.sh` must pass. A layer that deploys is not complete until one real run succeeds; this step was optional once and a NameError reached the cluster.
6. **Prompt** — P-entry in matching `ai-prompts/` file
7. **Commit** — `feat(layer): description`

## Layer map

| Layer | Tests | Prompt file |
|-------|-------|-------------|
| data_generation | data_generation | 03 |
| bronze | bronze | 04 |
| silver | silver | 05 |
| gold | gold | 06 |
| dashboard | manual + SQL | 07 |

Do not push with failing lint or tests.
