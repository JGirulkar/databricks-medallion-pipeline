---
name: deploy-ce-job
description: Deploy and run Databricks bundle jobs on CE using de-assessment-ce profile. Use for bundle validate, deploy, run.
---

# Deploy CE Job

Adapted for CE assessment scope.

## Prerequisites

```bash
source scripts/env.sh
databricks auth profiles   # de-assessment-ce must exist
```

## Workflow

```bash
cd databricks/bundle
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run job_data_generation -t dev
```

## Rules

- Profile **de-assessment-ce** only — never non-assessment profiles
- Deploy from **local laptop**, not CE UI bundle deploy
- Use direct deployment engine in `databricks.yml`
- Document run ID in `ai-prompts/08-testing-debugging-data.md`

## Poll with loop skill

```
/loop 2m Check databricks job run status for <job>. Summarize errors or confirm success.
```
