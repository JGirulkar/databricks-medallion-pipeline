# CE Deploy Guide

## Local (primary dev loop)

```bash
source scripts/env.sh
cd databricks/bundle
databricks bundle validate -t dev --strict
databricks bundle deploy -t dev
databricks bundle run job_data_generation -t dev
```

Profile must be `de-assessment-ce`.

## GitHub Actions (optional)

Manual deploy: Actions → **deploy-ce** → Run workflow.

Requires repo secrets — see [deploy-strategy.md](deploy-strategy.md) and [GITHUB.md](GITHUB.md).
