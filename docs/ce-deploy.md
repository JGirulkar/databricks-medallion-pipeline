# CE Deploy Guide

## Local (primary dev loop)

```bash
source scripts/env.sh
# From the repository root
bash scripts/deploy-all-ce-jobs.sh
bash scripts/run-medallion-e2e-ce.sh
```

Profile must be `de-assessment-ce`.

## GitHub Actions (optional)

Manual deploy: Actions → **deploy-ce** → Run workflow.

Requires repo secrets — see [deploy-strategy.md](deploy-strategy.md) and [GITHUB.md](GITHUB.md).
