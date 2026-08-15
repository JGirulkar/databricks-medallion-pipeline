# CE Deploy Guide

```bash
source scripts/env.sh
cd databricks/bundle
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run job_data_generation -t dev
```

Profile must be `de-assessment-ce`.
