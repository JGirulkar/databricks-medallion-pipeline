# Local Development

```bash
source scripts/env.sh
cd databricks && uv sync --all-packages --all-groups --no-group cluster
./scripts/run_job_tests.sh --list
```

Requires JDK 21.
