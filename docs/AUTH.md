# Databricks CE Auth — `de-assessment-ce`

Workspace: **https://dbc-06f970f4-0f19.cloud.databricks.com**

## One-time login (browser)

```bash
export PATH="$HOME/.local/bin:$PATH"
unset DATABRICKS_CLI_PATH

databricks auth login \
  --host https://dbc-06f970f4-0f19.cloud.databricks.com \
  --profile de-assessment-ce
```

Follow the browser prompt. Then verify:

```bash
databricks auth profiles
source scripts/env.sh
cd databricks/bundle && databricks bundle validate -t dev
```

## PAT alternative (if browser login fails)

1. In CE workspace: User Settings → Developer → Access tokens → Generate
2. Store token only in a local file (never commit):

```bash
databricks configure --profile de-assessment-ce \
  --host https://dbc-06f970f4-0f19.cloud.databricks.com \
  --token
```

## Isolation

- Profile name: `de-assessment-ce` only
- Do not use Intelo Azure SP or `DEFAULT` profiles for this repo
