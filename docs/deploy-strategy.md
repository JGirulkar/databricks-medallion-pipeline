# Deploy Strategy — CE Assessment

Isolated assessment workspace. Profile **`de-assessment-ce`** locally; GitHub Actions uses repo secrets.

## Local vs GitHub Actions

| Action | When | Command |
|--------|------|---------|
| **Develop / debug** | Daily | `source scripts/env.sh && cd databricks/bundle && databricks bundle deploy -t dev` |
| **Validate** | Before PR | `databricks bundle validate -t dev --strict` |
| **CI validate** | Every PR / push to `main` | `.github/workflows/validate.yml` |
| **CI deploy** | Manual only (recommended for CE) | `.github/workflows/deploy-ce.yml` → workflow_dispatch |

**Recommendation:** Use **GitHub Actions for validate** on every change; use **workflow_dispatch for deploy** so CE is not redeployed on every commit. Local `bundle deploy` stays the fast inner loop during development.

Enterprise multi-environment setups often use `workflow_call` + Jinja-generated bundles across dev/uat/prod. This assessment has **one CE target** — a single static `databricks.yml` is enough.

## Why a static `databricks.yml` (no Jinja generator)

Large org pipelines sometimes render bundle config from templates across many environments and pipelines. That pays off at **20+ pipelines × 3 environments**. This assessment has:

- **4 jobs**, **1 CE target**, static `databricks.yml`

**Decision:** Keep a hand-authored `databricks/bundle/databricks.yml`.

**Data generation** is separate: `databricks/jobs/data_generation/src/generate_sample_data.py` writes CSVs with intentional DQ issues (Faker + pandas).

## Databricks plugin vs AI Dev Kit MCP

Both are useful; they do different jobs:

| Layer | What | Scope | CE? |
|-------|------|-------|-----|
| **Databricks Cursor plugin** | Skills (`databricks-core`, `databricks-dabs`, …) — guides CLI/bundle patterns | Global plugin install | Yes (skills + CLI) |
| **AI Dev Kit MCP** | `databricks-de-assessment` in `.cursor/mcp.json` — agent tool calls against workspace | **This repo only**, profile `de-assessment-ce` | Partial (CE-limited APIs) |
| **`databricks aitools`** | CLI subcommand from newer CLI (≥0.292) | Global CLI | Yes when CLI upgraded |

**Keep both:**

- Plugin = how to work (skills, bundle validate/deploy patterns).
- AI Dev Kit MCP = assessment agent bridge wired to **`de-assessment-ce`**.

Do **not** point other projects' MCP configs or Azure profiles at this assessment repo.

## GitHub secrets (for CI deploy)

In the assessment GitHub repo → Settings → Secrets → Actions:

| Secret | Value |
|--------|-------|
| `DATABRICKS_HOST` | `https://dbc-06f970f4-0f19.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | CE personal access token (generate in CE workspace) |

Optional: create environment `ce-assessment` with required reviewers before deploy workflow runs.

## Isolation checklist

- [ ] Repo at `~/Desktop/Projects/databricks-medallion-pipeline/`
- [ ] `source scripts/env.sh` → `DATABRICKS_CONFIG_PROFILE=de-assessment-ce`
- [ ] MCP server name: `databricks-de-assessment`
- [ ] `gh` authenticated with **ttn** GitHub account for this repo only
