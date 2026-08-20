# GitHub Setup — JGirulkar (assessment account only)

**Do not log out** of other GitHub accounts. Add JGirulkar as a second account and use project-local git config + one-shot `GH_TOKEN` for this repo only.

Skill: `.cursor/skills/github-assessment/SKILL.md`

## 1. Add JGirulkar (no logout)

```bash
gh auth login -h github.com   # sign in as JGirulkar in browser
gh auth status                # should list JGirulkar (and other accounts if configured)
```

## 2. Local git identity (this repo only)

```bash
cd ~/Desktop/Projects/databricks-medallion-pipeline
git config --local user.name "Your Name"
git config --local user.email "your-ttn-email@..."
```

## 3. Project GitHub MCP (`.cursor/mcp.json`)

This repo declares `github-de-assessment` in `.cursor/mcp.json` (project scope, committed with the repo). Token is **`JGirulkar` only**, resolved from:

```bash
source scripts/env.sh   # exports GITHUB_DE_ASSESSMENT_TOKEN from gh
```

Reload Cursor after MCP or auth changes. You can disable the user-scoped GitHub plugin once this project server is enabled.

## 4. Create remote and push (JGirulkar token only)

```bash
cd ~/Desktop/Projects/databricks-medallion-pipeline
GH_TOKEN=$(gh auth token -u JGirulkar) gh repo create JGirulkar/databricks-medallion-pipeline \
  --private --source=. --remote=origin
GH_TOKEN=$(gh auth token -u JGirulkar) git push -u origin main
```

If the repo already exists on GitHub:

```bash
cd ~/Desktop/Projects/databricks-medallion-pipeline
git remote add origin https://github.com/JGirulkar/databricks-medallion-pipeline.git
GH_TOKEN=$(gh auth token -u JGirulkar) git push -u origin main
```

## Not for this assessment

| Tool | Host | Use here? |
|------|------|-----------|
| `gh` + `GH_TOKEN -u JGirulkar` | github.com | Yes |
| Cursor `origin` / `new-repo` skill | origin.cursor.com | No |
| GitLab MCP plugin | gitlab.com | No |

## CI secrets (for deploy workflow)

Add in GitHub → Settings → Secrets and variables → Actions:

| Secret | Value |
|--------|-------|
| `DATABRICKS_HOST` | `https://dbc-06f970f4-0f19.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | CE personal access token |

See [deploy-strategy.md](deploy-strategy.md) for local vs workflow deploy.

**CE workspace:** `https://dbc-06f970f4-0f19.cloud.databricks.com` — see [AUTH.md](AUTH.md) before `bundle deploy`.

## Workflows

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `validate.yml` | PR + push to `main` | Lint, unit tests |
| `deploy-ce.yml` | Manual (`workflow_dispatch`) | `bundle deploy -t dev` to CE |

Use **workflow_dispatch deploy** for CE; use **local** `bundle deploy` for day-to-day development.

## Isolation reminder

This repo is standalone. Do not add remotes from other organizations or copy proprietary code from other projects.
