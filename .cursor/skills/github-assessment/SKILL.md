---
name: github-assessment
description: Create and push the DE C1 assessment repo to GitHub (JGirulkar) without disturbing other GitHub accounts or remotes.
---

# GitHub — Assessment Repo (Isolated)

Use for **this project only**. Never log out of other accounts.

## Prerequisites

- `gh` CLI installed
- JGirulkar added as a **second** account: `gh auth login -h github.com` (no logout)
- Local git identity in this repo only:

```bash
cd ~/Desktop/Projects/databricks-medallion-pipeline
git config --local user.name "Your Name"
git config --local user.email "your-ttn-email@..."
```

## Create repo (JGirulkar only — one-shot token)

Does **not** change the global default `gh` account:

```bash
cd ~/Desktop/Projects/databricks-medallion-pipeline
GH_TOKEN=$(gh auth token -u JGirulkar) gh repo create JGirulkar/databricks-medallion-pipeline \
  --private --source=. --remote=origin
GH_TOKEN=$(gh auth token -u JGirulkar) git push -u origin main
```

If repo already exists on GitHub:

```bash
git remote add origin https://github.com/JGirulkar/databricks-medallion-pipeline.git
GH_TOKEN=$(gh auth token -u JGirulkar) git push -u origin main
```

## Verify

```bash
gh auth status                    # both accounts listed; default unchanged
git config --local --list         # local user.email only in this repo
git remote -v                     # JGirulkar/databricks-medallion-pipeline
```

## Not GitHub

- **Cursor `origin` / `new-repo` skill** → `origin.cursor.com`, not github.com
- **GitLab MCP plugin** → not used for this assessment
- **Do not** run `gh auth logout` or change global `git config user.*`

## CI secrets (after push)

GitHub → repo Settings → Secrets → Actions:

| Secret | Value |
|--------|-------|
| `DATABRICKS_HOST` | `https://dbc-06f970f4-0f19.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | CE personal access token |

See `docs/GITHUB.md` and `docs/deploy-strategy.md`.
