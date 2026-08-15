# GitHub Setup

## Auth (ttn email)

```bash
gh auth login
gh auth status
```

## Create remote and push

```bash
cd ~/Projects/databricks-medallion-pipeline
git add -A
git commit -m "chore: Path C Hybrid env scaffold"
gh repo create databricks-medallion-pipeline --private --source=. --remote=origin
git push -u origin main
```

Use your **ttn email** GitHub account per assessment requirements.

## Isolation reminder

This repo is separate from Intelo. Do not add Intelo remotes or copy proprietary code.
