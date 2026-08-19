# Tooling and Workflow Reference

This file documents the tooling stack used for the DE C1 assessment, what is committed to this repo, and what is intentionally local-only.

## Primary tools

- Cursor Agent (primary AI assistant workflow)
- Databricks CLI (`de-assessment-ce` profile)
- uv (Python environment and dependency sync)
- pytest + ruff (tests and linting)
- Git + GitHub CLI (`gh`) for source control and PR flow

## Cursor plugins

- `superpowers` (process skills such as brainstorming/debugging)
- `databricks` (Databricks routing/skills)
- `github` plugin is optional; current repo flow does not depend on it

## Project skills used

Local project skills live under `.cursor/skills/` and are committed as part of assessment evidence.

Examples:
- `assessment-artifacts`
- `layer-completion`
- `medallion-pipeline-local-test`
- `deploy-ce-job`
- `pr-description`
- `github-assessment`

## Hook automation in this repo

Project hooks are defined in `.cursor/hooks.json` and only apply when this repository is opened as the workspace root.

Current hook coverage:
- `sessionStart` -> initialize session capture state
- `beforeSubmitPrompt` -> capture user prompts
- `afterFileEdit` -> capture edited files
- `beforeShellExecution` -> block risky git commit/push content + run lint gate
- `stop` -> write session capture draft into `ai-prompts/capture/sessions/`

## Prompt history strategy

- `ai-prompts/capture/sessions/` = raw hook logs (working drafts)
- `ai-prompts/01-...10-...md` = curated evaluator-facing artifacts

Raw capture files are intentionally excluded from commit history:
- `ai-prompts/capture/sessions/*.md`
- `ai-prompts/capture/INDEX.md`

## Security and separation

- Use Databricks profile `de-assessment-ce` only for this repo.
- Keep private planning notes in:
  - `.private/`
  - `.cursor/private-notes/`
- Commit guard blocks accidental staging of private/raw-capture content on commit/push.

## Local-only vs committed

Committed:
- `.cursor/skills/`, `.cursor/rules/`, `.cursor/hooks.json`
- `scripts/cursor-hooks/`
- project docs and code

Local-only (not committed):
- plugin binaries/installations
- auth sessions and PAT tokens
- OS keychain credentials
