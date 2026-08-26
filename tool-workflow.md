# Tool Workflow — DE C1 Assessment

**Primary AI tool:** Cursor Agent  
**Approach:** Path C Hybrid  
**Profile:** `de-assessment-ce` (Databricks Community Edition only)

## Workflow model

| Layer | Tool | Role |
|-------|------|------|
| Planning | **Superpowers** (full plugin) | `brainstorming`, `executing-plans`, TDD/debug when needed |
| Implementation gates | **Project rules + skills + hooks** | Primary authority for code quality and layer completion |
| Session evidence | **Cursor hooks → `ai-prompts/capture/`** | Auto draft on session end; refine into numbered P-entries |
| Domain | Databricks CLI, AI Dev Kit MCP | Jobs API deploy, CE job runs |
| Evidence | `ai-prompts/` P1/P2 format | Prompt history with Accepted/Changed/Rejected |

## Superpowers — full install, selective use

**Installed:** `/add-plugin superpowers` (all skills available)

**When to invoke:**
- New layer or major feature → `brainstorming` first
- Multi-session work → `executing-plans` against `cursor-workflow/task-breakdown.md`
- Complex bugs → `systematic-debugging`
- New behavior → `test-driven-development` when appropriate

**When NOT to invoke:**
- Small fixes, lint, typo, single-file edits
- Layer sign-off → use `layer-completion` skill instead
- PR creation → use `pr-description` skill

**Why hybrid:** Superpowers provides process discipline; project `.cursor/rules/` and skills enforce medallion boundaries, CE profile, and assessment artifact standards. Rules/skills/hooks remain **primary for implementation gates**.

## Context setting

1. Open **this repo** as Cursor workspace root
2. `cursor-workflow/spec.md` — design authority
3. `AGENTS.md` — quick orientation
4. `source scripts/env.sh` — `de-assessment-ce` profile

## Lifecycle

1. **Requirement analysis** — Superpowers brainstorm → `requirements-analysis.md`
2. **Design** — `design-notes.md`, optional Canvas → summarize into markdown
3. **Implement** — `explore-before-change` rule → code in `databricks/jobs/`
4. **Validate** — `run_job_tests.sh` (unit/spark) → `layer-completion` checklist
5. **Deploy** — `deploy-ce-job` skill → CE via `scripts/deploy-all-ce-jobs.sh`
6. **Capture** — hooks write `ai-prompts/capture/sessions/` draft; refine into `ai-prompts/` P-entry per session
7. **Ship** — `pr-description` skill, atomic commits

## What I avoid sharing with AI

- Real customer PII
- Non-assessment production credentials, Azure SP secrets
- PAT tokens in prompts (use profile auth)

## Responsible AI judgment

- Reject suggestions that violate medallion boundaries (cleaning in Bronze, deleting bad rows in Silver)
- Document honest gaps (CE limitations, tests not yet run)
- Generate docs from actual code, not blueprint fiction

## Reuse in production

This pattern — spec-first, layer gates, local Spark tests before cluster, curated prompt history — applies to any medallion pipeline on Databricks.
