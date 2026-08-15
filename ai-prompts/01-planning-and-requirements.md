# AI Prompts — Planning & Requirements

---

## P1 — Environment setup plan (Path C Hybrid)

**Prompt:** Implement DE C1 Assessment Path C Hybrid env setup — isolated from Intelo, de-assessment-ce profile, copy-adapt skills.

**AI response:** Scaffolded repo at `~/Projects/databricks-medallion-pipeline/`, rules, skills, hooks, MCP config, docs/SETUP.md.

**Accepted:** Isolation matrix, senior ai-prompts structure, Intelo databricks/jobs layout.

**Changed:** Hooks in `scripts/cursor-hooks/` (`.cursor/hooks/` write-restricted).

**Rejected:** Global `~/.cursor/skills/` — project-only skills.

**Why:** Non-negotiable isolation from Intelo per plan.

---

## P2 — Superpowers full plugin

**Prompt:** Install whole Superpowers; document selective use in SETUP.md and tool-workflow.md.

**AI response:** Documented `/add-plugin superpowers` — all skills; selective use for planning/TDD; rules/skills primary for gates.

**Accepted:** Full plugin + hybrid workflow documentation.

**Why:** Path C requires both Superpowers discipline and senior-style project gates.
