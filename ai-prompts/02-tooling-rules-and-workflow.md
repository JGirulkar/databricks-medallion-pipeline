# AI Prompts — Tooling, Rules & Workflow

---

## P1 — Isolated MCP clone

**Prompt:** Use separate `~/.mcp/de-assessment-ai-dev-kit` — not Intelo path.

**AI response:** Cloned ai-dev-kit, built MCP venv, wired `.cursor/mcp.json` with `databricks-de-assessment` server + `de-assessment-ce` profile env.

**Accepted:** Separate clone and profile env.

**Rejected:** Editing Intelo `.mcp.json`.

**Why:** Full MCP isolation per user requirement.

---

## P2 — Skills copy-adapt

**Prompt:** Copy Intelo/senior skills into project `.cursor/skills/` without editing originals.

**AI response:** Created assessment-artifacts, medallion-pipeline-local-test, deploy-ce-job, layer-completion, pr-description, conventions-medallion.

**Accepted:** All six skills in repo.

**Why:** Assessment evidence + adapted patterns.
