# AI Prompt History

**Tool:** Cursor Agent  
**Project:** Databricks Medallion Pipeline (DE C1 Assessment)  
**Profile:** `de-assessment-ce`  
**Approach:** Path C Hybrid (Superpowers + project rules/skills)

## How to read each entry

| Field | Meaning |
|-------|---------|
| **Prompt** | What I asked (specific; quote when possible) |
| **Context provided** | Spec, rules, skills, files given to the agent |
| **AI response** | What the agent proposed |
| **Validation** | Commands/tests run and outcomes before accepting |
| **Accepted** | Kept as-is |
| **Changed** | Modified after review |
| **Rejected** | Declined + reason |
| **Why** | Rubric tie-in (Strong Cursor Usage signal) |

## Activity index

| File | Activities |
|------|------------|
| [01-planning-and-requirements.md](01-planning-and-requirements.md) | Requirements, spec, acceptance criteria |
| [02-tooling-rules-and-workflow.md](02-tooling-rules-and-workflow.md) | Env setup, Superpowers, rules, MCP, isolation |
| [03-architecture-design.md](03-architecture-design.md) | High-level medallion architecture + anchor spec |
| [04-data-generation.md](04-data-generation.md) | Sample CSV generator + DQ issues |
| [04-bronze-layer.md](04-bronze-layer.md) | Bronze ingest, CE E2E, manifest debugging |
| [05-silver-quality.md](05-silver-quality.md) | DQ checks + quality report |
| [06-gold-aggregations.md](06-gold-aggregations.md) | Gold tables + SQL |
| [07-dashboard-and-visualization.md](07-dashboard-and-visualization.md) | Dashboard tiles |
| [08-testing-debugging-data.md](08-testing-debugging-data.md) | pytest, CE runs, debugging |
| [09-git-pr-and-review.md](09-git-pr-and-review.md) | Commits, PRs, review |
| [10-assessment-documentation.md](10-assessment-documentation.md) | Meta: how docs were created |

## Omitted intentionally

- Repetitive `source scripts/env.sh` without new decisions
- System handoff messages
- Full pasted code blocks (see git history)

## Hook capture (raw drafts)

Cursor hooks auto-write session drafts to [`capture/sessions/`](capture/sessions/) — see [`capture/README.md`](capture/README.md). Refine those into the numbered files above; do not submit raw captures as-is.

## Authoring rules (team preference)

Use these rules when converting raw hook drafts into evaluator-facing prompt history:

1. **Raw -> curated flow is mandatory**
   - Start from `ai-prompts/capture/sessions/*.md`.
   - Curate into `01...10` files; never paste raw logs directly.
2. **Show user steering clearly**
   - Capture what the user asked to change, optimize, or constrain.
   - Include "user caught X" moments (e.g. MCP not loading, hook noise).
   - Show how the AI response was iterated (not just first response).
3. **Keep Accepted/Changed/Rejected explicit**
   - Every major entry must include concrete accept/change/reject signals and rationale.
4. **Context provided — prove persistent project context**
   - Every entry should cite what context was given: `cursor-workflow/spec.md`, rules, skills, prior files.
   - Avoid vague prompts; quote or near-quote the user's actual ask with constraints.
5. **Validation — test before accept**
   - When code/config changed, record commands run and outcomes (`pytest`, `ruff`, CI, row counts).
   - Do not mark Accepted without evidence when validation was possible.
6. **Record model-choice signals when present**
   - If raw capture includes model usage metadata, include at least one note on model selection/toggling.
7. **Protect private/internal notes**
   - Do not propagate internal strategy/inspiration notes into evaluator-facing entries.
   - Use `/nohistory` for non-assessment chats; do not curate those sessions.
8. **Narrative continuity**
   - `01` and `02` read as one setup story (planning + tooling).
   - `03` captures high-level architecture discussion and anchor spec approval.
   - `04`+ follow the same entry template (see `prompt-history-curation` skill).
9. **Rubric alignment**
   - Target Strong Cursor Usage signals from `docs/ASSESSMENT_FROM_PDF.md`.
   - Avoid weak signals: vague prompts, no validation, no reject reasoning, log spam.
