# AI Prompt History

**Tool:** Cursor Agent  
**Project:** Databricks Medallion Pipeline (DE C1 Assessment)  
**Profile:** `de-assessment-ce`  
**Approach:** Path C Hybrid (Superpowers + project rules/skills)

## How to read each entry

| Field | Meaning |
|-------|---------|
| **Prompt** | What I asked |
| **AI response** | What the agent proposed |
| **Accepted** | Kept as-is |
| **Changed** | Modified after review |
| **Rejected** | Declined + reason |
| **Why** | Rubric tie-in |

## Activity index

| File | Activities |
|------|------------|
| [01-planning-and-requirements.md](01-planning-and-requirements.md) | Requirements, spec, acceptance criteria |
| [02-tooling-rules-and-workflow.md](02-tooling-rules-and-workflow.md) | Env setup, Superpowers, rules, MCP, isolation |
| [03-data-generation.md](03-data-generation.md) | Sample CSV generator + DQ issues |
| [04-bronze-ingestion.md](04-bronze-ingestion.md) | Raw ingest |
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
   - Show how the AI response was iterated (not just first response).
3. **Keep Accepted/Changed/Rejected explicit**
   - Every major entry must include concrete accept/change/reject signals and rationale.
4. **Record model-choice signals when present**
   - If raw capture includes model usage metadata, include at least one note on model selection/toggling decisions in curated entries.
5. **Protect private/internal notes**
   - Do not propagate internal strategy/inspiration notes into evaluator-facing entries.
   - Keep those in local-only private notes.
6. **Narrative continuity**
   - `01` and `02` should read like one continuous setup story (planning + tooling), not isolated fragments.
