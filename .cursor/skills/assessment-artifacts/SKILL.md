---
name: assessment-artifacts
description: Generate and update assessment markdown — ai-prompts P-entries, README, design-notes, reflection. Use after substantive AI sessions or before submission.
---

# Assessment Artifacts

## P-entry template (ai-prompts/)

```markdown
## P{n} — {title}

**Prompt:** ...
**AI response:** ...
**Accepted:** ...
**Changed:** ...
**Rejected:** ...
**Why:** ...
```

## File routing

| Topic | File |
|-------|------|
| Planning | `01-planning-and-requirements.md` |
| Tooling | `02-tooling-rules-and-workflow.md` |
| Data gen | `03-data-generation.md` |
| Bronze | `04-bronze-ingestion.md` |
| Silver | `05-silver-quality.md` |
| Gold | `06-gold-aggregations.md` |
| Dashboard | `07-dashboard-and-visualization.md` |
| Testing | `08-testing-debugging-data.md` |
| Git/PR | `09-git-pr-and-review.md` |
| Meta docs | `10-assessment-documentation.md` |

## Doc generation prompts

Generate from `cursor-workflow/spec.md` and actual code — do not invent tables or endpoints.

- `design-notes.md` — architecture overview
- `README.md` — setup matching `scripts/` and `docs/SETUP.md`
- `reflection.md` — honest gaps included
