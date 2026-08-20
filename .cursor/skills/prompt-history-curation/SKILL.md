---
name: prompt-history-curation
description: Build first-pass curated ai-prompts entries from raw hook session files while preserving Accepted/Changed/Rejected evidence and user steering.
---

# Prompt History Curation

Use this skill after one or more working sessions when `ai-prompts/capture/sessions/*.md` has new raw drafts.

## Goal

Generate a strong first draft for evaluator-facing files under `ai-prompts/01...10` that demonstrates **Strong Cursor Usage** and avoids **Weak Cursor Usage** signals from `docs/ASSESSMENT_FROM_PDF.md`.

## Rubric checklist (apply to every entry)

### Strong signals — MUST show

| Signal | How to write it |
|--------|-----------------|
| Persistent context | `**Context provided:**` — cite `cursor-workflow/spec.md`, rules, skills, prior files |
| Specific prompts | Quote user ask with constraints; never summarize as "asked for code" |
| Iteration | Show first attempt → correction → final decision |
| Validation | `**Validation:**` — commands run + pass/fail outcome before accept |
| Reject off-architecture | Explicit **Rejected** with architecture/isolation/test reason |

### Weak signals — MUST avoid

| Anti-pattern | Fix |
|--------------|-----|
| Vague one-liner prompts | Use verbatim or near-verbatim user quotes |
| No testing evidence | Always add Validation block when code/config changed |
| "Generate code" with no spec | Reference spec section, table names, field lists |
| No accept/reject reasoning | Every entry needs Accepted/Changed/Rejected/Why |
| Log spam | One high-signal entry per decision, not every message |

## Inputs

- `ai-prompts/capture/sessions/*.md` (raw hook outputs)
- Existing curated target file (for continuity)
- `ai-prompts/README.md` authoring rules
- `docs/ASSESSMENT_FROM_PDF.md` § Strong/Weak Cursor Usage

## Required output style

For each curated entry:

```markdown
## P{n} — {title}

**Prompt:**  
"{verbatim or near-verbatim user ask with constraints}"

**Context provided:**  
{spec.md §, rules, skills, files referenced in session}

**AI response:**  
{what was proposed — concise}

**Validation:**  
- `{command}` → {outcome}
- {test/CI/row-count check if applicable}

**Accepted:** ...
**Changed:** ...
**Rejected:** ...

**Why:**  
{rubric tie-in: which strong signal this demonstrates}
```

Omit **Validation** only for pure planning prompts with no runnable artifact — still include **Context provided**.

## Curation checklist

1. Extract high-signal prompts only (skip repetitive noise).
2. Include user steering (constraints, corrections, "I caught X").
3. Preserve iteration: first attempt → refinement → decision.
4. Add **Context provided** — proves persistent project context.
5. Add **Validation** — proves test-before-accept.
6. Capture model-choice signals from raw metadata when present.
7. Keep `.private` / internal strategy out of curated output.
8. Chronological order within target file.
9. Cross-link `01` ↔ `02` where planning meets implementation.

## File routing

- Planning/setup intent -> `01-planning-and-requirements.md`
- Tool/plugin/hook/workflow mechanics -> `02-tooling-rules-and-workflow.md`
- Architecture / layer design -> `03-architecture-design.md`
- Data generation decisions -> `04-data-generation.md`
- Silver -> `05-silver-quality.md`
- Gold -> `06-gold-aggregations.md`
- Dashboard -> `07-dashboard-and-visualization.md`
- Testing/debugging -> `08-testing-debugging-data.md`
- Git/PR/review -> `09-git-pr-and-review.md`
- Documentation/meta -> `10-assessment-documentation.md`

## Guardrails

- Do not copy raw files verbatim.
- Do not invent prompts or outcomes not evidenced by raw logs.
- Do not include `.private` or internal-only notes.
- Do not write vague prompts — if raw log is vague, check transcript for fuller user message.
- Do not mark Accepted without Validation when code/config was involved.
