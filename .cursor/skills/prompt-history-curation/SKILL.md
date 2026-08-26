---
name: prompt-history-curation
description: Build curated ai-prompts entries from raw hook session files while preserving Accepted/Changed/Rejected reasoning and user steering.
---

# Prompt History Curation

Use this skill after one or more working sessions when
`ai-prompts/capture/sessions/*.md` has new raw drafts.

## Goal

Turn raw session captures into a readable engineering record under
`ai-prompts/01...10`: what was asked, what was proposed, how it was verified,
and why each proposal was accepted, changed, or rejected. The record should
read as a disciplined project log — decisions and their reasons — not as a
transcript and not as a document written at any audience.

## Quality checklist (apply to every entry)

### Must show

| Quality | How to write it |
|--------|-----------------|
| Persistent context | `**Context provided:**` — cite `cursor-workflow/spec.md`, rules, skills, prior files |
| The real ask | A polished 1–3 sentence summary of the user's ask, keeping intent and constraints; fix typos and drop filler — never a raw transcript quote, never a vague "asked for code" |
| Iteration | first attempt → correction → final decision |
| Verification | `**Validation:**` — commands run + pass/fail outcome before accepting |
| Engineering judgment | explicit **Rejected** entries with the architecture/isolation/test reason |

### Must avoid

| Anti-pattern | Fix |
|--------------|-----|
| Verbatim chat with typos or filler | summarize; keep the author's framing and constraints |
| Process chatter as an entry ("commit and start") | fold into the adjacent decision or state the substantive ask it represented |
| Justifying decisions by how the work will be judged | give the engineering reason (bisectable history, reviewable fixes, reproducibility) |
| No testing evidence | add the Validation block whenever code/config changed |
| No accept/reject reasoning | every entry needs Accepted/Changed/Rejected/Why |
| Log spam | one high-signal entry per decision, not every message |

## Inputs

- `ai-prompts/capture/sessions/*.md` (raw hook outputs)
- Existing curated target file (for continuity)
- `ai-prompts/README.md` authoring rules

## Required output style

For each curated entry:

```markdown
## P{n} — {title}

**Prompt:**
{polished summary of the ask — author's intent and constraints, no filler}

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
{the engineering reason this was the right (or wrong) call}
```

Omit **Validation** only for pure planning prompts with no runnable artifact —
still include **Context provided**.

## Curation checklist

1. Extract high-signal prompts only (skip repetitive noise).
2. Include user steering (constraints, corrections, "I caught X").
3. Preserve iteration: first attempt → refinement → decision.
4. Add **Context provided** — the record of what the tool was working from.
5. Add **Validation** — nothing marked Accepted without evidence.
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
- Do not write vague prompts — if a raw log is vague, check the transcript for
  the fuller user message, then summarize it faithfully.
- Do not mark Accepted without Validation when code/config was involved.
