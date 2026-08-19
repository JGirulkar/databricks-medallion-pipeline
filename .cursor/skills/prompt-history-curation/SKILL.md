---
name: prompt-history-curation
description: Build first-pass curated ai-prompts entries from raw hook session files while preserving Accepted/Changed/Rejected evidence and user steering.
---

# Prompt History Curation

Use this skill after one or more working sessions when `ai-prompts/capture/sessions/*.md` has new raw drafts.

## Goal

Generate a strong first draft for evaluator-facing files under `ai-prompts/01...10` with minimal manual rework.

## Inputs

- `ai-prompts/capture/sessions/*.md` (raw hook outputs)
- Existing curated target file (for continuity)
- `ai-prompts/README.md` authoring rules

## Required output style

For each curated entry:

```markdown
## P{n} — {title}

**Prompt:** ...
**AI response:** ...
**Accepted:** ...
**Changed:** ...
**Rejected:** ...
**Why:** ...
```

## Curation checklist

1. Extract only high-signal prompts/responses (skip repetitive noise).
2. Include user steering moments (constraints, corrections, optimization asks).
3. Preserve iteration narrative:
   - first attempt
   - correction/refinement
   - final decision
4. Capture model-choice signals when available in raw metadata.
5. Keep sensitive/internal strategy details out of curated output.
6. Ensure the target file remains readable and chronological.

## File routing

- Planning/setup intent -> `01-planning-and-requirements.md`
- Tool/plugin/hook/workflow mechanics -> `02-tooling-rules-and-workflow.md`
- Data generation decisions -> `03-data-generation.md`
- Bronze -> `04-bronze-ingestion.md`
- Silver -> `05-silver-quality.md`
- Gold -> `06-gold-aggregations.md`
- Dashboard -> `07-dashboard-and-visualization.md`
- Testing/debugging -> `08-testing-debugging-data.md`
- Git/PR/review -> `09-git-pr-and-review.md`
- Documentation/meta -> `10-assessment-documentation.md`

## Guardrails

- Do not copy raw files verbatim.
- Do not invent prompts or outcomes that are not evidenced by raw logs.
- Do not include `.private` or internal-only notes in curated files.
