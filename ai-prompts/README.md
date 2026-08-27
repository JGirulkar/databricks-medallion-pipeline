# AI Prompt History

**Project:** Databricks Medallion Pipeline (DE C1 Assessment)
**Tooling:** agentic coding assistant + project-defined skills, rules, hooks
and MCP servers (see [tool-workflow.md](../tool-workflow.md))
**Profile:** `de-assessment-ce`

This directory is the record of how the pipeline was actually built with AI:
the asks, the proposals, what was verified, and why each proposal was
accepted, changed, or rejected. Prompts are curated summaries of the real
asks — intent and constraints preserved, chat noise removed — as raw session
transcripts are captured separately by hooks and refined here.

## How to read each entry

| Field | Meaning |
|-------|---------|
| **Prompt** | the ask, summarized faithfully — intent and constraints, no filler |
| **Context provided** | spec, rules, skills, files, and live state given to the assistant |
| **AI response** | what it proposed |
| **Validation** | commands/tests run and their outcomes before accepting |
| **Accepted** | kept as-is |
| **Changed** | modified after review |
| **Rejected** | declined, with the reason |
| **Why** | the engineering reasoning behind the decision |

## Activity index

| File | Activities |
|------|------------|
| [01-planning-and-requirements.md](01-planning-and-requirements.md) | requirements, spec, acceptance criteria |
| [02-tooling-rules-and-workflow.md](02-tooling-rules-and-workflow.md) | environment, skills, rules, hooks, MCP, isolation |
| [03-architecture-design.md](03-architecture-design.md) | high-level medallion architecture + anchor spec |
| [04-data-generation.md](04-data-generation.md) | sample CSV generator + intentional quality issues |
| [04-bronze-layer.md](04-bronze-layer.md) | bronze ingest, first end-to-end runs, manifest debugging |
| [05-silver-quality.md](05-silver-quality.md) | silver design and build, then the repair pass: quality checks, quarantine, orphan flags, CDC |
| [06-gold-aggregations.md](06-gold-aggregations.md) | gold tables + SQL *(next phase)* |
| [07-dashboard-and-visualization.md](07-dashboard-and-visualization.md) | dashboard tiles *(next phase)* |
| [08-testing-debugging-data.md](08-testing-debugging-data.md) | test strategy, debugging method, contract tier |
| [09-git-pr-and-review.md](09-git-pr-and-review.md) | commits, PRs, review |
| [10-assessment-documentation.md](10-assessment-documentation.md) | how the documentation itself was produced |

Mapping to the activity names used in the brief: data-generation →
`04-data-generation` · bronze-layer → `04-bronze-layer` · silver-layer →
`05-silver-quality` · gold-layer → `06` · dashboard → `07` · debugging →
`08` (with method detail in [debugging-notes.md](../debugging-notes.md)) ·
documentation → `10`.

## Omitted intentionally

- Repetitive environment commands with no new decision
- Chat mechanics (commit confirmations, handoff messages)
- Full pasted code blocks — the git history is the code record

## Authoring rules

1. **Raw → curated is mandatory.** Hooks write session drafts to
   [`capture/sessions/`](capture/README.md); those are refined into the
   numbered files. Raw dumps are never committed as history.
2. **Prompts are summaries, not transcripts.** Keep the author's framing,
   constraints and corrections; fix typos; drop filler and process chatter.
3. **Show steering.** Where the author caught something — an inefficiency, an
   unnecessary destructive step, a wrong default — the entry says so, because
   those moments changed the design.
4. **Accept/Change/Reject with reasons, every entry.** A history without
   rejections is not a history of decisions.
5. **Validation before acceptance.** When code or config changed, the entry
   records what was run and what it showed. Nothing is "Accepted" on the
   assistant's word.
6. **Why = engineering reasoning.** Entries justify decisions by their merits
   — correctness, reproducibility, cost, reviewability — never by how the
   work might be perceived.
7. **Keep private material out.** `.private/` and internal notes never enter
   the curated files.
