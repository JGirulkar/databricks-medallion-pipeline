# Documentation — Prompt History

How the project's documentation was produced and kept current. The companion
files record what was built; this one records how the documentation itself was
maintained.

---

## P1 — Capture raw, curate deliberately

**Prompt:**
"Capture every working session automatically, but keep the raw output
separate from the curated history — hooks write the session drafts, and the
numbered `ai-prompts/` files get refined from them, never pasted."

**AI response:**
A `stop` hook writes each session to `ai-prompts/capture/sessions/`; a
curation skill (`prompt-history-curation`) defines the entry format and
quality bar; the raw drafts stay untracked so only curated history is
committed.

**Accepted:** the two-stage flow — capture everything, publish decisions.

**Why:** raw transcripts are working evidence, not documentation. The
committed record carries the decisions and their reasons; the drafts stay
local.

---

## P2 — Docs follow the code, with supersession not rewriting

**Prompt:**
"The design has moved well past the original specs — do a truth pass: update
every living document to the design as built, but don't rewrite the dated
specs. They're point-in-time records; give them supersession notes instead."

**AI response:**
Living docs (README, design-notes, data-model, data-quality-strategy,
test-strategy, requirements-analysis) rewritten to current state; each stale
dated spec got a short design-evolution note pointing at what changed and
where the current design lives. One drift was found to be dangerous rather
than cosmetic — the reference DDL was missing a column the code writes — so it
gained a test that cross-references it against the schemas the code defines.

**Accepted:** the split between living documents and dated records; the drift
guard.

**Rejected:** rewriting dated specs in place, which would erase the design's
actual trajectory and desync the history that cites them.

**Why:** documents that can drift from code eventually will; the fix is either
derivation or a guard, never diligence.

---

## Reusable rules from this activity

| Rule | Origin |
|---|---|
| Capture raw, publish curated | the hook → numbered-files flow |
| Living docs get updated; dated docs get superseded | the truth pass |
| A reference doc without a guard is future drift | the schema.sql incident |
