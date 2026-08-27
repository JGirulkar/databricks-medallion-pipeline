# Documentation — Prompt History

How the project's documentation itself was produced and kept honest. The
companion files record what was built; this one records how the record was
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

**Why:** raw transcripts are evidence but not communication. Curation is where
noise is removed and reasoning is made explicit — and keeping the raw drafts
local means curation can be honest without being performative.

---

## P2 — Prompts as summaries, in my voice

**Prompt:**
"I don't want line-for-line transcripts in the history — chat messages carry
typos, filler and process chatter, and quoting them verbatim makes the record
weaker, not more authentic. Each prompt is a polished summary in my voice,
keeping my framing, constraints and corrections."

**AI response:**
All history files re-edited to the summary style; the curation skill and the
`ai-prompts/README.md` authoring rules updated so future entries are written
that way from the start. Pure process chatter (commit confirmations, handoff
messages) was folded into the decisions it belonged to or dropped.

**Accepted:** summaries with intent preserved; typo-free; no chat mechanics.

**Rejected:** verbatim quotes as a claim to authenticity — the decisions and
their reasons are the authentic record, not the keystrokes.

**Why:** the history is read by people who were not in the room. It should
carry what they need — the ask, the constraint, the reasoning — at the polish
of any other engineering document.

---

## P3 — Reasons must be engineering reasons

**Prompt:**
"Some entries justify decisions by how the work will be perceived instead of
why it was right — remove that everywhere. Every 'why' stands on engineering
merit."

**AI response:**
Swept the history and the authoring rules: justifications now cite
correctness, reproducibility, reviewability, cost — e.g. atomic red→green
commits because they keep history bisectable and each fix independently
reviewable.

**Accepted:** merit-based reasoning as an authoring rule, enforced by grep in
the curation pass.

**Why:** a record that argues for itself is advocacy; a record that explains
itself is documentation.

---

## P4 — Docs follow the code, with supersession not rewriting

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
| A prompt record is a summary of intent, not a transcript | the voice pass |
| Justify by merit, not by audience | the reasons sweep |
| Living docs get updated; dated docs get superseded | the truth pass |
| A reference doc without a guard is future drift | the schema.sql incident |
