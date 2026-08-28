# Git, PRs & Review — Prompt History

> **Companions:** [`08-testing-debugging-data.md`](08-testing-debugging-data.md)
> (the verification method) · [`code-review-notes.md`](../code-review-notes.md)

How the repository's change discipline was set up and steered: branch and
commit conventions, the layered review model, and the calls made when a
review, the plan, and the code disagreed.

---

## P1 — One branch per layer, one PR per phase

**Prompt:**
"Each layer ships on its own branch cut fresh from main after the previous
one merges — bronze, silver, gold each get a full PR with everything that
phase touched: code, tests, docs, history. No long-lived branches, no
mixing phases."

**Context provided:**
- The phase gates (a layer is hardened — tests, docs, end-to-end evidence —
  before its PR opens)

**AI response:**
`cursor/bronze-layer`, `cursor/silver-layer`, `cursor/gold-layer`, each
started from the just-merged main; the PR body carries the phase's
verification evidence rather than a bare file list.

**Accepted:** Branch-per-layer; evidence-carrying PRs.

**Why:** A reviewer of a phase PR should be able to hold the whole phase in
their head — the branch boundary is the comprehension boundary.

---

## P2 — Commits are atomic and self-explaining, in red→green pairs

**Prompt:**
"Commit style: lowercase conventional subjects; the body states cause,
mechanism, and how it was verified. Where the work is test-first, the
failing test and the fix land as a readable pair. A commit message that
just restates the diff is a wasted message."

**Context provided:**
- The existing history's conventions from the earlier phases

**AI response:**
Applied throughout the gold branch — e.g. the runner-fix commit whose body
names the finding (a duplicated business predicate), the mechanism (one
constant, two interpolation sites), and the verification (rendered SQL
byte-identical, suite counts).

**Accepted:** The style, enforced at every commit on the branch.

**Why:** `git log` is the only documentation guaranteed to survive every
refactor.

---

## P3 — Layered review: every change reviewed, one whole-branch pass at the end

**Prompt:**
"Review happens twice, at two distances. Every task's diff gets a fresh
review against its requirements before the next task builds on it — spec
compliance and code quality both, with findings driven to fixed or
explicitly ruled on, never silently dropped. Then, before the PR, one
review of the entire branch as a single change, told which small findings
were deferred so it can decide what blocks the merge."

**Context provided:**
- The per-task requirement records and diffs
- The running list of deferred minor findings

**AI response:**
Seven per-change reviews plus a final whole-branch pass on the gold work.
The per-change reviews produced real findings (a duplicated predicate, a
weakened test, a dishonest failure record, an unproven guard); the final
pass triaged thirteen deferred minors — clearing all thirteen with reasons —
and found one item the earlier reviews had inherited without noticing
(P5 below).

**Validation:**
Every finding's disposition is recorded: fixed with the covering test named,
or ruled on with the reasoning written down. Zero findings closed without a
trace.

**Accepted:** The two-distance model.

**Rejected:** Reviewing only at the end — by then a task-level defect has
had six tasks built on top of it.

**Why:** A finding costs the least at the moment its diff is still the
newest thing on the branch.

---

## P4 — Commit metadata is part of the deliverable

**Prompt:**
"A review caught an auto-added attribution trailer in one commit message —
that violates this repo's authorship rules. Strip it with a message-only
amend, and sweep every commit on the branch so I know it was the only one."

**Context provided:**
- The flagged commit and the repo's authorship constraints

**AI response:**
Message-only amend (content untouched, verified by the unchanged tree
hash), then a trailer sweep across all branch commits — the flagged one was
the only occurrence. Later commits were checked at creation instead of
after the fact.

**Validation:**
`git log --format` sweep over the full branch range: zero trailers,
uniform author identity.

**Accepted:** The amend and the sweep; trailer checks folded into the
review checklist from then on.

**Why:** Commit messages ship with the repository exactly like code does —
hygiene rules that stop at the file boundary aren't hygiene rules.

---

## P5 — When a review contradicts the plan, the spec decides

**Prompt:**
"Some findings point at things the plan itself specified — the duplicated
predicate was written twice in the plan's own reference code, and one
'assertion' the plan mandated turned out to be tautological (it compared a
maximum against the column it was the maximum of). The plan doesn't get to
grade its own work: judge these against the design spec, fix what the spec
condemns, and write the ruling down."

**Context provided:**
- The findings labelled as plan-inherited
- The design spec as the binding authority over the plan

**AI response:**
Both plan-inherited defects were fixed against the plan's literal text: the
predicate collapsed to one constant (the spec's one-definition rule wins),
and the vacuous assertion deleted with a pointer to the ladder test that
actually covers the property. A third case went the other way — the final
review challenged documented figures that turned out to be exactly right
when traced to the run's emitted report, so the docs stood and the
challenge was answered with the source.

**Validation:**
Each ruling recorded with its reasoning; the affected suites re-run green
after each fix.

**Accepted:** Spec over plan, plan over convenience; verified numbers over
a reviewer's doubt.

**Why:** Every document in the chain — plan, review, even the spec — is
fallible; what keeps the process honest is that disagreements get resolved
against the most deliberate artifact, out loud.

---

## P6 — The merge bar: ready-with-fixes means fixes, then merge

**Prompt:**
"The whole-branch review came back 'ready with fixes' — one real item: a
docstring claiming a drift guard that doesn't exist for the manifest schema
copy. That's a false safety claim in code, our own rulebook calls it out.
Fix it honestly — either build the guard or reword to say the copies are
hand-aligned — plus the two trivial hygiene items, then push."

**Context provided:**
- The final review's findings and its triage of the deferred-minor list

**AI response:**
Chose the honest reword over a hurried guard (the guard is a known
follow-up, recorded as such), inlined a comment that pointed at a
working-directory file readers can't reach, and ignored local test litter
via `.gitignore`. Branch pushed only after the fix commit landed.

**Validation:**
The docstring now claims exactly what exists; the follow-up list carries
the guard.

**Accepted:** Honesty now, machinery later, with the later written down.

**Why:** "With fixes" is a contract, not a suggestion — and the cheapest
honest fix that keeps the docs truthful beats a rushed mechanism nobody
reviews properly.

---

## Reusable rules from this activity

| Rule | Origin |
|---|---|
| Branch boundary = comprehension boundary | branch-per-layer |
| The log outlives every doc — write commits for the reader at 2am | commit style |
| Review at two distances; findings end fixed or ruled, never dropped | the layered model |
| Commit metadata is deliverable | the trailer sweep |
| Disagreements resolve against the most deliberate artifact, out loud | plan-vs-review rulings |
| A false safety claim in code is a defect even when the code is right | the docstring finding |
