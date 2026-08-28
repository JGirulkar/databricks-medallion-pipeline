# Testing & Debugging — Prompt History

> **Companions:** [`05-silver-quality.md`](05-silver-quality.md) (the layer the
> method was forged on) · [`debugging-notes.md`](../debugging-notes.md) (every
> defect, organised by how it was found) · [`test-strategy.md`](../test-strategy.md)
> (the tiers and the scenario matrix)

This file records how the *verification method itself* was built and steered —
the prompts that changed how testing worked, rather than what any one test
covers.

---

## P1 — Establish the tiers and forbid silent skips

**Prompt:**
"Set up tiered tests — pure-Python unit, local Spark, and cluster-only — with
one runner for every job. And a skipped unit or Spark test counts as a defect,
not a pass."

**Context provided:** the uv workspace layout (one member per job), the local
Spark + Delta setup, and the repository's test markers.

**AI response:**
`run_job_tests.sh` running each job's suite in its own workspace member, with
`unit` / `spark` / `cluster` markers and a `--forbid-skips` gate.

**Validation:**
The gate itself was later found to be broken in two ways — the script had
unparseable line endings, and the skip check used a pytest flag that does not
exist — both found the first time the runner was actually executed, both fixed
and re-verified.

**Accepted:** the tier model and the anti-skip rule.

**Changed:** `--forbid-skips` reimplemented as a check on the pytest summary.

**Why:** a suite that silently skips its Spark tier reports green while testing
nothing — the gate has to fail loudly or it is decoration.

---

## P2 — "Test every scenario in one go"

**Prompt:**
"The sample data exercises every validation rule in one run — every bad-row
case present in the one generated dataset, so nothing needs a special fixture
to be provable."

**AI response:**
Turned the instruction into an executable invariant rather than a one-off
sweep: `test_dq_coverage.py` cross-references every rule the `dq_schema`
config declares against a map of generator scenarios, and fails in both
directions — a declared rule with no violating data, and a scenario naming a
rule nobody declares.

**Validation:** the gate went red naming 17 rules that could never have
failed; the 17 scenarios were added and it went green. Issue rows 585 → 725.

**Accepted:** the gate and the new scenarios.

**Rejected:** declaring `multiple_of` and `exclusive_maximum` just because the
validator supports them — neither has a natural meaning for these entities,
and an unexercisable rule is noise.

**Why:** a rule with no data that violates it reports a 100% pass rate
forever, which is indistinguishable from a rule that works.

---

## P3 — "This is too prolonged": the contract tier

**Prompt:**
"Stop the run-fix-run loop against the cluster. Trace the whole path
statically — generator output through to the silver tables — with the
expectations stated *before* running, each result matched against them, the
cause diagnosed for any mismatch, and regressions checked after every fix.
Use me for the hard calls, and drive it with unit tests."

**AI response:**
`test_pipeline_contract.py`: the real silver functions run over the real
generator output on local Spark, checked against expectations recomputed from
the input by hand — deliberately sharing no code with the implementation, so
the test cannot agree with a wrong implementation. About a minute per
iteration instead of twenty-five.

**Validation:**
Its first run found a genuine defect no cluster run had surfaced reliably:
survivorship was non-deterministic when duplicate keys carried different
values (no tie-break beyond batch and timestamp). It also exposed three
defects in the hand-written expectations themselves.

**Accepted:** expectations-first as the standing method; the cluster demoted
to final confirmation.

**Changed:** the expectations gained their own guard — a test that
cross-references them against every declared rule, so an omission fails as an
expectation error instead of masquerading as a code defect.

**Why:** every defect found on the cluster had been detectable locally first.
The method change, not any single fix, is what shortened the loop.

---

## P4 — Debugging discipline: hypothesis before fix

**Prompt (standing steer):** "Find the cause before touching anything — show
me current state, then the issue, then the proposed change. Never fix on a
guess."

**AI response (pattern across incidents):**
- The duplicate-key failure looked like two window functions disagreeing; a
  six-line diagnostic showed they agreed, and the real cause was the missing
  tie-break. The first theory would have changed working code and kept the bug.
- The double-task job failure was diagnosed by reading the Jobs API semantics
  (`update` merges the task array by key, so a rename is an add) rather than
  by deleting and recreating jobs — the first proposed workaround, discarded.
- The phantom quarantine counts were resolved by checking the *grain* of the
  query (which lineage axis) instead of deleting "polluted" rows.

**Validation:** each diagnosis carries its measurement in the commit that
fixes it — timings, row counts, or API output.

**Why:** a fix without a confirmed cause is a second bug with better manners.

---

## P5 — Asserting outcomes, not mechanisms

**Prompt (recurring steer):** "After every run, verify the *data* — how many
rows we generated, which issues were caught, in which layer, quarantined and
logged where — not whether the jobs went green."

**AI response:**
The end-to-end assertions were rebuilt around data invariants: every delivered
key is in silver or quarantine (checked per key, since the hash-gated merge
legitimately writes nothing on an unchanged re-delivery); no duplicate primary
keys; the orphan flag agrees with the data in both directions; per-category
quality counts scoped to the runs this execution produced.

**Validation:**
Three separate times a green run concealed wrong data — success with zero rows
admitted, a perfect pass rate from an unreachable check, and a passing run
with 38 mis-flagged rows whose assertion ("is anything still flagged?") was
satisfiable by one row out of thirty-nine. Each time the replacement assertion
would have failed immediately, and now does.

**Accepted:** state-vs-data invariants as the only assertion style for
pipeline verification.

**Why:** an assertion that tests the mechanism ("did it run") can pass while
the data is wrong; an assertion that relates state to the data it describes
cannot.

---

## P6 — The long gate must always report

**Prompt:**
"A 25-minute run just died on a transient API error and reported nothing but
a traceback — that's unacceptable. The e2e emits its report no matter what,
and we stop wasting runs."

**AI response:**
The runner owns its result object and emits the JSON report from an exception
handler with status `aborted`; only the *launch* calls retry (three attempts),
while polling and verification stay strict, because a failure there is a real
result about the pipeline rather than control-plane noise.

**Validation:** proven by forcing an early exception — the report still prints
and the exit code is non-zero.

**Why:** a gate that can end silently is not a gate; and retrying assertions
would convert real failures into flakes.

---

## P7 — A guard is not done until it has been seen firing

**Prompt:**
"The schema-drift guard went straight to green — the required failing run
was skipped. I don't accept that: a guard that has never been observed
failing is indistinguishable from a broken one, which is the exact lesson
the coverage gate taught us. Break the reference file on purpose, watch the
test fail with the right message, then restore."

**Context provided:**
- The drift test and the reference DDL it parses
- The repo's standing rule from the silver phase (every declared rule must
  be seen to fire on real data)

**AI response (and the trap inside it):**
The first two attempts at the proof defeated themselves the same way: the
test's fixture takes ten minutes to build the pipeline before the test body
reads the file, and the mutation was reverted while the fixture was still
building — so the test read the already-repaired file and passed. The
working procedure had to be stated as a sequence: mutate, run the test **to
process exit** touching nothing, capture the failure, only then restore.

**Validation:**
`AssertionError: gold.sales_by_product: schema.sql drift
{'total_revenue', 'total_revenuex'}` — the guard fired, named the right
table, and named both sides of the drift. File restored, tree verified
clean, and the green side was already on record from the full gate run.

**Accepted:** The live failure as the completion bar for any new guard.

**Rejected:** "The regex is obviously correct" as a substitute for watching
it fail — twice proposed, twice refused.

**Why:** Slow fixtures make it *feel* safe to clean up early; the test body
reads the world when it runs, not when it starts. The discipline is
mechanical: nothing is restored until the process exits.

---

## P8 — A claimed result without output is not a result

**Prompt:**
"Twice now a change came back with 'the full suite is running in the
background, expected to pass.' That is not evidence — a background claim
dies with the session that made it. From here: gates run in the foreground
to completion, and the report carries the real output, or the work isn't
done."

**Context provided:**
- Two changes whose full-gate runs had been reported as in-progress and
  never confirmed

**AI response:**
The gates were re-run to completion and the counts recorded (the standing
figures: gold 20 passed, fleet 160 passed, 0 skipped). Where a claim
couldn't wait, an independent confirmation run was made and its log kept
next to the change record.

**Validation:**
Both previously-unevidenced gates confirmed green — but only after the
re-runs; one of them had been claimed green for over an hour with no
output in existence.

**Accepted:** Foreground-to-completion as the reporting bar for any gate a
decision rests on.

**Why:** "Expected to pass" is a prediction wearing a result's clothes. The
suite that hasn't finished has found nothing yet.

---

## P9 — An assertion that can only see an empty world proves less than it reads

**Prompt:**
"The manifest test reads as append-proof — count before, run, count after —
but the fixture rebuilds the table per test, so 'before' is always zero and
the test degenerates to 'one write happened once'. Make it prove the thing
it names: run twice inside the test, show the ledger grew by exactly one
each time with distinct run ids."

**Context provided:**
- The fixture scoping change that had silently weakened the assertion
- What the production manifest actually experiences (appends onto an
  ever-growing table)

**AI response:**
The test now performs two consecutive runs and asserts growth of exactly
one row per run, distinct run ids, and the expected layer/status fields on
both rows — the append behaviour proven across runs rather than inferred
from a single write into a fresh table.

**Validation:**
Runner suite green (10 passed) with the strengthened test; the old version
would still have passed even if every run truncated the ledger first.

**Accepted:** The two-run form.

**Why:** Test setup decides what a test is able to observe. Isolation that
resets the world can quietly turn a durability check into a smoke test —
the fix is to create the history you claim to verify inside the test
itself.

---

## Reusable rules from this activity

| Rule | Origin |
|---|---|
| A green run is not evidence; the data is | three success-while-wrong incidents |
| A configured check with zero hits is a bug, not a clean bill of health | the unreachable uniqueness check |
| Expectations must be derived independently of the implementation — and guarded | the contract tier |
| When a test fails, "which side is wrong" is the first question | three expectation defects |
| Test the hypothesis before fixing it | the window-function theory that wasn't |
| Check a metric's grain before touching the data | the avoided quarantine DELETE |
| Long gates report always, retry only launches | the silent 25-minute failure |
| A new guard is finished when it has been SEEN failing | the drift-guard proof, self-defeated twice by early cleanup |
| A background "expected to pass" is a prediction, not a result | two unevidenced gate claims |
| Setup decides what an assertion can observe — reset worlds prove less | the weakened manifest append test |
| A claim's precision is bounded by its evidence | the "120 seconds apart" sentence that nobody measured |
