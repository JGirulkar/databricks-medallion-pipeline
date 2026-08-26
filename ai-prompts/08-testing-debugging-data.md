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
