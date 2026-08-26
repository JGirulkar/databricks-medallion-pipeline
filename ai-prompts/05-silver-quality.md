# Silver Layer — Prompt History

> **Continues from:** [`04-bronze-layer.md`](04-bronze-layer.md)
> **Companion:** [`08-testing-debugging-data.md`](08-testing-debugging-data.md) (the repair pass and its root causes)
> **Design spec:** `docs/superpowers/specs/2026-08-25-silver-layer-design.md`

Two phases, deliberately separated below. **P1–P10** are the design and build
pass: the architecture argued out before code. **P11–P16** are the repair pass,
after the first CE deploy failed — where the interesting evidence actually is,
because three of the defects passed every gate that existed and still produced
plausible numbers.

---

## P1 — Open silver chat: align on state before designing

**Prompt:**
"We've closed bronze and merged its PR — silver starts in this chat. Before we
design anything, align on the current state: go through the bronze design
spec, the implementation plan and the requirements doc, and reconcile them
against the codebase and what's actually deployed in the workspace. I want
silver starting from verified state, not assumptions."

**Context provided:**
- `docs/ASSESSMENT_FROM_PDF.md` — the four DQ checks, the ~700 intentional issue rows
- `docs/superpowers/specs/2026-08-20-medallion-bronze-architecture-design.md`
- `docs/superpowers/plans/2026-08-21-bronze-layer-implementation.md`
- The live CE workspace, not just the repo — deployed job list and existing bronze tables
- Superpowers `brainstorming` skill attached: design before code

**AI response:**
Read the specs and the deployed state, summarised what bronze already guarantees
(append-only, CDF enabled, `ops.pipeline_manifest` per run) and proposed silver
scope: incremental consumption of bronze CDF, validation, quarantine, merge.

**Validation:**
Deployed job list and bronze table row counts read from CE directly rather than
inferred from the repo.

**Accepted:** Reading live workspace state as part of context, not just files.

**Changed:** N/A (session opener).

**Rejected:** Designing silver from the repo alone.

**Why:** Persistent project context across sessions — bronze decisions carry
into silver, and the deployed state is the real starting point.

---

## P2 — Reuse a proven validator design, cut down to fit

**Prompt:**
"We're going for the full architecture in this chat. The mechanism: bronze
appends blindly with CDF on, so silver consumes only the rows added since its
last read; row hashes drive insert/update/delete; and validation runs in
silver against declared rules — take the shape of a proven production
validator, but build the lighter version of it."

**Context provided:**
- A production ingestion codebase as **read-only** inspiration, explicitly fenced:
  "do not edit anything, read only in this dir"
- Bronze's CDF-enabled tables as the incremental source

**AI response:**
Proposed a cut-down validator: column rules driven by config, entity-level
checks (not_null / uniqueness / fk_exists) evaluated separately, violations
accumulated into one array column rather than a boolean per rule.

**Validation:**
Compared the reference implementation's rule surface against what the assessment
actually requires — most of it was unnecessary for three entities.

**Accepted:** Config-driven column rules; violation array; CDF as the
incremental read mechanism.

**Changed:** Dropped the reference implementation's shadow/enforce dual mode
down to enforce only for silver (see P3).

**Rejected:** Copying the reference validator wholesale. The constraint given
was explicit: the concern was heavy code, more than heavy robustness.

**Why:** Reuse of a proven design, scoped deliberately, with the borrowed
repository never modified.

---

## P3 — Scope the validation mode, and push back on over-engineering

**Prompt:**
"Enforce-only in silver — shadow mode exists to protect live feeds during a
rollout, and we have no live feeds. Keep validation config-driven: extend the
existing config table with column-level rules and let one validation module
apply whatever's configured. And keep the code surface small — my concern is
heavy code, more than heavy robustness."

**AI response:**
Confirmed enforce-only is sufficient: shadow mode exists in the reference to
avoid breaking live feeds during rollout, which has no analogue here. Config
table extended with a per-source validation schema.

**Validation:** Traced what shadow mode protects against and confirmed it does
not apply to a greenfield pipeline.

**Accepted:** Enforce only; config-table-driven column validation.

**Changed:** N/A.

**Rejected:** Porting shadow mode — cost with no benefit at this scope.

**Why:** Explicit scope challenge from the user ("will it?"), answered with a
reason rather than a yes.

---

## P4 — Quarantine over delete, and one config column instead of many

**Prompt:**
"That was bronze — in silver we reject the bad rows, and quarantining beats
deleting them outright. Also: put the validation config in a single VARIANT
column on the existing config table — the config stays in one place, no new
tables for it."

**AI response:**
Adopted `silver.quarantine` as the sink for rejected rows, carrying the full
violation array plus run and batch identifiers. Config moved to a single
`dq_schema VARIANT` column on `config.source_config` rather than a new table or
a column-per-rule layout.

**Validation:**
Confirmed CE supports VARIANT and `parse_json`; added a legacy path
(`_ensure_dq_schema_column`) because CE lacks `ADD COLUMN IF NOT EXISTS`.

**Accepted:** Quarantine, not delete. One VARIANT column for the whole schema.

**Changed:** Bootstrap gained a column-add path for pre-existing config tables.

**Rejected:** A separate config table per rule type; hard deletes of bad rows.

**Why:** The assessment says "flag bad rows (don't delete)". Quarantine keeps
the row inspectable while keeping silver clean — a stricter reading than the
requirement, and the user's own call.

---

## P5 — Reject a state table: the metadata already exists

**Prompt:**
"Why do we need `silver.processing_state` when the streaming checkpoint
already records the last consumed version? A second cursor can only drift —
drop it. Trigger each silver job on its bronze table's update instead: three
thin per-entity entry points over one shared library, hash-compared merges,
each entity isolated so one failure can't take the others down."

**AI response:**
Dropped the proposed `silver.processing_state` table. Structured Streaming
checkpoints already record the last consumed CDF version, so a second copy of
that state could only drift. Adopted `table_update` triggers on the bronze
tables and three thin per-entity entry points over one shared library.

**Validation:**
Checkpoint directories confirmed under
`/Volumes/{catalog}/ops/checkpoints/silver/{entity}/`; trigger config verified
on the deployed jobs.

**Accepted:** No state table; checkpoints as the only cursor. Event-driven
`table_update` triggers. Per-entity jobs over a shared library.

**Changed:** Proposed design lost a table.

**Rejected:** `silver.processing_state`. Two sources of truth for one cursor.

**Why:** The user rejected a component on the grounds that the information
already existed elsewhere — the strongest kind of design pushback, and it
removed a whole class of drift bug.

---

## P6 — Challenge the FK requirement, then reverse on the evidence

**Prompt (challenge):**
"Delta doesn't error on an orphaned foreign key — so why are we treating this
specially? The core requirement stays: consume only the newly added bronze
rows, event-triggered, never re-reading the full table — exactly like
streaming."

**Prompt (reversal, same session):**
"The requirement does call for it — foreign keys must exist for every
customer_id and product_id — so we implement the check. Would one script
running the three entities in order be the cleaner way to guarantee parents
before children?"

**AI response:**
Confirmed Delta enforces no foreign keys, so orphans are not an engine-level
error — the check is a business rule, not a constraint. When the requirement was
re-read, implemented `fk_exists` as a left-anti join against the silver parent
tables, and ordered the entities `products → customers → orders` so parents are
current before orders are validated.

**Validation:**
Requirement text re-checked against `docs/ASSESSMENT_FROM_PDF.md`; ordering
covered by `test_orders_parent_refresh.py`.

**Accepted:** RI as an explicit check, plus parent-before-child ordering.

**Changed:** Position reversed mid-session once the requirement was confirmed.

**Rejected:** Skipping RI on the grounds that Delta does not enforce it.

**Why:** A documented reversal driven by re-reading the requirement, not by
being told. The distinction that survived — *engine constraint vs business
rule* — is the reusable part.

---

## P7 — One manifest for all layers, and migrate bronze onto it

**Prompt:**
"One manifest for all the layers, not a table per layer — a single place to
see what every layer ingested — and we migrate bronze onto it in this same
change, not as a follow-up. We don't need explicit i/u/d stamping in silver
either: CDF is on there too, so gold reads change types from the feed and
soft deletes ride the same mechanism. Home it in `ops` — weighed `config` and
a new `audit` schema, neither earns its keep."

**AI response:**
Single `ops.pipeline_manifest` with a `layer` discriminator, replacing
`ops.ingest_manifest`; bronze rewired to it in the same change so the two never
coexist. Explicit i/u/d stamping dropped: silver has CDF enabled, so gold can
read change types from the feed. Soft deletes via `_is_deleted` for snapshot
entities.

**Validation:**
Bronze ingest tests updated to assert `run_id` / `layer` instead of `batch_id`;
manifest rows verified per layer in CE.

**Accepted:** Unified manifest in `ops`; bronze migrated in the same pass; CDF
metadata instead of hand-stamped change types.

**Changed:** Bronze's existing manifest writes — not left as a follow-up.

**Rejected:** Per-layer manifest tables; a new `audit` schema when `ops`
already existed.

**Why:** The user insisted the migration was part of the change, not a
follow-up — the same lesson recorded earlier in this project about a consumer
bump belonging to the PR that causes it.

---

## P8 — Widen the validators, and widen the generator with them

**Prompt:**
"Add more validations while we're here — booleans, numeric min/max, string
rules, like the production set — and extend the data generator to produce
matching bad data for every new rule. And remember: the bronze jobs get
rewired to the new unified manifest in this same pass."

**AI response:**
Added string rules (length, pattern, format, enum), numeric bounds and datetime
window rules to the validator, and extended the generator with matching bad
rows: invalid emails, invalid enum values, negative prices, future signup dates,
non-positive quantities.

**Validation:** `test_validators.py`, `test_checks.py`; issue counts asserted in
`test_dq_spec.py`.

**Accepted:** Both halves in one pass — a rule and the data that violates it.

**Changed:** Generator issue counts rose from the assessment's baseline set.

**Rejected:** Adding validators without generating data that trips them.

**Why:** "Extend the data gen to add these sort of bad data too" is the whole
point — an unexercised rule is indistinguishable from a passing one. This
instinct became an enforced gate in P16.

---

## P9 — Spec approved; execute continuously with commits per task

**Prompt:**
"Spec looks right — commit it and keep going without pausing per step. Commit
at every meaningful slice, including the commits that fix errors: the repair
work should be as visible in history as the feature work."

**AI response:**
Committed the spec, then worked the implementation plan task by task with a
commit per task: package scaffold, validators, quarantine, metrics, manifest,
conform merge, CDF pipeline, bootstrap with seeds, bundle jobs, entry points.

**Validation:** 18 silver tests passing at the end of the build pass.

**Accepted:** Commit-per-task; continuous execution without pausing per step.

**Changed:** Bronze ingest failure test asserts `run_id`/`layer`.

**Rejected:** Pausing for approval between plan tasks.

**Why:** Commit granularity was a deliberate instruction: small commits keep history
bisectable and make each fix independently reviewable.

---

## P10 — Per-entity jobs replace the single conform_all

**Prompt:**
"Regenerate `schema.sql` and run the unit tier before anything deploys —
deploy and e2e only after that's green. And this work goes on a fresh branch
with its own PR."

**AI response:**
Split `conform_all` into three per-entity jobs, each triggered by its own
bronze table update, with the legacy job deleted on deploy. Updated
`database/schema.sql`, ran the unit tier, moved the work to a dedicated branch.

**Validation:** Unit tier run before deploy; `schema.sql` regenerated from the
actual DDL.

**Accepted:** Per-entity jobs; branch discipline; tests before deploy.

**Changed:** Orchestration model — one job became three plus a parent refresh.

**Rejected:** Deploying before the unit tier ran.

**Why:** The user gated deploy on tests. The gate was real; what it *could not
see* is P11.

---

## P11 — First CE deploy fails, and a job grew a second task

**Prompt:**
"The silver jobs are failing, and every one of them now has two tasks.
Root-cause it from the deployed job definitions backwards, not from the code —
I want the duplication explained before anything gets patched."

**Context provided:** The live CE workspace, the failing job runs, the
assessment requirements, and the repo.

**AI response:**
Every silver conform run was dying with
`NameError: name 'annotate_violations' is not defined` inside `foreachBatch`,
surfacing as `STREAM_FAILED` — 35 failed manifest rows against 13 successes.
`main.py` called `annotate_violations` and `write_quarantine` without importing
either. `ruff` reports both as `F821` in under a second.

The two tasks had a separate cause: `upsert_job` used `databricks jobs update`,
which **merges the tasks array by `task_key`**. Renaming `conform_orders` →
`orders` added the new key and kept the old one, so two identical tasks raced
for one CDF checkpoint — which is also why failures appeared in pairs.

**Validation:**
`databricks jobs get` on all three jobs showed both task keys;
`ruff check --select F821` reproduced the import defect locally in under a
second.

**Accepted:** Both diagnoses, each confirmed against the workspace.

**Changed:** The in-progress fix was a helper that detected stale task keys and
then *deleted and recreated* the job — discarded in favour of `jobs reset`,
which overwrites all settings while preserving `job_id` and run history.

**Rejected:** Delete-and-recreate. It treated the symptom, lost run history,
and contradicted the script's own no-delete contract.

**Why:** The general rule is worth more than the fix: **a rename against a
merge-by-key API is an add, not a rename.** `max_concurrent_runs: 1` gave no
protection — it caps concurrent runs of a job, not tasks within a run.

---

## P12 — "It cannot even drive the e2e tests" — the invisible defect

**Prompt:**
"The harness itself is broken — it can't even drive the e2e tests the way we
run them. Fix the harness first: debugging pipeline code behind an unrunnable
gate proves nothing."

**AI response:**
`databricks/scripts/run_job_tests.sh` had CRLF line endings in the working
tree, so bash could not parse it — `syntax error near unexpected token $'in\r'`,
preceded by `set: pipefail: invalid option name`. Because `.gitattributes`
declares `*.sh text eol=lf`, git normalises on read and reported the file as
**unmodified**. The committed blob was clean; a fresh clone worked. The runner
named in `AGENTS.md` and in the `layer-completion` skill had never executed on
this machine.

Two more defects sat behind it: `test_sink_metrics.py` had a `SyntaxError`, so
the whole silver suite failed at *collection*; and `PYSPARK_PYTHON` was unset,
so every spark-marked test died with `PYTHON_VERSION_MISMATCH` (worker 3.12,
driver 3.11) surfacing as a misleading `TASK_WRITE_FAILED` from Delta.

**Validation:**
`bash -n` on the runner; `pytest` collection error; suite went from
"cannot run" to **93 passing, 0 skipped** across the three layers.

**Accepted:** All three fixes, each as its own commit.

**Changed:** `--forbid-skips` was invalid bash *and* not a real pytest flag —
reimplemented as a check on the pytest summary, so the anti-silent-skip gate in
`test-strategy.md` actually works.

**Rejected:** Debugging pipeline logic while the test harness could not run.

**Why:** **`git status` clean does not mean the file on disk matches HEAD.**
Attribute normalisation compares normalised content. When a script behaves
impossibly, check `bash -n` and `file` before suspecting its logic.

---

## P13 — Commits must show the repair, not just the result

**Prompt:**
"Commit every repair as it lands — the failing test first, then the fix that
turns it green. The fix sequence stays in history as atomic, reviewable
steps; nothing gets squashed away after the fact."

**AI response:**
Committed the repair as an atomic sequence rather than one lump, with an
explicit red→green pair: `test(silver): cover the process_conform_batch wiring`
fails with the exact cluster `NameError`, then
`fix(silver): import annotate_violations and write_quarantine` turns it green.
Each message records cause, mechanism and verification.

**Validation:** Every commit body names the command and the outcome that
proves it.

**Accepted:** Atomic commits; red committed before green.

**Changed:** Commit granularity — one lump became nine.

**Rejected:** Squashing the repair into a single commit, which would have
hidden the repair sequence the history is meant to carry.

**Why:** `accepting → testing → fixing → refining` is only visible if the
history is shaped that way on purpose.

---

## P14 — Why the wiring had no test

**AI response (unprompted finding):**
`process_conform_batch` is the function the CDF stream actually calls, and it
had **zero** coverage. Every collaborator around it was unit-tested in
isolation, and the one test touching the orchestration path
(`test_orders_parent_refresh.py`) patches out `run_entity_conform` entirely — so
nothing ever executed the wiring. A mock over the orchestrator hid the defect in
the very code it stood in for.

**Validation:**
New `test_process_conform_batch.py` exercises the real chain — conform, column
rules, entity checks, merge, quarantine, metrics — mocking only config reads and
table-name resolvers. It reproduced the cluster failure locally in seconds.

**Accepted:** Integration-level coverage of the wiring, not just the units.

**Rejected:** Adding a mock-based test that would have passed against broken
code.

**Why:** Unit coverage of every part does not cover the assembly. The gate that
let this through was `layer-completion` step 5 — *"CE run — optional smoke"*.
That one word is where a `NameError` reached the cluster.

---

## P15 — Two defects that passed every gate

**AI response (unprompted findings):**

**Uniqueness was unreachable code.** `process_conform_batch` ran conform first,
whose `_latest_per_pk` collapsed duplicates with `row_number() == 1`. By the
time `_apply_uniqueness` ran, every key was unique, so
`count(*) over (partition by pk) > 1` was never true. Evidence from CE: **zero**
uniqueness violations across 509 quarantined rows, while `config.source_config`
declared uniqueness checks on `customers.customer_id` and `orders.order_id`.
Duplicates were silently dropped — the requirement says flag them.

**The DQ report was fabricated per check.** `_category_metrics` counted the
batch once and reused the same three numbers for all four categories, so one
orders run reported completeness, uniqueness, type_logic and referential all at
99.575%.

**A third, found the same way:** `customers.email` was `nullable: True` with
only a format rule, so all 50 intentionally-NULL emails passed silently — the
customers quarantine held zero `not_null` violations on email, against an
assessment that names email as a critical completeness field.

**Validation:** All three found by querying the *data*, not by reading code —
a declared check with zero violations is the signal.

**Accepted:** Validate before survivorship; count each category from the rows
whose violations contain it; add a `not_null` entity check on email.

**Changed:** `conform_snapshot_batch` / `conform_incremental_batch` replaced by
`split_validated_batch`, which ranks within the key *after* tagging. The
surviving row of a duplicate group is admitted; the losers are quarantined
carrying their uniqueness violation, so every bronze row is now either in silver
or in quarantine.

**Rejected:** Quarantining both rows of a duplicate pair, which would have lost
legitimate data.

**Why:** These are worse than the crash. A crash announces itself; these
produced confident, plausible, wrong numbers and passed every gate.

---

## P16 — "Test every scenario in one go"

**Prompt:**
"The generator itself carries every bad-row case we validate for — one run
exercises the entire validator surface. No scenario should depend on a
hand-built fixture to be provable."

**AI response:**
Turned the instruction into an executable invariant rather than a one-off sweep.
`test_dq_coverage.py` enumerates every rule the `dq_schema` seed declares,
cross-references a coverage map, and **fails in both directions**: a declared
rule with no bad-row scenario, and a mapped scenario the generator does not
implement. It went red naming 17 missing scenarios, which were then added.

**Validation:**
Coverage gate red with 17 named gaps → green. Suites: data_generation 8,
silver 32, bronze 59 — all with `--forbid-skips`.

**Accepted:** The gate, plus 17 new scenarios covering min/max length, pattern,
numeric bounds, exclusive minimum, and the datetime window.

**Changed:** Intentional issue rows 585 → 725. The set the assessment names
explicitly is unchanged and still separately auditable; additions are grouped
under "extended coverage".

**Rejected:** `multiple_of` and `exclusive_maximum` — declared nowhere and
recorded as deliberate, since neither has a natural meaning for these three
entities. Declaring them would be validator theatre.

**Why:** A rule with no data that violates it reports 100% pass forever. The
instinct from P8 became a gate that cannot silently regress — which is the
difference between catching this once and catching it always.

---

## P17 — "This is too prolonged": switch from run-fix-run to expectations-first

**Prompt:**
"This is too prolonged. Before the next e2e, trace the whole path from the
generator to the silver landing tables statically and eliminate everything
catchable up front. Every test states what it checks and what it expects;
match the results against those expectations — met, or met for the wrong
reason; diagnose the cause of any miss; and after each fix, check nothing
else broke. Keep me in the loop for the hard calls, and drive it with unit
tests."

**AI response:**
Stopped the cluster loop and built a contract tier: the real silver path run
over the real generator output on local Spark, checked against expectations
recomputed independently from the input. One minute per iteration instead of
twenty-five.

**Validation:**
The contract test immediately found a defect no cluster run had surfaced
reliably — survivorship was non-deterministic when duplicate keys carried
different values — and exposed three defects in the hand-written expectations
themselves, now guarded by a rule-coverage cross-check.

**Accepted:** Expectations-first as the standing method; E2E demoted to the
final confirmation, not the discovery tool.

**Changed:** The whole verification workflow, mid-project.

**Rejected:** Continuing to use 25-minute cluster runs as the primary debugger.

**Why:** The instruction reset the economics: every defect found on the
cluster so far had been detectable locally first. Directing the *method* rather
than the individual fix is what changed the trajectory.

---

## P18 — Pin the layer boundary: bronze rejects nothing

**Prompt:**
"Nothing gets rejected or deleted in bronze — badly formatted data goes to
the rescue column, that's it. All validation, soft deletes and updates happen
in silver."

**AI response:**
Verified the requirement already held — the only bronze filters are read-only
metrics, every write is `mode("append")`, `rescuedDataColumn` is configured,
and a write failure re-raises rather than advancing the checkpoint — then
encoded it as source-level guards so it cannot silently regress: no merge,
delete, update, overwrite, de-duplication, `DROPMALFORMED`, `FAILFAST`, or
silver-layer column anywhere in bronze.

**Validation:** `test_bronze_is_append_only.py`, three guards, green.

**Accepted:** The invariant as a test, not a review note.

**Rejected:** Leaving the boundary as a convention someone could quietly break.

**Why:** A one-time grep proves today; a guard proves every commit after it.

---

## P19 — "Why delete? Don't we have a lineage chain?"

**Prompt:**
"Why delete? We have a lineage chain — query the quarantined rows for the
batch instead of destroying history."

**AI response (the proposal being challenged was mine):**
The quarantine metric looked polluted by earlier broken runs and a cleanup
DELETE was proposed. The question was right: quarantine carries two lineage
columns answering different questions — `bronze_batch_id` (where a row came
from) and `silver_run_id` (which run rejected it) — and the metric had been
scoped by the wrong one, summing every silver run that ever touched a batch.
Measured: 20,020 rows by bronze batch vs 10,010 by a single silver run, for a
10,010-row delivery.

**Validation:** The breakdown now walks manifest → `silver_run_id` →
quarantine; the phantom `uniqueness: 10,215` became the true 48.

**Accepted:** Fix the query's grain; keep every historical row.

**Rejected:** The DELETE. A data-quality table you prune to fix a metric is a
table you can no longer audit.

**Why:** When a metric looks wrong, check the grain before touching the data.
A destructive fix was converted into a one-line query change by asking what
the lineage already provided.

---

## P20 — "Why sequential, when all three can trigger simultaneously?"

**Prompt:**
"Why is this sequential? The three entities are independent — fire all three
triggers simultaneously."

**AI response:**
The sequencing was a leftover: the old design needed parents conformed before
orders' FK check. The orphan-flag redesign had already removed that dependency
— arrival order no longer changes the outcome, only when healing happens — so
the constraint being enforced no longer existed. Before flipping it, the write
contention was mapped: bronze and silver entity tables are disjoint, the shared
sinks are Delta appends, and the one real conflict is `refresh_orphan_flags`
(every parent's job writes to `silver.orders`), which now retries on a Delta
concurrency conflict, recomputing from the data each attempt.

**Validation:** Unit suite green; the parallel wave's first live run launched
all three ingests back-to-back.

**Accepted:** Parallel triggers, with the retry as the concurrency guard.

**Rejected:** Keeping an ordering whose reason had been deleted.

**Why:** A constraint should be re-examined when the design that created it
changes. The catch also relied on the retry being safe to add at all — the
flag refresh is idempotent (it writes only rows whose flag disagrees), which is
what makes recompute-and-retry correct under contention.

---

## Working rhythm worth recording

Cluster runs take ~25 minutes, and none of that time was spent waiting. Each
E2E ran as a background task while the foreground continued — writing the
debugging notes and data-quality strategy during one run, rebuilding this
prompt history during another, hardening docs during a third. The long gate
was also made safe to background: it now emits its JSON report even when a
step throws, so an unattended run can never end silently.

The division of labour that emerged: the human catches inefficiencies and
challenges destructive or unnecessary work (P17, P19, P20 — pacing, the
avoided DELETE, the parallelism); the assistant executes, measures, and turns
each catch into a test or a guard so it holds permanently.

---

## P21 — The closing run: parallel wave, every invariant green

**Prompt:** (the standing instruction) "Harden everything through silver,
prove it end to end, then we raise the PR."

**AI response:**
Final E2E on Databricks CE — two deliveries, all three entities triggered in
parallel — `status: success`, zero errors:

| Invariant | Result |
|---|---|
| every delivered key in silver or quarantine | `unaccounted_keys: 0 / 0 / 0` |
| duplicate primary keys in silver | 0 across all three tables |
| INSERT — new order keys | 500, ids disjoint from the seed batch |
| UPDATE — hash-gated merge | 30 rows rewritten of 10,015 delivered |
| DELETE — snapshot omission | 3 products soft-deleted (5 / 10 cumulative) |
| orphan flag vs data | wrongly_cleared 0 · wrongly_set 0 · NULL 0 |
| orphans genuinely waiting | 688 — healing distinguishable from not-checking |
| quality report (this run's scope) | completeness 363 · type_logic 247 · uniqueness 48 |

**Validation:** all numbers read from the tables, not the run status; 139
local tests green (`--forbid-skips`); ruff clean.

**Accepted:** silver is complete. The layer gate — a real end-to-end run must
pass — is satisfied with assertions that compare state to the data it
describes, in both directions.

**Why:** this table is what "fully functional" means here: not that ten jobs
went green, but that every row the source delivered is accounted for and every
flag agrees with the data.

---

## Reusable lessons from this layer

| Lesson | Where it came from |
|---|---|
| A rename against a merge-by-key API is an **add** | P11 — two tasks per job |
| `git status` clean ≠ file on disk matches HEAD | P12 — CRLF, invisible to git |
| A mock over the orchestrator hides the wiring it stands for | P14 — zero coverage on the real entry point |
| Order of operations *is* correctness — dedup before validate silently deletes evidence | P15 — unreachable uniqueness check |
| A declared rule with no violating data reports 100% pass forever | P16 — the coverage gate |
| Find dead checks in the **data**: a configured check with zero hits | P15 — 0 uniqueness rows in 509 |
| An unread config change fails quietly, not loudly | P5, P15, and the bootstrap gate in `08` |
| Check the metric's grain before touching the data | P19 — the avoided DELETE |
| Re-examine a constraint when the design that created it changes | P20 — parallel triggers |
| Fix the method, not the instance, when iteration is the bottleneck | P17 — the contract tier |
