# Gold Layer — Prompt History

> **Continues from:** [`05-silver-quality.md`](05-silver-quality.md)
> **Design spec:** `docs/superpowers/specs/2026-08-27-gold-layer-design.md`

Six decisions, each one closing a question the assessment brief leaves open:
how hard to work at freshness, where the code should live, what "revenue"
even means, when gold should run, how the segment ladder should read a
customer, and how to guarantee two tables can't quietly disagree.

---

## P1 — Per-aggregation eligibility, not one blanket call

**Prompt:**
"Don't make one incremental-vs-recompute call for all four tables — analyze
eligibility per aggregation. Where it's genuinely easy, use incremental;
recompute the rest, and put the effort into making the recompute itself
lean — partitioning and scan-side tuning, not maintenance algebra. Before
you commit to anything: do we have a guarantee that only new rows ever
arrive on the orders feed — no subtractions, no updates to rows already
delivered?"

**Context provided:**
- The silver layer's CDC guarantees as already built and tested (supersession
  on restated keys, in-place orphan-flag healing)
- CDF enabled on silver, so per-table change preimages exist
- The four gold table shapes agreed from the brief's required columns

**AI response:**
Ran the eligibility check per table: `daily_weekly_trends`,
`sales_by_product` and `revenue_by_customer` are additive (SUM/COUNT), so
incremental delta algebra is technically possible; `customer_segmentation`
is not — a single new order can move a customer between segments, and
Inactive changes with the passage of time even with zero new events, which
no change feed can express. Then answered the guarantee question directly:
no — silver's own contract allows a later delivery to supersede a
previously-landed key, and `refresh_orphan_flags` flips `_is_orphan` on
existing rows in place. Both are legitimate silver-layer writes that a
CDF-based incremental aggregate would have to reverse.

**Validation:**
Traced the two counter-examples against the actual silver code paths
(`split_validated_batch` for supersession, `refresh_orphan_flags` for
in-place flag flips) rather than asserting them from memory. Confirmed after
deployment: the gold manifest's `rows_read` (100,200) matches silver's exact
orders count on every run, and rerunning against unchanged silver reproduces
identical tables (`test_rerun_is_idempotent`).

**Accepted:** Full recompute for all four tables, with the per-table
eligibility analysis kept in the design record rather than discarded once
the answer came back "no" everywhere.

**Changed:** N/A.

**Rejected:** Incremental machinery — CDF-based delta algebra, per-table
checkpoints, exactly-once bookkeeping, a rebuild path for when the stateful
aggregate drifts — for `daily_weekly_trends`, `sales_by_product` and
`revenue_by_customer` even though they were technically eligible. Also
rejected: partitioning the four gold tables themselves — at a few hundred to
ten thousand rows each, that's the small-files anti-pattern; the lean
scan-side lever belongs on silver's layout, not gold's tiny output.

**Why:** The guarantee question is what actually decides the case —
"additive" is a necessary condition for incremental, not a sufficient one,
and the answer here was no even for the aggregation that looked easiest.

---

## P2 — Tables over materialized views; SQL files as the executed source

**Prompt:**
"Default to SQL here, not Python — reach for PySpark only if it shows a
major win over declarative SQL for this layer. And I want the SQL files the
brief's own layout asks for to be the thing the job actually runs, not a
set of docs that describe what the code does somewhere else."

**Context provided:**
- The brief's required repo layout, naming `src/gold/01…04.sql`
- Databricks materialized views as a candidate storage mechanism
- The existing pattern from silver: config-driven rules over hand-written
  imperative branches

**AI response:**
Checked whether PySpark bought anything over SQL for this layer and it
didn't — gold is pure declarative aggregation with no routing, merges or
loops, so SQL wins by the stated rule. Considered materialized views next:
rejected because MV refresh has no local-Spark equivalent (the contract-test
method that caught real defects in silver would have nothing to run
against), and a refresh doesn't go through `ops.pipeline_manifest`, breaking
the lineage story every other layer keeps. Landed on plain tables via
`CREATE OR REPLACE TABLE … AS SELECT`, with the four `.sql` files under
`sql/` as the code the runner loads and executes — not a parallel
description of it.

**Validation:**
Confirmed the contract tier runs the identical `.sql` files against local
Spark + Delta that the deployed job loads on the cluster — no second copy to
drift.

**Accepted:** Plain gold tables, full replace per run; the four SQL files as
the single executed source, substituted only for schema names and the
pinned constants.

**Changed:** N/A.

**Rejected:** PySpark as the default implementation language for this
layer. Materialized views, on both grounds — no test parity, manifest
bypass.

**Why:** A reference file the code doesn't execute is drift waiting to
happen — the `schema.sql` incident already taught this once, and the fix
here was making sure it couldn't happen a second time in gold.

---

## P3 — Revenue semantics: the doc-silence finding

**Prompt:**
"The brief never actually defines revenue — order_status shows up as a
schema column, that's it. So this is our call, and I want it made explicit
rather than assumed. Before you pick Completed-only: what about carrying all
three statuses into gold and letting the dashboard filter by status
instead?"

**Context provided:**
- The brief text itself, checked for every mention of "revenue" and
  "status" — neither co-occurs with a rule
- The four gold table grains as already agreed (one row per product /
  customer / day / segment)

**AI response:**
Confirmed the brief is silent — `order_status` exists only as a schema
column, and no acceptance criterion ties it to a revenue definition. Weighed
the filter-in-dashboard alternative and rejected it: carrying
Pending/Cancelled into gold would put status into every table's grain, a BI
filter can't re-run the segment ladder (segmentation isn't a filterable
view, it's a computed classification), and three filterable tables sitting
next to one baked table would stop cross-footing. Chose Completed-only,
defined once in a shared `qualifying_orders` view every SQL file reads, with
revenue as `total_amount` — the transactional record, never silently
recomputed from quantity times unit price.

**Validation:**
Checked that `total_amount` and `quantity × unit_price` reconcile for every
row that reaches silver in the generated data — the contract test
(`test_revenue_column_reconciles`) asserts this, which doubles as evidence
for the correctness acceptance criterion. The excluded volume (Pending,
Cancelled, orphaned, deleted) is logged into the manifest per run so the
haircut is auditable rather than a silent gap between silver's row count and
gold's.

**Accepted:** Completed-only, one global rule in one place; `total_amount`
as the revenue column; the excluded-rows breakdown in the manifest.

**Changed:** N/A.

**Rejected:** Carrying all three statuses into gold and filtering
downstream.

**Why:** An unstated rule left implicit across four separate SQL files would
drift the first time one of them was edited alone — writing it once as a
shared view is what keeps it from becoming four rules by accident.

---

## P4 — Trigger topology: staleness and cost, not just "wire it up"

**Prompt:**
"Three silver tables feed this and they won't update in lockstep — gold has
to stay fresh without re-running itself on every little dimension nudge.
Walk me through the trigger options before picking one: waiting on all
three sources, triggering off just the busiest one, cron, and the
debounce-based option."

**Context provided:**
- The silver trigger design (per-entity `table_update` triggers, already
  live)
- The observation that orders is the highest-volume, most frequently
  changing silver table

**AI response:**
Ran each option against the unaligned-arrival problem. `ALL_UPDATED`
starves: gold would wait on whichever source updates last, serving stale
revenue even when orders is already fresh. Triggering off orders alone
leaves a silent hole — a customer-segment correction or a category rename
with no accompanying order would never propagate. Cron is blind to data: it
runs on empty days and can sit stale for up to a full interval after a real
change. Landed on `table_update` across all three silver tables with
`ANY_UPDATED` and a 120-second debounce, so an unaligned delivery wave
coalesces into one or two runs instead of firing once per source.

**Validation:**
Registered the trigger in `scripts/ce_job_registry.py` and confirmed on the
deployed job that it fires on any of the three tables with the debounce
applied. The E2E's gold phase waits for the trigger-launched run itself
rather than invoking the job directly, which is what actually proves the
topology and not just the SQL underneath it — the run was never launched by
the harness, only awaited, and both runs it observed report `trigger:
TABLE`. The trigger fired autonomously twice across the two-wave E2E, one
run per wave, coalesced under the 120-second debounce rather than firing
once per source.

**Accepted:** `table_update`, `ANY_UPDATED`, 120s debounce, on all three
silver tables.

**Changed:** N/A.

**Rejected:** `ALL_UPDATED` (starves under unaligned cadences);
single-source triggering (dimension-only changes go dark); cron (blind to
data); chaining from each silver job directly (reintroduces the
orchestration coupling the stored-flag design in silver exists to remove).

**Why:** Because the referential verdict already lives on the silver row,
any combination of per-table snapshots gold reads is valid — which means the
trigger choice was purely a freshness/cost dial, never a correctness
question, and could be picked on those terms alone.

---

## P5 — The segment ladder, and the future-date incident

**Prompt:**
"Recency has to outrank value in this ladder — a customer who spent big but
hasn't ordered in three months is the one a retention team needs flagged,
not buried in High-Value. And anchor 'recent' to the data itself, not the
clock — a static dataset can't age against today's date without every run
producing a different answer for the same input."

**Context provided:**
- The measured per-customer lifetime-revenue distribution from the seed
  data (p90 ≈ 4,983)
- Silver's `order_date` window validation (min 2020-01-01, max today) as an
  existing guard

**AI response:**
Built the ladder Inactive-first: no qualifying order in the 90 days before
`as_of` beats every other test, including High-Value — so a lapsed big
spender reads Inactive, which is the actionable signal. Anchored `as_of` to
`MAX(order_date)` over the qualifying orders rather than `current_date()`,
and pinned the High-Value threshold at the round 5,000 from the measured
p90 instead of computing a percentile per run, which would silently move a
customer's segment as other customers' behavior changed. Then measured why
the anchor is safe: on the raw seed CSV, before silver's date-window check
runs, a single future-dated bad row drags `MAX(order_date)` from
2025-12-31 to 2026-10-10, which empties three of the four segments — the
data-anchored design leans entirely on silver's window guard already
quarantining that row.

**Validation:**
Reproduced the future-date failure directly against the raw CSV to measure
it, then confirmed against silver's validated output that the guard removes
the row and the anchor lands back on 2025-12-31. `test_as_of_is_data_anchored`
and `test_every_segment_is_reachable` guard both directions on the pinned
constants; measured on the deployed run: High-Value 379, Repeat 1,941,
One-Time 92, Inactive 7,598.

**Accepted:** Recency-first ladder; data-anchored `as_of`; fixed, pinned
thresholds over per-run percentiles.

**Changed:** N/A.

**Rejected:** Value-first ordering — it hides the win-back signal.
Percentile-per-run thresholds — unstable, and not statable ahead of a run.

**Why:** A dependency between layers is only safe if it's provable, not
assumed — measuring the future-date failure is what turned "silver's window
check protects this" from a claim into a demonstrated fact.

---

## P6 — Cross-footing by construction

**Prompt:**
"Segmentation should read off the customer table you already built, not go
back to silver and re-aggregate — I don't want the pie chart able to
disagree with the customer table by accident."

**Context provided:**
- The already-built `revenue_by_customer` table shape, including
  `last_order_date`
- The four-file execution order in the runner

**AI response:**
Rewrote `customer_segmentation` to select from `gold.revenue_by_customer`
instead of re-scanning `silver.orders` — the segment ladder now runs
entirely over the numbers `revenue_by_customer` already computed, so a
segment's customer count and revenue total can't drift from the customer
table by a second, slightly different aggregation. This makes ordering a
real constraint for the first time in this layer:
`02_revenue_by_customer.sql` has to run before
`04_customer_segmentation.sql`, stated in both file headers and enforced by
execution order in the runner.

**Validation:**
`test_gold_tables_cross_foot` asserts the four segment counts sum to the
customer table's row count and the four revenue totals sum to its total; the
schema-drift guard was proven live during this pass — mutating a column
name broke `test_schema_sql_matches_built_gold_tables` with
`AssertionError: gold.sales_by_product: schema.sql drift {'total_revenue',
'total_revenuex'}` — then the column was restored, and the manifest append
test was strengthened to prove the guarantee holds across repeated runs, not
just within one. Full suite green after:
`run_job_tests.sh --all --forbid-skips` — gold 20 (unit 6 · runner 4 ·
contract 9 · drift 1), 0 skipped.

**Accepted:** Segmentation derived from `revenue_by_customer`; the one
ordering constraint, documented rather than left implicit.

**Changed:** `customer_segmentation`'s source — from an independent
aggregation over `silver.orders` and `silver.customers` to a read of
`gold.revenue_by_customer`.

**Rejected:** Independently re-aggregating segmentation from silver, which
would have made cross-footing something a test had to prove rather than
something the design guaranteed.

**Why:** A guarantee built into the data flow survives every future edit; a
guarantee only a test checks survives until someone forgets to run it.

---

## P7 — One predicate, one place — including the metrics query

**Prompt:**
"The review of the runner flagged the qualifying rule written out twice —
once in the view, once in the input-breakdown query. That contradicts the
whole point of the shared view: if someone changes what qualifies, the
breakdown silently keeps counting the old rule. Collapse it to a single
constant that both statements interpolate."

**Context provided:**
- The review finding with both line references in `runner.py`
- The design rule the duplication violated (one stated definition,
  everything reads it)

**AI response:**
Introduced a module-level `QUALIFYING_PREDICATE` constant; both the
`qualifying_orders` view DDL and the breakdown's `COUNT_IF` interpolate it,
so a future change to the rule changes both readers or neither.

**Validation:**
The rendered SQL was compared byte-for-byte against the previous hardcoded
strings — identical output, so no behaviour change — then the runner suite
re-ran green (10 passed).

**Accepted:** The shared constant.

**Rejected:** Leaving the duplication because "the two strings are
identical today" — identical today is exactly how drift starts.

**Why:** The observability query is the first place anyone looks when the
numbers are questioned; it must be constitutionally incapable of applying a
different rule than the tables it describes.

---

## P8 — The manifest must tell the truth about failure

**Prompt:**
"Two honesty problems in the runner: a failed run writes a manifest row
claiming all four files were processed and zero rows read, whatever
actually happened — and the module docstring promises a failed run 'leaves
the previous version intact', which is only true per table. Fix the row to
record real progress, and reword the docstring: tables replace one at a
time, so a mid-run failure leaves a mix of new and old."

**Context provided:**
- The failure-path code and the docstring's atomicity claim
- How the replace loop actually executes (four sequential
  `CREATE OR REPLACE` statements, no cross-table transaction)

**AI response:**
The failure path now records how many files actually executed and the real
input count when the breakdown query completed before the failure; the
docstring states per-table atomicity and the mixed mid-run state
explicitly.

**Validation:**
Runner suite green after the change; the failure-path row's fields were
traced through the except path by review rather than left to the happy
path's assumptions.

**Accepted:** Honest progress fields; honest docstring.

**Why:** A manifest row is what someone debugs from at 2am. A row that says
"4 files, 0 rows" for a run that died on file one costs an hour of
confusion; a docstring promising atomicity the system doesn't have costs
more.

---

## P9 — The end-to-end proof: never launch gold, wait for it

**Prompt:**
"For the end-to-end run, the harness must never start the gold job itself —
the whole point is proving the table trigger fires on its own. Wait for a
run to appear after the silver wave, then keep re-checking the tables
against a live recompute from silver until they agree: timing is allowed to
delay the pass, but nothing may manufacture one. And restate the qualifying
rule literally in the harness SQL — if the check imported the deployed
definition, it couldn't catch the deployed definition being wrong."

**Context provided:**
- The deployed trigger config (all three silver tables, any-update, 120s
  coalescing window)
- The existing harness's state-vs-data verification style from the silver
  phase

**AI response:**
A converge-then-assert phase: record a cutoff before the delta wave, wait
for a gold run newer than it, then loop (bounded at ten minutes)
re-deriving every table from current silver with independently written SQL
until no run is pending and every invariant holds. The cutoff is
deliberately over-inclusive — a stale run picked up early just fails the
recompute and the loop keeps polling, whereas a too-late cutoff can hang a
paid run for its full timeout.

**Validation:**
Every verification query was exercised against the already-deployed tables
*before* the paid run, catching syntax problems for free. The real run:
two runs appeared, both confirmed trigger-launched (`trigger: TABLE` on
both run ids — the harness's code path provably never calls run-now for
gold), all ten invariants held, and the latest manifest row's input count
matched the live silver orders count exactly.

**Accepted:** Converge-then-assert; the literal restated predicate; the
over-inclusive cutoff with its reasoning written into the code comment.

**Rejected:** Launching gold from the harness "to make the test
deterministic" — that would test the SQL while silently skipping the
topology, which is the part a green local suite can't prove.

**Why:** The trigger firing is a claim about the deployed system, not about
the code; only the deployed system can prove it.

---

## P10 — Claims are bounded by their evidence

**Prompt:**
"Two precision problems in the write-ups. The trigger entry says the two
runs came '120 seconds apart, exactly as designed' — nobody measured that;
120 is the configured floor, and the observed gap was about 118. Say what
the evidence shows and nothing more. And the order-breakdown figures cited
in the test docs — pending, cancelled, orphan — verify them against the
run's own emitted numbers before they stand."

**Context provided:**
- The emitted end-to-end report (the breakdown block and both run
  timestamps)
- The exact sentences under challenge

**AI response:**
The trigger sentence now reads that the trigger fired autonomously twice,
one run per wave, coalesced under the 120-second window — a statement about
the mechanism, not a fabricated measurement. The breakdown figures checked
out exactly against the report (33,428 pending · 33,388 cancelled · 688
orphan · 0 deleted of 100,200 total), including the apparent mismatch with
the qualifying count — 232 rows are Completed *and* orphaned, so the
categories overlap and the arithmetic closes.

**Validation:**
Each challenged number traced to the report field it came from; the one
sentence that couldn't be traced was rewritten.

**Accepted:** The softened trigger claim; the breakdown figures, now
source-verified.

**Why:** A document that states one measured-sounding number nobody
measured teaches readers to distrust every number in it — precision has to
be earned per claim.
