# Reflection

## What I Built

A three-source e-commerce pipeline — customers, orders, products — through
bronze, silver, gold and a published dashboard, verified end to end on
Databricks Community Edition. Bronze lands what each source sends,
append-only, no validation. Silver reads its rules from config and turns
every failing row into one of three outcomes — reject to quarantine, flag as
orphaned and heal later once the parent shows up, or supersede on a later
delivery — never a delete. Gold rebuilds four aggregation tables per run
from one shared revenue/eligibility rule and a recency-first customer
segment ladder, launched by the silver tables' own commit trigger rather
than a schedule. A Sales Overview dashboard sits on gold, deployed the same
way as everything else here: a committed source file, published by a
script. 167 local tests plus one cluster end-to-end run — all ten
independently-recomputed gold invariants passing — back all four layers.

## How I Used AI (Across the Lifecycle)

Every layer started as a design conversation before a line of pipeline
code, not after. Silver's quarantine-over-delete call, the single VARIANT
config column, gold's six brief-silent decisions (what "revenue" even
means, how the segment ladder should treat a lapsed big spender) all got
argued out loud with alternatives on the table, and a reason written down
for the one that lost. One question did more work than any of them: before
picking an incremental-vs-recompute strategy for gold, I asked whether the
orders feed is guaranteed append-only. It isn't — silver can supersede a
restated key and flip an orphan flag in place — which killed an
incremental path that had looked easy right up until that question. From
there it was test-first — a red test standing for the missing behavior,
then the fix, landed as a pair. The 25-minute cluster runs never blocked
anything; they ran in the background while the next test or the day's docs
got written. And the last pass on every phase was a docs-truth pass — every
measured-sounding number had to trace back to something a run actually
emitted, not to what sounded right.

## What AI Helped With Most

The mechanical layers moved fast — Auto Loader wiring, the config-driven
validator, the generator's issue-injection — because none of that needed a
design call, just execution against an agreed spec. The bigger win was
structural: every catch became a permanent test or guard instead of a
one-off fix, so the same class of defect couldn't recur quietly. And the
contract tier — silver's real code, run over the real generator output,
checked against expectations worked out by hand from the input — cut the
verification loop from a 25-minute cluster round-trip to about a minute.
Every defect the cluster ever found had already been catchable there.

## What AI Got Wrong

Specifically, not just "it made mistakes":

- A deploy with no lint let two functions reach the cluster unimported
  (`annotate_violations`, `write_quarantine`) — `ruff` would have caught it
  in under a second, and now runs first, every time.
- Renaming a task under `jobs update` looked harmless and instead added a
  second task, because the API merges the task array by key. Two identical
  tasks then raced for one checkpoint, and failures started arriving in
  pairs — only explained by reading the API semantics instead of guessing.
- Orphan healing was wrong twice, in two different directions, before it
  was right: an event-driven version reacting to one parent's arrival
  wrongly cleared 38 rows whose other parent was still missing, and a
  clear-only version missed 624 rows whose parent had been withdrawn.
- Survivorship among duplicate keys was silently non-deterministic — six
  rows shared a batch id and an ingest timestamp with no further tie-break,
  so the same input could pick a different winner on different runs, which
  an integration test would have read as noise, not as a bug.
- Two defects in gold's SQL survived every green test because no test could
  see them: a qualifying predicate written out twice (once in a view, once
  in a breakdown query) that agreed today and would only diverge on a
  future edit to one copy, and an `as_of` assertion that checked a column
  against the max of itself — true by construction, green forever. Both
  were found by reading the diff against its requirements, not by running
  anything, which is the reason review and the test suite stayed two
  separate, both-required verdicts instead of one.

## How I Validated AI Output

Never on a green run by itself. Three separate times a run reported
`success` while the data was wrong: one admitted zero rows, one had a
configured check reporting a 100% pass rate while unreachable, one passed
with 38 mis-flagged rows because the assertion asked "did anything get
flagged" instead of "did the *right* rows get flagged." So every tier
asserts against the data — 160 fleet tests plus 7 dashboard structural
guards, `--forbid-skips` so a silently-skipped Spark test counts as a
defect, the contract tier's independently-derived expectations, and a
table-level SQL check after every cluster run. When a contract assertion
disagreed with the pipeline, the first question was always which side was
wrong — three times it was the expectations, not the code. A guard itself
isn't trusted until it has been watched fail: the schema-drift guard first
went green without ever being seen firing, and the first two attempts at
proving it self-defeated the same way — a slow fixture reverted my
deliberate mutation before the test body ever read the file. The fix was
mechanical: mutate, run to process exit touching nothing, capture the
failure, only then restore. And any claim that sounded like a measurement
had to trace back to a number a run actually emitted: a write-up once said
two gold runs landed "120 seconds apart, exactly as designed," and nobody
had measured that. The true, checkable statement was that the trigger's
120-second debounce coalesced both delivery waves — that is what the
report could actually support, and that is what stayed.

## What I Would Improve Next

The contract tier should have been the first thing built in silver, not the
thing that arrived after weeks of run-fix-run against the cluster — every
defect it later caught, it could have caught from day one. I'd also design
the orphan-flag model — derive the state from the data, rather than
reacting to individual events — before writing the first parent-refresh
job, instead of discovering the right shape after two event-driven
versions failed in opposite directions. And I'd budget cluster iterations
more deliberately from the start: a CE run is a shared, finite resource.

## Reusable Workflow

What I'd carry into any pipeline: an append-only raw layer curated
deliberately downstream, never inferred; a three-outcome quality model
(reject, flag, supersede) instead of a binary pass/fail, because a
referential failure is often timing, not fact; state derived from the data
rather than reacted to from events, so it's idempotent and safe for
concurrent writers to retry; and guards encoded as tests and hooks rather
than habits, so the gate runs whether or not anyone remembers to ask for
it. The one habit worth keeping above the rest: after a run, the first
question is never "did it succeed" — it's "do the numbers add up."
