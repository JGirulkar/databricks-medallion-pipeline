# Final AI Usage Summary

## Where AI sat in the workflow

The assistant worked inside a harness built for this project, not on its
own judgment: nine project-authored skills, ten always-on rules, five
lifecycle hooks, and two MCP servers pinned to the assessment profile and
account. Specs and gates decided what "done" meant — a dated design doc, a
phase's acceptance criteria, a test tier's pass bar — and the assistant
executed against them, measured the result, and reported back. Design ran
as a dialogue before code; code ran test-first; long gates ran in the
background rather than blocking the next task. That structure did real
work at least once: the Databricks and GitHub MCP servers were pinned to
the assessment profile and account in configuration, not left to
discipline, so a request touching the wrong workspace or the wrong GitHub
account had no path to succeed rather than a rule to remember. Full
inventory: [tool-workflow.md](tool-workflow.md).

## By lifecycle stage

- **Requirements** — the assessment PDF was extracted into a living
  reference doc, re-audited at every phase gate; the audit caught a real
  contradiction in the brief (gold: "all 4 aggregations" vs "three tables")
  and a stale reference schema.
- **Design** — dialogue with explicit alternatives, each accepted or
  rejected with a written reason, before any code — silver's quarantine
  model, gold's six brief-silent calls.
- **Code** — written against the agreed spec; atomic commits, red tests
  before green fixes, so iteration is visible in the log.
- **Testing** — tiered (unit, local-Spark, contract, cluster E2E), scenario
  coverage enforced mechanically rather than hoped for.
- **Debugging** — reproduce cheaply, state the hypothesis, test it before
  touching code, then guard the class so it can't recur silently.
- **Data quality** — rules declared as config, enforced in silver, one
  violating scenario generated per declared rule.
- **Documentation** — written, then checked: every measured-sounding number
  had to trace back to a run's own emitted output before it was allowed to
  stand.

## The division of labour that emerged

I steered method and caught waste; the assistant executed, measured, and
turned each catch into something permanent. Four concrete examples: moving
verification off a 25-minute cluster loop onto a 1-minute contract test
once the loop itself became the problem; stopping an unnecessary quarantine
DELETE by pointing at the lineage chain the table already carried instead
of pruning history to fix a metric; removing sequential job triggers whose
ordering constraint the new orphan-flag design had already made obsolete;
and pinning the bronze/silver boundary — bronze rejects, mutates and
deduplicates nothing — as a source-level guard instead of a review note,
because a one-time check proves today and a guard proves every commit after
it. Elsewhere I pushed back on scope the assistant volunteered: declaring
validator rules the entities had no natural use for, and incremental
aggregation machinery for gold tables whose upstream guarantee (no restated
or in-place-flipped rows) didn't actually hold it up. And I caught a
written claim that outran its evidence — two gold runs "120 seconds apart,
exactly as designed" — that nobody had measured; the debounce window was
120 seconds, the observed gap was about 118.

## Numbers

167 local tests (160 fleet across data_generation/bronze/silver/gold plus 7
dashboard structural guards), one cluster end-to-end run on Databricks CE
with all ten independently-recomputed gold invariants passing, and 11
prompt-history files (`ai-prompts/01`–`10`, two files under `04`) carrying
103 individually dispositioned decisions — each accepted, changed, or
rejected with a stated reason. Silver alone carries 21 of them; the
shortest file carries 2.

## The one thing I'd tell someone starting this

The catches that mattered most weren't bug catches — they were scope
catches. Left alone, the assistant would have shipped validator rules the
entities had no use for, and incremental-aggregation machinery for a
guarantee (no restated or in-place-flipped rows) that didn't hold. An
assistant that executes well keeps executing even on work nobody asked
for; the discipline that pays isn't just verifying what got built, it's
deciding what's not worth building in the first place.
