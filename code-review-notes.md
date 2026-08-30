# Code Review Notes

How code was reviewed on this project, and what the reviews found. Review here
is not a single event but four mechanisms, in increasing strictness.

## The mechanisms

1. **Structured diff review per task.** During the bronze build, each
   implementation task's diff was reviewed as a unit before merging into the
   branch (the review briefs and diffs are preserved under
   `.superpowers/sdd/`). Findings were fixed in place before the next task
   started.
2. **The contract test as the standing reviewer.** For silver, the strongest
   review turned out not to be reading the diff but confronting it with
   independently derived expectations — the contract tier recomputes what the
   silver tables *must* contain from the generator output alone and fails on
   any disagreement. It caught what eyeballing had missed (non-deterministic
   survivorship) and, honestly, also caught defects in its own expectations
   three times.
3. **Data review after every cluster run.** Row-level checks against the
   tables — counts by category, orphan flags vs parent existence, key
   accounting — treated as part of review, not as monitoring. Several accepted
   changes were reopened this way (the double-counting parent refresh, the
   one-directional orphan healing).
4. **Review of the diff after the suite was already green (gold).** Five
   gold defects survived every test because each was true by the code's own
   construction — nothing a present-tense test could have been written to
   distrust. Found only by reading the change against its requirements
   before the next change built on top of it.

## Findings from the bronze, gold and dashboard passes

| Layer | Finding | Severity | Disposition |
|---|---|---|---|
| Bronze | `COMMON_METADATA_FIELDS` was a mutable list, unguarded against accidental mutation | correctness risk | changed to a tuple, **guarded by a test** asserting immutability |
| Bronze | A metadata contract test asserted against `fields[-5:]` — a hardcoded slice, not the named contract | fragile test | replaced with a test that derives the field set from `COMMON_METADATA_FIELDS` by name, so a schema change can't silently invalidate it |
| Bronze | `_ingest_timestamp` assertions compared datetimes directly, which happened to pass | latent bug | Spark converts UTC-aware literals to JVM local time; fixed with a timezone-safe comparison helper, caught by a test that failed the naive comparison first |
| Gold | A failure-path manifest row claimed a prior failure had been "fixed" | fabricated progress claim | the failure-path test only asserted the row existed, not that its numbers were honest; it now asserts the claim itself |
| Gold | A docstring promised a guard that doesn't exist | misleading | docstrings carry no test tier; the claim read as covered because a *different* schema guard happened to exist |
| Gold | The schema-drift guard itself had never been seen firing | untested guard | a guard passes identically whether it works or not until something actually breaks in front of it; proven only by deliberately breaking the reference file and not restoring it until the test process exited |
| Dashboard | A second y-series (orders) proposed for the revenue trend chart | rejected | scales differ by three orders of magnitude; the smaller series would flatline |
| Dashboard | A refresh schedule, a view layer, and index-equivalents were all considered before calling the layer done | reviewed, none built | freshness already inherits from the pipeline trigger, gold's tables are already the materialized/named-query layer, and the platform has no index equivalent worth adding at this size — the reasoning and the at-scale path are documented instead |

## Findings from the pre-PR review pass (silver hardening)

| Finding | Severity | Disposition |
|---|---|---|
| `conform_all.py` was a dead entrypoint: its job was removed from the workspace when per-entity triggers landed, and nothing invoked it since | dead code | **removed**, along with `run_conform_all` |
| `create_silver_tables.py` was a never-implemented stub carrying a `TODO: implement silver DQ` — misleading, since silver DQ is implemented (validation in `silver/validators.py` + `silver/checks.py`, DDL in `bootstrap_silver.py`) | misleading | **removed**; the README's layout mapping covers the naming |
| `database/schema.sql` had drifted from the code (missing `_is_orphan`) | doc drift | fixed earlier in the pass and **guarded by a test** so it cannot recur |
| `quality_check_result` is `PASS` on every silver row — rejection detail lives in quarantine, referential state in `_is_orphan` | accepted | kept for compatibility with the stated column contract; documented in design-notes |
| `refresh_orphan_flags` re-joins all flagged rows on every parent conform | accepted | cost is O(flagged), not O(table); correctness (both directions, idempotent) was chosen over incremental cleverness that failed twice |
| gold's `create_gold_tables.py` placeholder | accepted | the marker for the next phase; out of scope for this review |

## Review lessons recorded

- **Dead code lies.** Both removals were files that *described* behaviour the
  system no longer had. A reader trusts an entrypoint that exists; deleting it
  is documentation.
- **A reviewer that derives, beats a reviewer that reads.** Human review
  missed the missing survivorship tie-break for days; the contract test found
  it in its first run, because it asserted an outcome instead of inspecting
  intent.
- **Review the data too.** Two accepted-and-merged changes were wrong in ways
  only the tables showed. "Approved" is provisional until a real run's numbers
  add up.
- **Assert the contract by name, not by position.** `fields[-5:]` passes
  today and breaks silently the day the schema gains or loses a field;
  naming the fields makes the test defend the actual contract instead of
  its current shape.
- **Review also validates correct inaction.** The dashboard's
  no-schedule/no-views/no-indexes conclusion was a reviewed decision, not a
  skipped one — the reasoning is what makes "nothing to do here"
  trustworthy instead of just convenient.
- **Tests defend what you predicted; review defends what you didn't.**
  Every gold defect in the table above left the suite green, because each
  was true by the code's own construction, not a case any test had been
  written to distrust. "The suite is green" and "this is right" are
  different questions, and both verdicts are required.
