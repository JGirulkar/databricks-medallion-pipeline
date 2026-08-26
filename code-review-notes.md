# Code Review Notes

How code was reviewed on this project, and what the reviews found. Review here
is not a single event but three mechanisms, in increasing strictness.

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
