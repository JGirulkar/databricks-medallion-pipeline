# Debugging Notes

Organised by **how each defect was found**, not chronologically. The method is
the transferable part; the individual bugs are not.

The single most useful thing learned: **a green run is not evidence.** Three
times a job reported `success` while the data was wrong — jobs succeeded while
admitting zero rows, a check reported a 100% pass rate while being unreachable,
and an end-to-end run passed with 38 mis-flagged rows. In every case the
assertion tested the *mechanism* ("did it finish", "did the check run", "is
anything flagged") rather than the *outcome*.

---

## 1. Found by lint or by parsing the file

Cheapest class. All three had already reached the cluster.

| Defect | How | Why it escaped |
|---|---|---|
| `annotate_violations` and `write_quarantine` used, never imported | `ruff` reports `F821` in under a second | the deploy uploaded sources with no lint, no test and no import check |
| Test runner unparseable — CRLF line endings | `bash -n` | `.gitattributes` normalises on read, so `git status` called the file **unmodified** and a fresh clone worked |
| `SyntaxError` in a test module | pytest collection | it stopped the **whole** silver suite from collecting, not just that file |

**Rule:** when a script behaves impossibly, run `bash -n` and `file` on it
before debugging its logic. `git status` clean does not mean the file on disk
matches HEAD.

---

## 2. Found in the data, not the code

The highest-yield class. Each was invisible in code review and obvious in a
`GROUP BY`.

| Defect | The tell |
|---|---|
| Uniqueness check unreachable | **0** uniqueness violations among 509 quarantined rows, while the config declared the check |
| Per-check DQ report fabricated | completeness, uniqueness, type_logic and referential all reported **99.575%** |
| 50 NULL emails never caught | **0** `not_null` violations on `email` — the column was `nullable: true` with only a *format* rule |
| Uniqueness judged across the wrong window | 102,613 quarantined rows holding only **99,996 distinct keys** |
| Orphan healing clearing the wrong rows | 38 orders whose customer did not exist were flagged `_is_orphan = false` |
| Parent refresh double-counting | 47 quarantine rows under **each of two** `silver_run_id`s for one batch |
| Segment anchor dragged by one bad row | measuring the raw CSV put `as_of` at **2026-10-10** — ten months past the data's real end — and emptied three of four segments; one future-dated order row was the whole cause |

**Rule:** a configured check with zero hits is a bug, not a clean bill of
health. After a run, don't ask *"did it succeed?"* — ask *"do the numbers add
up?"*

---

## 3. Found only on the cluster

Two defects that no local test could reproduce, which is what a cluster run is
actually for.

- **`.cache()` is rejected on serverless.** `NOT_SUPPORTED_WITH_SERVERLESS`
  inside `foreachBatch`. Local Spark accepts it silently, so the entire local
  suite passed. Replaced with a single-pass aggregation — fewer passes than the
  cached version it replaced — and guarded by a test that scans the sources,
  since a serverless restriction is unreproducible locally by definition.

- **Serverless auto-optimization retries independently of `max_retries`.**
  `max_retries: 0` was set and is documented to mean never retry, covering
  `INTERNAL_ERROR`. Yet every failed task ran twice. Timing the attempts settled
  it: 45s, fail; 4s later, 31s, identical failure. The lever is the task's
  `disable_auto_optimization` flag. **A platform default can silently override
  explicit config** — when config and behaviour disagree, the config is not the
  whole story.

---

## 4. Found by writing the test before the fix

- **`process_conform_batch` had zero coverage.** Every collaborator around it
  was unit-tested; the function wiring them together was not, and the one test
  touching the orchestration path patched out the very function under test. A
  mock over the orchestrator hid the defect in the code it stood in for.

- **Soft delete always returned 0.** The first soft-delete test failed
  immediately: `missing` is a lazy plan over the target table filtered to
  `_is_deleted = false`, counted *after* the merge that flags those rows.
  Latent, since nothing consumed the return — but the manifest would have
  reported zero deletes forever.

- **Survivorship was non-deterministic.** Six rows carried `product_id = 1`
  with different values, all sharing a batch id and ingest timestamp. The
  `ORDER BY` had no further tie-break, so the winner was arbitrary and decided
  whether the key was admitted or rejected. The same input could produce
  different silver content run to run — which an integration test would show as
  noise, not as a bug.

---

## 5. Interactions introduced by an optimisation

Adding a skip-if-unchanged gate to the merge broke two things at once.

- `_is_orphan` was never **set** on rows already in silver: values unchanged →
  hash matched → merge skipped → the flag stayed NULL. Fixed with a null-safe
  comparison in both directions.
- `_bronze_batch_id` silently changed meaning, from *last delivered* to *last
  changed*, invalidating two E2E assertions that counted rows stamped with the
  current batch.

**Rule:** before adding a write-skip, list every column the write maintains and
ask what each one means when the write does not happen. Lineage columns and
state flags both stop being current, and only one of the two is obvious.

---

## 6. Defects in my own expectations

Worth recording because it is the most common failure while writing tests.

Three contract assertions failed and the **expectation** was wrong, not the
code: the hand-written rules omitted the datetime checks, so rows the pipeline
had correctly quarantined looked like rows it had wrongly dropped. A fourth
compared orders against the keys the CSV delivered, when the pipeline correctly
compares against the keys that reached **silver** — a quarantined parent
legitimately orphans its children.

Both are now guarded: a test cross-references every rule the schema declares
against the rules the expectations evaluate, in both directions.

**Rule:** when a test fails, "which side is wrong" is the *first* question, not
the last.

---

## 7. Diagnostic habits that paid off

- **Test the hypothesis before fixing it.** The duplicate-key failure looked
  like two window functions disagreeing. A six-line diagnostic showed they
  agreed, and the real cause was a missing tie-break. Fixing the first theory
  would have changed working code and left the bug in place.

- **Read the API docs rather than inferring semantics.** `jobs update` merges
  the task array *by `task_key`*, so renaming a task is an **add**: two
  identical tasks then raced for one checkpoint, which showed up as failures
  arriving in pairs. `jobs reset` overwrites while preserving `job_id` and run
  history.

- **Cascades hide behind small numbers.** Nulling a parent primary key in place
  looked like 3 rows of completeness defect; it orphaned 562 child rows against
  a spec of 30, because 100k orders over 500 products gives each parent ~200
  children.

- **A long gate must always report.** One transient `run-now` rejection killed a
  25-minute run and produced no output but a traceback. The report is now
  emitted from an `except` block with status `aborted`, and only the launch
  calls retry — polling and verification stay strict, because a failure there is
  a real result about the pipeline.

## 8. Found by review of the diff, not by any test

Gold's suites were green through all of these; each was caught by reading
the change against its requirements before the next change built on it.

| Defect | Why every test missed it |
|---|---|
| Qualifying predicate written twice (view + breakdown query) | both copies were identical, so every number agreed — the defect was the *future* divergence, which no present-tense test can see |
| An assertion that could never fail | `as_of` was computed as the max of a column, then the column was asserted `<= as_of` — vacuously true by construction, and green forever |
| Failure manifest claiming fixed progress | only exercised on the failure path, and the failure-path test asserted the row existed, not that its numbers were honest |
| A docstring promising a guard that doesn't exist | docstrings have no tier; the claim read as covered because a *different* schema guard exists |
| The drift guard itself, never seen firing | a guard passes identically whether it works or not until something breaks in front of it — proving it required breaking the reference file on purpose and **not restoring it until the test process exited** (a slow fixture makes early cleanup feel safe; the file is read when the test body runs, not at launch) |

**Rule:** tests defend against regressions you predicted; review defends
against the categories you didn't. Both verdicts are required per change —
"the suite is green" answers a different question than "is this right".
