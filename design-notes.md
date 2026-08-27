# Design Notes

The **current** design, as built and verified. The dated specs under
[`docs/superpowers/specs/`](docs/superpowers/specs/) are point-in-time records
of how the design was reached; where they disagree with this file, this file
is right and the spec carries a design-evolution note.

## Architecture overview

```
 generate_sample_data.py  (seed | delta)
        │  timestamped CSVs
        ▼
 /Volumes/de_assessment/landing/raw/{entity}/            LANDING
        │  Auto Loader (file-arrival trigger for orders,
        │  manual/scheduled for snapshots)
        ▼
 bronze.{customers,orders,products}                       BRONZE
        │  append-only, CDF enabled, _rescued_data,
        │  no validation, no dedup, no updates
        │  table_update triggers (all three fire in parallel)
        ▼
 silver.{customers,orders,products}                       SILVER
        │  streaming CDF consumption per entity,
        │  config-driven validation → three outcomes,
        │  survivorship, soft deletes, orphan flags + healing
        ▼
 gold.*  (next phase)                                     GOLD
        ▼
 SQL dashboard  (next phase)
```

Ten Databricks jobs (serverless), deployed by `scripts/deploy-all-ce-jobs.sh`
through the Jobs API — the same script locally and in CI, so the two cannot
drift. No asset bundle, no Terraform (removed deliberately; the bundle could
not run on this CLI and had drifted to describe jobs that did not exist).

## The layer contract

**Bronze lands what the source sent. Silver decides what it means.**

| | Bronze | Silver |
|---|---|---|
| rejects rows | never | permanent defects → quarantine |
| mutates rows | never (append-only, guarded by tests) | MERGE upserts, hash-gated |
| bad format | captured in `_rescued_data` | validated against `dq_schema` |
| duplicates | landed as delivered | survivorship; loser quarantined |
| deletes | never | soft delete (`_is_deleted`) from snapshot omission |
| referential | not its concern | flagged (`_is_orphan`), healed when parents arrive |

## Silver design — the parts that matter

**Config-driven validation.** Rules live in a `dq_schema` VARIANT column on
`config.source_config`, read at runtime: per-column rules (format, enum,
pattern, length, numeric bounds, date windows) and entity checks (not_null,
uniqueness, fk_exists). Adding a rule is a config change, not a code change.

**Three outcomes, not two.** Permanent defects are rejected to
`silver.quarantine` (full row + violations + lineage). Referential failures
are *temporal* — the parent may simply be late — so the row lands in silver
flagged `_is_orphan = true`. A key restated by a later delivery is superseded:
latest wins, nothing quarantined, bronze retains the history.

**Survivorship is deterministic.** Within a key: later delivery → later
business event date (`signup_date` / `order_date`) → ingest time → row hash.
The hash exists only so ties are reproducible; without it the same input could
put a key in silver on one run and in quarantine on the next.

**Orphan flags are derived, not event-driven.** `refresh_orphan_flags`
recomputes the flag from the data after any parent conforms — setting it when
a parent disappears, clearing it when the last missing parent arrives. Two
event-driven versions failed first: reacting to one parent's arrival wrongly
cleared rows whose *other* parent was still missing (38 rows), and clear-only
logic missed parents being withdrawn (624 rows). Deriving state from data is
the pattern that survived, and it is idempotent — which is exactly what makes
concurrent writers safe to retry.

**The merge skips unchanged rows.** `_row_hash` is derived from the entity
schema (never a hand-kept column list — one drifted) and gates the update;
escape clauses force a write for a returning soft-deleted key or a flag
change. Accepted consequence: `_bronze_batch_id` means "batch that last
*changed* this row", so batch accounting is done by key, not by row counts.

**Parallelism.** The three entities ingest and conform concurrently — the old
parents-before-orders ordering existed only for the abandoned quarantine-RI
design. The one write contention (`refresh_orphan_flags` from both parent
jobs targeting `silver.orders`) retries on a Delta conflict, recomputing from
the data on each attempt.

## Observability

One `ops.pipeline_manifest` row per entity per run, both layers (rows read /
written / quarantined, Delta versions, status, timings). `silver.dq_metrics`:
one row per check per entity per run, each with its own pass rate, computed
from which rows actually carry that violation. Quarantine rows carry both
lineage axes — `bronze_batch_id` (where the row came from) and `silver_run_id`
(which run rejected it). They answer different questions; conflating them once
inflated a report to 20,020 rows for a 10,010-row delivery.

## Deviations from the assessment template, and why

| Template | Here | Rationale |
|---|---|---|
| flag bad rows in-table via `quality_check_result` | quarantine table + in-table flags for referential | nothing is deleted, and quarantine preserves the violation detail an in-table flag cannot; `quality_check_result` still exists on every silver row |
| five numbered quality scripts | one config-driven validator | rules as data; the five checks are categories in `dq_schema`, not files |
| `src/…` layout | `databricks/jobs/…` uv workspace | packaging and testability; mapping table in the README |

## Debugging approach

Reproduce cheaply (the contract test runs the real silver path over the real
generator output against independently derived expectations), state the
expectation before looking, test the hypothesis before fixing it, then guard
the class (source-level tests for serverless restrictions, schema drift, and
layer boundaries). Full method and findings:
[`debugging-notes.md`](debugging-notes.md).

## Gold obligations (inherited by the next phase)

- filter `_is_orphan = false` and `_is_deleted = false`, or flagged rows leak
  into the aggregations
- `segment_type` in the customer-segmentation table is **derived**
  (High-Value / Repeat / One-Time / Inactive) — not the CSV's
  Premium/Standard/Basic column
- build all four aggregations: the assessment's structure names four files
  while its prose says three tables; four satisfies both readings
