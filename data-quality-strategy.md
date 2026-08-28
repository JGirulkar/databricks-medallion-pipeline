# Data Quality Strategy

## The rule that shapes everything else

**Bronze lands what the source sent. Silver decides what it means.**

Bronze appends every row it reads, unchanged, and captures content it cannot
parse in `_rescued_data`. It rejects nothing and de-duplicates nothing. If
bronze cleaned, there would be no record of what the source actually delivered,
and no way to show a downstream number came from the data rather than from the
ingest. Every check below therefore runs in silver.

`databricks/jobs/bronze/tests/test_bronze_is_append_only.py` enforces this: it
fails if a merge, delete, update, overwrite, de-duplication, `DROPMALFORMED`,
`FAILFAST` or any silver-layer column appears anywhere in the bronze sources.

## The four checks

Checks are declared as data, in a `dq_schema` VARIANT column on
`config.source_config`, and read at runtime. Adding a rule is a config change,
not a code change.

| Category | What it asks | Where it is declared |
|---|---|---|
| **completeness** | is a required value present | `not_null` entity checks |
| **uniqueness** | is this key repeated inside one delivery | `uniqueness` entity checks |
| **referential** | does the parent this row points at exist | `fk_exists` entity checks |
| **type_logic** | is the value well-formed and in range | per-column `validation` rules |

`type_logic` covers email format, enum membership, string length bounds, a
country pattern, numeric minimum / maximum / exclusive-minimum, and a
`min_date` / `max_date` window.

## Three outcomes, not two

A failed check does not always mean a bad row, and this is the distinction the
design turns on.

| Outcome | Which failures | Where the row goes |
|---|---|---|
| **Rejected** | completeness, type_logic — the value is wrong | `silver.quarantine`, never in the entity table |
| **Flagged** | referential — the parent may simply be late | the entity table, with `_is_orphan = true` |
| **Superseded** | a later delivery restates the same key | dropped from silver; bronze still holds it |

Quarantining a referential failure was the original design and it was wrong:
an order rejected because its customer had not arrived stayed rejected forever,
because nothing revisited it. Orphans now land in silver flagged, and
`heal_orphans` clears the flag once **every** parent exists. It re-evaluates the
row rather than reacting to one parent's arrival — clearing the flag because one
parent showed up wrongly cleared 38 orders whose customer was still missing.

Uniqueness is scoped to a single delivery. Bronze is append-only and each
ingest restates the same key space, so a change-feed window spanning several
deliveries legitimately holds every key many times. Judging uniqueness across
the window marked every row a duplicate: 102,613 quarantined rows holding only
99,996 distinct keys, with nothing reaching silver.

## Survivorship

When one delivery carries a key twice, the winner is chosen by: later delivery,
then the later business event date (`signup_date`, `order_date`), then ingest
time, then the row hash. The hash is last and exists only so the result is
reproducible — without it, rows sharing a batch and a timestamp tie and the
winner is arbitrary, so the same input could put a key in silver on one run and
in quarantine on the next.

The losing row is quarantined under `uniqueness`, so a duplicate is visible
rather than silently dropped.

## Intentional issues in the sample data — 725 rows

The counts the assessment names explicitly, verified against the committed CSVs:

| Issue | Rows | Catches |
|---|---|---|
| NULL email | 50 | completeness |
| duplicate `customer_id` | 10 | uniqueness |
| NULL `orders.customer_id` | 100 | completeness |
| NULL `orders.product_id` | 200 | completeness |
| `customer_id` not in customers | 50 | referential |
| `product_id` not in products | 30 | referential |
| duplicate `order_id` | 20 | uniqueness |

Extended coverage, so that a single run exercises every declared rule rather
than the handful that happened to have data:

| Issue | Rows | | Issue | Rows |
|---|---|---|---|---|
| invalid email format | 30 | | negative cost | 8 |
| invalid customer_segment | 20 | | overlong product_name | 6 |
| invalid order_status | 20 | | negative stock_quantity | 6 |
| non-positive quantity | 25 | | excessive stock_quantity | 6 |
| negative price | 15 | | NULL `order_id` | 5 |
| future signup_date | 15 | | excessive quantity | 12 |
| NULL `customer_id` | 5 | | zero unit_price | 12 |
| short customer_name | 8 | | negative total_amount | 10 |
| overlong customer_name | 8 | | pre-launch order_date | 12 |
| invalid country | 12 | | future order_date | 12 |
| negative lifetime_value | 10 | | duplicate `product_id` | 5 |
| NULL `product_id` | 3 | | | |

`multiple_of` and `exclusive_maximum` are supported by the validator and
deliberately **not** declared: neither has a natural meaning for these three
entities, so declaring them would be validator theatre.

### A rule with no violating data is indistinguishable from a passing rule

`databricks/jobs/silver/tests/test_dq_coverage.py` fails in both directions — a
declared rule with no scenario, and a scenario naming a rule nobody declares.
It caught 17 rules that could never have failed.

Two counting notes, so the numbers are not misread. NULL-primary-key rows are
**appended**, not written over an existing key: nulling a parent key in place
removes it from the parent table and orphans every child, which cascaded 3
nulled products into 562 unintended orphan orders against a spec of 30.
And injected counts are floors, not totals — a duplicate is an exact copy taken
after injection, so copying a row that already had a NULL email adds another.

## The quality report

`silver.dq_metrics` gets one row per check category per entity per run:
`rows_evaluated`, `rows_passed`, `rows_quarantined`, `pass_pct`.

Each category is counted from the rows whose violations actually contain it, in
a single aggregation pass. The first implementation counted the batch once and
reused the same three numbers for all four categories, so one orders run
reported completeness, uniqueness, type_logic and referential all at 99.575% —
a report that looked complete and said nothing. A row failing several checks is
counted under each, so the categories do not sum to the batch size; that is
correct for a per-check report.

`ops.pipeline_manifest` records one row per entity per run — rows read, written
and quarantined, plus status and Delta versions — for both bronze and silver.

## One delivery's journey — the full path, with real numbers

The numbers below are from one verified end-to-end execution (batch
`20260827T105554Z`): two delivery waves generated, pushed through every
layer, and every count re-derived independently at the end.

**Generated.** Wave one: `customers.csv` 10,015 rows · `products.csv` 508 ·
`orders.csv` 100,025, with ~725 rows carrying planted defects across every
declared rule. Wave two: 500 brand-new orders, a full customer snapshot
with 20 changed rows plus 10 late-arriving customers, and a product
snapshot missing 3 products plus 5 late arrivals — no planted defects, so
anything quarantined afterwards is attributable to wave one.

**Bronze appended everything and judged nothing.** 508 / 10,015 / 100,025
rows landed exactly, bad rows included — one manifest row per ingest with
rows read = rows written and the Delta version step recorded.

**Silver sorted every row into one of three outcomes.**
Rejected to quarantine (blocking defects — null keys, bad formats, negative
money, future dates, in-file duplicate losers): products 48 · customers 167
· orders 428 = **643 rows**, counted per category as completeness 363 ·
type_logic 247 · uniqueness 48 (a row can fail several). Flagged and kept:
**688 orders** whose parents are genuinely absent sit in silver with
`_is_orphan = true` — and the flag agreed with the data in both directions
(0 wrongly set, 0 wrongly cleared, 0 NULL). Merged: 500 new orders
inserted, 30 customer rows updated (20 changed + 10 late parents), 3
products soft-deleted for vanishing from their snapshot — while the
re-delivered, unchanged seed rows wrote **exactly 0** rows, because the
row-hash gate refuses no-op rewrites. Closing the books: every delivered
key accounted for in silver or quarantine (0 / 0 / 0 unaccounted), zero
duplicate primary keys.

**Gold aggregated only what qualifies.** Of 100,200 order rows in silver:
33,152 qualify (`Completed`, not orphaned, not deleted); excluded were
33,428 Pending, 33,388 Cancelled and the 688 orphans (categories overlap —
232 rows are Completed *and* orphaned, which is why the parts exceed the
whole). The four tables rebuilt to 502 products · 10,010 customers · 1,096
trend days · 4 segments (379 / 1,941 / 92 / 7,598 — summing exactly to the
customer table), launched twice by the table trigger itself and never by
the harness, and an independent recompute from live silver re-derived every
number — down to the revenue total, 25,328,509.67, matching to the last
paisa.

The excluded two-thirds is not a leak: it is the stated business rule, and
the manifest logs the breakdown on every run so the haircut stays
auditable.

## Gold: consumes flags, does not re-validate

Gold reads the referential verdict silver already stored on the row
(`_is_orphan`, `_is_deleted`) rather than re-deriving it — the check runs
once, in silver, and gold trusts the result. The Completed-only revenue rule
narrows further, but that is a business definition of "what counts", not a
data-quality check. The haircut is not silent: the gold runner logs the
input breakdown (total / qualifying / pending / cancelled / orphan /
deleted) into `ops.pipeline_manifest` on every run, so how much of a
delivery gold excluded, and why, is always auditable from the manifest
rather than inferred from the output totals.

## How this is verified

| Level | What it proves | Cost |
|---|---|---|
| unit | one rule, one predicate | seconds |
| **contract** (`test_pipeline_contract.py`) | generated CSV through to the silver tables, against expectations derived from the input | ~1 min |
| cluster (`run-medallion-e2e-ce.sh`) | two real deliveries on Databricks: insert, update, delete, orphan healing | ~25 min |

The contract test exists because a cluster run reporting `success` repeatedly
turned out not to mean the data was right — jobs reported success while
admitting zero rows, and a check reported a perfect pass rate while being
unreachable. Its expectations are recomputed from the input rather than
hardcoded, so it cannot quietly agree with a wrong implementation, and a guard
cross-references every rule the schema declares so an incomplete expectation
fails as an expectation error rather than looking like a code defect.
