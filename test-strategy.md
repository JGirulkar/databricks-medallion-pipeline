# Test Strategy

## The principle the whole strategy follows

**A green run is not evidence — the data is.** Three times during this project
a job reported `success` while the data was wrong: jobs succeeded while
admitting zero rows, a configured check reported a 100% pass rate while being
unreachable, and an end-to-end run passed while 38 rows carried the wrong
flag. Every tier below therefore asserts *outcomes against the data*, not
whether a mechanism ran.

## The tiers

| Tier | What it proves | Cost | Command |
|---|---|---|---|
| `unit` | one rule, one function, no JVM | seconds | `bash databricks/scripts/run_job_tests.sh <job>` |
| `spark` | real transforms on local Spark + Delta | minutes | same (marker `spark`) |
| **contract** | generated CSVs through the real silver (and, for gold, the real SQL) path, checked against expectations derived independently from the input | ~1 min | silver's `test_pipeline_contract.py`; gold's `test_gold_contract.py` |
| cluster E2E | real deliveries on Databricks CE: bronze/silver insert, update, delete, orphan healing; gold's trigger-launched run checked against a live recompute | ~25 min | `bash scripts/run-medallion-e2e-ce.sh` |

160 tests: data_generation 17 · bronze 62 · silver 61 · gold 20 (unit 6 ·
runner 4 · contract 9 · drift 1). Run everything:

```bash
bash databricks/scripts/run_job_tests.sh --all --forbid-skips
```

**Gate:** a skipped unit/spark test is a defect (`--forbid-skips` enforces it —
the flag failed silently once, so the runner now checks the pytest summary).
A layer is not complete until one real E2E run passes; that step was "optional"
once and a `NameError` reached the cluster.

## Scenario matrix — every case, where it is proven

Every intentional case in the sample data, the outcome it must produce, the
test that proves it locally, and the live evidence from the CE runs.

### Completeness

| Scenario (rows) | Expected outcome | Local proof | Live evidence |
|---|---|---|---|
| NULL email (50) | quarantined; never in silver | `test_pipeline_contract` (defects absent from silver, present in quarantine) | 363 completeness violations/run |
| NULL `orders.customer_id` (100), `product_id` (200) | quarantined | same | same |
| NULL primary keys, appended (5+3+5) | quarantined; **no cascade** onto children | `test_null_pk_rows_are_appended_not_mutated` | orphans exactly 50+30, cascade 0 |

### Uniqueness

| Scenario | Expected outcome | Local proof | Live evidence |
|---|---|---|---|
| duplicate keys in ONE delivery (10 cust / 20 ord / 5 prod, exact copies) | one row survives, loser quarantined `uniqueness` | `test_duplicate_within_one_delivery_is_flagged_and_quarantined` | 48 uniqueness/run; 0 duplicate PKs in silver |
| same key across deliveries | **supersession, not duplication** — latest wins, nothing quarantined | `test_same_key_across_deliveries_is_supersession_not_duplication` | the 102,613-rows-for-99,996-keys failure, fixed |
| duplicates carrying different values | deterministic winner: delivery → event date → ingest time → row hash | `test_split_quarantines_a_duplicate_inside_one_delivery` + hash tie-break | reproducible across runs |

### Referential integrity

| Scenario | Expected outcome | Local proof | Live evidence |
|---|---|---|---|
| orphan FK (50 cust + 30 prod refs) | **in silver, `_is_orphan=true`** — flagged, not rejected | `test_order_with_missing_parent_lands_in_silver_flagged` | 688 flagged; wrongly_cleared 0, wrongly_set 0, null 0 |
| parent arrives late (10+5 in delta) | flag cleared for those children only | `test_healing_clears_the_flag_once_every_parent_exists` | wrongly_cleared 0 |
| ONE parent arrives, another still missing | flag **stays** | `test_healing_leaves_a_row_flagged_while_any_parent_is_missing` | the 38-wrongly-cleared failure, fixed |
| parent soft-deleted later | flag **set** on its children | `test_flag_is_set_when_a_parent_is_soft_deleted` | the 624-unflagged failure, fixed |

### Type / business logic (one scenario per declared rule)

| Rule family | Scenarios | Local proof |
|---|---|---|
| format (email), enum (segment, status), pattern (country), length (names) | 30 · 20+20 · 12 · 8+8+6 | `test_validators.py` per rule + contract test |
| numeric bounds (price, cost, stock, quantity, totals, exclusive-min) | 15+8+6+6+25+12+12+10 | same |
| date windows (signup ≤ today; order date in [2020-01-01, today]) | 15+12+12 | same |

**Coverage is enforced, not hoped for:** `test_dq_coverage.py` fails if a
declared rule has no violating scenario in the generator, or if a scenario
names a rule nobody declares — in both directions. It caught 17 rules that
could never have failed.

### Change data capture (delta delivery)

| Scenario | Expected outcome | Local proof | Live evidence |
|---|---|---|---|
| 500 new orders, disjoint ids | inserted | generator tests + contract | `new_orders: 500` |
| 20 customers with changed values | updated — and **only** changed rows rewritten | `test_changed_value_is_written`, `test_identical_redelivery_updates_nothing` | 30 updated of 10,015 (hash gate) |
| 3 products omitted from snapshot | soft-deleted, row retained | `test_soft_delete.py` (5 tests incl. idempotence) | `products_soft_deleted: 3` |
| identical re-delivery | writes ~nothing; **no key lost** | key-accounting in contract test | `unaccounted_keys: 0/0/0` |
| returning soft-deleted key | revived even when byte-identical | `test_returning_key_is_revived_even_when_identical` | — |

### Gold aggregation contract

| Scenario | Expected outcome | Local proof | Live evidence |
|---|---|---|---|
| status exclusion (Pending, Cancelled) | excluded from every table via the shared `qualifying_orders` view | `test_only_qualifying_orders_count`, `test_revenue_column_reconciles` | E2E order breakdown: 33,428 pending + 33,388 cancelled excluded of 100,200 total |
| orphan exclusion (`_is_orphan`) | excluded from every table | the four `test_*_matches_independent_recompute` tests (pandas expectations apply the same predicate) | E2E order breakdown: 688 orphan excluded |
| deleted exclusion (`_is_deleted`) | excluded from orders and from dimension rows | same | E2E order breakdown: 0 deleted this run |
| zero-activity products/customers | kept, `avg_order_value` NULL not 0 | `test_zero_activity_rows_are_kept` | 502 products, 10,010 customers — the full population, not just the active ones |
| segment reachability | all four segments non-empty at the pinned constants | `test_every_segment_is_reachable` (coverage gate, both directions) | High-Value 379 / Repeat 1,941 / One-Time 92 / Inactive 7,598 |
| cross-footing | `customer_segmentation` sums agree with `revenue_by_customer` | `test_gold_tables_cross_foot` | segment counts sum to 10,010, the customer table's row count |
| idempotent rerun | a second run against unchanged silver reproduces identical tables | `test_rerun_is_idempotent` | manifest `rows_read` = 100,200 = the exact silver orders count, every run |

The schema-drift guard (`test_schema_sql_matches_built_gold_tables`) was
proven live during this pass: mutating a column name broke it with
`AssertionError: gold.sales_by_product: schema.sql drift {'total_revenue',
'total_revenuex'}`, then the column was restored.

### Layer-boundary guards (source-level, cannot regress silently)

| Invariant | Test |
|---|---|
| bronze never rejects, mutates, merges, overwrites or de-duplicates; rescue column configured | `test_bronze_is_append_only.py` |
| no serverless-rejected operation (`.cache()`, `sparkContext`, …) in job sources | `test_serverless_constraints.py` |
| `database/schema.sql` carries every column the code defines | `test_reference_schema_sql_matches_the_entity_schemas` |
| expectations in the contract test cover every declared rule | `test_expectations_cover_every_declared_rule` |

## Why the contract tier exists

Every defect found on the cluster was detectable locally first, at a minute a
run instead of twenty-five. The contract test recomputes its expectations from
the generator output by hand — deliberately sharing no code with the
implementation — so it cannot quietly agree with a wrong implementation. When
it disagrees with the pipeline, the first question is *which side is wrong*:
three of its first failures were defects in the expectations, and a guard now
cross-references them against the declared rules so an omission fails loudly.

## What only the cluster can prove

Serverless restrictions (`NOT_SUPPORTED_WITH_SERVERLESS`), platform behaviour
(auto-optimization retrying failed tasks independently of `max_retries`),
trigger wiring (`table_update` firing silver after bronze commits), and true
concurrency (three entities ingesting in parallel; concurrent
`refresh_orphan_flags` writers retrying on Delta conflicts). The E2E asserts
against the tables, emits its JSON report even when a step throws (status
`aborted`), and retries only the launch calls — a failure in polling or
verification is a real result.
