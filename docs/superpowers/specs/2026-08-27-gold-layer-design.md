# Gold Layer Design — Aggregations & Analytics

**Status:** Approved — pending implementation plan
**Date:** 2026-08-27
**Parent anchor:** [2026-08-25-silver-layer-design.md](./2026-08-25-silver-layer-design.md)
**Scope:** the four gold aggregation tables, their business-rule contract, the gold job, its trigger topology, and the test tiers that prove the numbers. The dashboard is a downstream consumer and gets its own design.
**Environment:** Databricks Free Edition (serverless only), profile `de-assessment-ce`, jobs deployed via `scripts/deploy-all-ce-jobs.sh` (Jobs API `reset`; no bundle).

---

## 1. Goals

1. **Business-ready aggregates** — four gold tables a dashboard can read without further joins or filters.
2. **One semantic contract** — a single, stated definition of "what counts" applied identically to every table, so the tables cross-foot.
3. **Provably correct numbers** — "aggregation calculations are correct (sum, count, avg)" is an explicit acceptance criterion; every number must be reproducible by an independent recomputation.
4. **Idempotent and self-healing** — any run, at any time, from any silver state, produces a correct gold layer; no run depends on the previous one.
5. **Lean** — the brief warns against expanding pipeline complexity at the expense of lifecycle artifacts. Where a heavier mechanism was considered, this document records it and why it lost.

## 2. Requirements traced

The brief's prose names three aggregation tables; its required repository structure names four gold SQL files (adding `daily_weekly_trends`). Building four satisfies both readings.

| Table | Required columns (brief) |
|---|---|
| `sales_by_product` | product_id, product_name, category, total_orders, total_revenue, avg_order_value |
| `revenue_by_customer` | customer_id, customer_name, customer_segment, total_orders, total_revenue, avg_order_value, lifetime_value_actual |
| `daily_weekly_trends` | revenue/orders by day and week |
| `customer_segmentation` | segment_type (High-Value/Repeat/One-Time/Inactive), customer_count, avg_revenue, total_revenue |

`segment_type` is **derived from behaviour** — it is not the CSV's declared Premium/Standard/Basic column (that one appears, as delivered, in `revenue_by_customer.customer_segment`).

The brief defines **no revenue semantics** — `order_status` appears only as a schema column and "revenue" only inside column names. Every rule in §3 is therefore a design decision, chosen by real-world convention and stated here as the contract.

## 3. The semantic contract

### 3.1 Qualifying orders — one definition, one place

```sql
-- created once per run by the runner as a temp view; every gold SQL file reads it
qualifying_orders = silver.orders
  WHERE order_status = 'Completed'
    AND NOT _is_orphan
    AND NOT _is_deleted
```

- **Completed only** ("recognized revenue"). Pending is bookings, not revenue; Cancelled reconciles with nothing. The status filter cascades into *every* measure — order counts, averages, and the segment ladder — so it is defined once and shared, not repeated per file. The `= 'Completed'` predicate also excludes the contractually-possible NULL status.
- **`NOT _is_orphan AND NOT _is_deleted`** — the silver contract stores the referential verdict on the row, which is what lets gold read a valid snapshot at any instant with no cross-table barrier. Healed rows enter gold on the next run because healing is an ordinary silver commit.
- Dimensions filter `NOT _is_deleted` only (orphanhood is an orders-side concept).

### 3.2 Revenue column

`total_amount`, as delivered. It is the transactional record; `quantity × unit_price` is a derivation that in real systems legitimately diverges (discounts, tax, shipping). Money is never silently recomputed. In our generated data the two are equal by construction for every row that reaches silver — the contract test asserts that reconciliation, which doubles as evidence for the correctness criterion.

### 3.3 Segment ladder (mutually exclusive, evaluated top-down)

```text
as_of      = MAX(order_date) over qualifying orders
1. Inactive    no qualifying order in the 90 days before as_of
               (includes customers with no qualifying orders at all)
2. High-Value  active AND lifetime qualifying revenue >= 5,000
3. Repeat      active AND >= 2 lifetime qualifying orders
4. One-Time    active AND exactly 1
```

- **Recency outranks value**: a high spender with no order in 90 days is Inactive — "lapsed VIP" is the actionable win-back signal. The alternative (value first) hides exactly the customers a retention team needs to see.
- **as_of is data-anchored, not wall-clock.** A static dataset ages: under `current_date()` every customer eventually drifts to Inactive with no data change, and results depend on when the job runs. Anchoring to the data's own business date makes every run deterministic for a given silver state. This anchor is safe **only because** silver's `order_date` window check (min 2020-01-01, max today) quarantines future-dated rows — measured on the raw seed CSV, a single future-dated bad row dragged MAX(order_date) from 2025-12-31 to 2026-10-10 and emptied three of four segments. Gold's determinism explicitly leans on that silver check.
- **Thresholds are fixed constants, pinned from the measured seed distribution** (per-customer lifetime qualifying revenue p90 ≈ 4,983 → pinned at the round 5,000). Percentile-per-run was rejected: a customer's segment would change with other customers' behaviour and expectations could not be stated ahead of a run. Measured seed segment counts at these constants: High-Value 376, Repeat 1,956, One-Time 94, Inactive 7,589 — all four segments reachable, which the contract test guards in both directions.

### 3.4 Zero-activity rows are kept

Products and customers with no qualifying orders appear with zero totals (`avg_order_value` NULL, not 0 — an average over nothing is unknown, not zero). The revenue histogram needs its zero bucket, the segment ladder needs the full customer population, and a product silently missing from a sales report is indistinguishable from a pipeline bug.

## 4. Table definitions

All gold tables live in `gold`, written by `CREATE OR REPLACE TABLE … AS SELECT` (atomic replace: a failed run leaves the previous version intact; Delta history preserves lineage). No partitioning — the result grains are tiny (≈500 products, ≈10K customers, ≈1,100 days, 4 segments) and partitioning them is the small-files anti-pattern. The scale levers live on the scan side (silver layout) and are documented, not implemented, at this volume.

| Table | Grain | Columns | Source shape |
|---|---|---|---|
| `gold.sales_by_product` | product | product_id, product_name, category, total_orders BIGINT, total_revenue DECIMAL(18,2), avg_order_value DECIMAL(18,2) | products (not deleted) ⟕ qualifying_orders |
| `gold.revenue_by_customer` | customer | customer_id, customer_name, customer_segment (declared), total_orders, total_revenue, avg_order_value, lifetime_value_actual DECIMAL(18,2), last_order_date DATE | customers (not deleted) ⟕ qualifying_orders |
| `gold.daily_weekly_trends` | day | order_date DATE, week_start DATE, total_orders, total_revenue, avg_order_value | qualifying_orders grouped by day |
| `gold.customer_segmentation` | segment | segment_type, customer_count BIGINT, avg_revenue DECIMAL(18,2), total_revenue DECIMAL(18,2) | `gold.revenue_by_customer` + ladder |

- `lifetime_value_actual` = the customer's summed qualifying revenue — *actual*, sitting next to the CSV's *declared* `lifetime_value` upstream, a free declared-vs-actual comparison for the dashboard. It equals `total_orders`-scoped `total_revenue` today because both are lifetime sums over the same qualifying set; the brief names both columns, and they diverge the moment `total_revenue` is ever windowed (e.g. trailing 12 months), so both are kept.
- `last_order_date` (beyond the brief's floor) = the customer's latest qualifying order date; NULL for zero-activity customers. It is what the segment ladder's recency test reads, and `as_of` = MAX over it.
- **`customer_segmentation` is derived from `gold.revenue_by_customer`, not from silver** — the pie cross-foots with the customer table by construction rather than by test alone. This is the one inter-file dependency: 02 must run before 04.
- **Weekly is derived, not duplicated**: trends is daily grain with a `week_start` column; weekly views GROUP BY it. One table, both grains, one set of numbers.
- Every join is fact⟕dim with dims far under the broadcast threshold; the engine broadcast-joins them unhinted.

## 5. Compute model — full recompute per run

**Chosen:** every run rebuilds all four tables from current silver.

Per-aggregation incremental eligibility was analysed (each table's own verdict, not one blanket call):

| Table | Additive? | Incremental verdict |
|---|---|---|
| daily_weekly_trends | SUM/COUNT by date | eligible — but only under an append-only contract silver deliberately does not offer (supersession updates and orphan-flag flips mutate order rows in place) |
| sales_by_product | SUM/COUNT by product | eligible; needs the dim change stream coordinated too |
| revenue_by_customer | SUM/COUNT by customer | eligible; same dim caveat |
| customer_segmentation | not additive | **impossible from CDF alone** — one new order moves a customer between segments (−1 row here, +1 there), and Inactive depends on recency, so segments change with the passage of time and zero change events |

Incremental would require: filter-aware delta algebra `f(post) − f(pre)` over silver CDF (CDF is enabled on silver, so the preimages exist), per-table checkpoints, exactly-once application, and a rebuild story for when the stateful aggregate drifts. At ~100K orders a full recompute costs seconds; the correctness criterion is trivially provable under recompute and much harder under maintenance algebra. Full recompute is also self-healing: any bug fixed upstream is cured by the next run, whereas a drifted incremental aggregate stays wrong forever. **Rejected as machinery; kept as this analysis.**

**Materialized views rejected** for the same layer: MV refresh cannot run on local Spark (the contract-test method dies), refreshes bypass `ops.pipeline_manifest` (lineage story breaks), and the deliverable is aggregation code. Noted as the managed-production alternative.

**Status-as-dimension rejected**: carrying all three statuses into gold and filtering in the dashboard forces status into every table's grain, breaks the brief's one-row-per-entity shapes, and cannot work for segmentation (a BI filter cannot re-run the ladder) — three filterable tables and one baked table would stop cross-footing. The dashboard's required filters are served by category/country/date instead.

## 6. Code shape — SQL files are the executed source

```text
databricks/jobs/gold/
├── src/gold/
│   ├── sql/
│   │   ├── 01_sales_by_product.sql
│   │   ├── 02_revenue_by_customer.sql
│   │   ├── 03_daily_weekly_trends.sql
│   │   └── 04_customer_segmentation.sql
│   ├── config.py        # catalog default, thresholds, SQL loading + substitution
│   ├── runner.py        # qualifying_orders view → execute files in order → manifest row
│   └── main.py          # wheel-task entry point (argparse --catalog)
└── tests/
```

Gold logic is pure declarative SQL over silver — no routing, merges, or loops — so the four SQL files the deliverable structure names are the *single executed source of truth*, not documentation mirrors (a reference file that code does not execute is future drift; the schema.sql incident already taught this once). The runner substitutes schema-level placeholders (`{silver}`, `{gold}` — on the cluster `de_assessment.silver`/`de_assessment.gold`, in local tests plain test schemas, which is what lets the contract tier execute the identical files) and the pinned threshold constants, executes the files in order (01–03 read only the shared view and silver dims; 04 reads the table 02 built — the one ordering constraint, stated in both file headers), and writes one `ops.pipeline_manifest` row per run recording input row counts by status — the Completed-only haircut is auditable, never silent — and rows written per table. Python stays a thin loader; no `sys.exit` on the wheel path.

## 7. Trigger topology

**Chosen:** one gold job, `table_update` trigger on all three silver tables, `condition: ANY_UPDATED`, `min_time_between_triggers_seconds: 120`. Registered in `scripts/ce_job_registry.py` with the fleet settings (`jobs reset`, `max_retries: 0`, `disable_auto_optimization: true`).

Because the referential verdict is stored on silver rows (§3.1), any combination of per-table snapshots is valid — the trigger choice is purely a freshness/cost dial, never a correctness question. A delivery wave (three unaligned silver commit bursts plus healing commits) coalesces under the debounce into 1–2 runs, each idempotent.

| Rejected | Why |
|---|---|
| `ALL_UPDATED` | starves under unaligned arrival cadences — gold waits on the slowest source and serves stale revenue despite fresh orders |
| trigger on the most frequent source only | silent staleness hole: dimension-only changes (customer renames, segment updates) never propagate until the next orders delivery |
| cron | blind to data: runs on empty days, stale up to a full interval after real changes |
| chaining from each silver job | re-introduces orchestration coupling the stored-flag design exists to remove |

Assumption stated for the record: sources arrive daily, independently, at unaligned times (the brief says only "daily sales data").

## 8. Testing

Same method the silver layer proved out — expectations first, cluster last.

1. **Contract tier (local Spark, ~1 min):** the four *real* SQL files run against silver tables produced by the *real* silver code from *real* generator output. Expectations are recomputed independently in pandas (sharing no code with the SQL): per-product and per-customer totals, segment counts under the pinned ladder, spot-day trend values, `total_amount = quantity × unit_price` reconciliation, and as_of derived from validated rows only.
2. **Coverage guards, both directions:** every segment non-empty in generated data (an unreachable segment is indistinguishable from a broken ladder); every gold table non-empty; zero-activity rows present with NULL averages.
3. **Schema drift guard:** `database/schema.sql` gains the four gold DDLs and the existing cross-reference test extends to them.
4. **Unit tier:** runner mechanics — SQL file loading, placeholder substitution, manifest write, execution order.
5. **E2E gold phase (`scripts/medallion_e2e.py`):** after the silver wave, wait for the *trigger-launched* gold run (this proves the topology, not just the SQL), then assert gold tables against a live recompute from current silver — state-vs-data invariants, never run-status. Report always emits; only launches retry.

## 9. Deliverable mapping & docs

- README mapping table: brief's `src/gold/01…04.sql` + `create_gold_tables.py` ↔ `databricks/jobs/gold/src/gold/sql/*` + `main.py`; the stub `databricks/jobs/gold/src/create_gold_tables.py` is replaced by the package.
- design-notes.md gold section rewritten to as-built when implementation lands; this file stays a point-in-time record.
- data-model.md gains the four gold tables and the semantic contract; data dictionary states the qualifying-orders rule.
- `ai-prompts/06-gold-aggregations.md` written as the work happens.

## 10. Out of scope

The BI dashboard (3+ tiles, filters) — next phase, consuming these tables read-only.
