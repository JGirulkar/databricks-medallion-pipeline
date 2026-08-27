# Data Model

Full DDL: [`database/schema.sql`](database/schema.sql) (guarded by a test that
cross-references it against the schemas the code defines, so it cannot drift).

## Business entities

| Table | Grain | Business columns |
|---|---|---|
| `customers` | one row per customer | `customer_id` (PK), `customer_name`, `email`, `country`, `signup_date`, `customer_segment` (Premium/Standard/Basic), `lifetime_value` |
| `orders` | one row per order line | `order_id` (PK), `customer_id` (FK→customers), `order_date`, `product_id` (FK→products), `quantity`, `unit_price`, `total_amount`, `order_status` (Pending/Completed/Cancelled), `payment_date` (nullable) |
| `products` | one row per product | `product_id` (PK), `product_name`, `category`, `price`, `cost`, `stock_quantity`, `reorder_level` |

Delivery patterns (declared in `config.source_config`): customers and products
are **full snapshots** — each file states the complete world, so an absent key
is a delete; orders are **incremental** — each file carries only new rows.

## Bronze: business columns + ingest metadata (CDF enabled)

| Column | Meaning |
|---|---|
| `_ingest_timestamp` | when Auto Loader landed the row |
| `_source_file` | which landing file it came from |
| `_batch_id` | the ingest run (UUID) — the unit silver's uniqueness check is scoped to |
| `_delivery_pattern` | snapshot / incremental, copied from config |
| `_rescued_data` | raw text Auto Loader could not parse into the declared schema — bronze never drops a row |

## Silver: business columns + control columns (CDF enabled)

| Column | Meaning | Written by |
|---|---|---|
| `quality_check_result` | `PASS` for every admitted row | conform |
| `_row_hash` | hash of every business column (derived from the schema, not a hand-kept list); gates the merge so unchanged rows are not rewritten | conform |
| `_is_deleted` | soft delete — the key vanished from a snapshot delivery; the row is kept | snapshot pass |
| `_is_orphan` | a foreign key of this row does not resolve to a live parent **yet**; set *and* cleared by recomputing from the data | `refresh_orphan_flags` |
| `_silver_updated_at` | UTC time of the last actual change | conform |
| `_bronze_batch_id` | bronze batch that last **changed** this row (not last delivered — the hash gate skips no-op writes) | conform |

## Gold: aggregation tables (rebuilt every run)

Full DDL: [`database/schema.sql`](database/schema.sql), guard-tested against
execution (`test_schema_sql_matches_built_gold_tables`). Every table is
replaced whole by `CREATE OR REPLACE TABLE … AS SELECT` from current silver —
no incremental state, nothing to migrate.

| Table | Grain | Columns |
|---|---|---|
| `gold.sales_by_product` | product | `product_id`, `product_name`, `category`, `total_orders` BIGINT, `total_revenue` DECIMAL(18,2), `avg_order_value` DECIMAL(18,2) |
| `gold.revenue_by_customer` | customer | `customer_id`, `customer_name`, `customer_segment` (declared, Premium/Standard/Basic), `total_orders`, `total_revenue`, `avg_order_value`, `lifetime_value_actual` DECIMAL(18,2), `last_order_date` DATE |
| `gold.daily_weekly_trends` | day | `order_date` DATE, `week_start` DATE, `total_orders`, `total_revenue`, `avg_order_value` |
| `gold.customer_segmentation` | segment | `segment_type`, `customer_count` BIGINT, `avg_revenue` DECIMAL(18,2), `total_revenue` DECIMAL(18,2) |

**The qualifying-orders rule** — defined once, in the gold runner, and read
by every SQL file: `order_status = 'Completed' AND NOT _is_orphan AND NOT
_is_deleted`. Dimensions (`products`, `customers`) filter `NOT _is_deleted`
only — orphanhood is an orders-side concept. Revenue is `total_amount`;
zero-activity products and customers are kept with `avg_order_value` NULL,
not 0 (an average over nothing is unknown, not zero).

**The segment ladder** — mutually exclusive, evaluated top-down, thresholds
pinned from the measured seed distribution:

| Segment | Rule |
|---|---|
| Inactive | no qualifying order in the 90 days before `as_of` (`as_of = MAX(last_order_date)`, data-anchored); includes customers with no qualifying orders at all |
| High-Value | active AND lifetime qualifying revenue ≥ 5,000 |
| Repeat | active AND ≥ 2 lifetime qualifying orders |
| One-Time | active AND exactly 1 |

`customer_segmentation` derives from `gold.revenue_by_customer`, not from
silver — the pie cross-foots with the customer table by construction. The one
ordering constraint: `02_revenue_by_customer.sql` runs before
`04_customer_segmentation.sql`.

## Quality and operations tables

| Table | Grain | Notes |
|---|---|---|
| `silver.quarantine` | one row per rejected row per rejecting run | full original row (`data` JSON) + `violations` array (category, rule, column, value) + **both lineage axes**: `bronze_batch_id` = where the row came from, `silver_run_id` = which run rejected it. Different questions — do not conflate. |
| `silver.dq_metrics` | one row per check category per entity per run | `rows_evaluated`, `rows_passed`, `rows_quarantined`, `pass_pct` — each category counted from the rows that actually carry it |
| `ops.pipeline_manifest` | one row per entity per run, bronze **and** silver | rows read/written/quarantined/rescued, Delta versions before/after, status, timings, error message |
| `config.source_config` | one row per source | paths, delivery pattern, CDF flag, and the `dq_schema` VARIANT holding every validation rule |

## Key semantics worth knowing

- **No engine-enforced FKs** — Delta does not enforce referential integrity;
  `fk_exists` is a business rule evaluated in silver, and its failures are
  flagged, not rejected.
- **Uniqueness is per delivery** (`_batch_id`): bronze is append-only and each
  ingest restates the key space, so the same key across deliveries is
  supersession, not duplication.
- **Nothing in bronze is ever updated or deleted**; nothing in silver is ever
  hard-deleted. Every row the source sent is recoverable somewhere.
