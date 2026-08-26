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
