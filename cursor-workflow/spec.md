# Specification — Medallion Pipeline

The working spec the assistant builds against. Layer-level detail lives in the
dated specs under [`docs/superpowers/specs/`](../docs/superpowers/specs/); the
current design as built is [`design-notes.md`](../design-notes.md).

## Scope

Bronze → Silver → Gold → Dashboard for customers, orders, products CSVs, on
Databricks CE (serverless), profile `de-assessment-ce` only.

## Bronze — built, verified

Auto Loader ingest of raw CSVs into Delta, **append-only**: no transforms, no
validation, no dedup, unparseable content in `_rescued_data`, CDF enabled, one
`ops.pipeline_manifest` row per run. Guarded by source-level tests.

## Silver — built, verified

Streaming CDF consumption per entity; validation rules declared in the
`dq_schema` config column and applied at runtime. A failed check has three
outcomes: permanent defects → `silver.quarantine`; referential failures → in
silver flagged `_is_orphan`, healed by recomputing the flag from the data;
same key in a later delivery → superseded. Deterministic survivorship
(delivery → event date → ingest time → row hash), hash-gated merge, soft
deletes from snapshot omission, per-check `dq_metrics`. All three entities
run in parallel.

## Gold — next phase

Four aggregation tables, reading silver filtered to
`NOT _is_orphan AND NOT _is_deleted`:

1. `sales_by_product` — product_id, name, category, total_orders,
   total_revenue, avg_order_value
2. `revenue_by_customer` — customer_id, name, segment, total_orders,
   total_revenue, avg_order_value, lifetime_value_actual
3. `daily_weekly_trends` — revenue/orders by day and week
4. `customer_segmentation` — **derived** `segment_type`
   (High-Value / Repeat / One-Time / Inactive), customer_count, avg_revenue,
   total_revenue

## Dashboard — next phase

3+ queries with visualizations: top-10 products by revenue (bar), customer
revenue distribution (histogram), segmentation (pie); filters included.

## Data quality

725 intentional issue rows across the three CSVs; every declared rule has a
violating scenario, enforced by a coverage gate. Details:
[`data-quality-strategy.md`](../data-quality-strategy.md).

## Verification

Unit → local Spark → contract (expectations derived independently from the
generator output) → cluster E2E (two deliveries; asserts against the tables).
A layer is complete only when a real end-to-end run passes.
