# Requirement Analysis

## Problem statement

An e-commerce company receives daily data from three sources — a customer
database, an order system, and a product catalog — and needs it ingested into
Databricks, cleaned and validated, aggregated into business-ready tables, and
surfaced on a dashboard. The sources are files, they arrive on different
rhythms, and they contain the defects real feeds contain: missing values,
duplicates, references to entities that don't exist (or don't exist *yet*), malformed and out-of-range values. The hard part is not moving CSVs into tables. It is deciding **what a failed check means** — some failures are permanent facts about a row,
some are timing, and some are just a later version of the same key — and
building the pipeline so each is handled as what it is, with every decision
auditable afterwards.

## Functional requirements


| #   | Requirement                                                                                | Interpretation applied                                                                                                               |
| --- | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| F1  | Sample data generator with realistic data and intentional issues                           | seeded and reproducible; two delivery modes (`seed`, `delta`) so change-data behaviour is provable, not assumed                      |
| F2  | Bronze: raw ingest of all three sources                                                    | append-only, no validation or dedup; unparseable content kept in `_rescued_data`; ingest metadata per run                            |
| F3  | Silver: completeness, uniqueness, referential integrity, type/business logic               | declared as config, enforced per delivery; three outcomes (reject / flag / supersede), never hard-delete                             |
| F4  | Flag bad rows, don't delete; quality report per check                                      | quarantine with full row + violations + lineage; `silver.dq_metrics` per check per run                                               |
| F5  | Gold: sales by product, revenue by customer, customer segmentation (+ daily/weekly trends) | the brief's own count varies between three and four — building four; `segment_type` is behaviour-derived, not the CSV segment column |
| F6  | Dashboard: 3+ queries and visualizations with filters                                      | delivered — published AI/BI dashboard from a committed source (see [DASHBOARD_GUIDE](databricks/dashboards/DASHBOARD_GUIDE.md))      |
| F7  | Schema/setup script and seed data in the repo                                              | `database/schema.sql` (drift-guarded) + committed CSVs                                                                               |




## Non-functional requirements

- **Reproducibility** — seeded generation; deterministic survivorship; same
deploy script locally and in CI so environments cannot drift
- **Auditability** — nothing destroyed anywhere: bronze append-only, silver
soft-deletes, quarantine keeps rejected rows with both lineage axes
- **Verifiability** — tests at the cost tier that can catch each defect class;
end-to-end assertions compare state to data, not to run status
- **Isolation** — everything under one CE profile (`de-assessment-ce`) and one
dedicated account; enforced by configuration (scoped MCP servers, rules)
- **Cost discipline** — serverless jobs, no automatic retries of
deterministic failures, unchanged rows not rewritten



## Assumptions

- Sources deliver files (CSV) to a landing area; no direct DB connections
- Customers and products arrive as **full snapshots** (the file states the
whole world, so absence = deletion); orders arrive **incrementally**
- Later data supersedes earlier data for the same key; within one delivery the
same key twice is a defect
- A missing parent is usually *late*, not wrong — orders may legitimately
arrive before the customer they reference
- Community Edition constraints hold: serverless only, no classic clusters,
workspace-file job sources



## Edge cases identified (all present in the sample data)

- NULL values in critical and non-critical fields, including NULL primary keys
- Duplicate keys **within** one delivery vs the same key **across** deliveries
— different meanings, handled differently
- Foreign keys referencing parents that never arrive vs parents that arrive
late vs parents that are later withdrawn
- Values out of range, wrong format, outside enums, outside date windows
- Re-delivery of identical data (must not rewrite everything or double-count)
- A row failing several checks at once (counted under each, quarantined once)



## Clarifications needed → decisions taken

The brief leaves several points open; each was resolved explicitly rather than
silently:


| Open point                                                                | Decision                                                                                                               |
| ------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| gold: "three tables" vs "all 4 aggregations" appear in different sections | build four                                                                                                             |
| "flag bad rows in-table" vs preserving detail                             | quarantine table holds rejects with full violation detail; referential failures stay in-table flagged; nothing deleted |
| what "duplicate" means for an append-only raw layer                       | scoped to one delivery; across deliveries it is supersession                                                           |
| whether referential failures are errors                                   | treated as temporal: flagged and healed when the parent arrives                                                        |


