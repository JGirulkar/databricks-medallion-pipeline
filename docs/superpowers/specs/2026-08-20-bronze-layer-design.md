# Bronze Layer Design — DE Medallion Assessment

**Status:** Approved — implementation plan written 2026-08-21  
**Date:** 2026-08-20  
**Parent anchor:** [2026-08-20-medallion-bronze-architecture-design.md](./2026-08-20-medallion-bronze-architecture-design.md)  
**Scope:** Bronze infrastructure only — bootstrap, UC objects, source config, ingest jobs, manifest. Data generation is a separate chat.

**Environment:** Databricks Free Edition, profile `de-assessment-ce`, Asset Bundle under `databricks/bundle/`

---

## 1. Goals

1. Create idempotent bronze infrastructure (catalog, schemas, volumes, Delta tables, seed config).
2. Append every newly discovered assessment CSV to its source-specific Bronze Delta table.
3. Log every ingest run to `ingest_manifest` with `batch_id` as the run identifier.
4. Enable CDF on customers and orders for Silver incremental consumption.
5. Keep assessment scope light: Intelo-inspired patterns without enterprise weight.

---

## 2. Architectural approach

**Chosen:** Bootstrap job + shared ingest library (Approach A).

| Component | Role |
|-----------|------|
| `bootstrap_bronze.py` | One-time / idempotent UC + Delta DDL + seed `source_config` |
| `bronze/` Python package | Shared Autoloader, manifest, schemas, config resolution |
| `ingest_{customers,orders,products}.py` | Thin entrypoints per source |
| `ingest_all.py` | Manual smoke: bootstrap → three ingests |

---

## 3. Catalog, schemas, and volumes

```
de_assessment                              ← CREATE CATALOG
├── bronze                                 ← Delta tables + config
│   ├── customers
│   ├── orders
│   ├── products
│   ├── ingest_manifest
│   └── source_config                      ← seeded static rows (Intelo-lite)
├── landing
│   └── raw                                ← managed volume (CSV landing)
│       ├── products/
│       ├── customers/
│       └── orders/
│           ├── incoming/
│           └── processed/                 ← optional archive after orders ingest
└── ops
    └── checkpoints                        ← managed volume (Autoloader checkpoints)
        ├── customers/
        ├── orders/
        └── products/
```

### Path conventions

| Purpose | Path |
|---------|------|
| Raw products | `/Volumes/de_assessment/landing/raw/products/` |
| Raw customers | `/Volumes/de_assessment/landing/raw/customers/` |
| Raw orders | `/Volumes/de_assessment/landing/raw/orders/incoming/` |
| Orders archive | `/Volumes/de_assessment/landing/raw/orders/processed/` |
| Checkpoints | `/Volumes/de_assessment/ops/checkpoints/{source_name}/` |
| Schema hints (Autoloader) | `/Volumes/de_assessment/ops/checkpoints/{source_name}/_schema/` |

CE constraint: use **managed UC volumes only** (no external locations / custom storage credentials).

---

## 4. Configuration split — table vs module

Inspired by Intelo patterns (**read-only reference:** `/home/jay-ajaykumar/Desktop/Projects/Intelo.ai/retail-agents-backend` — never edit that repo from this workspace).

| Intelo (production) | Assessment (Intelo-lite) |
|---------------------|--------------------------|
| `common/config/delta.py` — catalog/schema/table name constants, FQN helpers | `bronze/config.py` — `CATALOG`, schema names, metadata col names, FQN builders |
| `config.source_definition` UC table — per-source operational config, read at runtime | `de_assessment.bronze.source_config` — 3 seeded rows (paths and delivery patterns) |
| Per-job `config.py` — composes FQNs + tunable knobs (`ingestion_writer/config.py`) | Same split: module holds `SOURCE_CONFIG_TABLE`, `MANIFEST_TABLE`, rescue thresholds |
| `load_source_definitions(spark, org_id)` in classifier | `get_source_config(spark, source_name)` in shared ingest lib |

**Operational / per-source settings live in UC; code-level constants live in Python.**

### 4.1 UC table: `de_assessment.bronze.source_config`

Bootstrap creates and **seeds** three rows (idempotent `MERGE`). Ingest jobs **read this table at runtime** to resolve paths and source delivery patterns. Intended for values that may be tweaked and re-read without code changes.

| Column | Type | Description |
|--------|------|-------------|
| `source_name` | `STRING` | PK logical name: `customers`, `orders`, `products` |
| `target_table` | `STRING` | FQN: `de_assessment.bronze.{source_name}` |
| `raw_path` | `STRING` | Autoloader input directory |
| `checkpoint_path` | `STRING` | Autoloader checkpoint root |
| `schema_hint_path` | `STRING` | Autoloader `cloudFiles.schemaLocation` |
| `archive_path` | `STRING` | Nullable; orders post-ingest move target |
| `file_format` | `STRING` | `csv` |
| `delivery_pattern` | `STRING` | `full_snapshot` \| `incremental` |
| `cdf_enabled` | `BOOLEAN` | Table property hint (customers/orders true) |
| `schedule_hint` | `STRING` | Documentation: `daily` / `weekly` / `on_arrival` |
| `is_active` | `BOOLEAN` | Skip ingest when false |
| `updated_at` | `TIMESTAMP` | Last seed or manual update |

**Seed rows (bootstrap):**

| source_name | delivery_pattern | cdf_enabled | schedule_hint |
|-------------|---------------|-------------|---------------|
| products | full_snapshot | true | weekly |
| customers | full_snapshot | true | daily |
| orders | incremental | true | on_arrival |

### 4.2 Python module: `bronze/config.py`

Code-level constants — not duplicated in the seed table. Stable across runs; change requires bundle redeploy.

| Constant / helper | Examples |
|-------------------|----------|
| Catalog / schema names | `CATALOG = "de_assessment"`, `BRONZE_SCHEMA = "bronze"`, `LANDING_SCHEMA`, `OPS_SCHEMA` |
| Table name constants | `SOURCE_CONFIG_TABLE_NAME = "source_config"`, `MANIFEST_TABLE_NAME = "ingest_manifest"` |
| Table FQNs (derived) | `source_config_table()`, `manifest_table()`, `bronze_table(name)` — mirror Intelo FQN composition |
| Metadata column names | `_ingest_timestamp`, `_source_file`, `_batch_id`, `_delivery_pattern`, `_rescued_data`, `_row_hash` |
| Autoloader defaults | `rescuedDataColumn`, `availableNow` trigger, `inferColumnTypes=false` |
| Hash spec (customers) | SHA-256 over sorted business columns, exclude metadata |
| Manifest status enum | `success`, `failed` |
| `load_source_configs(spark)` | Reads active rows from `source_config`; raises if zero active |
| `get_source_config(spark, name)` | Single-row lookup for ingest entrypoints |

**Split rule:**

| Put in UC `source_config` | Put in Python `config.py` |
|---------------------------|---------------------------|
| Paths that may move on CE | Column name constants |
| Delivery pattern per source | Catalog/schema naming |
| Active flag / schedule hints | Hash algorithm definition |
| Archive path (orders) | Manifest / config table FQNs |

Unit tests use **in-memory `SourceConfig` dataclass** instances (same shape as table rows) so tests never require UC.

---

## 5. Bronze entity table DDL

Source columns match [docs/ASSESSMENT_FROM_PDF.md](../../../docs/ASSESSMENT_FROM_PDF.md). Data generator will align in a later chat.

### 5.1 Shared metadata columns (all entity tables)

| Column | Type |
|--------|------|
| `_ingest_timestamp` | `TIMESTAMP` |
| `_source_file` | `STRING` |
| `_batch_id` | `STRING` |
| `_delivery_pattern` | `STRING` |
| `_rescued_data` | `STRING` |

**Customers only:** `_row_hash` (`STRING`) — SHA-256 of business columns for Silver change detection.

### 5.2 `de_assessment.bronze.customers`

| Column | Type |
|--------|------|
| `customer_id` | `INT` |
| `customer_name` | `STRING` |
| `email` | `STRING` |
| `country` | `STRING` |
| `signup_date` | `DATE` |
| `customer_segment` | `STRING` |
| `lifetime_value` | `DECIMAL(18,2)` |
| + metadata + `_row_hash` | |

**Properties:** `delta.enableChangeDataFeed = true`

### 5.3 `de_assessment.bronze.orders`

| Column | Type |
|--------|------|
| `order_id` | `INT` |
| `customer_id` | `INT` |
| `order_date` | `DATE` |
| `product_id` | `INT` |
| `quantity` | `INT` |
| `unit_price` | `DECIMAL(18,2)` |
| `total_amount` | `DECIMAL(18,2)` |
| `order_status` | `STRING` |
| `payment_date` | `DATE` |
| + metadata | |

**Properties:** `delta.enableChangeDataFeed = true`  
Duplicate `order_id` values are preserved for Silver uniqueness checks.

### 5.4 `de_assessment.bronze.products`

| Column | Type |
|--------|------|
| `product_id` | `INT` |
| `product_name` | `STRING` |
| `category` | `STRING` |
| `price` | `DECIMAL(18,2)` |
| `cost` | `DECIMAL(18,2)` |
| `stock_quantity` | `INT` |
| `reorder_level` | `INT` |
| + metadata | |

**Properties:** `delta.enableChangeDataFeed = true`

---

## 6. Ingest manifest

**Table:** `de_assessment.bronze.ingest_manifest`

Run identifier is **`batch_id`** (same UUID on manifest row and all `_batch_id` values on landed rows for that run).

| Column | Type | Description |
|--------|------|-------------|
| `batch_id` | `STRING` | PK — UUID per ingest run |
| `source_name` | `STRING` | From `source_config` |
| `delivery_pattern` | `STRING` | From source config |
| `source_path` | `STRING` | Autoloader input used |
| `files_processed` | `INT` | |
| `rows_read` | `BIGINT` | |
| `rows_written` | `BIGINT` | |
| `rows_rescued` | `BIGINT` | Non-null `_rescued_data` |
| `delta_version_before` | `BIGINT` | Silver cursor aid |
| `delta_version_after` | `BIGINT` | |
| `started_at` | `TIMESTAMP` | |
| `completed_at` | `TIMESTAMP` | |
| `status` | `STRING` | `success` \| `failed` |
| `error_message` | `STRING` | Nullable |

On failure: write manifest row with `status=failed` and re-raise (job fails). Never delete bronze data.

---

## 7. Ingest behaviour

### 7.1 Flow (each ingest job)

```
1. load_source_config(spark, source_name)  ← read UC table
2. batch_id = uuid4()
3. delta_version_before = table history version
4. Autoloader read (typed schema, rescued column)
5. Add metadata columns (+ _row_hash for customers)
6. Append the complete discovered-file batch
7. Compute counts; delta_version_after
8. Append ingest_manifest row (batch_id)
9. (orders) optionally move files incoming/ → processed/
```

### 7.2 Autoloader settings (common)

- `cloudFiles.format = csv`
- Explicit `StructType` from `bronze/schemas.py` (no inference)
- `rescuedDataColumn = _rescued_data`
- `cloudFiles.schemaLocation` from `source_config.schema_hint_path`
- `.trigger(availableNow=True)` — batch per job run (CE-friendly)
- Checkpoint: `source_config.checkpoint_path`

### 7.3 Per-source write semantics

| source_name | Delivery | Bronze write |
|-------------|----------|--------------|
| products | Weekly full snapshot | Append whole discovered file |
| customers | Daily full snapshot | Append whole discovered file |
| orders | Incremental file | Append whole discovered file |

Auto Loader checkpoints provide **file-level replay protection**. Bronze does not
deduplicate keys or infer business updates. Silver compares snapshots, deduplicates
business keys, and applies I/U/D semantics.

### 7.4 Bronze rules (non-negotiable)

| Do | Do not |
|----|--------|
| Land values as read | DQ checks or quarantine |
| Log to manifest | Delete rows |
| Enable CDF (customers, orders) | I/U/D stamps |
| Read paths from `source_config` | Hardcode paths in ingest scripts |

---

## 8. Jobs and bundle

Replace monolithic `job_bronze_ingest`. Use **serverless** compute (CE requirement). Bundle variable: `catalog: de_assessment` (replace `hive_metastore`).

| Bundle key | Schedule | Entrypoint |
|------------|----------|------------|
| `job_bronze_bootstrap` | Manual / pre-deploy | `bootstrap_bronze.py` |
| `job_bronze_ingest_products` | Weekly cron | `ingest_products.py` |
| `job_bronze_ingest_customers` | Daily cron | `ingest_customers.py` |
| `job_bronze_ingest_orders` | Manual / frequent poll on CE | `ingest_orders.py` |
| `job_bronze_ingest_all` | Manual smoke | `ingest_all.py` |

Profile: **`de-assessment-ce`** on all bundle targets and CLI.

---

## 9. Code layout

```
databricks/jobs/bronze/
├── pyproject.toml
├── src/
│   ├── bootstrap_bronze.py
│   ├── ingest_customers.py
│   ├── ingest_orders.py
│   ├── ingest_products.py
│   ├── ingest_all.py
│   └── bronze/
│       ├── config.py           # constants + load_source_config()
│       ├── schemas.py          # StructType per entity
│       ├── bootstrap.py        # DDL + volume dirs + seed source_config
│       ├── ingest.py           # Autoloader + write + metadata
│       ├── manifest.py         # manifest append
│       └── hash.py             # customer _row_hash
└── tests/
    ├── test_config.py          # negative: missing source, inactive
    ├── test_schemas.py
    ├── test_hash.py
    ├── test_manifest.py
    ├── test_bootstrap.py       # spark
    └── test_ingest.py          # spark
```

Entrypoints: `SparkSession.getActiveSession()`; never `sys.exit()`.

---

## 10. Testing strategy (Superpowers TDD)

Implement **red → green → refactor** per component. Write failing tests first; include negative cases.

### 10.1 Unit (no Spark / minimal Spark)

| Test file | Negative cases |
|-----------|----------------|
| `test_config.py` | Unknown `source_name`; `is_active=false`; empty config table |
| `test_schemas.py` | Missing required fields; wrong types |
| `test_hash.py` | Null business fields; column order stability |
| `test_manifest.py` | Failed run row shape; missing batch_id |

### 10.2 Spark local

| Test file | Negative cases |
|-----------|----------------|
| `test_ingest.py` | Empty input batch; malformed values; duplicate business keys remain present |

UC catalog/volume DDL and Auto Loader's `cloudFiles` source are not available in
plain local Spark. Test DDL generation and seed semantics as unit contracts;
verify actual bootstrap idempotency, rescued-data behavior, and checkpoint replay
on CE cluster.

### 10.3 CE cluster

Deploy bundle → run bootstrap twice → confirm three config rows only → place test
CSVs in volume paths → run `ingest_all` twice → confirm checkpoint replay writes
zero additional rows on the second run.

Run: `./databricks/scripts/run_job_tests.sh` (unit → spark → cluster tiers).

---

## 11. Bootstrap sequence

Idempotent steps in `bootstrap_bronze.py`:

1. `CREATE CATALOG IF NOT EXISTS de_assessment`
2. `CREATE SCHEMA IF NOT EXISTS` bronze, landing, ops
3. `CREATE VOLUME IF NOT EXISTS` landing.raw, ops.checkpoints
4. `mkdirs` landing subdirs (products, customers, orders/incoming, orders/processed)
5. Create Delta entity tables + manifest + source_config (if not exists)
6. `ALTER TABLE` CDF properties on customers, orders
7. `MERGE` seed rows into `source_config`

Safe to re-run before every deploy smoke.

---

## 12. Out of scope (this spec)

- Sample data generation (separate chat)
- Silver CDF consumption and DQ
- Gold aggregations
- External volumes / S3 paths
- Enterprise quarantine or shadow/enforce frameworks
- Dynamic admin UI for `source_config` edits

---

## 13. References

- [2026-08-20-medallion-bronze-architecture-design.md](./2026-08-20-medallion-bronze-architecture-design.md)
- [docs/ASSESSMENT_FROM_PDF.md](../../../docs/ASSESSMENT_FROM_PDF.md)
- [data-model.md](../../../data-model.md)
- [database/schema.sql](../../../database/schema.sql) — update FQN to `de_assessment.bronze.*` during implementation

---

## 14. Column defaults and generated values

**Question:** Use DB defaults / generated columns to avoid repetitive stamping in ingest code?

### Feasibility by column

| Column | DEFAULT / GENERATED? | Feasibility | Benefit |
|--------|----------------------|-------------|---------|
| `_ingest_timestamp` | `DEFAULT current_timestamp()` on entity tables | Possible in Delta DDL | **Low** — Autoloader batch still sets explicitly for testability and consistent batch timing |
| `_batch_id` | DEFAULT | **No** — varies per job run | N/A |
| `_source_file` | DEFAULT | **No** — from Autoloader `_metadata.file_path` per row | N/A |
| `_delivery_pattern` | DEFAULT | **No** — from `source_config` row | N/A |
| `_row_hash` | GENERATED ALWAYS AS | **Poor fit** — hash over many cols; harder to unit test; Spark expression brittle | **Low** |
| `_rescued_data` | DEFAULT | **No** — Autoloader rescued column | N/A |
| `source_config.updated_at` | `DEFAULT current_timestamp()` | **Yes** | **Medium** — seed MERGE can omit on insert |
| `ingest_manifest.started_at` | set at run start in code | Code only | — |
| `ingest_manifest.completed_at` | nullable until success/fail | Code only | — |

### Decision

**Do not rely on generated columns for entity metadata.** Repetition is eliminated in **one place**: shared `bronze/ingest.py` functions (`add_metadata_columns()`, `compute_row_hash()`) called by all three entrypoints — same pattern as Intelo centralizing logic in the pipeline library, not DDL magic.

**Do use SQL DEFAULT where audit-only and static:**

```sql
-- source_config only
updated_at TIMESTAMP DEFAULT current_timestamp()
```

**Benefit summary:** Generated/DEFAULT saves little on entity tables (most metadata is per-run or per-file). Shared Python helpers give **high benefit, full testability** (TDD negative cases on hash and metadata). DDL defaults on audit columns only — **small win, low cost**.

---

## 15. Operational workflow and tooling

### How we drive bronze (recommended — not overkill)

| Phase | Who / what | Action |
|-------|------------|--------|
| **Develop** | Local + TDD | Red-green unit/spark tests → `bundle validate` |
| **PR** | GitHub Actions | `validate.yml` only (lint + unit) — **no auto-deploy** |
| **Deploy to CE** | Developer | Local `bundle deploy -t dev` **or** manual `deploy-ce.yml` workflow_dispatch |
| **Bootstrap** | Developer | `bundle run job_bronze_bootstrap` once (idempotent) |
| **Ingest** | Developer / schedule | Run per-table jobs or `ingest_all` smoke |
| **Verify** | AI Dev Kit MCP + CLI | Query manifest, row counts, `source_config` |

**Auto-deploy on merge to main:** **Overkill for this assessment.** CE has fair-use limits, one target, and [docs/deploy-strategy.md](../../../docs/deploy-strategy.md) already recommends validate-on-PR + manual deploy. Merge-triggered deploy adds little value and risks quota burn.

### Tooling matrix (plugin + AI Dev Kit + skills)

| Task | Primary tool |
|------|----------------|
| Bundle authoring / serverless jobs | **Databricks plugin** → `databricks-dabs`, `databricks-jobs` skills |
| UC DDL, volumes, grants patterns | **Databricks plugin** → `databricks-unity-catalog` skill |
| CLI deploy / run / validate | **`deploy-ce-job`** skill + `de-assessment-ce` profile |
| Post-deploy verification (tables, manifest SQL) | **AI Dev Kit MCP** (`databricks-de-assessment`) or `databricks experimental aitools tools query` |
| Explore catalog / schema before coding | **AI Dev Kit MCP** or CLI with unity-catalog skill |
| Local pytest tiers | **`medallion-pipeline-local-test`** skill |
| Implementation TDD loop | **Superpowers** `test-driven-development` |
| Prompt / artifact capture | **`assessment-artifacts`** skill |

**Rule:** Plugin skills guide *how* to write and deploy; MCP verifies *what* landed in the workspace after deploy. Use both — not either/or.

### Typical bronze delivery loop (implementation phase)

```
1. Write failing test (unit)
2. Implement in bronze/ shared lib
3. ./databricks/scripts/run_job_tests.sh  (unit → spark)
4. source scripts/env.sh && cd databricks/bundle && databricks bundle validate -t dev
5. databricks bundle deploy -t dev          # deploy-ce-job skill
6. databricks bundle run job_bronze_bootstrap -t dev
7. MCP/CLI: SELECT * FROM de_assessment.bronze.source_config
8. databricks bundle run job_bronze_ingest_all -t dev  (when test CSVs exist)
9. MCP/CLI: SELECT * FROM de_assessment.bronze.ingest_manifest ORDER BY started_at DESC LIMIT 5
10. Document run IDs in ai-prompts/08-testing-debugging-data.md
```

GitHub deploy workflow remains **manual trigger only** unless assessment requirements change.

---

**Review gate:** Approve or request edits before implementation plan (`writing-plans` skill).
