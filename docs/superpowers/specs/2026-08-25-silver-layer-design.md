# Silver Layer Design — DE Medallion Assessment

**Status:** Approved — implementation plan 2026-08-25  
**Date:** 2026-08-25  
**Parent anchor:** [2026-08-20-medallion-bronze-architecture-design.md](./2026-08-20-medallion-bronze-architecture-design.md)  
**Scope:** Silver conformance, DQ enforcement (quarantine), unified pipeline manifest (Bronze migration), bundle jobs. Gold and Dashboard are downstream consumers only.

**Environment:** Databricks Free Edition, profile `de-assessment-ce`, Asset Bundle under `databricks/bundle/`

---

## 1. Goals

1. **Incremental Silver** — process only Bronze CDF deltas since the last successful Silver run (no full Bronze table scans).
2. **Conform** — merge Bronze changes into Silver entity tables with hash-based updates and dimension soft deletes.
3. **Enforce DQ** — valid rows land in `silver.{entity}`; failing rows go to `silver.quarantine` (reject from valid Silver, never delete from the lake).
4. **Observable pipeline** — one `ops.pipeline_manifest` for Bronze, Silver, and Gold run history.
5. **Gold-ready Silver** — active dimension rows (`_is_deleted = false`), clean orders in valid tables, CDF enabled on Silver for optional incremental Gold later.
6. **Assessment-aligned** — all four DQ checks (completeness, uniqueness, referential integrity, type/business logic) with metrics report; RI enforced via **ordered** conform (products → customers → orders).

---

## 2. Architectural approach

**Chosen:** CDF streaming checkpoint + **ordered orchestrator job** + shared `silver/` library (mirrors Bronze Auto Loader pattern).

| Component | Role |
|-----------|------|
| `bootstrap_silver.py` | Silver DDL, `dq_schema` VARIANT seeds, unified manifest table |
| `silver/` Python package | CDF consume, conform merge, validators, quarantine, metrics, manifest append |
| `conform_all.py` | **Primary entrypoint** — products → customers → orders in one run (RI-safe order) |
| `conform_{customers,orders,products}.py` | Thin manual/debug entrypoints calling `run_entity_conform()` |
| Bronze migration | `bronze/manifest.py` → `ops.pipeline_manifest` (`layer=bronze`); deprecate `bronze.ingest_manifest` |
| Data generation | Extend intentional bad rows for extended validators (§5.2, §14) |

**Intelo reference (read-only):** `ingestion_writer/validators.py`, `_write_invalid_rows` quarantine pattern — adapted at Silver with enforce-only policy.

---

## 3. Catalog, schemas, and volumes

```
de_assessment
├── config
│   └── source_config              # existing ingest config + dq_schema VARIANT
├── bronze
│   ├── customers | orders | products   # append-only, CDF on
│   └── ingest_manifest            # DEPRECATED — migrate writes to ops.pipeline_manifest
├── silver
│   ├── customers | orders | products   # valid conformed rows, CDF on
│   ├── quarantine                 # unified invalid sink
│   └── dq_metrics                 # per-run % passed per check category
├── ops
│   ├── checkpoints/               # existing Bronze Auto Loader + Silver CDF checkpoints
│   │   ├── products/ | customers/ | orders/     # Bronze
│   │   └── silver/{entity}/                     # Silver CDF position
│   └── pipeline_manifest          # unified run log (bronze | silver | gold)
├── landing / gold                 # unchanged in this spec
```

### Why `ops.pipeline_manifest` (not `config`, not `audit`, not default)

| Schema | Use in this project | Manifest fit |
|--------|---------------------|--------------|
| **ops** | Checkpoints, runtime infrastructure | **Yes** — run telemetry alongside checkpoints |
| **config** | `source_config` ingest/DQ rules | No — config is declarative, not per-run history |
| **audit** | Could work semantically | Adds a schema for one table; `ops` already exists on CE |
| **default** | Not used for medallion tables | Avoid |

---

## 4. Unified pipeline manifest

### 4.1 Table: `de_assessment.ops.pipeline_manifest`

Single observability table for all layers. Replaces `bronze.ingest_manifest` after Bronze migration.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | `STRING` | UUID per job run (replaces bronze-only `batch_id` as primary run key) |
| `layer` | `STRING` | `bronze` \| `silver` \| `gold` |
| `entity_name` | `STRING` | Source/entity: `customers`, `orders`, `products`, or gold table name |
| `parent_run_id` | `STRING` | Optional link to upstream run (e.g. silver → bronze `run_id`) |
| `delivery_pattern` | `STRING` | Nullable; bronze only (`full_snapshot` \| `incremental`) |
| `source_path` | `STRING` | Nullable; bronze raw path or checkpoint path |
| `files_processed` | `INT` | Bronze file count; 0 for silver/gold |
| `rows_read` | `BIGINT` | Rows in CDF batch / source scan |
| `rows_written` | `BIGINT` | Rows merged to valid target |
| `rows_quarantined` | `BIGINT` | Rows sent to quarantine (silver) |
| `rows_rescued` | `BIGINT` | Bronze `_rescued_data` count; 0 for silver |
| `delta_version_before` | `BIGINT` | Nullable; bronze or silver table version before |
| `delta_version_after` | `BIGINT` | Nullable; version after |
| `started_at` | `TIMESTAMP` | Run start |
| `completed_at` | `TIMESTAMP` | Nullable until success |
| `status` | `STRING` | `success` \| `failed` |
| `error_message` | `STRING` | Nullable failure detail |

**Bronze migration (in Silver implementation scope):**

1. Create `ops.pipeline_manifest` in silver bootstrap (or shared ops DDL).
2. Refactor `bronze/manifest.py` → append `layer=bronze` rows to `ops.pipeline_manifest`; keep `run_id` = existing `batch_id` for traceability.
3. Update bronze tests for new FQN and column mapping.
4. Stop writing `bronze.ingest_manifest`; table may remain on CE for history; no new rows.
5. Update E2E scripts/skills to query `ops.pipeline_manifest WHERE layer='bronze'`.

---

## 5. Configuration — `source_config.dq_schema` VARIANT

Extend `de_assessment.config.source_config` with **`dq_schema VARIANT`**.

Lite Intelo v1.0 wire shape (seeded at bootstrap, `MERGE WHEN NOT MATCHED` only):

```json
{
  "$schemaVersion": "1.0",
  "validationMode": "enforce",
  "columns": [
    {
      "name": "email",
      "type": "string",
      "nullable": true,
      "validation": { "kind": "string", "format": "email" }
    }
  ],
  "checks": [
    { "kind": "not_null", "column": "customer_id", "category": "completeness" },
    { "kind": "uniqueness", "column": "order_id", "category": "uniqueness" }
  ]
}
```

- **`columns[]`** → `silver/validators.py` column predicates (Intelo-lite — §5.2).
- **`checks[]`** → `silver/checks.py` entity rules (`not_null`, `uniqueness`, `fk_exists`).
- **`validationMode`** → always `enforce` at Silver (no shadow branch).

Runtime: `load_dq_schema(spark, source_name)` reads VARIANT → lite dataclasses in `silver/config.py` (no `databricks-schema` package).

### 5.2 Extended column validations (Intelo-lite subset)

Bronze is already typed — implement **value-stage** predicates only (skip wire-stage `*_parseable` rules). Supported `validation.kind` values seeded in `dq_schema`:

| Kind | Supported rules | Example assessment use |
|------|-----------------|------------------------|
| **string** | `min_length`, `max_length`, `pattern`, `format` (`email`), `enum` | Bad emails, invalid `customer_segment`, invalid `order_status` |
| **numeric** | `minimum`, `maximum`, `exclusive_minimum`, `exclusive_maximum`, `multiple_of` | `quantity > 0`, `price >= 0`, `lifetime_value >= 0` |
| **boolean** | (typed column — no extra rules) | — |
| **datetime** | `min_date`, `max_date`, `format` | `signup_date` not in future; valid date strings |

Entity **`checks[]`** kinds:

| Kind | Category | Notes |
|------|----------|-------|
| `not_null` | completeness | Critical PK/FK columns |
| `uniqueness` | uniqueness | `customer_id`, `order_id` within entity |
| `fk_exists` | referential | `orders.customer_id` → `silver.customers`, `orders.product_id` → `silver.products` — **runs only after products + customers conform in same job** (§10) |

Copy predicate logic from Intelo `validators.py` (read-only); trim to the rule kinds above — one module, no pydantic envelope package.

### 5.3 Data generation extensions

Extend `generate_sample_data.py` with additional intentional issues so extended validators are testable (new keys in `DQ_ISSUE_COUNTS`, documented in `DATA_GENERATION_NOTES.md`):

| Issue | Target check | Suggested count |
|-------|--------------|-----------------|
| Invalid email format (non-null) | `format: email` | ~30 |
| Invalid `customer_segment` | `enum` | ~20 |
| Invalid `order_status` | `enum` | ~20 |
| `quantity <= 0` | `minimum: 1` | ~25 |
| Negative `price` / `unit_price` | `minimum: 0` | ~15 |
| Future `signup_date` | `max_date: today` | ~15 |

Keep base assessment counts (~700 rows) **and** add these (~105) — total ~800 problematic rows, still ~0.8% at order scale. Implementation plan may phase: base DQ first, extended issues second commit.

---

## 6. Silver entity tables

### 6.1 Valid tables: `silver.customers`, `silver.orders`, `silver.products`

Business columns match assessment schema (same types as Bronze source, without Bronze ingest metadata columns).

**Silver control columns:**

| Column | Purpose |
|--------|---------|
| `quality_check_result` | `PASS` on all rows in valid tables (enforce-only — failures never land here) |
| `_row_hash` | Content hash; customers reuse Bronze `_row_hash`; products computed in Silver |
| `_is_deleted` | `BOOLEAN` default `false`; soft delete for dimensions |
| `_silver_updated_at` | Last Silver merge timestamp |
| `_bronze_batch_id` | Provenance from Bronze `_batch_id` |

**Table properties:** `delta.enableChangeDataFeed = true` on all three entity tables.

**No persisted I/U/D column** — Delta CDF records insert/update/delete technically; business I/U/D counts may appear in `pipeline_manifest` / logs only.

### 6.2 Quarantine: `silver.quarantine`

| Column | Purpose |
|--------|---------|
| `entity_name` | `customers` \| `orders` \| `products` |
| `primary_key` | Business PK as string |
| `data` | `STRING` JSON snapshot of row |
| `violations` | `ARRAY<STRUCT<category, rule, column, value>>` |
| `quarantined_at` | `TIMESTAMP` |
| `silver_run_id` | Links to `ops.pipeline_manifest.run_id` |
| `bronze_batch_id` | Bronze `_batch_id` provenance |

### 6.3 Metrics: `silver.dq_metrics`

Per entity run, per check category:

| Column | Purpose |
|--------|---------|
| `silver_run_id` | FK to manifest |
| `entity_name` | Entity |
| `check_category` | `completeness` \| `uniqueness` \| `type_logic` \| `referential` |
| `rows_evaluated` | Rows in conform batch |
| `rows_passed` | Rows merged to valid |
| `rows_quarantined` | Rows in quarantine for this category |
| `pass_pct` | `rows_passed / rows_evaluated * 100` |
| `run_at` | Timestamp |

---

## 7. Incremental consumption — CDF + checkpoints

Per entity checkpoint:

```
/Volumes/de_assessment/ops/checkpoints/silver/{entity}/
```

**Job pattern (per run, event-triggered):**

```python
(
    spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .table(bronze_table(entity))
)
.writeStream.foreachBatch(process_cdf_batch)
.option("checkpointLocation", silver_checkpoint_path)
.trigger(availableNow=True)
.start()
.awaitTermination()
```

`process_cdf_batch(batch_df, batch_id)`:

1. Filter CDF to insert/post-image rows (ignore pre-images except for merge logic if needed).
2. Conform per delivery pattern (§8).
3. `annotate_violations` + entity checks.
4. Split: clean → merge valid Silver; dirty → `silver.quarantine`.
5. Append `silver.dq_metrics` + `ops.pipeline_manifest` (`layer=silver`).
6. Checkpoint advances only on successful `awaitTermination`.

**No** `silver.processing_state` table — checkpoint is the cursor.

---

## 8. Conformance semantics

| Entity | Bronze `delivery_pattern` | Conform logic |
|--------|---------------------------|---------------|
| **customers** | `full_snapshot` | Within CDF batch: latest row per `customer_id` (max `_batch_id` / `_ingest_timestamp`). Hash compare vs Silver → merge insert/update. PKs active in Silver but absent from latest snapshot batch → `UPDATE _is_deleted = true` (soft delete). |
| **products** | `full_snapshot` | Same as customers; compute `_row_hash` in Silver from business columns. |
| **orders** | `incremental` | Dedupe `order_id` within CDF batch; merge insert/update. No soft delete. |

Merge keys: `customer_id`, `product_id`, `order_id` respectively.

---

## 9. DQ validation (Intelo-lite, enforce only)

| Module | Role |
|--------|------|
| `validators.py` | Column predicates from `dq_schema.columns[]` |
| `checks.py` | Entity checks from `dq_schema.checks[]` |
| `quarantine.py` | Append failures to `silver.quarantine` |

**Assessment checks implemented:**

| Check | Implementation | Intentional issues caught |
|-------|------------------|---------------------------|
| Completeness | `not_null` on `email`, `customer_id`, `product_id` | 50 NULL emails, 100 NULL customer_id, 200 NULL product_id |
| Uniqueness | `uniqueness` on `order_id`, `customer_id` | 10 duplicate customer_id, 20 duplicate order_id |
| Type / business logic | `enum`, `format_email`, numeric bounds | Invalid segments, bad emails, invalid quantities/status |
| Referential integrity | `fk_exists` → `silver.customers` / `silver.products` (after ordered conform) | ~50 orphan customer_id, ~30 orphan product_id |
| Type / business logic (extended) | §5.2 enum, min/max, email, dates | Extended data gen issues §5.3 |

Valid rows: `quality_check_result = 'PASS'` (assessment column requirement satisfied on valid tables).

---

## 10. Jobs, orchestration, and triggers

> **Superseded:** Production orchestration is defined in
> `docs/superpowers/specs/2026-08-25-silver-per-entity-orchestration-design.md`
> (per-entity silver jobs + parent refresh on orders). The content below is v1 history.

### 10.1 Why one ordered orchestrator (not three independent triggered jobs)

Referential integrity on `orders` requires `silver.customers` and `silver.products` to reflect the latest conform state **before** order FK checks run. Bronze lands on different schedules (weekly / daily / event-driven), so three independent table-update jobs can run **out of order** and false-quarantine valid orders.

**Chosen:** one **ordered** conform pipeline per job run:

```
products → customers → orders
```

Each step still uses its **own Silver CDF checkpoint** (incremental). Steps with no pending CDF rows exit quickly (empty batch, manifest zero counts, checkpoint unchanged).

| Approach | RI reliable? | Code effort | Fail isolation |
|----------|--------------|-------------|----------------|
| **A) Single `conform_all.py` job (chosen)** | Yes — order enforced in one process | **Low** — one orchestrator + shared lib | One job fails if any step raises; per-entity manifest rows still logged |
| B) Three independent triggered jobs | No — race on async landing | Medium | High isolation |
| C) Multi-task job (3 tasks, dependencies) | Yes | **Higher** — bundle YAML + 3 tasks | Task-level isolation (products ok, orders skipped if customers fails) |

Optional later: **C** if task-level isolation matters without sacrificing RI. For assessment, **A** is sufficient and matches Bronze `ingest_all.py`.

### 10.2 Bundle resources

**File:** `databricks/bundle/resources/silver.job.yml`

| Job key | Entrypoint | Trigger |
|---------|------------|---------|
| `job_silver_bootstrap` | `bootstrap_silver.py` | Manual |
| `job_silver_conform_all` | `conform_all.py` | `table_update` on **any** of `bronze.products`, `bronze.customers`, `bronze.orders` (all three triggers invoke the **same** job; PAUSED initially) |

Serverless tasks (no `new_cluster`), matching Bronze bundle pattern.

**`conform_all.py` orchestration:**

```python
ORCHESTRATION_ORDER = ("products", "customers", "orders")
parent_run_id = new_run_id()
for entity in ORCHESTRATION_ORDER:
    run_entity_conform(entity, catalog, parent_run_id=parent_run_id)
```

- Each entity appends its own `ops.pipeline_manifest` row (`layer=silver`, shared `parent_run_id` optional wrapper run id).
- Per-entity `try/except`: log + manifest `failed` for that entity; **abort remaining entities** if a dimension step fails (orders skipped — avoids RI against stale parents). Products failure → skip customers and orders.

**Manual / debug:** `conform_customers.py` etc. call `run_entity_conform("customers")` alone — **not** for production triggers (RI not guaranteed).

### 10.3 Bronze manifest wiring (same PR)

Refactor `databricks/jobs/bronze/src/bronze/manifest.py` to write `de_assessment.ops.pipeline_manifest` with `layer=bronze`, `run_id` = existing `batch_id`. Update `bronze/config.py` FQN helper (`pipeline_manifest_table()`). Migrate tests and `scripts/bronze_e2e.py` / `bronze-e2e-ce` skill queries to:

```sql
SELECT * FROM de_assessment.ops.pipeline_manifest
WHERE layer = 'bronze'
ORDER BY started_at DESC;
```

Stop appending to `bronze.ingest_manifest`.

---

## 11. Assessment alignment

### 11.1 Requirements matrix

| Assessment requirement | Design answer | Status |
|------------------------|---------------|--------|
| Bronze raw ingest, no cleaning | Bronze layer (done) | Done |
| Silver DQ — completeness | `not_null` checks → quarantine | Planned |
| Silver DQ — uniqueness | Window/count checks → quarantine | Planned |
| Silver DQ — referential integrity | `fk_exists` on orders → quarantine orphans (~80 rows) | Planned (ordered orchestrator §10) |
| Silver DQ — type/business logic | `enum`, email format, bounds → quarantine | Planned |
| Flag bad rows (`quality_check_result`) | Valid rows `PASS`; bad rows in quarantine with `violations` | Planned |
| Quality metrics report (% per check) | `silver.dq_metrics` + manifest row counts | Planned |
| Log ingestion metadata | `ops.pipeline_manifest` all layers | Planned (+ Bronze migrate) |
| Gold: `sales_by_product` | Gold reads `silver.orders` + `silver.products` (active, not deleted) | Enabled |
| Gold: `revenue_by_customer` | Gold reads `silver.orders` + `silver.customers` (`_is_deleted = false`) | Enabled |
| Gold: `customer_segmentation` | Gold from `silver.customers` + order rollups | Enabled |
| Dashboard: top 10 products bar | Query `gold.sales_by_product` | Enabled |
| Dashboard: revenue histogram | Query `gold.revenue_by_customer` | Enabled |
| Dashboard: segmentation pie | Query `gold.customer_segmentation` | Enabled |
| ~700+ intentional issues surfaced | Quarantine: completeness, uniqueness, RI, type logic (+ extended §5.3) | Planned |
| Tests verify DQ catches issues | Unit/spark all four categories + extended validators | Planned |

### 11.2 Referential integrity — ordered conform

RI is **required** by the assessment PDF. Enforcement:

- `fk_exists` checks in `dq_schema.checks[]` for `orders.customer_id` and `orders.product_id`.
- Join targets: `silver.customers` and `silver.products` (active rows, `NOT _is_deleted`).
- Runs only in **step 3** of `conform_all` after products and customers steps complete in the **same job run**.
- Intentional orphans (~50 + ~30 rows) → `silver.quarantine` with `category = referential`.

**Trigger implication:** any Bronze table update invokes the full orchestrator. When only `bronze.orders` changed, products/customers steps may process empty CDF batches (fast no-ops) before orders runs with current dimension snapshots.

### 11.3 Gold and Dashboard enablement

**Gold reads Silver only** — never Bronze.

```sql
-- Example Gold inputs (conceptual)
FROM silver.orders o
JOIN silver.products p ON o.product_id = p.product_id AND NOT p._is_deleted
JOIN silver.customers c ON o.customer_id = c.customer_id AND NOT c._is_deleted
```

- **Quarantined rows** (NULL emails, duplicates, type failures) excluded from valid Silver → excluded from Gold revenue.
- **Soft-deleted dimensions** excluded via `_is_deleted`.
- **Orphan FK orders** quarantined at Silver — excluded from `silver.orders` valid table and from Gold aggregations.

**Silver CDF for Gold:** enabled on Silver tables; Gold assessment scope uses batch refresh from Silver tables. Incremental Gold via `table_changes` on Silver is optional stretch, not required for acceptance.

### 11.4 PDF vs implementation structure

Assessment template lists separate `01_quality_*.py` scripts. Implementation uses **one shared `silver/` package + ordered `conform_all` orchestrator** with shared `validators.py` — functionally equivalent, matches `databricks/jobs/` layout. Prompt history should note mapping.

---

## 12. Error handling

| Scenario | Behavior |
|----------|----------|
| DQ failures | Partial success — valid rows merge; failures quarantine; run `success` if conform completed |
| Conform/merge exception | Manifest `failed`; checkpoint **not** advanced; retry on next trigger |
| Empty CDF batch | Manifest `success`, zero counts |
| Bronze migration dual-write | Not used — cut over manifest writes in one PR |

---

## 13. Testing

| Tier | Scope |
|------|--------|
| **unit** | VARIANT parsing, predicate builders, manifest record validation |
| **spark** | annotate_violations (extended rules), uniqueness, FK, quarantine split, merge + soft delete |
| **cluster (CE)** | Table-update or manual conform after Bronze E2E; manifest rows; quarantine counts vs intentional issues |

Run: `./databricks/scripts/run_job_tests.sh silver -m "unit or spark"`

---

## 14. Implementation boundaries (this chat / plan)

**In scope:**

- Silver package, bootstrap, **ordered `conform_all` job**, bundle YAML
- Bronze manifest migration + bronze job wiring to `ops.pipeline_manifest`
- `dq_schema` VARIANT seeds (extended Intelo-lite rules + RI)
- `silver.quarantine`, `silver.dq_metrics`, entity tables with CDF
- Data generation extensions for new validator test cases (§5.3)
- Thin per-entity entrypoints for manual debug only

**Out of scope:**

- Gold aggregations, Dashboard SQL
- Three independent Silver trigger jobs (superseded by orchestrator §10)
- `bronze.ingest_manifest` DROP (deprecate writes only)
- Auto-deploy on merge

---

## 15. References

- [docs/ASSESSMENT_FROM_PDF.md](../../ASSESSMENT_FROM_PDF.md)
- [2026-08-20-medallion-bronze-architecture-design.md](./2026-08-20-medallion-bronze-architecture-design.md)
- [2026-08-20-bronze-layer-design.md](./2026-08-20-bronze-layer-design.md)
- `data-quality-strategy.md`, `cursor-workflow/spec.md`
- Intelo (read-only): `ingestion_writer/validators.py`, `pipeline.py` (`_write_invalid_rows`)

---

**Review gate:** Approve or request edits before Silver implementation plan (`writing-plans`).
