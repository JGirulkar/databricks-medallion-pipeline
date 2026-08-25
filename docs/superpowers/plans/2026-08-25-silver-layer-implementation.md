# Silver Layer Implementation Plan

> **For agentic workers:** Use **inline** `superpowers:executing-plans` in the parent session — **not** subagent-driven-development (cost/token budget). Work task-by-task with checkpoints. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an incremental Silver layer that consumes Bronze CDF via streaming checkpoints, conforms entities in RI-safe order (products → customers → orders), enforces Intelo-lite DQ from `source_config.dq_schema` VARIANT (quarantine on failure), unifies run history in `ops.pipeline_manifest` (including Bronze migration), and extends sample data for extended validator coverage.

**Architecture:** A shared `silver/` library implements CDF `availableNow` streams per entity checkpoint, hash-based merge with dimension soft deletes, validators/checks driven by UC VARIANT config, and quarantine writes. `conform_all.py` orchestrates entities in order; table-update triggers invoke one job. Bronze `manifest.py` migrates to `ops.pipeline_manifest` in the same delivery.

**Tech Stack:** Python 3.11+, PySpark, Delta Lake (CDF), Unity Catalog VARIANT, Databricks Asset Bundles, uv, pytest, Ruff.

**Spec:** [2026-08-25-silver-layer-design.md](../specs/2026-08-25-silver-layer-design.md)

## Global Constraints

- Databricks profile is **only** `de-assessment-ce`; `source scripts/env.sh` and pass `--profile de-assessment-ce` on every CLI command.
- Serverless bundle tasks — omit `new_cluster`, `job_cluster_key`, `existing_cluster_id`.
- Silver **enforce only** — valid rows → `silver.{entity}`; failures → `silver.quarantine` (never delete from lake).
- **Ordered conform** — `products` → `customers` → `orders` in one job run for RI; no three independent production triggers.
- Per-entity Silver CDF checkpoints under `/Volumes/de_assessment/ops/checkpoints/silver/{entity}/`.
- `dq_schema` on `de_assessment.config.source_config` — lite Intelo wire shape; no `databricks-schema` package.
- Intelo reference `/home/jay-ajaykumar/Desktop/Projects/Intelo.ai/retail-agents-backend` is read-only.
- Red → green → refactor; skipped unit/Spark tests are defects.
- Do not auto-deploy on merge; CE deploy remains manual.

## Execution mode (cost-conscious)

- **Inline only** — one agent session implements tasks sequentially; no per-task subagents.
- **Checkpoint between tasks** — run tests/lint, brief status to user, then next task.
- Same rigor as subagent-driven (red → green → refactor) without duplicate model context.

## Commit cadence (assessment rubric)

The PDF expects visible iteration in git history (accept → test → fix → refine). **Do not squash** silver work into one commit.

| When | Commit type | Example message |
|------|-------------|-----------------|
| End of each task (plan Step 7) | Feature slice | `feat(silver): add intelo-lite validators` |
| Test failure → fix | Fix | `fix(silver): correct fk_exists join for soft-deleted parents` |
| Ruff / CI on PR | Fix | `fix(silver): satisfy ruff on conform entrypoints` |
| CE deploy iteration | Test / fix | `test(silver): record ce conform quarantine counts` |
| Docs / prompts only | Docs | `docs(silver): update dq strategy and prompt history` |

Rules:

- **Minimum:** one commit per plan task that touches code.
- **Additional commits** whenever tests, lint, or CE smoke fail and are fixed — separate `fix(...)` commits (not amend unless pre-commit hook only).
- **Bronze manifest migration** commits stay in bronze-scoped messages even though done in this plan.
- Gold / dashboard chats follow the same cadence.

---

## File Map

### Create

- `databricks/jobs/silver/pyproject.toml`
- `databricks/jobs/silver/src/silver/__init__.py`
- `databricks/jobs/silver/src/silver/config.py` — FQNs, `load_dq_schema`, orchestration order
- `databricks/jobs/silver/src/silver/schemas.py` — Silver entity + quarantine + metrics StructTypes
- `databricks/jobs/silver/src/silver/manifest.py` — `PipelineManifestRecord`, append to `ops.pipeline_manifest`
- `databricks/jobs/silver/src/silver/validators.py` — Intelo-lite column predicates + `annotate_violations`
- `databricks/jobs/silver/src/silver/checks.py` — uniqueness, `not_null`, `fk_exists`
- `databricks/jobs/silver/src/silver/quarantine.py` — append to `silver.quarantine`
- `databricks/jobs/silver/src/silver/metrics.py` — append `silver.dq_metrics`
- `databricks/jobs/silver/src/silver/conform.py` — snapshot/incremental merge + soft delete
- `databricks/jobs/silver/src/silver/cdf.py` — streaming CDF consumer + `foreachBatch` wiring
- `databricks/jobs/silver/src/silver/main.py` — `run_entity_conform`, `run_conform_all`, `parse_catalog`
- `databricks/jobs/silver/src/bootstrap_silver.py`
- `databricks/jobs/silver/src/conform_all.py`
- `databricks/jobs/silver/src/conform_customers.py` / `conform_orders.py` / `conform_products.py`
- `databricks/jobs/silver/tests/test_config.py`
- `databricks/jobs/silver/tests/test_validators.py`
- `databricks/jobs/silver/tests/test_checks.py`
- `databricks/jobs/silver/tests/test_manifest.py`
- `databricks/jobs/silver/tests/test_conform.py`
- `databricks/jobs/silver/tests/test_quarantine.py`
- `databricks/bundle/resources/silver.job.yml`

### Modify

- `databricks/pyproject.toml` — add `jobs/silver` workspace member; ruff per-file-ignores for entrypoints
- `databricks/jobs/bronze/src/bronze/config.py` — `pipeline_manifest_table()`; deprecate `manifest_table()` alias or redirect
- `databricks/jobs/bronze/src/bronze/manifest.py` — write `layer=bronze` to `ops.pipeline_manifest`
- `databricks/jobs/bronze/src/bronze/bootstrap.py` — `ops.pipeline_manifest` DDL; `source_config.dq_schema` column
- `databricks/jobs/bronze/tests/test_manifest.py` / `test_config.py` / `test_bootstrap.py`
- `databricks/jobs/data_generation/src/generate_sample_data.py` — extended DQ issues (§5.3)
- `databricks/jobs/data_generation/tests/test_dq_spec.py`
- `scripts/bronze_e2e.py` — query `ops.pipeline_manifest`
- `.cursor/skills/bronze-e2e-ce/SKILL.md` — manifest query path
- `.github/workflows/validate.yml` — silver unit tests
- `database/schema.sql` — silver tables + `ops.pipeline_manifest`
- `data-quality-strategy.md` — RI via ordered orchestrator
- `cursor-workflow/task-breakdown.md` — silver progress
- `ai-prompts/05-silver-quality.md` — P-entries

---

### Task 1: Unified `ops.pipeline_manifest` + Bronze migration

**Files:**
- Modify: `databricks/jobs/bronze/src/bronze/config.py`
- Modify: `databricks/jobs/bronze/src/bronze/manifest.py`
- Modify: `databricks/jobs/bronze/src/bronze/bootstrap.py`
- Modify: `databricks/jobs/bronze/tests/test_config.py`
- Modify: `databricks/jobs/bronze/tests/test_manifest.py`
- Modify: `databricks/jobs/bronze/tests/test_bootstrap.py`

**Interfaces:**
- Produces: `pipeline_manifest_table(catalog) -> str`, `PipelineManifestRecord` (bronze layer uses same column layout as spec §4.1), `append_pipeline_manifest(spark, record)`.
- Consumes: existing `ManifestRecord` fields from ingest; maps `batch_id` → `run_id`, adds `layer="bronze"`.

- [ ] **Step 1: Write failing config test for `pipeline_manifest_table`**

```python
from bronze.config import pipeline_manifest_table

def test_pipeline_manifest_table_fqn() -> None:
    assert pipeline_manifest_table() == "de_assessment.ops.pipeline_manifest"
```

- [ ] **Step 2: Run test — expect FAIL**

```bash
cd databricks && uv run pytest jobs/bronze/tests/test_config.py::test_pipeline_manifest_table_fqn -q
```

- [ ] **Step 3: Add FQN helper and pipeline manifest DDL in bootstrap**

Add to `config.py`:

```python
PIPELINE_MANIFEST_TABLE_NAME = "pipeline_manifest"

def pipeline_manifest_table(catalog: str = DEFAULT_CATALOG) -> str:
    return f"{catalog}.{OPS_SCHEMA}.{PIPELINE_MANIFEST_TABLE_NAME}"
```

Add `_pipeline_manifest_ddl(catalog)` in `bootstrap.py` with all columns from spec §4.1. Keep `_ingest_manifest_ddl` generation removed from `bootstrap_ddl()` output (table deprecated, not dropped on CE).

- [ ] **Step 4: Refactor `manifest.py` to append to `ops.pipeline_manifest`**

Map existing `ManifestRecord` to unified row: `run_id=batch_id`, `layer="bronze"`, `entity_name=source_name`, silver-only columns (`rows_quarantined`, `parent_run_id`) = null/0.

- [ ] **Step 5: Update manifest tests** — FQN, column order, `layer=bronze` in `as_row()`.

- [ ] **Step 6: Run bronze unit tests**

```bash
./databricks/scripts/run_job_tests.sh bronze -m unit -q
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add databricks/jobs/bronze
git commit -m "feat(ops): unify pipeline manifest and migrate bronze writes"
```

---

### Task 2: Silver package scaffold + config / DQ VARIANT parsing

**Files:**
- Create: `databricks/jobs/silver/pyproject.toml`, `src/silver/__init__.py`, `src/silver/config.py`
- Create: `databricks/jobs/silver/tests/test_config.py`
- Modify: `databricks/pyproject.toml`

**Interfaces:**
- Produces: `silver_table(name, catalog)`, `quarantine_table(catalog)`, `dq_metrics_table(catalog)`, `silver_checkpoint_path(entity, catalog)`, `ORCHESTRATION_ORDER`, `DqSchema`, `load_dq_schema(spark, source_name, catalog)`.
- Consumes: `source_config_table()` from bronze config pattern (read `dq_schema` VARIANT column).

- [ ] **Step 1: Add silver workspace member**

```toml
[tool.uv.workspace]
members = ["jobs/data_generation", "jobs/bronze", "jobs/silver"]
```

- [ ] **Step 2: Write failing tests** — FQN helpers, `ORCHESTRATION_ORDER == ("products", "customers", "orders")`, VARIANT JSON parsed to `DqSchema` with `columns` and `checks`.

- [ ] **Step 3: Implement `config.py`** — lite dataclasses (`ColumnRule`, `EntityCheck`, `DqSchema`) parsing dict from `row.dq_schema` (Spark VARIANT → Python via `asDict(recursive=True)`).

- [ ] **Step 4: Run tests**

```bash
cd databricks && uv sync --all-packages --all-groups --no-group cluster
uv run pytest jobs/silver/tests/test_config.py -m unit -q
```

- [ ] **Step 5: Commit**

```bash
git add databricks/jobs/silver databricks/pyproject.toml
git commit -m "feat(silver): add package scaffold and dq_schema parsing"
```

---

### Task 3: Intelo-lite validators + entity checks

**Files:**
- Create: `databricks/jobs/silver/src/silver/validators.py`, `checks.py`
- Create: `databricks/jobs/silver/tests/test_validators.py`, `test_checks.py`

**Interfaces:**
- Produces: `annotate_violations(df, dq_schema) -> DataFrame` with `_violations` array column; `apply_entity_checks(df, dq_schema, spark, catalog) -> DataFrame`.
- Consumes: `DqSchema` from Task 2.

- [ ] **Step 1: Write failing validator tests** — `not_null`, `enum`, `format_email`, `minimum`, `max_date`; multiple violations per row; empty violations on clean row.

- [ ] **Step 2: Implement `validators.py`** — value-stage predicates only (adapt Intelo `column_predicates` / `annotate_violations`; skip wire-stage).

- [ ] **Step 3: Write failing check tests** — uniqueness flags duplicate `order_id`; `fk_exists` flags row when parent missing from `silver.customers`.

- [ ] **Step 4: Implement `checks.py`** — window duplicate detection; FK left-join anti-join for `fk_exists` (parent tables `silver.customers` / `silver.products`, `NOT _is_deleted`).

- [ ] **Step 5: Run spark tests**

```bash
uv run pytest jobs/silver/tests/test_validators.py jobs/silver/tests/test_checks.py -m spark -q
```

- [ ] **Step 6: Commit**

```bash
git add databricks/jobs/silver
git commit -m "feat(silver): add intelo-lite validators and entity checks"
```

---

### Task 4: Quarantine, metrics, pipeline manifest (silver layer)

**Files:**
- Create: `databricks/jobs/silver/src/silver/quarantine.py`, `metrics.py`, `manifest.py`, `schemas.py`
- Create: `databricks/jobs/silver/tests/test_quarantine.py`, `test_manifest.py`

**Interfaces:**
- Produces: `write_quarantine(spark, df, entity_name, run_id, ...)`, `append_dq_metrics(spark, rows)`, `append_silver_manifest(spark, record)`.
- Consumes: `_violations` column; `PipelineManifestRecord` with `layer="silver"`.

- [ ] **Step 1: Define StructTypes** in `schemas.py` for quarantine, dq_metrics, manifest rows.

- [ ] **Step 2: Test quarantine write** — failing row lands in temp table with `violations` preserved.

- [ ] **Step 3: Implement `quarantine.py`** — mirror Intelo invalid sink shape (entity_name, primary_key, data JSON, violations array).

- [ ] **Step 4: Implement `metrics.py`** — one row per check category per entity run.

- [ ] **Step 5: Implement silver `manifest.py`** — append `layer=silver` rows to `ops.pipeline_manifest`.

- [ ] **Step 6: Run tests and commit**

```bash
uv run pytest jobs/silver/tests/test_quarantine.py jobs/silver/tests/test_manifest.py -m spark -q
git commit -m "feat(silver): add quarantine, metrics, and manifest writers"
```

---

### Task 5: Conform merge (hash, soft delete, incremental orders)

**Files:**
- Create: `databricks/jobs/silver/src/silver/conform.py`
- Create: `databricks/jobs/silver/tests/test_conform.py`

**Interfaces:**
- Produces: `conform_snapshot_batch(batch_df, entity, spark, catalog) -> DataFrame` (latest per PK + soft deletes); `conform_incremental_batch(batch_df, entity, spark, catalog) -> DataFrame` (dedupe order_id); `merge_to_silver(df, entity, spark, catalog)`.
- Consumes: Bronze metadata columns `_batch_id`, `_row_hash` (customers), business columns from `bronze.schemas`.

- [ ] **Step 1: Spark tests** — snapshot: hash change updates row; missing PK from latest batch sets `_is_deleted=true`. Incremental: duplicate `order_id` in batch deduped.

- [ ] **Step 2: Implement `conform.py`** — Delta `merge` with `whenMatchedUpdate` / `whenNotMatchedInsert`; products `_row_hash` via same sha2 pattern as bronze `metadata.py`.

- [ ] **Step 3: Valid rows get `quality_check_result='PASS'`** before merge.

- [ ] **Step 4: Run tests**

```bash
uv run pytest jobs/silver/tests/test_conform.py -m spark -q
```

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(silver): add conform merge and soft delete semantics"
```

---

### Task 6: CDF streaming consumer + entity conform pipeline

**Files:**
- Create: `databricks/jobs/silver/src/silver/cdf.py`, `main.py`
- Modify: `databricks/jobs/silver/tests/` — integration-style spark test with injected batch callback

**Interfaces:**
- Produces: `run_entity_conform(entity_name, catalog, parent_run_id=None) -> None`; `run_cdf_stream(entity, process_batch)`.
- Consumes: Tasks 3–5; `load_dq_schema`; checkpoint path from config.

- [ ] **Step 1: Implement `cdf.py`** — `readStream` + `readChangeFeed` + `availableNow` + `foreachBatch`; filter to insert/update post-images.

- [ ] **Step 2: Implement `run_entity_conform`** — conform → annotate → split valid/quarantine → merge → metrics → manifest; on exception append failed manifest and re-raise.

- [ ] **Step 3: Implement `run_conform_all`** — loop `ORCHESTRATION_ORDER`; on entity failure log, write failed manifest, **break** (skip remaining entities).

- [ ] **Step 4: Spark test with fake `foreachBatch`** — inject DataFrame, assert quarantine count and merge count without real streaming.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(silver): wire cdf streaming conform pipeline"
```

---

### Task 7: Silver bootstrap + `dq_schema` seeds

**Files:**
- Create: `databricks/jobs/silver/src/silver/bootstrap.py`, `src/bootstrap_silver.py`
- Modify: `databricks/jobs/bronze/src/bronze/bootstrap.py` — add `dq_schema` column to `source_config` DDL if not in Task 1

**Interfaces:**
- Produces: `bootstrap_silver(spark, catalog)` — silver schema, entity tables (CDF on), quarantine, dq_metrics, silver checkpoint dirs, seed `dq_schema` VARIANT on `source_config`.

- [ ] **Step 1: DDL** for `silver.customers|orders|products`, `silver.quarantine`, `silver.dq_metrics` per spec §6.

- [ ] **Step 2: Seed `dq_schema`** JSON for all three entities — all four check categories + extended column rules (§5.2).

- [ ] **Step 3: `mkdirs` for silver checkpoint paths**

- [ ] **Step 4: Unit test** — `bootstrap_ddl()` contains `silver.quarantine`, `delta.enableChangeDataFeed`, `dq_schema`.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(silver): add bootstrap ddl and dq_schema seeds"
```

---

### Task 8: Entrypoints, bundle jobs, Bronze E2E manifest query

**Files:**
- Create: `conform_all.py`, `conform_*.py`, `databricks/bundle/resources/silver.job.yml`
- Modify: `scripts/bronze_e2e.py`, `.cursor/skills/bronze-e2e-ce/SKILL.md`

- [ ] **Step 1: Thin entrypoints** — `conform_all` calls `run_conform_all`; singles call `run_entity_conform` (debug only).

- [ ] **Step 2: `silver.job.yml`** — `job_silver_bootstrap`, `job_silver_conform_all` with `table_update` triggers on three bronze tables (PAUSED), serverless.

- [ ] **Step 3: Bundle validate**

```bash
source scripts/env.sh
cd databricks/bundle
databricks bundle validate --strict -t dev --profile de-assessment-ce
```

- [ ] **Step 4: Update bronze E2E** to query `ops.pipeline_manifest WHERE layer='bronze'`.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(silver): add bundle jobs and conform entrypoints"
```

---

### Task 9: Data generation extensions

**Files:**
- Modify: `databricks/jobs/data_generation/src/generate_sample_data.py`
- Modify: `databricks/jobs/data_generation/tests/test_dq_spec.py`

- [ ] **Step 1: Add `DQ_ISSUE_COUNTS` keys** — invalid_email, invalid_segment, invalid_status, non_positive_quantity, negative_price, future_signup_date (counts from spec §5.3).

- [ ] **Step 2: Implement issue injectors** after clean base generation.

- [ ] **Step 3: Update `test_dq_spec.py`** — assert new counts.

- [ ] **Step 4: Run data_gen tests**

```bash
uv run pytest jobs/data_generation/tests/ -m unit -q
```

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(data-gen): extend intentional dq issues for silver validators"
```

---

### Task 10: CI, schema docs, CE smoke, assessment artifacts

**Files:**
- Modify: `.github/workflows/validate.yml`, `database/schema.sql`, `data-quality-strategy.md`, `cursor-workflow/task-breakdown.md`
- Create: `ai-prompts/05-silver-quality.md`

- [ ] **Step 1: CI** — add `jobs/silver/tests/` to unit pytest invocation.

- [ ] **Step 2: Document schema** — silver tables, `ops.pipeline_manifest`, deprecate `bronze.ingest_manifest` in comments.

- [ ] **Step 3: Local full test**

```bash
source scripts/env.sh
./databricks/scripts/run_job_tests.sh bronze -m unit -q
./databricks/scripts/run_job_tests.sh silver -m "unit or spark" -q
./scripts/lint.sh
```

- [ ] **Step 4: CE deploy smoke** (manual) — bootstrap silver, bronze ingest, `job_silver_conform_all`; verify manifest silver rows, quarantine counts vs intentional issues, `silver.dq_metrics` pass_pct.

- [ ] **Step 5: Layer-completion gate** — `.cursor/skills/layer-completion/SKILL.md`; update `ai-prompts/05-silver-quality.md`.

- [ ] **Step 6: Commit docs**

```bash
git commit -m "docs(silver): ci, schema, and assessment artifacts"
```

---

## Plan Self-Review

| Spec section | Task |
|--------------|------|
| §4 Unified manifest + Bronze migrate | Task 1, 8 |
| §5 dq_schema VARIANT + extended validators | Task 2, 3, 7, 9 |
| §6 Silver tables, quarantine, metrics | Task 4, 7 |
| §7 CDF + checkpoints | Task 6 |
| §8 Conform semantics | Task 5 |
| §9 DQ enforce + four checks | Task 3, 6 |
| §10 Ordered orchestrator + triggers | Task 6, 8 |
| §11 Assessment alignment / RI | Task 3 (`fk_exists`), 6 (order) |
| §13 Testing | All task test steps, Task 10 |

**Placeholder scan:** none — all tasks have concrete paths and commands.

**Type consistency:** `run_entity_conform`, `DqSchema`, `pipeline_manifest_table` defined in Tasks 2, 1, 6 before use.

---

## Execution Handoff

**Chosen:** inline execution with task checkpoints and incremental commits (see **Execution mode** and **Commit cadence** above). Start with Task 1 when implementation begins in a new session or continuation of this chat.
