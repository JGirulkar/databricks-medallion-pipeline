# Silver Per-Entity Orchestration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace monolithic `conform_all` CE production path with three entity-scoped silver jobs and parent-refresh on orders for RI.

**Architecture:** One `table_update` trigger per bronze table → matching silver conform job. Orders job drains products/customers CDF before FK checks. Shared `silver/` library unchanged.

**Tech Stack:** Python 3.11+, PySpark, Delta CDF, Databricks Jobs (Lakeflow), Asset Bundle, CE profile `de-assessment-ce`.

## Global Constraints

- Profile **`de-assessment-ce`** only; `source scripts/env.sh` before CLI.
- Deploy ALL jobs via `scripts/deploy-all-ce-jobs.sh` (upsert, preserve job_ids).
- Bronze boundaries unchanged; Gold reads Silver only.
- Assessment DQ: quarantine bad rows, never delete from lake.
- `max_retries: 0` on silver task definitions.

---

### Task 1: Parent refresh orchestration in `silver.main`

**Files:**
- Modify: `databricks/jobs/silver/src/silver/main.py`
- Modify: `databricks/jobs/silver/src/conform_orders.py`
- Create: `databricks/jobs/silver/tests/test_orders_parent_refresh.py`

- [ ] **Step 1:** Add `PARENT_ENTITIES_FOR_ORDERS = ("products", "customers")` and `run_orders_conform_with_parent_refresh(spark, catalog)` — loop parents with try/except (log, no raise), then `run_entity_conform` for orders (raises on failure). Shared `parent_run_id`.
- [ ] **Step 2:** Unit test — mock `run_entity_conform`; products raises, customers and orders still called; orders raise fails wrapper.
- [ ] **Step 3:** Update `conform_orders.py` to call `run_orders_conform_with_parent_refresh`.
- [ ] **Step 4:** Run `pytest jobs/silver/tests/test_orders_parent_refresh.py jobs/silver/tests/test_main.py -q`.
- [ ] **Step 5:** Commit: `feat(silver): parent refresh before orders conform for RI`

---

### Task 2: Bundle — three silver conform jobs

**Files:**
- Modify: `databricks/bundle/resources/silver.job.yml`

- [ ] **Step 1:** Remove `job_silver_conform_all`. Add `job_silver_conform_products`, `job_silver_conform_customers`, `job_silver_conform_orders` with entrypoints `conform_products.py`, `conform_customers.py`, `conform_orders.py`.
- [ ] **Step 2:** Each job: single `table_update` on one bronze table; `pause_status: PAUSED`; `max_concurrent_runs: 1`.
- [ ] **Step 3:** Orders job optional `min_time_between_triggers_seconds: 60`.
- [ ] **Step 4:** `databricks bundle validate -t dev` from `databricks/bundle/`.
- [ ] **Step 5:** Commit: `feat(silver): bundle per-entity conform jobs`

---

### Task 3: CE job registry and deploy

**Files:**
- Modify: `scripts/ce_job_registry.py`
- Modify: `scripts/medallion_e2e.py`

- [ ] **Step 1:** Replace `de_assessment_silver_conform_all` with three jobs in `all_job_settings()` — single-table triggers, `UNPAUSED` for E2E.
- [ ] **Step 2:** **Migrate CE:** update existing job_id for conform_all → `de_assessment_silver_conform_orders` settings; create products + customers jobs (upsert).
- [ ] **Step 3:** Update `medallion_e2e.py` `SILVER_JOB_NAMES` and wait logic — map `products`/`customers`/`orders` bronze steps to matching silver jobs.
- [ ] **Step 4:** Run `./scripts/deploy-all-ce-jobs.sh de_assessment`.
- [ ] **Step 5:** Commit: `feat(ops): CE registry for per-entity silver conform jobs`

---

### Task 4: Deprecate production `conform_all`

**Files:**
- Modify: `databricks/jobs/silver/src/conform_all.py` (module docstring)
- Modify: `.cursor/skills/bronze-e2e-ce/SKILL.md` if referenced
- Modify: `AGENTS.md` silver job list if present

- [ ] **Step 1:** Document `conform_all.py` / `run_conform_all()` as manual debug only; not in CE registry.
- [ ] **Step 2:** Grep repo for `conform_all` / `silver_conform_all`; update docs/skills.
- [ ] **Step 3:** Commit: `docs(silver): mark conform_all as debug-only entrypoint`

---

### Task 5: CE E2E verification

**Files:**
- Modify: `scripts/medallion_e2e.py` (verify section if needed)

- [ ] **Step 1:** Deploy silver workspace code (included in deploy-all).
- [ ] **Step 2:** Run `python3 scripts/medallion_e2e.py run` (no bootstrap if already done).
- [ ] **Step 3:** Confirm JSON `passed: true`; one silver run per bronze ingest; quarantine ~800 batch rows; silver manifest success per entity.
- [ ] **Step 4:** Commit any E2E script fixes: `fix(ops): medallion E2E for per-entity silver jobs`

---

### Task 6: Prompt artifact

**Files:**
- Modify: `ai-prompts/05-silver-layer.md`

- [ ] **Step 1:** P-entry — orchestration v2 decision, superseded conform_all, CE results.
- [ ] **Step 2:** Commit: `docs(prompts): silver orchestration v2 P-entry`
