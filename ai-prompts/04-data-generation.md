# Data Generation — Prompt History

> **Continues from:** [`04-bronze-layer.md`](04-bronze-layer.md) P6–P7 (bronze schemas, volumes, jobs deployed first)  
> **Requirements:** `docs/ASSESSMENT_FROM_PDF.md`, `database/seed-data-notes.md`  
> **Code:** `databricks/jobs/data_generation/src/generate_sample_data.py`

---

## P1 — Start data gen after bronze contract exists

**Prompt:**  
"Now we get to the data generator — keep it simple: generate **good data first**, then add **bad data as in the assessment PDF**. The script you see is a dummy and needs polishing. Implement the attached **Data Generator — Simple Good-Then-Bad Plan**; do not edit the plan file. To-dos already created — mark in progress and complete all."

**Context provided:**  
- Bronze already locked (prior chat — see [`04-bronze-layer.md`](04-bronze-layer.md) P1–P7): typed schemas in `databricks/jobs/bronze/src/bronze/schemas.py` (`IntegerType` IDs), landing paths in `de_assessment.config.source_config`, volume layout `landing/raw/{entity}/incoming/`  
- Assessment DQ table: `docs/ASSESSMENT_FROM_PDF.md` — e.g. 50 null email, 10 dup `customer_id`, 100 null `customer_id` on orders, 200 null `product_id`, orphan FKs, 20 dup `order_id`  
- Full PDF scale required: **500** products, **10,010** customers, **100,020** orders  
- Existing placeholder: `databricks/jobs/data_generation/src/generate_sample_data.py`  
- Superpowers **executing-plans** + **test-driven-development**; tests in `databricks/jobs/data_generation/tests/test_dq_spec.py`  
- **Pandas only** for generation — bronze ingest stays PySpark/Auto Loader (clarified when debugging rescues later)  
- Deploy via `scripts/deploy-data-gen-ce.sh` → unified `deploy-all-ce-jobs.sh`

**AI response:**  
Two-phase pipeline: `generate_clean_*` → `inject_*_issues`; PDF enums; disjoint injector indices; orphan IDs ≥900001; volume CLI; pytest on injectors + small-scale integration.

**Validation:**  
`./databricks/scripts/run_job_tests.sh data_generation` → pass; injector tests assert exact PDF counts.

**Accepted:**  
Two-phase generate/inject; exact DQ counts; INT IDs aligned to bronze schemas.

**Changed:**  
Injector order — duplicate customers from pre-null copy; disjoint null/orphan indices on orders.

**Rejected:**  
Smaller dev-scale row counts for CE (user required full PDF scale).

**Why:**  
Bronze schemas were the contract — data gen followed bronze, not the other way around; Silver needs predictable bad-row counts.

---

## P2 — CE failure visible on first run (not retry only)

**Prompt:**  
"The **original run failed** and the 1st retry is running — learn to catch failure in the original run. I want try/except and proper logs across job code so we can trace what went wrong in run output."

**Context provided:**  
First CE data-gen job failure (opaque in UI until retry); serverless volume restrictions; bronze `job_log.py` pattern from same sprint.

**AI response:**  
Added `databricks/jobs/data_generation/src/job_log.py` (stdout logging); `run_main` wrapper on CE entrypoint; phased logs; fixed volume write — `dbutils.fs.put` directly to UC volume, not `/tmp` copy.

**Validation:**  
Re-run on CE → phase + stack trace in run output before re-raise; success logs `batch_id`, paths, row counts.

**Accepted:**  
Structured stdout logging on all failure paths.

**Changed:**  
Volume write path for CE serverless.

**Rejected:**  
Relying on Databricks job retry without driver-side error detail.

**Why:**  
User caught retry-only visibility — validation before accept on CE behavior.

---

## P3 — Timestamped landing filenames for Auto Loader + E2E

**Prompt:**  
"Fix data generator so filenames include current UTC timestamp — stable pattern, new name each run for Auto Loader."

**Context provided:**  
Auto Loader checkpoints skip same-path overwrites; bronze E2E (`scripts/bronze_e2e.py`) needs one correlated batch across three entities; bronze `_batch_id` and manifest keyed by same id (bronze P4).

**AI response:**  
`landing_batch_id()` → `YYYYMMDDTHHMMSSZ`; writes `{entity}_{batch_id}.csv`; optional `--batch-id` CLI; stats return `batch_id` + `files`.

**Validation:**  
CE run → e.g. `products_20260821T084559Z.csv` under volume incoming paths; bronze ingest without checkpoint reset.

**Accepted:**  
Single shared UTC batch id per generator run across products/customers/orders.

**Changed:**  
Replaced fixed `products.csv` / `customers.csv` / `orders.csv`.

**Rejected:**  
Independent per-file timestamps (breaks manifest batch correlation).

**Why:**  
Links generator output to bronze E2E verification — continued from bronze P8 orchestration.

---

## P4 — Unified deploy (same fix as bronze P7)

**Prompt:**  
"Deploy all jobs together — partial deploy deletes jobs and loses run history."

**Context provided:**  
`scripts/ce_job_registry.py`, `deploy-all-ce-jobs.sh` (bronze P7); data gen job must stay registered with stable job_id across redeploys.

**AI response:**  
Data gen deploy wrapper delegates to unified registry using `jobs update` upsert.

**Validation:**  
Redeploy after code change → same job IDs; continuous CE run history.

**Accepted:**  
Single deploy upserts all six jobs including data generation.

**Changed:**  
Replaced delete+recreate in layer-specific scripts.

**Rejected:**  
Partial deploy that orphans prior run IDs.

**Why:**  
Operational constraint discovered during bronze E2E — same story, data gen side.

---

## P5 — Int64 CSV columns (pandas float → bronze mass rescues)

**Prompt:**  
(User driven from bronze E2E P11) Orders manifest showed `rows_rescued=100021` — explain rescued counts; trace root cause; **bronze must stay raw**, do not patch downstream only.

**Context provided:**  
Bronze Auto Loader `IntegerType` on `customer_id`/`product_id`; orders DQ injection sets NULL FKs (100 + 200 rows); pandas promotes columns with NaN to float → CSV writes `2869.0`; bronze rescues every row. Batch `20260821T095201Z`.

**AI response:**  
`coerce_integer_csv_columns()` + `frame_to_csv()` — nullable pandas `Int64` for id columns before CSV write; nulls become empty cells, not float strings.

**Validation:**  
Unit tests on CSV round-trip for null integer columns; fresh CE E2E pending for `rows_rescued ≈ 0` on orders.

**Accepted:**  
Int64 coercion at generator write time.

**Changed:**  
Raw `to_csv` on mixed null/int columns.

**Rejected:**  
Fixing only in bronze ingest (violates raw landing — bronze P11 kept header fix there, typing fix here).

**Why:**  
User drove cross-layer debugging from manifest symptom back to generator — assessment DQ preserved without type corruption.

---

## P6 — Commit cadence and repo hygiene

**Prompt:**  
"Commit incrementally like Superpowers — one concern per slice — and include
the `.cursor` MCP and settings files in the commits; they are part of the
project, not local state."

**Context provided:**  
`ai-prompts/02-tooling-rules-and-workflow.md` commit cadence; PR #3.

**AI response:**  
Logical git slices on `cursor/bronze-layer`; `.cursor` config committed with the code it configures.

**Validation:**  
`git log origin/main..HEAD` — one-concern commits; PR mergeable after conflict resolution with `main`.

**Accepted:**  
One-concern commit slices; config tracked in the repo.

**Changed:**  
Nothing.

**Rejected:**  
A single squashed commit for the whole pass.

**Why:**  
Incremental commits keep each change reviewable; the history records every accept/reject with its reason.

---

## CE runs (reference)

| batch_id | Data gen | Notes |
|----------|----------|-------|
| `20260821T084559Z` | SUCCESS | Bronze manifest bug — [`04-bronze-layer.md`](04-bronze-layer.md) P8–P9 |
| `20260821T095201Z` | SUCCESS | +1 / rescues → bronze P11 header + P5 Int64 here |
| (pending) | `./scripts/run-bronze-e2e-ce.sh --deploy` | Exact PDF row counts |
