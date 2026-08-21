# Data Generation — Prompt History

> **Read first:** [`03-architecture-design.md`](03-architecture-design.md) (schemas locked) → [`04-bronze-layer.md`](04-bronze-layer.md) (ingest + E2E — this work started *after* bronze was designed)  
> **Requirements source:** `docs/ASSESSMENT_FROM_PDF.md` (row counts + intentional DQ issues)  
> **Code:** `databricks/jobs/data_generation/src/generate_sample_data.py`

---

## How I started this work (context I gave the agent)

Data generation was **not** the first chat. I deliberately finished bronze schemas, landing volume paths, and ingest jobs first so the generator would write CSVs that match typed bronze tables (`IntegerType` IDs, assessment column list).

**What I said when we opened data gen**

- Keep it **simple**: generate **good data first**, then inject **bad data exactly as in the assessment PDF** (~700 intentional issue rows across customers and orders).
- The existing `generate_sample_data.py` was a **dummy** — polish it against PDF counts, not invent a new design.
- Use **full assessment scale** on CE: 500 products, 10,010 customers, 100,020 orders — not a shrunk dev sample.
- **Pandas only here** — data generation runs as a lightweight CE job; bronze ingest stays **PySpark / Auto Loader** (I clarified this when rescued-row debugging blurred the boundary).
- After the first CE run **failed silently** and only showed up on retry, I insisted on **try/except + structured logs** in job code so failures appear in the **first** run output.
- Landing files must use **timestamped names** so Auto Loader picks up each E2E batch without resetting checkpoints.

**How this connects to bronze**

| Bronze gave us | Data gen had to respect |
|----------------|-------------------------|
| Typed schemas in `bronze/schemas.py` | Integer IDs, PDF enums, column names |
| Volume paths in `config.source_config` | Write to `landing/raw/{entity}/incoming/` |
| Auto Loader per source | One shared `batch_id` across three files per run |
| Append-only raw landing | Preserve NULLs and duplicates — do not "fix" in generator |
| E2E script `bronze_e2e.py` | Return `batch_id` + file paths for ingest verification |

---

## Where things live (doc → code map)

| What | Where |
|------|--------|
| Assessment DQ counts (50 null email, 10 dup customer_id, etc.) | `docs/ASSESSMENT_FROM_PDF.md`, `database/seed-data-notes.md` |
| Bronze column types (contract for CSV) | `databricks/jobs/bronze/src/bronze/schemas.py` |
| Generator + injectors | `databricks/jobs/data_generation/src/generate_sample_data.py` |
| CE structured logging | `databricks/jobs/data_generation/src/job_log.py` |
| Injector unit tests | `databricks/jobs/data_generation/tests/test_dq_spec.py` |
| CE data gen job entry | `databricks/jobs/data_generation/src/run_generate_ce.py` (bundle) |
| Deploy wrapper | `scripts/deploy-data-gen-ce.sh` → `deploy-all-ce-jobs.sh` |
| E2E that calls data gen then ingest | `scripts/bronze_e2e.py`, `.cursor/skills/bronze-e2e-ce/SKILL.md` |
| Bronze header/rescued fix (downstream) | `databricks/jobs/bronze/src/bronze/ingest.py` — see [`04-bronze-layer.md`](04-bronze-layer.md) P10 |

**Output naming pattern**

```
/Volumes/de_assessment/landing/raw/products/incoming/products_20260821T095201Z.csv
/Volumes/de_assessment/landing/raw/customers/incoming/customers_20260821T095201Z.csv
/Volumes/de_assessment/landing/raw/orders/incoming/orders_20260821T095201Z.csv
```

Same `batch_id` suffix on all three — ties to bronze `_batch_id` and manifest rows.

---

## Story in phases

### Phase 1 — Good-then-bad generator (PDF-aligned)

After bronze jobs were deployed, I attached the **Data Generator — Simple Good-Then-Bad Plan** and asked the agent to implement it without editing the plan file — Superpowers executing-plans style, todos already created.

---

#### P1 — Implement good-then-bad generator

**Prompt:**  
"Data generator: generate good data first, then add bad data as in the assessment PDF. Implement the attached plan; do not edit the plan file."

**Context provided:**  
Bronze `IntegerType` schemas, `docs/ASSESSMENT_FROM_PDF.md`, dummy `generate_sample_data.py`, Superpowers executing-plans.

**AI response:**  
Two-phase pipeline: `generate_clean_*` → `inject_*_issues`; PDF enums; disjoint injector indices; orphan IDs ≥900001; volume CLI; pytest on injectors + small integration.

**Validation:**  
`./databricks/scripts/run_job_tests.sh data_generation` → pass; injector tests assert exact PDF counts.

**Accepted:** Two-phase generate/inject; exact DQ counts; INT IDs aligned to bronze.  
**Changed:** Injector order — dup customers from pre-null copy; disjoint indices on orders.  
**Rejected:** Smaller dev scale (user required 500 / 10,010 / 100,020).  
**Why:** Silver needs predictable bad-row counts for assessment evidence.

---

### Phase 2 — CE observability (I caught retry-only failures)

The first CE run failed; I only noticed when Databricks started a retry. I stopped accepting that pattern.

---

#### P2 — Structured logging + first-run failure visibility

**Prompt:**  
"The original run failed and the 1st retry is running — catch failure in the original run. Add try/except and proper logs so we can trace what went wrong."

**AI response:**  
`job_log.py` (stdout logging); `run_main` wrapper; phased logs; fixed volume write — `dbutils.fs.put` directly, not `/tmp` copy (serverless restriction).

**Validation:**  
Re-run on CE → phase + stack trace in run output before re-raise; success logs `batch_id`, paths, counts.

**Accepted:** Structured stdout logging on all paths.  
**Changed:** Volume write uses direct UC `fs.put`.  
**Rejected:** Relying on job retry without driver-side detail.  
**Why:** User steering — observability before accept.

---

### Phase 3 — Timestamped files for Auto Loader + E2E

Fixed filenames caused checkpoint skips on redeploy. I asked for UTC timestamps so each E2E run lands fresh files.

---

#### P3 — Timestamped landing filenames

**Prompt:**  
"Fix data generator so filenames include current UTC timestamp — new name each run for Auto Loader."

**AI response:**  
`landing_batch_id()` → `YYYYMMDDTHHMMSSZ`; `{entity}_{batch_id}.csv`; optional `--batch-id` CLI; stats return `batch_id` + `files`.

**Validation:**  
CE run → e.g. `products_20260821T084559Z.csv`; bronze ingest without checkpoint reset.

**Accepted:** Single shared UTC batch id per run across three entities.  
**Changed:** Replaced fixed `products.csv` / `customers.csv` / `orders.csv`.  
**Rejected:** Independent per-file timestamps (breaks batch correlation).  
**Why:** Links generator output to bronze `_batch_id` and E2E verification.

---

### Phase 4 — Deploy integration (same story as bronze P6)

Partial deploy was deleting jobs and losing CE run history. Data gen deploy was folded into the unified registry — documented here because the generator cannot be tested in isolation once E2E exists.

---

#### P4 — Unified CE deploy

**Prompt:**  
"Deploy all jobs together; partial deploy deletes jobs and loses run history."

**AI response:**  
`ce_job_registry.py` + `deploy-all-ce-jobs.sh` using `jobs update` upsert.

**Validation:**  
Redeploy → same job IDs; continuous history in CE UI.

**Accepted:** Single deploy upserts all six jobs.  
**Rejected:** Delete+recreate per layer.  
**Why:** Operational constraint discovered during bronze E2E iteration.

---

### Phase 5 — Cross-layer bug from E2E (pandas → bronze rescues)

During bronze E2E batch `20260821T095201Z`, orders showed `rows_rescued=100021` (every row). I asked for an explanation — not a bronze-only patch. Tracing manifest → CSV → generator showed pandas promoted NULL FK columns to float, writing `2869.0` strings that Auto Loader rescued against `IntegerType`.

---

#### P5 — Nullable Int64 CSV columns

**Prompt:**  
(User driven from E2E) Explain rescued counts; trace root cause across data gen and bronze — bronze must stay raw.

**Context provided:**  
Orders DQ injections (100 NULL `customer_id`, 200 NULL `product_id`); bronze `IntegerType`; pandas NaN → float column behavior.

**AI response:**  
`coerce_integer_csv_columns()` + `frame_to_csv()` with nullable pandas `Int64` before write — nulls become empty CSV cells, not `2869.0`.

**Validation:**  
Unit tests on CSV round-trip for null integer columns; fresh CE E2E pending for `rows_rescued ≈ 0`.

**Accepted:** Int64 coercion on all id columns at write time.  
**Changed:** Raw `to_csv` on mixed null/int columns.  
**Rejected:** Fixing only in bronze ingest (violates raw landing rule).  
**Why:** User drove symptom → upstream root cause across layer boundary.

---

### Phase 6 — Eval capture (commits + prompt history)

---

#### P6 — Incremental commits and curated history

**Prompt:**  
"Commit incrementally like Superpowers; include tooling; prompt history in two parts for the PR."

**AI response:**  
Separate commits for data gen, CE scripts, cursor MCP/settings, docs; this file curated from session evidence.

**Validation:**  
`git log` shows one-concern commits on `cursor/bronze-layer`.

**Accepted:** Evaluator-facing curated files, not raw hook dumps.  
**Rejected:** Single squashed commit hiding iteration.  
**Why:** Rubric asks for git history showing accept → test → fix → refine.

---

## CE runs referenced

| batch_id | Data gen outcome | Notes |
|----------|------------------|-------|
| `20260821T084559Z` | SUCCESS — three timestamped files | Bronze manifest bug (not gen) — see bronze P7–P8 |
| `20260821T095201Z` | SUCCESS | +1 row / mass rescues → header fix (bronze) + Int64 fix (here) |
| (pending) | `./scripts/run-bronze-e2e-ce.sh --deploy` | Expect exact PDF row counts, minimal rescues |

---

## Rubric checklist (Strong Cursor Usage)

| Signal | Where shown above |
|--------|-------------------|
| Persistent context | Opening — bronze-first sequencing, PDF counts, pandas vs PySpark |
| Specific prompts | Verbatim asks with scale, logging, filename constraints |
| Iteration | Dummy → PDF-aligned → CE logging → timestamped files → Int64 fix |
| Validation | pytest injectors, CE run output, E2E batch correlation |
| Reject off-architecture | No PySpark in gen; no shrinking scale; no bronze-side "fix" for CSV typing |
