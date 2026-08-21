# Data Generation — Prompt History

> **Continues from:** [`03-architecture-design.md`](03-architecture-design.md) (bronze schemas locked first)  
> **Companion file:** [`04-bronze-layer.md`](04-bronze-layer.md) (ingest, E2E, manifest)  
> **Spec / plan:** assessment PDF → good-then-bad generator plan (Superpowers executing-plans)  
> **Code:** `databricks/jobs/data_generation/src/generate_sample_data.py`

## Session overview (evaluator narrative)

Bronze schemas and landing paths were fixed **before** data generation started — generator work was deliberately deferred so CSV columns match typed bronze schemas. Implementation followed a **spec-driven** loop: assessment PDF counts → written plan → TDD injectors → CE deploy → recursive cluster validation → bug fixes from run output.

| Workflow signal | How it showed up here |
|-----------------|------------------------|
| **Modes** | Agent for implementation; Ask-style clarification on pandas vs PySpark; Plan/brainstorming for good-then-bad design before coding |
| **Plugins / skills** | Superpowers `executing-plans`, `test-driven-development`; project `deploy-ce-job`, `medallion-pipeline-local-test`; Databricks `databricks-jobs` for bundle deploy |
| **Persistent context** | `docs/ASSESSMENT_FROM_PDF.md`, `cursor-workflow/spec.md`, bronze `IntegerType` schemas, intentional ~700 DQ rows |
| **User steering** | Full PDF scale (not dev shrink); catch CE failures in first run (not retry-only); timestamped filenames for Auto Loader |
| **Recursive testing** | pytest injectors → small integration → CE job run → read run logs → fix volume write / logging → E2E with bronze ingest |
| **Bug caught with context** | pandas `NaN` promoted FK columns to float → CSV wrote `2869.0` → bronze rescued every order row (fixed with nullable Int64 before write) |

---

## Rubric alignment (Strong Cursor Usage)

| Strong signal | Evidence in this file |
|---------------|----------------------|
| Specific prompts | PDF row counts, exact DQ injections, filename pattern, logging requirements |
| Iteration | Generator dummy → PDF-aligned → CE failure → observability → Int64 CSV fix |
| Validation | pytest + CE run IDs + row-count checks against injectors |
| Reject off-architecture | No PySpark in data gen; no shrinking assessment scale for convenience |

---

## P1 — Good-then-bad generator (assessment PDF)

**Prompt:**  
"Data generator: keep it simple — generate good data first, then add bad data as in the assessment PDF; implement the attached plan; do not edit the plan file."

**Context provided:**  
Bronze schemas (`IntegerType` IDs), `docs/ASSESSMENT_FROM_PDF.md` DQ table, Superpowers executing-plans skill, existing dummy `generate_sample_data.py`.

**AI response:**  
Two-phase pipeline: `generate_clean_*` → `inject_*_issues`; PDF enums; disjoint injector indices; orphan IDs ≥900001; volume CLI; `deploy-data-gen-ce.sh`; pytest on injectors and small-scale integration.

**Validation:**  
- `./databricks/scripts/run_job_tests.sh data_generation` → pass  
- Injector tests assert exact counts (50 null email, 10 dup customer_id, 100 null customer_id on orders, etc.)

**Accepted:**  
Two-phase generate/inject; exact DQ counts per PDF; INT IDs aligned to bronze schemas.

**Changed:**  
Injector order — duplicate customers from pre-null copy; disjoint null/orphan row indices on orders so injections do not collide.

**Rejected:**  
Smaller dev-scale row counts for the assessment CE run (user required full PDF scale: 500 / 10,010 / 100,020).

**Why:**  
Silver layer needs predictable bad-row counts; bronze IntegerType rejects float FK strings if generator drifts types.

---

## P2 — CE failure → structured logging (user caught retry-only pain)

**Prompt:**  
"The original run failed and the 1st retry is running — catch failure in the original run; add try/except and proper logs across job code so we can trace what went wrong."

**Context provided:**  
Failed CE data-gen run (opaque error); serverless volume restrictions; bronze `job_log.py` pattern from same session.

**AI response:**  
Added `job_log.py` (stdout logging for Databricks run output); `run_main` wrapper on CE entrypoints; phased logs in generate/write; fixed volume write to use `dbutils.fs.put` directly instead of `/tmp` copy.

**Validation:**  
- Re-run data gen on CE → failure visible in run output with phase + stack trace before re-raise  
- Success path logs batch_id, paths, row counts

**Accepted:**  
Structured stdout logging; exception logging on all failure paths.

**Changed:**  
Volume write path — direct UC volume `fs.put` (CE serverless cannot rely on local `/tmp` staging).

**Rejected:**  
Relying on Databricks job retry without driver-side error detail.

**Why:**  
User steering on observability — rubric "validation before accept" requires traceable CE failures, not blind retries.

---

## P3 — Timestamped landing filenames (Auto Loader checkpoint)

**Prompt:**  
"Fix data generator so filenames include current UTC timestamp; stable pattern, new name each run for Auto Loader."

**Context provided:**  
Auto Loader checkpoints skip same-path overwrites; bronze E2E needs correlated batch across three entities.

**AI response:**  
`landing_batch_id()` → `YYYYMMDDTHHMMSSZ`; writes `{entity}_{batch_id}.csv`; optional `--batch-id` CLI; stats return `batch_id` + `files` for E2E orchestration.

**Validation:**  
- CE data gen run → three files e.g. `products_20260821T084559Z.csv` under volume incoming paths  
- Bronze ingest picks up new files without manual checkpoint reset

**Accepted:**  
Single shared UTC batch id per generator run across products/customers/orders.

**Changed:**  
Replaced fixed `products.csv` / `customers.csv` / `orders.csv` names.

**Rejected:**  
Independent per-file timestamps (would break manifest batch correlation).

**Why:**  
Ties data gen output to bronze `_batch_id` and E2E verification in one run identifier.

---

## P4 — Unified CE deploy (preserve job run history)

**Prompt:**  
"Deploy all jobs together; partial deploy deletes jobs and loses run history."

**Context provided:**  
Prior `jobs delete` + recreate assigned new job IDs; user wanted eval-friendly run history in CE UI.

**AI response:**  
`scripts/ce_job_registry.py` + `deploy-all-ce-jobs.sh` using `jobs update` upsert; per-layer deploy scripts delegate to unified registry.

**Validation:**  
- Redeploy after code change → same job IDs retained  
- CE UI shows continuous run history for data gen + bronze jobs

**Accepted:**  
Single deploy uploads all code and upserts six jobs without delete/recreate.

**Changed:**  
Replaced delete+recreate pattern in layer-specific deploy scripts.

**Rejected:**  
Partial deploy that orphans prior run IDs.

**Why:**  
Operational constraint from user — demonstrates iteration after observing CE UI behavior.

---

## P5 — Nullable Int64 CSV columns (pandas float promotion bug)

**Prompt:**  
(Implicit from E2E) — orders manifest showed `rows_rescued=100021`; user asked to explain rescued counts and drove root-cause analysis across data gen + bronze.

**Context provided:**  
Bronze Auto Loader `IntegerType` on `customer_id`/`product_id`; orders DQ injection sets NULL FKs; pandas default float column when NaN present; CSV output like `2869.0`.

**AI response:**  
`coerce_integer_csv_columns()` + `frame_to_csv()` — nullable pandas `Int64` for id columns before CSV write so nulls stay empty cells, not float strings.

**Validation:**  
- Unit tests on CSV round-trip for null integer columns  
- Pending fresh CE E2E: expect `rows_rescued ≈ 0` on orders after redeploy

**Accepted:**  
Int64 coercion for all integer id columns written to landing CSVs.

**Changed:**  
Raw pandas `to_csv` on mixed null/int columns.

**Rejected:**  
Fixing only in bronze ingest (bronze must stay raw; root cause is upstream CSV typing).

**Why:**  
Cross-layer debugging — user drove context from manifest symptom back to generator; classic assessment DQ preservation vs type corruption.

---

## P6 — Incremental commits for eval capture

**Prompt:**  
"Commit incrementally like Superpowers; include tooling in commits; add prompt history to PR."

**Context provided:**  
`ai-prompts/02-tooling-rules-and-workflow.md` commit cadence note; PR #3 bronze layer branch.

**AI response:**  
Logical slices: data gen fix commit, CE scripts commit, cursor MCP/settings commit, docs commit — separate from bronze ingest library commit.

**Validation:**  
- `git log origin/main..HEAD` shows one-concern commits with `feat|fix(scope): why` messages

**Accepted:**  
This file (`04-data-generation.md`) curated as eval-facing history, not raw hook dumps.

**Changed:**  
N/A (process entry).

**Rejected:**  
Single squashed commit hiding iteration story.

**Why:**  
Assessment rubric asks for git history showing accept → test → fix → refine.

---

## CE runs referenced

| Run purpose | batch_id / note |
|-------------|-----------------|
| First E2E landing | `20260821T084559Z` — exposed manifest `rows_written=0` (bronze bug, not gen) |
| Post metrics-fix E2E | `20260821T095201Z` — gen succeeded; +1 row/header issues led to P5 + bronze header fix |
| Fresh validation | Pending — `./scripts/run-bronze-e2e-ce.sh --deploy` after Int64 + `header=true` |

## Related skills / files created

| Artifact | Role |
|----------|------|
| `deploy-ce-job` skill (tailored) | Lean bundle deploy without Terraform SP flow |
| `scripts/deploy-data-gen-ce.sh` | Data gen only wrapper → unified registry |
| `databricks/jobs/data_generation/src/job_log.py` | Shared CE logging helper |
