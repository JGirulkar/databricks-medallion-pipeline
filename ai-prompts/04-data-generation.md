# Data Generation — Prompt History

## P1 — Align generator to assessment PDF

**Prompt:** Implement data generator: good data first, then intentional DQ issues per PDF; full scale 10k/100k/500; write to UC volumes; lean CE deploy.

**AI response:** Refactored `generate_sample_data.py` with INT IDs, PDF enums, disjoint injectors, volume CLI; tests on injectors + small-scale integration; `deploy-data-gen-ce.sh`.

**Accepted:** Two-phase generate/inject pattern; exact DQ counts; orphan IDs as INT ≥900001; lean jobs create (no Terraform).

**Changed:** Injector order (dup customers from pre-null copy; disjoint null/orphan indices on orders).

**Rejected:** Smaller dev scale for production CE run (user chose full PDF scale).

**Why:** Bronze schemas use IntegerType; Silver needs exact issue counts for assessment evidence.

## P2 — CE observability (logging + try/except)

**Prompt:** Original data gen run failed on `/tmp` volume copy; add try/except and logs across job code for traceability in run output.

**AI response:** Added `job_log.py` (stdout logging), wrapped CE entrypoints with `run_main`, phased logs in generate/ingest/bootstrap, fixed volume write to `dbutils.fs.put`.

**Accepted:** Structured logs to stdout (visible in Databricks run output); `logger.exception` on failure paths.

**Changed:** Volume write uses direct `fs.put` not `file:/tmp` cp (CE serverless restriction).

**Rejected:** N/A

**Why:** Failures were opaque in run output; logs now show phase, paths, and stack traces before re-raise.

## P3 — Timestamped landing filenames

**Prompt:** Fix data generator so filenames include current UTC timestamp; stable pattern, new name each run for Auto Loader.

**AI response:** Added `landing_batch_id`, `landing_filename`, `landing_paths`; writes `{entity}_{batch_id}.csv`; optional `--batch-id` CLI; stats return `batch_id` + `files`.

**Accepted:** UTC `YYYYMMDDTHHMMSSZ` shared across products/customers/orders in one run.

**Changed:** Replaced fixed `products.csv` / `customers.csv` / `orders.csv` names.

**Rejected:** Per-file independent timestamps (would break batch correlation in manifest).

**Why:** Auto Loader checkpoints skip same-path overwrites; timestamped names ingest without manual checkpoint reset.

## P4 — Unified CE deploy (preserve run history)

**Prompt:** Deploy all jobs together; partial deploy deletes jobs and loses run history.

**AI response:** Added `deploy-all-ce-jobs.sh` + `ce_job_registry.py` using `jobs update` (not delete+create); bronze/data-gen wrappers delegate to unified deploy.

**Accepted:** Single deploy uploads all code and upserts all 6 jobs; existing job_ids retained.

**Changed:** Replaced delete+recreate pattern in per-layer deploy scripts.

**Rejected:** N/A

**Why:** `jobs delete` assigns new job_id and orphan prior run history in the UI.
