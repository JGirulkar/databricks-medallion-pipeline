# Bronze Layer — Prompt History

## P1 — Move source_config to config schema

**Prompt:** source_config should live in config schema, not bronze.

**AI response:** Added `CONFIG_SCHEMA = "config"`; `source_config_table()` → `de_assessment.config.source_config`; bootstrap DDL creates config schema before table; tests updated.

**Accepted:** Operational config separated from entity bronze tables.

**Changed:** FQN from `bronze.source_config` to `config.source_config`.

**Rejected:** N/A

**Why:** Bronze schema holds landed entity data; config holds operational metadata per architecture.

## P2 — Unified CE deploy + E2E

**Prompt:** Redeploy all jobs together; run E2E with file-arrival orders, manual products/customers, verify tables/manifest.

**AI response:** `deploy-all-ce-jobs.sh` upserts 6 jobs; `run-bronze-e2e-ce.sh` orchestrates bootstrap → data gen → ingest → verify; fixed serverless `__file__` in data gen.

**Accepted:** Job IDs preserved on redeploy; orders file-arrival UNPAUSED; timestamped landing files `batch_id=20260821T084559Z`.

**Changed:** Fixed `wait_after_last_change_seconds` ≥ 120 for file trigger; removed `df.persist()` on serverless (prior runs).

**Rejected:** N/A

**Why:** Partial deploy deleted job history; first-run failures now visible in structured logs.

**CE E2E run (2026-08-21):** bootstrap SUCCESS (`config.source_config` seeded); data gen SUCCESS; orders auto-trigger run `503876239140998` SUCCESS; products `170066026142814` / customers `401741768513931` SUCCESS. Root cause of `rows_written=0` logs: serverless `foreachBatch` worker isolation — writes succeeded (proven via Delta history + `_batch_id` on rows) but driver `totals` never updated. Bronze holds two loads (old `*.csv` + batch `20260821T084559Z`).

## P3 — Sink-derived manifest metrics (serverless fix)

**Prompt:** Derive manifest metrics from Delta history after stream drains; stamp writes with `userMetadata = batch_id`; sum `numOutputRows` for concurrency-safe row counts.

**AI response:** `append_batch` stamps `userMetadata`; new `rows_written_from_history()` + `metrics_from_sink()` resolve `rows_written`, files, rescued from history + `_batch_id` rows on driver after `awaitTermination()`. Removed broken worker-side `totals` merge.

**Accepted:** Driver-side observability matches actual Delta commits on serverless Spark Connect.

**Changed:** `ingest.py`, `test_ingest.py` (+6 tests).

**Rejected:** N/A

**Why:** ForeachBatch callbacks run in worker process; closure mutation on driver always reported zero despite successful appends.

## P4 — Redeploy + CE rerun with new batch (metrics fix validated)

**Prompt:** Redeploy and rerun with newly generated files.

**AI response:** `deploy-all-ce-jobs.sh`; data gen run `456017315302910` → batch `20260821T095201Z`; orders auto `625412828694514`, products `526931804916539`, customers `1027999282096770` — all SUCCESS.

**Accepted:** Manifest now reports non-zero rows: orders=100021, products=501, customers=10011; `metrics_from_sink` logs on driver; orders file archived to `processed/`.

**Changed:** N/A (validation run).

**Rejected:** N/A

**Why:** Sink-derived metrics fix confirmed on CE; bronze layer sign-off unblocked pending header-row (+1) cleanup if desired.
