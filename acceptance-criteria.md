# Acceptance Criteria

Requirement → status → evidence. Kept current; re-audited at every phase gate
(before each layer's PR, and once more before submission).

## Pipeline

| Requirement | Status | Evidence |
|---|---|---|
| Sample data generator with realistic data + intentional issues | ✅ | [`generate_sample_data.py`](databricks/jobs/data_generation/src/generate_sample_data.py), seeded, two delivery modes; committed CSVs in [`data/`](data/); [notes](databricks/jobs/data_generation/DATA_GENERATION_NOTES.md) |
| Named issue counts (50 NULL emails, 10 dup customers, 100/200 NULL FKs, 50/30 orphans, 20 dup orders) | ✅ exact | verified against the committed CSVs; enforced by generator tests |
| Bronze ingests all three sources, raw, with metadata logging | ✅ | Auto Loader → `bronze.*`, append-only + `_rescued_data` (guarded by [`test_bronze_is_append_only.py`](databricks/jobs/bronze/tests/test_bronze_is_append_only.py)); one `ops.pipeline_manifest` row per run |
| Silver implements all four quality checks | ✅ | completeness / uniqueness / referential / type-logic, config-driven; scenario matrix in [test-strategy.md](test-strategy.md) |
| Bad rows flagged, never deleted; `quality_check_result` column | ✅ | quarantine keeps the full row + violations + lineage; referential failures stay in silver flagged `_is_orphan`; nothing is hard-deleted anywhere |
| Quality report: % passed per check | ✅ | `silver.dq_metrics`, one row per check per entity per run, each with its own pass rate |
| Gold: aggregation tables (sales by product, revenue by customer, trends, segmentation) | ⏳ next phase | brief's own count varies (3 vs 4) — building all four; obligations pinned in [design-notes.md](design-notes.md) |
| Gold calculations correct | ⏳ | will extend the contract test + E2E |
| Dashboard: 3+ queries and visualizations, filters | ⏳ next phase | |
| Database schema / setup script | ✅ | [`database/schema.sql`](database/schema.sql), drift-guarded by a test |
| Input validation and error handling | ✅ | validation as config; failures quarantined with reasons; jobs fail loudly (no silent retries) |
| README setup instructions work end to end | ✅ | every quick-start step executed as written from a clean shell |

## Verification

| Requirement | Status | Evidence |
|---|---|---|
| Meaningful test tier(s) | ✅ | 139 tests: unit, local-Spark, contract (expectations derived independently from input), plus cluster E2E |
| Tests prove the checks catch the intentional issues | ✅ | coverage gate fails if any declared rule lacks a violating scenario; E2E asserts per-category counts |
| End-to-end run on a real workspace | ✅ | two-delivery E2E green: every key accounted (0/0/0 unaccounted), CDC proven (500 inserts / 30 hash-gated updates / 3 soft deletes), orphan flag agrees with the data in both directions |

## Artifacts

| Requirement | Status | Evidence |
|---|---|---|
| Full prompt history, organised by activity | ✅ through silver | [`ai-prompts/`](ai-prompts/README.md); 06/07 land with their phases |
| tool-workflow.md — all required points | ✅ | [tool-workflow.md](tool-workflow.md) |
| requirements-analysis / design-notes / data-model / data-quality-strategy | ✅ | current-state docs; dated specs kept as point-in-time records with evolution notes |
| test-strategy with scenario coverage | ✅ | [test-strategy.md](test-strategy.md) |
| debugging notes | ✅ | [debugging-notes.md](debugging-notes.md) |
| code-review notes | ⏳ | review pass before the silver PR |
| reflection + final AI-usage summary | ⏳ | written last, in the author's own words |
| candidate-info completed | ⏳ | pending name/date fields |
