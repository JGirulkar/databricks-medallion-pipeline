# Databricks Medallion Pipeline — DE C1 Assessment

An e-commerce medallion pipeline on Databricks: generated sample data →
**Bronze** (raw, append-only) → **Silver** (validated, conformed) → **Gold**
(aggregations) → SQL dashboard. Bronze and Silver are complete and verified end
to end on a real workspace; Gold and the dashboard are the next phase.

```
 CSVs (seeded generator, 725 intentional quality issues)
   ▼  Auto Loader
 bronze.*   append-only · CDF · _rescued_data · no validation
   ▼  table_update triggers, all entities in parallel
 silver.*   config-driven checks → reject / flag / supersede
   ▼
 gold.*  +  dashboard   (next phase)
```

The design turns on one rule — **bronze lands what the source sent; silver
decides what it means** — and on treating a failed check as having *three*
outcomes, not two: a bad value is rejected to quarantine, a missing parent is
flagged `_is_orphan` and healed when the parent arrives, and a key restated by
a later delivery is superseded. Details: [design-notes.md](design-notes.md).

## Quick start

All commands run from the repository root. Steps 1–3 need no Databricks
connection; 4–5 require the `de-assessment-ce` profile
([docs/SETUP.md](docs/SETUP.md), [docs/AUTH.md](docs/AUTH.md)).

```bash
# 1. Environment (profile, JDK for local Spark)
source scripts/env.sh

# 2. Python dependencies
(cd databricks && uv sync --all-packages --all-groups --no-group cluster)

# 3. Tests — 139 across unit, local-Spark and contract tiers
bash databricks/scripts/run_job_tests.sh --all --forbid-skips

# 4. Deploy the ten pipeline jobs to Databricks CE (Jobs API; no bundle)
bash scripts/deploy-all-ce-jobs.sh

# 5. End to end: two deliveries (seed + delta), verified against the tables
bash scripts/run-medallion-e2e-ce.sh
```

The E2E prints an `=== E2E JSON ===` report — emitted even if a step throws —
asserting, per entity, that every delivered key is in silver or quarantine,
that no duplicate keys survive, that the orphan flag agrees with the data in
both directions, and that the delta delivery produced real inserts, updates
and soft deletes.

## Seed data

[`data/`](data/) holds the three committed CSVs (10,015 customers · 508
products · 100,025 orders) so the data model and its intentional issues can be
inspected from a fresh clone. Regeneration and the reasoning behind every
issue: [DATA_GENERATION_NOTES.md](databricks/jobs/data_generation/DATA_GENERATION_NOTES.md).

## Repository map

| Path | What it is |
|---|---|
| `databricks/jobs/{data_generation,bronze,silver,gold}/` | pipeline code, one uv workspace member each, tests beside the code |
| `scripts/` | deploy (`deploy-all-ce-jobs.sh`), E2E (`run-medallion-e2e-ce.sh`), job registry |
| `data/`, `database/` | committed seed CSVs; reference DDL + notes |
| `ai-prompts/` | the full AI prompt history, organised by activity |
| `cursor-workflow/`, `.cursor/` | tool context: spec, task breakdown, rules, skills, hooks |
| `docs/` | setup, auth, deploy strategy, dated design specs (point-in-time records) |

Mapping to the template layout in the brief: `src/{data_generation,bronze,
silver,gold}` ↔ `databricks/jobs/*/src`; the five numbered silver quality
scripts are one config-driven validator (rules live in the `dq_schema` config
column — see [design-notes.md](design-notes.md), "Deviations").

## Project documents

| Document | | Document | |
|---|---|---|---|
| [tool-workflow.md](tool-workflow.md) | how AI is used, the harness, lessons | [test-strategy.md](test-strategy.md) | tiers + full scenario matrix |
| [requirements-analysis.md](requirements-analysis.md) | problem breakdown | [data-quality-strategy.md](data-quality-strategy.md) | checks, outcomes, 725 issues |
| [design-notes.md](design-notes.md) | current design | [data-model.md](data-model.md) | every table and control column |
| [debugging-notes.md](debugging-notes.md) | defects by how they were found | [ai-prompts/](ai-prompts/README.md) | prompt history index |
| [acceptance-criteria.md](acceptance-criteria.md) | requirement → evidence | [reflection.md](reflection.md) | closing reflection |
