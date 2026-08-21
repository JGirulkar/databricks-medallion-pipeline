# Bronze Layer — Prompt History

> **Read first:** [`01-planning-and-requirements.md`](01-planning-and-requirements.md) → [`02-tooling-rules-and-workflow.md`](02-tooling-rules-and-workflow.md) → [`03-architecture-design.md`](03-architecture-design.md)  
> **Companion:** [`04-data-generation.md`](04-data-generation.md) (landing CSVs — started *after* bronze schemas were locked)  
> **PR:** [#3](https://github.com/JGirulkar/databricks-medallion-pipeline/pull/3)

---

## How I started this work (context I gave the agent)

I did not open with "write bronze code." I scoped the chat, pointed at existing docs, and locked sequencing before implementation.

**What I said upfront**

- Use the **assessment PDF** (`docs/ASSESSMENT_FROM_PDF.md`) and the **high-level architecture spec** as sources — the arch doc was derived from the PDF but still needed tightening; I was open to suggestions on both.
- **This chat is bronze infrastructure only** — tables, raw landing layout, bootstrap, ingest jobs — **not** the data generator. The generator was a dummy placeholder; we would polish it in a separate pass once bronze schemas existed so CSV columns would not block us.
- Choose **layer schemas** (catalog `de_assessment`, separate bronze / landing / ops / config) as in the anchor architecture — not a flat DBFS dump.
- Lean toward **UC Volume + bootstrap DDL** but **verify CE feasibility** first (Community Edition limits); use the assessment Databricks account only — never Intelo credentials.
- Take **Intelo inspiration read-only** from `/home/jay-ajaykumar/Desktop/Projects/Intelo.ai/retail-agents-backend` — pattern reference, no edits from this repo.
- Pin tooling at **JSON level** (`.cursor/mcp.json`, plugins) because the UI was glitchy; **always** use profile `de-assessment-ce`.

**How I expected the agent to work**

| Mode / plugin | When I used it |
|---------------|----------------|
| Superpowers **brainstorming** | Design sections before any code — one question at a time until spec approved |
| Superpowers **writing-plans** + **executing-plans** | Turn approved spec into checkbox plan, then implement task-by-task |
| Superpowers **test-driven-development** | Fail-first unit tests, then fix — not "implement then maybe test" |
| Superpowers **systematic-debugging** | When E2E manifest said 0 rows but tables had data — prove root cause, don't guess |
| Databricks plugin skills | `databricks-core`, `databricks-dabs`, `databricks-jobs` for bundle + CE deploy |
| Project skills | `deploy-ce-job` (later trimmed for lean CE), **`bronze-e2e-ce`** (created after painful ad hoc polling) |

After mid-session **cost steering** ("we've spent $40, need the rest in $30"), I pushed the agent toward lean batch implementation and reusable scripts/skills instead of heavy subagent review cycles.

---

## Where things live (doc → code map)

Use this map to follow the story below — each phase references these artifacts.

| What | Where |
|------|--------|
| Assessment requirements (row counts, DQ issues, layer rules) | `docs/ASSESSMENT_FROM_PDF.md` |
| Project spec + task breakdown | `cursor-workflow/spec.md`, `cursor-workflow/task-breakdown.md` |
| High-level medallion anchor (Bronze → Silver → Gold) | `docs/superpowers/specs/2026-08-20-medallion-bronze-architecture-design.md` |
| Bronze-only design (bootstrap, volumes, manifest, jobs) | `docs/superpowers/specs/2026-08-20-bronze-layer-design.md` |
| Implementation plan (8 tasks, TDD steps) | `docs/superpowers/plans/2026-08-21-bronze-layer-implementation.md` |
| Shared ingest library | `databricks/jobs/bronze/src/bronze/ingest.py` |
| Bootstrap + manifest + config | `databricks/jobs/bronze/src/bronze/bootstrap.py`, `manifest.py`, `config.py` |
| Per-source entrypoints | `databricks/jobs/bronze/src/ingest_{products,customers,orders}.py` |
| Bundle job definitions | `databricks/bundle/resources/*.yml` |
| CE deploy (upsert, preserve job IDs) | `scripts/ce_job_registry.py`, `scripts/deploy-all-ce-jobs.sh` |
| Bronze E2E orchestration | `scripts/bronze_e2e.py`, `scripts/run-bronze-e2e-ce.sh` |
| E2E skill (low-token reruns) | `.cursor/skills/bronze-e2e-ce/SKILL.md` |
| Assessment isolation rule | `.cursor/rules/databricks-assessment-profile.mdc` |
| MCP + plugin config | `.cursor/mcp.json`, `.cursor/settings.json` |
| Prompt history (this file + data gen) | `ai-prompts/04-bronze-layer.md`, `ai-prompts/04-data-generation.md` |

**UC layout we landed on**

```
de_assessment
├── config.source_config      ← seeded paths, delivery patterns (Intelo-lite)
├── bronze.{customers,orders,products}
├── ops.ingest_manifest       ← one row per source per batch_id
└── landing.raw/.../incoming  ← CSV drop zone (data gen writes here)
```

---

## Story in phases

### Phase 1 — Scope and feasibility (design before code)

I narrowed the chat to bronze infrastructure and confirmed CE could host UC managed volumes + serverless jobs. The agent used Superpowers brainstorming (attached to the first message) and walked design sections for approval before opening an editor.

**Outcome:** Approach **A) Bootstrap job + shared ingest library** — not a monolithic notebook, not enterprise Intelo weight.

---

#### P1 — Bronze scope: infrastructure before data gen

**Prompt:**  
"This chat is about the bronze layer — making tables, deciding the raw dir, creating schemas — not the data generator. We will come to data generation once bronze is ready. Go with A) Layer schemas as in the high-level arch."

**Context provided:**  
Anchor spec, assessment PDF, dummy generator acknowledged as future work.

**AI response:**  
Scoped to bootstrap + ingest; three entity tables + manifest + config; landing volume paths; defer CSV generation.

**Validation:**  
Design doc §1–3 written before any generator work.

**Accepted:** Bronze-first sequencing.  
**Rejected:** Blocking bronze on a polished generator.  
**Why:** Schemas are the contract; data gen follows in [`04-data-generation.md`](04-data-generation.md).

---

#### P2 — UC Volume + bootstrap job on CE

**Prompt:**  
"Lean toward A) UC Volume + bootstrap DDL — check if allowed in Community Edition; use Databricks MCP on the assessment account."

**AI response:**  
Confirmed CE supports UC + volumes; dedicated bootstrap job; catalog `de_assessment` with `bronze`, `landing`, `ops`, `config` schemas.

**Validation:**  
`databricks current-user me --profile de-assessment-ce`; workspace catalog probe.

**Accepted:** Bootstrap job + shared library.  
**Rejected:** Notebook-only bootstrap; DBFS-only without UC.  
**Why:** Environment validated before DDL committed to spec.

---

### Phase 2 — Tooling I fixed when the agent couldn't see MCP

The Databricks **plugin** (skills) and **MCP** (workspace tools) are separate. I caught the agent saying "MCP not in catalog" while the plugin was installed — and asked for JSON-level setup with strict profile isolation.

---

#### P3 — MCP + plugin JSON setup

**Prompt:**  
"Set up Databricks plugin and MCP at JSON level — UI is glitchy. Always strictly use the assessment profile I gave you."

**AI response:**  
Pinned `databricks-de-assessment` MCP (AI Dev Kit) with `DATABRICKS_CONFIG_PROFILE=de-assessment-ce`; enabled Databricks + Superpowers plugins; disabled generic GitHub plugin → `github-de-assessment` MCP; added `databricks-assessment-profile.mdc`.

**Validation:**  
MCP venv imports cleanly; CLI auth valid; tools visible after Cursor restart.

**Accepted:** JSON config + always-applied isolation rule.  
**Changed:** Removed `defer_loading` that hid MCP tools.  
**Rejected:** Default CLI profile leakage.  
**Why:** Documented in [`02-tooling-rules-and-workflow.md`](02-tooling-rules-and-workflow.md) — user caught plugin vs MCP confusion.

---

### Phase 3 — Spec hardening (batch_id, source_config, append-only)

With tooling stable, I continued design in the same chat: renamed manifest key to `batch_id`, asked for Superpowers negative-test flow, and split config between a UC seed table (operational) and Python module (code constants) — Intelo-lite, not full enterprise config.

A critical correction came during planning: **append-only bronze for all sources**. The early anchor had products overwrite / orders merge, but that conflicts with intentional duplicate `order_id` rows in the assessment data. Bronze must preserve raw history; Silver owns dedup and I/U/D.

---

#### P4 — batch_id, TDD, and config.source_config

**Prompt:**  
"Use batch_id instead of manifest_id. Superpowers flow with negative testing. UC source_config seed table (Intelo-lite) — table for ops config, module for code constants."

**Context provided:**  
Read-only Intelo paths; bronze design §2–4.

**AI response:**  
`ingest_manifest.batch_id`; `de_assessment.config.source_config` seeded rows; `bronze/config.py` for constants; pytest fail-first on schemas and contracts.

**Validation:**  
`./databricks/scripts/run_job_tests.sh bronze` — negative cases first.

**Accepted:** Config schema separate from bronze entity tables.  
**Changed:** FQN from early draft `bronze.source_config` → `config.source_config`.  
**Rejected:** File-only source config.  
**Why:** User architecture steering + Superpowers TDD chain.

---

#### P5 — Written spec + implementation plan approved

**Prompt:**  
"Spec looks good — proceed with the plan." (Later, after cost feedback: implement leanly, fewer subagent cycles.)

**Context provided:**  
`2026-08-20-bronze-layer-design.md` approved → `2026-08-21-bronze-layer-implementation.md` (8 tasks).

**AI response:**  
Tasks 1–8: schemas → config → bootstrap → manifest → ingest → entrypoints → bundle → tests; branch `cursor/bronze-layer`.

**Validation:**  
Local pytest green; bundle validate/deploy to CE.

**Accepted:** Shared `ingest.py`; thin per-source entrypoints; orders file-arrival trigger.  
**Rejected:** GitHub Actions auto-deploy on merge (overkill for assessment).  
**Why:** Spec-driven build with mid-course efficiency steering.

---

### Phase 4 — Deploy to CE (lean path, no Terraform)

The stock `deploy-ce-job` skill still referenced Terraform / service principal flows. I redirected to lean bundle + job upsert for CE.

---

#### P6 — Lean CE deploy

**Prompt:**  
"Deploy skill used Terraform — tailor to lean CE. Use AI Dev Kit / Databricks plugin if simpler. Deploy all bronze jobs, not just bootstrap."

**AI response:**  
`ce_job_registry.py` + `deploy-all-ce-jobs.sh` using `jobs update` upsert; rewrote `deploy-ce-job/SKILL.md`; six jobs registered (data gen + bootstrap + 3 ingests + ingest_all smoke).

**Validation:**  
All jobs visible in CE; job IDs stable across redeploy (no delete+recreate).

**Accepted:** Unified deploy preserves run history.  
**Rejected:** Terraform SP provisioning for CE.  
**Why:** User rejected over-engineered path — matched Free Edition scope.

---

### Phase 5 — First full E2E (recursive cluster test)

Once bronze deployed, I moved to data gen (see companion file) and then asked for a **full bronze E2E**: unpause jobs, drop new timestamped files, verify each ingest job and manifest. I did **not** accept "looks good" from logs alone — I asked explicitly if we could close the bronze phase.

---

#### P7 — Bronze E2E orchestration

**Prompt:**  
"Unpause the 3 bronze jobs, generate new files with bad data, see if files go through, each bronze job works, and manifest gets rows as expected."

**AI response:**  
`bronze_e2e.py` + `run-bronze-e2e-ce.sh`: bootstrap → data gen → orders auto-trigger → manual products/customers → SQL verify.

**Validation (batch `20260821T084559Z`):**  
All runs SUCCESS; Delta rows had matching `_batch_id`; **manifest showed `rows_written=0`**.

**Accepted:** E2E script + JSON summary block.  
**Rejected:** Closing bronze on manifest alone.  
**Why:** Recursive test surfaced observability bug — data was landed, metrics were wrong.

---

### Phase 6 — Debugging with evidence (not speculation)

I pushed back on any "Auto Loader didn't ingest" narrative. The agent had to prove what happened using CE SQL and Delta history.

---

#### P8 — Root cause: zero-row manifest

**Prompt:**  
(User driven) Prove whether ingest failed or metrics lied — do not speculate.

**AI response:**  
Writes succeeded (`numOutputRows` in history, rows in bronze). Root cause: serverless Spark Connect runs `foreachBatch` in workers — driver `totals` dict never updated.

**Validation:**  
`SELECT count(*) ... WHERE _batch_id = '20260821T084559Z'` → non-zero; Delta history shows output rows.

**Accepted:** Diagnosis before fix.  
**Rejected:** "Missed ingest" hypothesis.  
**Why:** Strong eval signal — evidence from cluster, not driver logs alone.

---

#### P9 — Fix: sink-derived manifest metrics

**Prompt:**  
"Derive manifest metrics from Delta history after stream drains; stamp writes with userMetadata = batch_id."

**AI response:**  
`metrics_from_sink()`, `rows_written_from_history()` on driver after `awaitTermination()`; removed worker `totals`; +6 unit tests.

**Validation (batch `20260821T095201Z`):**  
Redeploy + fresh E2E; manifest non-zero (`501` / `10011` / `100021`); driver logs show sink metrics.

**Accepted:** Sink-derived observability.  
**Changed:** Worker accumulator pattern.  
**Why:** Accept → test → fix → refine on CE.

---

#### P10 — Follow-up: header row + rescued counts

E2E counts were off by one on dimensions and orders had mass rescues. Cross-layer analysis: missing `header=true` in Auto Loader (+1 row ingested as data); pandas float FK strings from data gen (fixed in companion file).

**AI response:**  
Bronze: `header=true`, `nullValue`, `emptyValue` in cloudfiles options.

**Validation:**  
Unit tests updated; fresh CE E2E pending for exact 500/10010/100020.

**Accepted:** Header parsing fix.  
**Rejected:** Hiding rescues in bronze (assessment needs raw parse failures visible).  
**Why:** Second E2E cycle — symptom → two root causes → two fixes.

---

### Phase 7 — Reusable skill + PR artifacts

After repeated expensive E2E polling in chat, I asked for a repeatable low-token path and eval-facing prompt history.

---

#### P11 — New skill: bronze-e2e-ce

**Prompt:**  
(Cost steering) Don't rewrite orchestration in chat each time — one script, one skill.

**AI response:**  
`.cursor/skills/bronze-e2e-ce/SKILL.md`; wired in `AGENTS.md`; parse `=== E2E JSON ===` from script stdout.

**Accepted:** Project skill as bronze sign-off gate.  
**Why:** Meta Cursor usage — co-created reusable workflow from iteration pain.

---

#### P12 — Prompt history + PR #3

**Prompt:**  
"Prompt history in two parts (data gen + bronze); showcase modes, spec-driven work, plugins, bug catching, recursive tests, new skills; add to PR. Flow should start from context I gave."

**AI response:**  
This file + [`04-data-generation.md`](04-data-generation.md); incremental commits; merge conflict resolution with `main`.

**Accepted:** Curated evaluator narrative, not raw hook dumps.  
**Why:** Assessment requires full prompt history with accept/reject reasoning.

---

## CE E2E batches (reference)

| batch_id | What happened | What we learned |
|----------|---------------|-----------------|
| `20260821T084559Z` | Ingest OK; manifest `rows_written=0` | Worker/driver isolation (P8) |
| `20260821T095201Z` | Manifest fixed; counts +1 / mass rescues | Sink metrics OK; header + Int64 fixes (P9–P10) |
| (pending) | `./scripts/run-bronze-e2e-ce.sh --deploy` | Expect 500 / 10010 / 100020, rescues ≈ 0 |

---

## Rubric checklist (Strong Cursor Usage)

| Signal | Where shown above |
|--------|-------------------|
| Persistent context | Opening section — docs, profile, Intelo read-only, sequencing |
| Design spec before code | Phase 1–3 → specs/plan before Task 1 |
| Specific prompts | Quoted user asks with constraints |
| Iteration | Append-only correction, MCP fix, lean deploy, sink metrics |
| Validation | pytest, CE run IDs, SQL row counts, Delta history |
| Reject off-architecture | No Terraform SP, no bronze DQ, no closing on bad manifest |
