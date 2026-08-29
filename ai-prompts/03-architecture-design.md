# AI Prompts — Architecture Design (High-Level Anchor)

> **Continues from:** [`02-tooling-rules-and-workflow.md`](02-tooling-rules-and-workflow.md) (env, tooling)  
> **Anchor spec:** [`docs/superpowers/specs/2026-08-20-medallion-bronze-architecture-design.md`](../docs/superpowers/specs/2026-08-20-medallion-bronze-architecture-design.md)  
> **Next activity files:** data gen → `04`; silver/gold → `05`+

## How this activity was run

Design locked and written down before any layer implementation. The prompts
set layer boundaries, load strategies, the phased CDF model and job triggers;
several rounds of refinement (load modes, watermark → CDF batch, spec length)
are recorded, along with what was rejected as off-architecture: DQ or deletes
in bronze, enterprise-weight quarantine frameworks, and full bronze rescans in
silver.

---

## P1 — Medallion architecture kickoff

**Prompt:**  
"Brainstorm high-level architecture from assessment PDF and schema: star schema, three bronze tables, DBFS vs S3, load strategies, CDF, scheduled vs event-driven jobs, silver validation approach, single vs three raw tables — bronze as foundation with silver and gold aligned."

**Context provided:**  
`docs/ASSESSMENT_FROM_PDF.md`, `cursor-workflow/spec.md`, `data-model.md`, `data-quality-strategy.md`, Superpowers brainstorming skill.

**AI response:**  
Dimensional model (orders = fact), three bronze Delta tables, DBFS/UC Volumes on CE, phased Bronze → Silver → Gold, lightweight ingest manifest, DQ and I/U/D owned by Silver.

**Validation:**  
- Cross-checked assessment bronze/silver requirements  
- Confirmed pipeline code not yet implemented (placeholders only)

**Accepted:**  
- Three-table bronze landing zone  
- Strict layer boundaries  
- Production patterns scoped to assessment effort  

**Changed:**  
- Corrected fact table to **orders**  

**Rejected:**  
- Single combined raw table  
- Heavy config/quarantine framework  
- Cleaning or DQ in bronze  

**Why:**  
Establishes the design-first workflow: no layer code before its written design.

---

## P2 — Load strategies across sources

**Prompt:**  
"Lock load strategy: products full refresh; exercise three ingest types across files; orders merge on primary key (option B); row hash on customers for change detection downstream."

**Context provided:**  
P1 architecture; intentional ~700 DQ rows remain in source CSVs.

**AI response:**  
Type 1 overwrite (products), Type 2 append / new-row semantics (customers), Type 3 incremental merge (orders); `_row_hash` on customers.

**Accepted:**  
- Three distinct strategies mapped to three entities  
- Idempotent orders merge  

**Changed:**  
- Customers bronze path refined in P4 (append whole file)  

**Rejected:**  
- Non-idempotent orders append on replay  

**Why:**  
User explicitly locked strategies before CDF/Autoloader details.

**Later correction (Bronze implementation planning):**  
This decision was superseded after testing it against the assessment's
intentional duplicate `order_id` requirement. `MERGE ON order_id` cannot both
preserve duplicate raw rows and remain replay-safe. The accepted design became
append-only Bronze for all sources, with Auto Loader checkpoint idempotency and
business-key merge/deduplication in Silver.

---

## P3 — Ingest engine and job triggers

**Prompt:**  
"Autoloader + checkpoint for new files; event-driven orders; scheduled dimensions; `availableNow` per run instead of always-on streaming on CE."

**Context provided:**  
CE constraints; `docs/deploy-strategy.md`.

**AI response:**  
Autoloader batch per table; file arrival for orders; cron for products/customers; three bronze jobs plus manual smoke orchestrator.

**Accepted:**  
- Autoloader + checkpoint  
- `availableNow` / trigger-once  
- Mixed trigger model (event + schedule)  

**Rejected:**  
- 24/7 streaming cluster as default on CE  

**Why:**  
Balances production realism with CE cost and ops limits.

---

## P4 — Layer ownership (Bronze land / Silver conform)

**Prompt:**  
"Typed schema + rescued data column; CDF on customers and orders; ingest whole files in bronze; silver owns I/U/D merge and DQ — not bronze."

**Context provided:**  
Assessment typed-schema requirement; Autoloader rescued-data option.

**AI response:**  
Bronze: typed schema, `_rescued_data`, metadata, append history where applicable, CDF enabled on customers/orders. Silver: hash + CDF batch merge, soft D for dimensions, `quality_check_result`.

**Accepted:**  
- Typed + rescued (not all-varchar bronze)  
- CDF on customers + orders  
- I/U/D and soft delete in silver only  

**Rejected:**  
- Bronze I/U/D stamps or deletes  
- All-varchar staging table as default  

**Why:**  
Clear medallion contract documented before implementation chats.

---

## P5 — Phased CDF consumption (Silver contract)

**Prompt:**  
"Bronze enables CDF on Delta; silver reads only unconsumed CDF since last run; merge scoped to that batch — not full bronze history; use `_change_type` where it applies."

**Context provided:**  
Draft architecture; minimal processing cursor per table.

**AI response:**  
`table_changes(last_consumed_version + 1, end_version)` with catch-up; orders map CDF types to I/U; customers dedupe within batch + hash; `silver.processing_state` holds cursor.

**Accepted:**  
- Incremental silver via CDF batch  
- No full bronze rescan  
- Minimal version cursor (not heavy watermark framework)  

**Changed:**  
- Renamed watermark concept → `silver.processing_state`  

**Rejected:**  
- Silver scanning entire bronze each run  
- Expecting business updates via CDF on append-only customers bronze  

**Why:**  
User's phased ingest/conform model captured as cross-layer architecture decision.

---

## P6 — Write architecture anchor spec

**Prompt:**  
"Make the spec doc from brainstorming; I will review before implementation plans and layer-specific chats."

**Context provided:**  
Superpowers brainstorming output path `docs/superpowers/specs/`; all decisions from P1–P5.

**AI response:**  
Authored architecture spec; linked from `design-notes.md`; review gate before coding.

**Accepted:**  
- Spec file committed to repo for anchor reference  
- Review before bronze/silver/gold hardening  

**Rejected:**  
- Implementation before spec review  

**Why:**  
Brainstorming hard-gate — the design exists as a written artifact before code.

---

## P7 — High-level anchor polish + retire chat

**Prompt:**  
"No third-party project names anywhere in the design or docs. Keep the spec high-level only — harden bronze, silver, gold in separate chats from this anchor."

**Context provided:**  
Assessment submission standards; user retiring architecture chat.

**AI response:**  
Shortened spec to architecture-level (~269 lines); scrubbed external references repo-wide; created this prompt history file.

**Validation:**  
- Repo grep — no blocked external project references  
- Spec focuses on principles, diagrams, layer contracts  

**Accepted:**  
- High-level anchor doc as single source for layer chats  
- This file (`03-architecture-design.md`) for architecture discussion history  

**Rejected:**  
- Implementation detail in anchor spec  
- External project names in submission artifacts  

**Why:**  
A stable anchor document and a clean narrative before parallel implementation work begins.

---

## Locked decisions (summary)

| Area | Decision |
|------|----------|
| **Model** | orders = fact; customers, products = dimensions |
| **Bronze** | 3 Delta tables; Autoloader + checkpoint; typed + `_rescued_data` |
| **Loads** | Append every newly discovered file; source delivery is full-snapshot or incremental |
| **CDF** | On all three append-only Bronze entity tables |
| **Triggers** | Orders file-driven; dimensions scheduled; `availableNow` per run |
| **Silver** | CDF batch + `processing_state`; I/U/D + DQ; no full bronze scan |
| **Gold** | Reads silver; three aggregations + dashboard (detail in later chats) |
| **Anchor** | `docs/superpowers/specs/2026-08-20-medallion-bronze-architecture-design.md` |
| **Bronze detail** | `docs/superpowers/specs/2026-08-20-bronze-layer-design.md` |

---

## P6 — Bronze layer design (bootstrap + source_config split)

**Prompt:**  
"Continue bronze design: bootstrap job + shared library; UC managed volumes; batch_id not manifest_id; TDD with negative tests; source_config UC seed table (config-driven) + Python config module split."

**Context provided:**  
Anchor spec, assessment PDF schema, CE UC/volume feasibility probe, tooling JSON hardening.

**AI response:**  
Approach A locked; catalog `de_assessment.{bronze,landing,ops}`; manifest uses `batch_id`; `bronze.source_config` seeded table for paths and delivery patterns; `bronze/config.py` for code constants; Superpowers red-green-refactor test tiers documented.

**Validation:**  
- Cross-checked assessment column types and append-only layer boundary  
- CE managed volumes + serverless bundle constraint noted  
- Spec self-reviewed for TBD/contradictions

**Accepted:**  
- UC `source_config` + module split (operational vs code-level)  
- Dedicated bootstrap job  
- TDD flow with explicit negative test cases

**Changed:**  
- Dropped `manifest_id` in favor of `batch_id`  
- Config: table-driven reads instead of file-only config

**Rejected:**  
- Monolithic ingest_all-only job  
- SQL-only bootstrap without bundle job  
- A full enterprise config framework

**Why:**  
User wanted table-driven config without assessment scope creep.

---

## P7 — Correct Bronze to append-only

**Prompt:**  
"Ingest the whole file in Bronze for all sources. Full snapshots and incremental
files are easy to distinguish; changed-row handling belongs in Silver."

**Context provided:**  
Implementation planning exposed a conflict between `MERGE ON order_id`,
file-replay idempotency, and the assessment's intentional duplicate order IDs.

**AI response:**  
Revised Bronze to append every newly discovered file. Auto Loader checkpoints
own file-level idempotency; CDF is enabled on all three entity tables; Silver
owns key deduplication, snapshot comparison, and I/U/D semantics.

**Accepted:**  
- Append-only Bronze for products, customers, and orders  
- Preserve duplicate business keys and raw snapshot history  
- `delivery_pattern` (`full_snapshot` / `incremental`) replaces write strategy

**Changed:**  
- Products overwrite → append full snapshot  
- Orders merge → append incremental file  
- CDF enabled on products

**Rejected:**  
- Bronze deduplication, which would hide intentional DQ issues  
- Non-idempotent manual replay logic; rely on Auto Loader checkpoints

**Why:**  
It resolves the duplicate-key conflict and makes the medallion boundary
consistent: Bronze preserves delivered records; Silver interprets them.


---

## P8 — Orchestration shape: separate triggered jobs, not one DAG

**Prompt:**
"Don't collapse the pipeline into one job with bronze/silver/gold as tasks.
I considered it — one run row, one place to look — but it's wrong for this
data. The three sources arrive independently at unaligned times, so a
single DAG has no natural start event: whatever wakes it, every layer runs
on every wave whether its input changed or not. And we specifically
designed silver so that any snapshot is valid — the stored flags removed
the need for a cross-layer barrier, and a DAG is exactly that barrier,
smuggled back in. Keep the small jobs woven by data triggers: each layer
wakes on its own event, fails in isolation, and shows its own run history
and cost."

**Context provided:**
- The trigger topology as deployed (file-arrival → bronze, table-update →
  silver, table-update → gold with a debounce)
- The requirement doc, which prescribes the layers but is silent on
  orchestration shape

**AI response:**
Confirmed the trade holds in both directions and recorded it: a multi-task
DAG wins in a batch world where all sources land together on one schedule —
one run to inspect, explicit edges; it loses here on wasted layer runs,
entangled retries, and re-coupling what the flag design decoupled. The
reasoning went into design-notes as an "orchestration shape" section so a
reviewer sees the decision instead of inferring an accident.

**Validation:**
The end-to-end run is the live evidence for the chosen shape: gold was
launched twice by its table trigger alone — the harness never started it —
and the silver layers ran only for the sources that actually delivered.

**Accepted:** Separate triggered jobs, with the trade documented.

**Rejected:** One pipeline job with the layers as tasks — a barrier the
data model made unnecessary, paid for on every wave.

**Why:** Orchestration should mirror how the data actually arrives; when
the sources are independent, the jobs should be too.

---

## Follow-ups

- [x] Anchor spec approved; bronze, silver hardened in their own activity files (`04`, `05`)
