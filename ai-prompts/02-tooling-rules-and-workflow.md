# AI Prompts — Tooling, Rules, and Workflow

## P1 — Plugin and tooling verification (specific check)

**Prompt:**  
Verify exactly what I configured is actually present and usable:
- Is `databricks` plugin installed and usable?
- Is `superpowers` plugin installed and usable?
- Is `github` plugin installed and configured properly?
- Are related skills/hooks available and working?
- Based on that, decide what is required vs optional for execution.

**AI response:**  
Performed plugin-by-plugin/tool-by-tool validation and separated outcomes into:
- **Working and required for current execution:** Databricks skill path, Superpowers process skills, local hook automation, Databricks CLI/uv/JDK.
- **Configured but unstable/non-blocking:** GitHub plugin MCP path (repeated server discovery/auth errors despite token reconfiguration attempts).
- **Reliable fallback for GitHub operations:** `gh` CLI with account-safe workflow.

**Accepted:**  
- Explicit verification of `databricks`, `superpowers`, and `github` plugin paths instead of assuming availability.
- Databricks + Superpowers as primary, working plugin/skill paths.
- `gh` CLI as operational GitHub path when plugin path is unstable.
- Required vs optional classification made explicit:
  - required: Databricks skill path, env/toolchain, hooks
  - optional: GitHub MCP plugin integration

**Changed:**  
- Initial expectation that GitHub plugin would be the main repo-management path was revised after multiple validation attempts failed (`not_found`/server discovery errors).
- Prompt-history notes were updated to reflect your exact instruction: verify plugin presence/config quality first, then choose execution path.

**Rejected:**  
- Treating GitHub plugin instability as a hard blocker for project progress.
- Assuming “installed” equals “operational” without runtime verification.

**Why:**  
This kept velocity high while preserving a robust, auditable workflow.

---

## P2 — Databricks routing and profile discipline

**Prompt:**  
Route Databricks operations through appropriate skills and keep strict profile isolation.

**AI response:**  
Used Databricks skill/routing guidance, validated CE profile usage, and ensured setup/docs emphasize `de-assessment-ce` boundary.

**Accepted:**  
- Profile-specific execution and CE-only isolation for this repo.
- Databricks task handling through skill-guided flow, not ad hoc random commands.

**Changed:**  
- Setup steps were tightened to call out profile leakage risk explicitly.

**Rejected:**  
- Implicit/default profile behavior when ambiguity existed.

**Why:**  
Reduces accidental cross-environment usage and improves reproducibility.

---

## P3 — GitHub account safety and repo workflow

**Prompt:**  
Use JGirulkar account without disrupting other account contexts; keep project operations isolated.

**AI response:**  
Designed project-safe workflow with local git identity and non-disruptive auth behavior; documented GitHub flow in repo skills/docs.

**Accepted:**  
- No forced global logout pattern.
- Project-first workflow guidance for account separation.

**Changed:**  
- Generic auth suggestions were refined based on your explicit “do not disturb other accounts” requirement.

**Rejected:**  
- Any flow that depended on resetting global account state.

**Why:**  
Demonstrates controlled multi-account operations under real-world constraints.

---

## P4 — Hook architecture for prompt capture and controls

**Prompt:**  
Ensure prompt history capture is automated and maintain commit hygiene safeguards.

**AI response:**  
Confirmed hook chain (`sessionStart`, `beforeSubmitPrompt`, `afterFileEdit`, `beforeShellExecution`, `stop`), fixed execution issues, and enforced guardrails for private/raw files.

**Accepted:**  
- Keep capture hooks enabled for reliability.
- Keep curated prompt-history files separate from raw capture logs.
- Maintain commit/push guard in hook path.

**Changed:**  
- Fixed CRLF script failures in hook scripts.
- Fixed invalid hook JSON behavior by suppressing non-JSON stdout.
- Extended guard rules for raw capture files and private notes.

**Rejected:**  
- Manual-only prompt tracking.
- Committing raw hook capture dumps to evaluator-facing history.

**Why:**  
Combines evidence completeness with artifact-quality control.

---

## P5 — Ignore policy and private working boundaries

**Prompt:**  
Keep internal strategy and non-evaluator content local while preserving structured repo artifacts.

**AI response:**  
Implemented layered exclusion strategy: `.gitignore` boundaries + hook-level blocking of sensitive/non-final paths.

**Accepted:**  
- Ignore and block `.private/` and `.cursor/private-notes/`.
- Ignore and block `ai-prompts/capture/sessions/*.md` and capture index outputs.

**Changed:**  
- Policy evolved from simple ignore-only to defense-in-depth with hook validation.

**Rejected:**  
- Trusting ignore rules alone without staged-file guard checks.

**Why:**  
Prevents accidental leakage into commits and keeps evaluation artifacts intentional.

---

## P6 — Workflow and CI status

**Prompt:**  
Confirm whether workflows/review support exist or need creation.

**AI response:**  
Verified existing workflow files (`validate.yml`, `deploy-ce.yml`) and confirmed review skills are available for optional quality gates.

**Accepted:**  
- Reuse current workflows rather than overbuilding new CI immediately.
- Keep review skills available as on-demand quality tools.

**Changed:**  
- Clarified that repo is already workflow-enabled; effort should move to implementation and artifact quality.

**Rejected:**  
- Creating extra workflows before core pipeline work begins.

**Why:**  
Focuses effort on deliverables that directly affect assessment scoring.

---

## P7 — Tooling documentation as evidence

**Prompt:**  
Document tooling and assessment source context in-repo for reviewer clarity.

**AI response:**  
Added:
- `docs/TOOLING.md` (what was used, what is local-only, hook/skill model)
- `docs/ASSESSMENT_FROM_PDF.md` (full in-repo assessment reference)

**Accepted:**  
- Keep reviewer-facing source-of-truth docs inside repo.

**Changed:**  
- Converted assessment doc from summary form to full-detail reference when requested.

**Rejected:**  
- Lightweight summary-only references when complete context was needed.

**Why:**  
Improves traceability, reduces ambiguity, and preserves context across sessions.

---

## P8 — Hook output optimization and quality steering

**Prompt:**  
Optimize hook capture because too many session markdown files were being created with weak identifiers. Keep date, but improve filename clarity so raw files are easy to use for curation and easy to prune/delete afterward. Also ensure this inconsistency detection and correction is captured in prompt history.

**AI response:**  
Upgraded session capture behavior to be more operator-friendly:
- filename enriched to include conversation id, model, status, and topic slug from the first prompt
- automatic retention added (keeps latest 15 raw session files)
- index generation refreshed from current files each run
- capture README updated to document naming and retention behavior

**Accepted:**  
- Rich, human-scannable file naming for raw capture sessions.
- Auto-pruning strategy to reduce accumulation noise.
- Explicitly recording this refinement as a user-driven quality iteration.

**Changed:**  
- Previous filename pattern (`timestamp + short id`) replaced with metadata-rich naming.
- Session-index maintenance changed from append-style to refresh-style for consistency after pruning.

**Rejected:**  
- Keeping unbounded raw capture growth.
- Generic filenames that make curation and cleanup hard.

**Why:**  
This improves day-to-day curation speed, keeps raw artifacts manageable, and shows strong prompt leadership: identifying inconsistency and driving targeted workflow optimization.

---

## P9 — Model-toggling evidence in capture flow

**Prompt:**  
Ensure future superpowers/subagent-driven workflows also leave evidence of model usage decisions so evaluator narrative can show active, intentional model selection.

**AI response:**  
Extended capture parser to collect model strings from transcript payloads and write model-usage signals into each raw session markdown file metadata/section.

**Accepted:**  
- Keep model-usage signals in raw capture artifacts for later curation into evaluator-facing prompt history.

**Changed:**  
- Session capture now includes:
  - `Models observed in transcript` metadata line
  - `Model usage signals` section when model keys are present

**Rejected:**  
- Relying only on memory/manual notes for model-choice evidence.

**Why:**  
This makes model strategy observable and easier to cite when explaining efficient model toggling and workflow control.

---

## P10 — Curation rules and reusable skill creation

**Prompt:**  
Add explicit authoring rules for how prompt history should be generated (including curation from raw hook files) and create a reusable skill to automate first-pass drafting so repetitive work is reduced while preserving manual final edits.

**AI response:**  
Implemented two workflow assets:
- updated `ai-prompts/README.md` with team-style curation rules (raw->curated flow, steering visibility, accepted/changed/rejected discipline, model-signal usage, private-note boundaries, narrative continuity)
- created `.cursor/skills/prompt-history-curation/SKILL.md` to generate first-pass curated entries from raw captures

**Accepted:**  
- "Automate base draft, manually polish final narrative" operating model.
- Codifying your preferences into reusable rules so future sessions do not require repeated correction.
- Explicit mention that curated files are built from raw hook markdown inputs.

**Changed:**  
- Prompt-history process evolved from ad hoc edits to documented/skill-backed repeatable workflow.

**Rejected:**  
- Repeating the same formatting/process instructions manually in every new session.
- Fully automatic final write-up without human curation pass.

**Why:**  
This demonstrates efficient AI usage maturity: using skills/rules to reduce repetitive work while keeping human judgment in the final evaluator-facing output.

---

## Coverage trace — everything configured so far

This section explicitly lists setup/config additions made so far and why they exist, so evaluator-facing history does not rely on implicit context.

### Tooling and environment
- `scripts/env.sh` — CE profile and Java path setup; stabilized for reliable sourcing.
- `databricks/pyproject.toml` + `uv sync` flow — local dev/test dependency baseline.
- JDK/Databricks/GitHub auth verification steps documented and validated in session flow.

### Databricks bundle/workflow
- `databricks/bundle/databricks.yml` — sync-path and single-node cluster configuration refinements for CE compatibility.
- `.github/workflows/validate.yml` — lint + unit validation path.
- `.github/workflows/deploy-ce.yml` — manual deploy/run workflow path.

### Prompt capture and hook reliability
- `.cursor/hooks.json` wiring confirmed for:
  - `sessionStart`
  - `beforeSubmitPrompt`
  - `afterFileEdit`
  - `beforeShellExecution`
  - `stop`
- `scripts/cursor-hooks/session_capture.py` improvements:
  - fixed reliability issues
  - richer session filename metadata
  - session retention pruning
  - index refresh
  - model-signal extraction for curation evidence
- `scripts/cursor-hooks/prompt-capture.sh` and related scripts — line-ending and execution stability fixes.

### Guardrails and privacy boundaries
- `.gitignore` updates:
  - private local note paths
  - raw hook capture outputs
  - preserve structural placeholders (`.gitkeep`) only
- `scripts/cursor-hooks/lint-before-git.sh`:
  - block private/raw-capture staging on commit/push
  - JSON-safe hook responses to avoid Cursor hook safety blocks

### Documentation and evaluation source-of-truth
- `docs/TOOLING.md` — what is used vs local-only, and why.
- `docs/ASSESSMENT_FROM_PDF.md` — full in-repo assessment reference.
- `ai-prompts/README.md` — explicit curation rules requested by user.

### Reusable process automation
- `.cursor/skills/github-assessment/SKILL.md` — GitHub repo flow without disturbing other account contexts.
- `.cursor/skills/prompt-history-curation/SKILL.md` — first-pass curation skill from raw capture files.

### User-driven iteration signals captured
- Plugin verification was requested explicitly and recorded explicitly.
- Hook file-noise issue was user-identified and optimized.
- Prompt-history style/detail constraints were user-directed and reflected in curated entries.
- “Automate repetitive base work but keep manual final control” was user-directed and implemented.
