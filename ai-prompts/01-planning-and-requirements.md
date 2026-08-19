# AI Prompts — Planning and Requirements

## P1 — Assessment kickoff and readiness check

**Prompt:**  
Start working from the shared DE C1 evaluation PDF, verify whether environment is ready, then set up GitHub repo and project structure.

**AI response:**  
Parsed the PDF requirements, audited local environment/tools/auth state, compared current repo structure against expected assessment artifacts, and identified readiness gaps before implementation.

**Accepted:**  
- Environment-first sequencing before implementation.
- Requirement-driven approach using PDF + existing repo docs (`README`, `SETUP`, workflow docs).
- Gap report style (what is ready vs blocked).

**Changed:**  
- Instead of immediately creating/pushing repo, flow was changed to explicit readiness verification first.
- Setup plan was rewritten to include both technical prerequisites and submission-artifact prerequisites.

**Rejected:**  
- Starting implementation without validating auth/tooling isolation.

**Why:**  
This reduced rework risk and ensured the project starts from a reproducible baseline.

---

## P2 — Account isolation and non-disruption requirement

**Prompt:**  
Do not log out/switch existing accounts globally; keep Intelo/retail context untouched; use project-scoped behavior only.

**AI response:**  
Designed a non-disruptive account strategy: local repo identity, project-scoped git/GitHub behavior, no global account rewrites, and clear separation between assessment and other workspaces.

**Accepted:**  
- Keep existing accounts intact; avoid global auth changes.
- Prefer project-local config and explicit per-repo workflow.
- Treat account isolation as a hard constraint.

**Changed:**  
- Initial generic GitHub auth guidance was refined to multi-account-safe flow after your constraint.
- Messaging/docs updated to explicitly avoid disturbing other repos/accounts.

**Rejected:**  
- Any instruction that implied logging out as required first step.

**Why:**  
Shows controlled, enterprise-style workspace hygiene while still moving forward efficiently.

---

## P3 — Databricks and toolchain setup validation

**Prompt:**  
Validate JDK, venv, Databricks CE profile, and GitHub auth status end-to-end.

**AI response:**  
Ran checks for Java/uv/pyspark/pytest, Databricks auth profiles, and GitHub account state; fixed blocking script issues and verified working local environment.

**Accepted:**  
- JDK and venv verification as mandatory baseline.
- Databricks CE profile validation before any deploy steps.
- Keep unit-test readiness separate from deploy readiness.

**Changed:**  
- Hook/script CRLF issues discovered during setup were fixed because they blocked reliable automation.
- Bundle validation troubleshooting was separated from environment readiness (different problem layers).

**Rejected:**  
- Treating one successful command as proof that all setup dimensions are complete.

**Why:**  
This demonstrates evidence-based setup verification rather than assumptions.

---

## P4 — Hook automation strategy for prompt history

**Prompt:**  
Confirm whether prompt history is being captured, where hook output lands, and whether it is evaluator-appropriate.

**AI response:**  
Audited hook chain (`sessionStart`, `beforeSubmitPrompt`, `afterFileEdit`, `stop`), identified why capture files were not appearing (stop-hook CRLF), fixed it, and validated output path behavior.

**Accepted:**  
- Keep hooks enabled for reliable raw capture.
- Keep curated evaluator artifacts separate from raw logs.
- Use capture as input; curate manually into numbered files.

**Changed:**  
- Capture process clarified: `ai-prompts/capture/sessions/*.md` is raw, not final evaluator output.
- Hook reliability improved after line-ending and JSON-output fixes.

**Rejected:**  
- Replacing hooks with manual-only prompt logging.
- Treating raw session dumps as final assessment prompt history.

**Why:**  
Balances automation reliability with intentional curation quality.

---

## P5 — Private/internal information boundary

**Prompt:**  
Keep internal planning/inspiration notes out of commits/PR/evaluation while still using them locally.

**AI response:**  
Implemented layered controls: private folders, gitignore rules, and commit/push guard checks to block accidental staging of private or raw-capture files.

**Accepted:**  
- `.private/` and `.cursor/private-notes/` as local-only zones.
- Guardrails at both ignore and hook levels.
- Raw capture (`ai-prompts/capture/sessions/*.md`, `INDEX.md`) blocked from commit history.

**Changed:**  
- Guard script was iterated after hook JSON safety popup to ensure clean JSON-only output.

**Rejected:**  
- Relying only on `.gitignore` without pre-commit/push safeguards.

**Why:**  
Shows mature artifact governance and prevents accidental leakage in evaluated commit history.

---

## P6 — GitHub plugin/MCP and workflow decision

**Prompt:**  
Configure GitHub plugin/MCP; if unstable, decide whether it blocks progress.

**AI response:**  
Diagnosed repeated GitHub MCP server discovery failures (`serverStatus: error`), advised secure token handling/rotation, and moved workflow to `gh` CLI to avoid blocking project progress.

**Accepted:**  
- Continue delivery even when optional integration is unstable.
- Use `gh` as reliable fallback for repo operations.

**Changed:**  
- Plan shifted from plugin-dependent flow to CLI-first flow.

**Rejected:**  
- Waiting indefinitely for MCP recovery before continuing assessment work.

**Why:**  
Demonstrates pragmatic tool selection and continuity under integration instability.

---

## P7 — Documentation as reviewer-facing source of truth

**Prompt:**  
Add complete docs for tooling choices and assessment reference so the repo is self-contained.

**AI response:**  
Added `docs/TOOLING.md` and expanded `docs/ASSESSMENT_FROM_PDF.md` into a full in-repo reference to avoid repeated PDF parsing and preserve requirement context.

**Accepted:**  
- Dual-document model:
  - tool/process reference (`TOOLING.md`)
  - requirement/source reference (`ASSESSMENT_FROM_PDF.md`)

**Changed:**  
- Initial short summary doc was replaced with full-detail assessment reference per your instruction.

**Rejected:**  
- Keeping only a summarized PDF abstraction when full context was requested.

**Why:**  
Improves traceability and review clarity while reducing context loss across sessions.

---

## P8 — Setup narrative quality for evaluator reading

**Prompt:**  
Make prompt history read naturally, showing user steering, iterations, and selective acceptance/rejection (without appearing inefficient).

**AI response:**  
Reworked this file into prompt-by-prompt entries showing your guidance, AI adjustments, and reasoned decisions as deliberate iteration.

**Accepted:**  
- Explicitly record user-supplied constraints (PDF, isolation, curation policy, plugin decisions).
- Show iteration as quality control, not indecision.

**Changed:**  
- Converted from single compressed summary to timeline-style entries.

**Rejected:**  
- Over-compressed setup note that masked your steering inputs.

**Why:**  
This better demonstrates effective AI collaboration: clear direction, controlled iteration, and decision accountability.

---

## P9 — Prompt-history quality governance from user feedback

**Prompt:**  
Before moving to later prompt-history files, make `01` and `02` stronger by explicitly capturing what I drove you to change (e.g., hook file-noise optimization, skill creation direction, and format preferences), so entries look realistic and intentional.

**AI response:**  
Reworked narrative quality in early files to foreground user steering events, quality corrections, and iterative decisions rather than generic summaries.

**Accepted:**  
- Treat prompt history as evaluator evidence of active guidance, not passive logging.
- Record "user-identified inconsistency -> requested optimization -> implemented change" loops explicitly.

**Changed:**  
- Early setup/tooling entries were revised to include direct user-driven refinements and the reasoning behind each change.

**Rejected:**  
- Moving ahead to next activity files with under-detailed setup/tooling history.

**Why:**  
This creates a stronger assessment signal: the workflow is collaborative, quality-controlled, and intentionally driven by review feedback.

---

## Coverage note (planning/setup scope)

Planning/setup coverage in this file intentionally includes:
- requirement-source handling (PDF as primary brief),
- setup sequencing decisions (readiness before implementation),
- account/profile isolation constraints,
- prompt-history governance decisions.

Detailed file-by-file tooling/configuration trace is maintained in:
- `ai-prompts/02-tooling-rules-and-workflow.md` -> **Coverage trace — everything configured so far**.
