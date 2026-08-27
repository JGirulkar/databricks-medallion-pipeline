# AI Prompts — Planning and Requirements

> **Continues in:** [`02-tooling-rules-and-workflow.md`](02-tooling-rules-and-workflow.md) (implementation trace, hooks, MCP, CI)  
> **Project context:** `cursor-workflow/project-context.md`, `cursor-workflow/spec.md`, `cursor-workflow/task-breakdown.md`

## How this file is written

Curated from raw hook captures (`ai-prompts/capture/sessions/`) into a
readable record. Every entry carries the ask with its constraints, what was
verified before anything was marked ready, and an explicit
Accepted/Changed/Rejected with the reason — including the flows rejected for
breaking isolation, skipping tests, or leaking private artifacts.

---

## P1 — Assessment kickoff and readiness check

**Prompt:**  
"Start working from the shared DE C1 evaluation PDF — check if we have the necessary env ready. If yes, set up the GitHub repo and project structure here."

**Context provided:**  
DE C1 PDF (requirements), existing `README.md`, `docs/SETUP.md`, `cursor-workflow/spec.md`.

**AI response:**  
Parsed PDF requirements, audited local toolchain/auth state, compared repo layout against expected assessment artifacts, and produced a readiness gap report before any implementation.

**Validation:**  
- Checked Databricks CLI, uv, JDK, gh availability  
- Compared repo tree to PDF deliverable list  
- Identified blockers (auth, venv, bundle sync path) before coding

**Accepted:**  
- Environment-first sequencing before implementation  
- Requirement-driven approach using PDF + in-repo docs  
- Gap report style (ready vs blocked)

**Changed:**  
- Flow changed from "create repo immediately" to explicit readiness verification first  
- Setup plan expanded to cover submission artifacts, not just code

**Rejected:**  
- Starting pipeline implementation without validating auth/tooling isolation

**Why:**  
The context was given upfront and readiness was verified by running the checks, not assumed.

---

## P2 — Account isolation and non-disruption requirement

**Prompt:**  
"I do not want to logout — add JGirulkar for this project only. Do not change or disturb the other GitHub account or retail project; can't stress on it enough."

**Context provided:**  
Assessment isolation rules, `docs/GITHUB.md`, multi-account `gh` constraint.

**AI response:**  
Designed non-disruptive account strategy: local repo git identity, project-scoped GitHub flow, no global auth rewrites, explicit separation from other workspaces.

**Accepted:**  
- Keep existing accounts intact; avoid global auth changes  
- Project-local config and per-repo workflow only  
- Account isolation as a hard constraint

**Changed:**  
- Generic GitHub auth guidance refined to multi-account-safe flow after this constraint  
- Docs updated to explicitly avoid disturbing other repos/accounts

**Rejected:**  
- Any flow implying logout as a required first step

**Why:**  
Controlled multi-account operation under a hard constraint: the isolation was steered explicitly, and the risky default was rejected.

---

## P3 — Databricks and toolchain setup validation

**Prompt:**  
"JDK is installed, Databricks auth is done — check once and tell me the rest of the steps."

**Context provided:**  
`scripts/env.sh`, `de-assessment-ce` profile, `databricks/pyproject.toml`, local test tiers from `AGENTS.md`.

**AI response:**  
Ran end-to-end checks for Java, uv venv, pytest, Databricks profiles, and GitHub account state; fixed blocking script issues; verified working local baseline.

**Validation:**  
- `java -version` — JDK 21  
- `uv sync` + `uv run pytest --version` — venv OK  
- `databricks auth profiles` — `de-assessment-ce` present  
- `gh auth status` — JGirulkar active  
- Fixed CRLF in `scripts/env.sh` after syntax failure

**Accepted:**  
- JDK and venv verification as mandatory baseline  
- Databricks CE profile validation before deploy steps  
- Unit-test readiness separate from deploy readiness

**Changed:**  
- Hook/script CRLF issues fixed when they blocked automation  
- Bundle validation troubleshooting separated from environment readiness

**Rejected:**  
- Treating one successful command as proof all setup dimensions are complete

**Why:**  
Setup readiness was proven by running the checks, not assumed.

---

## P4 — Hook automation for session capture

**Prompt:**  
"Are we recording prompt history in the hooks? I will check and we will refine the hook if needed."

**Context provided:**  
Assessment requirement for full prompt history, `ai-prompts/README.md` curation model.

**AI response:**  
Audited hook chain (`sessionStart`, `beforeSubmitPrompt`, `afterFileEdit`, `stop`), identified capture silence (CRLF on stop hook), fixed it, validated output under `ai-prompts/capture/sessions/`.

**Validation:**  
- Triggered agent session end → session markdown appeared under `capture/sessions/`  
- Confirmed INDEX refresh and filename metadata

**Accepted:**  
- Hooks enabled for reliable raw capture  
- Curated history kept separate from raw logs  
- Capture as input; curate into numbered files

**Changed:**  
- Clarified: `capture/sessions/*.md` is a raw draft, never the published record  
- Hook reliability improved after line-ending and JSON-output fixes

**Rejected:**  
- Replacing hooks with manual-only prompt logging  
- Treating raw session dumps as final assessment prompt history

**Why:**  
Automation plus deliberate curation — a record of decisions, not log spam.

---

## P5 — Private/internal information boundary

**Prompt:**  
"Inspiration links and other local working notes stay git-ignored — they're
not project documentation."

**Context provided:**  
Published-vs-local artifact policy.

**AI response:**  
Implemented layered controls: `.private/`, `.cursor/private-notes/`, gitignore rules, and commit/push guard in `lint-before-git.sh`.

**Accepted:**  
- Private folders as local-only zones  
- Guardrails at ignore and hook levels  
- Raw capture blocked from commit history

**Changed:**  
- Guard script iterated after hook JSON safety popup (lint stdout suppressed for valid JSON response)

**Rejected:**  
- Relying only on `.gitignore` without pre-commit/push safeguards

**Why:**  
Artifact governance that prevents accidental leakage of working notes.

---

## P6 — GitHub integration path (plugin MCP + CLI fallback)

**Prompt:**  
"Check if GitHub MCP is available; we need it for repo/PR work. If plugin path fails, don't block — use `gh` CLI with JGirulkar only."

**Context provided:**  
`docs/GITHUB.md`, `github-assessment` skill, JGirulkar account isolation.

**AI response:**  
Diagnosed plugin MCP discovery/auth issues across sessions; secured workflow with `gh` CLI; later verified `plugin-github-github` as **ready** authenticated as **JGirulkar**; added project-level `.cursor/mcp.json` + `.cursor/settings.json` for team portability.

**Validation:**  
- `GetMcpTools` → `plugin-github-github` serverStatus: ready  
- `get_me` → login: JGirulkar  
- `gh auth status` → JGirulkar active  
- `gh pr list --repo JGirulkar/databricks-medallion-pipeline` → PR #1 visible

**Accepted:**  
- `gh` CLI as reliable operational path  
- GitHub plugin MCP when healthy  
- **JGirulkar only** for all GitHub operations

**Changed:**  
- Initial plan assumed plugin-only; revised to CLI-first, then plugin verified  
- User caught MCP not loading at project scope → added `.cursor/mcp.json` entry + `scripts/env.sh` token export

**Rejected:**  
- Waiting indefinitely for MCP before any repo progress  
- Using non-assessment GitHub accounts or remotes for this project

**Why:**  
Pragmatic tool selection with verification — the config gap was caught, fixed, and re-tested before relying on it.

---

## P7 — Documentation as the source of truth

**Prompt:**  
"Add the PDF content to docs so we reference the MD file, not the PDF — don't miss anything. Also add tooling docs."

**Context provided:**  
`docs/ASSESSMENT_FROM_PDF.md`, `docs/TOOLING.md` scope.

**AI response:**  
Added `docs/TOOLING.md` and expanded `docs/ASSESSMENT_FROM_PDF.md` to full in-repo reference including Strong/Weak Cursor Usage criteria.

**Accepted:**  
- Dual-document model: tooling reference + requirement reference  
- Full PDF transcription when summary was insufficient

**Changed:**  
- Short summary replaced with complete assessment reference per instruction

**Rejected:**  
- Keeping only a summarized PDF abstraction

**Why:**  
One source of truth that every future session starts from.

---

## P8 — First PR as integration test (setup phase)

**Prompt:**  
"PR is failing checks — do we have hook/lint coverage? Use this first PR to test env, workflow, and hooks end-to-end."

**Context provided:**  
PR #1 (`chore/remove-prompt-placeholders`), `.github/workflows/validate.yml`, `scripts/lint.sh`.

**AI response:**  
Investigated CI failures (executable bit on hook script, unused import in tests), fixed locally, validated lint path; treated PR #1 as setup/integration test rather than feature delivery.

**Validation:**  
- CI lint failures traced to `EXE001` (shebang not executable) and `F401` (unused import)  
- `chmod +x` + ruff fix applied  
- Re-ran lint path before push

**Accepted:**  
- PR #1 as workflow/hook/lint integration test  
- Fix root cause, not silence checks

**Changed:**  
- Confirmed hooks must return JSON-only stdout (earlier `lint-before-git.sh` fix)

**Rejected:**  
- Skipping hooks or `--no-verify` to green CI

**Why:**  
Nothing accepted unverified — the commit history carries the test → fix → refine sequence itself.

---

## Architecture design (continued in layer prompt file)

High-level medallion architecture discussion and anchor spec are documented in **[03-architecture-design.md](03-architecture-design.md)** (P1–P7). Planning file stops at setup/planning scope.

---

## Coverage note (planning/setup scope)

Planning/setup in this file covers:
- requirement source (PDF → in-repo docs)  
- setup sequencing (readiness before implementation)  
- account/profile isolation  
- prompt-history governance and authoring standards  

Technical file-by-file trace → **`02-tooling-rules-and-workflow.md`**.
