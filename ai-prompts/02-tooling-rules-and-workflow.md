# AI Prompts — Tooling, Rules, and Workflow

> **Planning rationale:** [`01-planning-and-requirements.md`](01-planning-and-requirements.md)  
> **Rules/skills:** `.cursor/rules/`, `.cursor/skills/`, `cursor-workflow/cursor-rules-or-instructions.md`

## Rubric alignment (Strong Cursor Usage)

| Strong signal | How we show it here |
|---------------|---------------------|
| `.cursorrules` / project standards | `.cursor/rules/*.mdc`, skills, hooks, lint gates |
| Specific prompts | Verbatim plugin/hook/MCP asks with named tools and expected outcomes |
| Validation | Commands and CI outcomes recorded before accepting changes |
| Reject off-architecture suggestions | e.g. manual-only capture, skip hooks, global logout |
| Git iteration | PR #1 test cycle documented with fix evidence; **commit after each logical slice** (see workflow below) |

### Git commit cadence (assessment eval)

After each logical implementation slice — same rhythm as Superpowers task completion — **commit before moving on**:

- One concern per commit (ingest library, CE deploy, data-gen fix, docs/prompts)
- Message: `feat|fix(scope): why in one line`
- Do not accumulate entire sessions into one commit at PR time
- Push branch and open PR when layer milestone is ready

---

## P1 — Plugin and tooling verification (specific check)

**Prompt:**  
"Check if plugins I added are there and configured well — `databricks`, `superpowers`, `github` — and tell me what is required vs optional before we proceed."

**Context provided:**  
`AGENTS.md`, `docs/TOOLING.md`, installed plugins under `~/.cursor/plugins/cache/`.

**AI response:**  
Plugin-by-plugin validation:
- **Databricks** — installed; skills/routing available  
- **Superpowers** — installed; process skills available selectively  
- **GitHub** — plugin MCP verified **ready** as **JGirulkar** after auth; `gh` CLI confirmed active

**Validation:**  
- `GetMcpTools(pattern: github)` → `plugin-github-github` serverStatus: ready  
- `get_me` → JGirulkar  
- `gh auth status` → JGirulkar active; other GitHub account inactive

**Accepted:**  
- Verify presence **and** runtime operability, not just installation  
- Databricks + Superpowers as primary skill paths  
- GitHub via plugin MCP + `gh` CLI (JGirulkar only)

**Changed:**  
- Early sessions reported plugin instability; after user re-auth and MCP config, status updated to **working**  
- Required vs optional made explicit in docs

**Rejected:**  
- Assuming "installed" equals "operational" without testing  
- Treating transient MCP errors as permanent blocker

**Why:**  
Specific verification prompt with evidence — avoids weak "is it set up?" vagueness.

---

## P2 — Databricks routing and profile discipline

**Prompt:**  
"Route Databricks work through skills; use `de-assessment-ce` only — never non-assessment profiles."

**Context provided:**  
`scripts/env.sh`, `.cursor/mcp.json` (`databricks-de-assessment`), `databricks-routing` rule.

**AI response:**  
Used Databricks skill/routing guidance; validated CE profile; documented isolation in setup docs and `AGENTS.md`.

**Validation:**  
- `databricks auth profiles` shows `de-assessment-ce`  
- MCP config uses `DATABRICKS_CONFIG_PROFILE=de-assessment-ce`

**Accepted:**  
- Profile-specific execution; CE-only boundary  
- Skill-guided flow, not ad hoc commands

**Changed:**  
- Setup docs tightened to call out profile leakage risk

**Rejected:**  
- Implicit/default profile when ambiguity exists

**Why:**  
Architecture boundary enforced — reject cross-environment leakage.

---

## P3 — GitHub account safety and repo workflow

**Prompt:**  
"Use JGirulkar without disturbing other accounts; push and access only that account for this project."

**Context provided:**  
`github-assessment` skill, `docs/GITHUB.md`, remote `JGirulkar/databricks-medallion-pipeline`.

**AI response:**  
Project-safe workflow: local git identity, `GH_TOKEN=$(gh auth token -u JGirulkar)` pattern, rules in `AGENTS.md` and `project-overview.mdc`.

**Validation:**  
- `git remote -v` → JGirulkar repo URL  
- `gh repo view JGirulkar/databricks-medallion-pipeline` — accessible

**Accepted:**  
- No forced global logout  
- JGirulkar-only rule committed to always-applied project rules

**Changed:**  
- Generic auth suggestions refined per multi-account constraint

**Rejected:**  
- Resetting global account state to switch users

**Why:**  
Real-world multi-account hygiene with explicit user constraint.

---

## P4 — Hook architecture for prompt capture and controls

**Prompt:**  
"Confirm hooks capture prompts/responses and block bad commits. Fix the popup: `beforeShellExecution returned stdout that is not valid JSON`."

**Context provided:**  
`.cursor/hooks.json`, `scripts/cursor-hooks/`, assessment artifact policy.

**AI response:**  
Confirmed full hook chain; fixed CRLF failures; fixed invalid JSON by suppressing lint stdout in `lint-before-git.sh`; added private/raw-capture staging guard.

**Validation:**  
- Hook popup resolved after lint stdout redirect  
- Staging `.private/` or `capture/sessions/` blocked with clear JSON message  
- Session files written on `stop` after CRLF fix

**Accepted:**  
- Capture hooks enabled  
- Curated files separate from raw logs  
- Commit/push guard in hook path

**Changed:**  
- CRLF → LF on hook scripts  
- Guard extended for raw capture and private notes

**Rejected:**  
- Manual-only prompt tracking  
- Committing raw hook dumps to evaluator history

**Why:**  
Tested automation before trusting it — rubric validation signal.

---

## P5 — Ignore policy and private working boundaries

**Prompt:**  
"Keep internal strategy notes git-ignored; add guard even though gitignore exists — why do we need both?"

**Context provided:**  
`.gitignore`, pre-commit safety requirements.

**AI response:**  
Defense-in-depth: gitignore for normal workflow + hook guard for accidental `git add` of private/raw paths.

**Accepted:**  
- Both ignore and block layers  
- `.private/`, `.cursor/private-notes/`, raw capture excluded

**Changed:**  
- Policy evolved from ignore-only to ignore + hook validation

**Rejected:**  
- Trusting ignore rules alone

**Why:**  
User questioned design → explained and kept stronger guard (iteration + reasoning).

---

## P6 — Workflow and CI as integration gate

**Prompt:**  
"Are we ready for the first PR? Use it to validate workflow, lint, and hooks."

**Context provided:**  
`.github/workflows/validate.yml`, `.github/workflows/deploy-ce.yml`, PR #1 branch.

**AI response:**  
Verified workflow files exist; used PR #1 to surface CI gaps; fixed lint issues; confirmed validate workflow path.

**Validation:**  
- PR #1 opened: `docs: remove unused prompt-history placeholders`  
- CI failed → root-caused → fixed executable bit + unused import  
- Re-push after local lint pass

**Accepted:**  
- Reuse existing workflows; don't overbuild CI before pipeline code  
- PR #1 as setup integration test

**Changed:**  
- Shifted focus from "create more workflows" to "make existing gate pass"

**Rejected:**  
- Extra workflows before core medallion deliverables  
- Bypassing hooks to green CI

**Why:**  
Git iteration with test evidence — strong rubric signal.

---

## P7 — Tooling documentation as evidence

**Prompt:**  
"Add tooling.md and full PDF reference in docs — evaluators should not need external files."

**AI response:**  
Added `docs/TOOLING.md`, `docs/ASSESSMENT_FROM_PDF.md` (full transcription including Strong/Weak Cursor Usage).

**Accepted:**  
- Reviewer-facing source-of-truth inside repo

**Changed:**  
- Summary → full reference when user requested complete context

**Rejected:**  
- Lightweight summary-only when full context was needed

**Why:**  
Persistent context for all future agent sessions.

---

## P8 — Hook output optimization (user-identified)

**Prompt:**  
"Too many hook session files with weak names — improve filenames, add retention, and record that I caught this."

**Context provided:**  
`ai-prompts/capture/sessions/` noise, curation workflow pain.

**AI response:**  
Enriched filenames (`timestamp_id_model_status_topic.md`), retention (latest 15), index refresh, model signals in metadata; updated `capture/README.md`.

**Validation:**  
- New sessions use metadata-rich names  
- Old files pruned beyond cap  
- `models_seen` appears in session markdown when present in transcript

**Accepted:**  
- Human-scannable raw filenames  
- Auto-pruning to reduce noise  
- User-driven optimization recorded in history

**Changed:**  
- Append-style index → refresh-style after prune

**Rejected:**  
- Unbounded raw capture growth  
- Generic filenames blocking curation

**Why:**  
User steering → measurable improvement — not vague "make it better."

---

## P9 — Model-toggling evidence in capture flow

**Prompt:**  
"When using superpowers/subagent workflows, capture model usage so evaluators see intentional model toggling."

**AI response:**  
Extended `session_capture.py` to extract model strings from transcripts into session metadata and `Model usage signals` section.

**Accepted:**  
- Model signals in raw capture for later curation into `03`–`10`

**Changed:**  
- Parser writes `Models observed in transcript` line per session

**Rejected:**  
- Relying on memory for model-choice evidence

**Why:**  
Observable model strategy for assessment narrative.

---

## P10 — Curation skill and authoring rules

**Prompt:**  
"Create a skill to draft curated prompt history from hook files; add rules for how I want entries written so I don't repeat corrections."

**AI response:**  
Created `.cursor/skills/prompt-history-curation/SKILL.md`; expanded `ai-prompts/README.md` authoring rules (raw→curated, steering, accept/reject, model signals, private boundaries).

**Accepted:**  
- Automate first draft, human polish final  
- Codify preferences into reusable skill + README rules

**Changed:**  
- Ad hoc curation → documented repeatable workflow

**Rejected:**  
- Fully automatic final write-up without human pass

**Why:**  
Efficient AI usage: skill reduces repetition; human keeps quality.

---

## P11 — `/nohistory` opt-out for non-project chats

**Prompt:**  
"Some chats won't be for assessment history — how do I exclude them from hooks?"

**Context provided:**  
Hook capture scope, evaluator vs operational chat separation.

**AI response:**  
Added opt-out markers in `session_capture.py`: start any prompt with `/nohistory` or `#nohistory` to disable capture for that session; documented in `ai-prompts/capture/README.md`.

**Validation:**  
- Prompt beginning with `/nohistory` → `capture_disabled` in session state  
- No session markdown written on `stop` for opted-out chats

**Accepted:**  
- Per-session opt-out without disabling hooks globally  
- Keeps evaluator history focused on assessment work

**Changed:**  
- Previously all chats captured; now selective by user prefix

**Rejected:**  
- Disabling hooks entirely for convenience

**Why:**  
User identified scope problem → targeted fix; avoids polluting history with unrelated chats (editorial judgment).

---

## P12 — GitHub MCP project configuration (user caught + tested)

**Prompt:**  
"GitHub MCP is private/user scope — change it to project level in JSON. I caught it wasn't loading; re-auth JGirulkar and recheck after reload."

**Context provided:**  
`.cursor/mcp.json`, `.cursor/settings.json`, `scripts/env.sh`, JGirulkar isolation.

**AI response:**  
Added `github-de-assessment` to `.cursor/mcp.json` (remote endpoint + `${env:GITHUB_DE_ASSESSMENT_TOKEN}`); enabled GitHub plugin in project `.cursor/settings.json`; extended `env.sh` to export token from `gh auth token -u JGirulkar`; updated `AGENTS.md` / `project-overview.mdc` with JGirulkar-only rule.

**Validation:**  
- `gh auth status` → JGirulkar active after re-auth  
- `plugin-github-github` → ready, `get_me` → JGirulkar  
- `gh pr list` → PR #1 visible  
- Project `mcp.json` servers may require Tools & MCP enable + env inheritance; **plugin MCP confirmed working** for agent use

**Accepted:**  
- Project-level JSON config for team portability  
- Plugin MCP as operational path when project stdio servers not loaded  
- User catch-and-fix loop documented

**Changed:**  
- From "plugin only at user scope" to committed project config + verified plugin auth

**Rejected:**  
- Blocking all GitHub work until project stdio MCP loads  
- Using non-JGirulkar accounts

**Why:**  
User detected gap AI missed → configured, tested, documented — strong collaboration signal.

---

## P13 — Databricks MCP + plugin JSON hardening (bronze brainstorm)

**Prompt:**  
"Set up Databricks plugin and MCP at JSON level (UI glitchy). Always use assessment profile only. Continue with bootstrap job (A)."

**Context provided:**  
`.cursor/mcp.json`, `.cursor/settings.json`, CE workspace probe via CLI, architecture spec.

**AI response:**  
Removed `defer_loading` from MCP servers; pinned `DATABRICKS_CONFIG_PROFILE` + `DATABRICKS_HOST` in MCP env; enabled `databricks` + `superpowers` plugins and disabled global `github` plugin in project settings; added always-applied `databricks-assessment-profile.mdc`; hardened `scripts/env.sh` and session-start context.

**Validation:**  
- MCP server venv + `run_server.py` smoke test (stdio starts)  
- CLI `databricks current-user me --profile de-assessment-ce` succeeds  
- CE UC managed catalog `workspace` confirmed

**Accepted:**  
- JSON-first project config over UI toggles  
- Strict `de-assessment-ce` profile + host pinning  
- Dedicated bootstrap job direction for bronze (pending design doc)

**Changed:**  
- `env.sh` no longer allows profile override via pre-set env var

**Rejected:**  
- Relying on UI-only MCP enablement  
- Using non-assessment Databricks profiles

**Why:**  
User needs reliable agent access to CE workspace without profile leakage.

---

## Coverage trace — everything configured so far

### Tooling and environment
- `scripts/env.sh` — CE profile, Java path, `GITHUB_DE_ASSESSMENT_TOKEN` export from `gh`  
- `databricks/pyproject.toml` + `uv sync` — local dev/test baseline  
- JDK / Databricks / GitHub auth verified in session flow

### Cursor project config
- `.cursor/mcp.json` — `databricks-de-assessment`, `github-de-assessment` (profile + host pinned; no defer_loading)  
- `.cursor/settings.json` — `databricks` + `superpowers` enabled; `github` plugin disabled  
- `.cursor/rules/databricks-assessment-profile.mdc` — always-applied CE profile isolation  
- `.cursor/rules/*.mdc` — always-applied standards (isolation, explore-before-change, artifacts)  
- `.cursor/skills/` — `github-assessment`, `prompt-history-curation`, layer/deploy/test skills

### Databricks bundle / CI
- `databricks/bundle/databricks.yml` — sync paths, single-node CE clusters  
- `.github/workflows/validate.yml` — lint + unit (PR #1 integration-tested)  
- `.github/workflows/deploy-ce.yml` — manual CE deploy

### Prompt capture and hooks
- `.cursor/hooks.json` — sessionStart, beforeSubmitPrompt, afterFileEdit, beforeShellExecution, stop  
- `scripts/cursor-hooks/session_capture.py` — reliability, rich filenames, retention, model signals, `/nohistory` opt-out  
- `scripts/cursor-hooks/lint-before-git.sh` — private/raw guard, JSON-safe lint gate

### Guardrails and privacy
- `.gitignore` — private notes, raw capture  
- Hook blocks staging of `.private/`, `capture/sessions/`, `INDEX.md`

### Documentation
- `docs/TOOLING.md`, `docs/ASSESSMENT_FROM_PDF.md`, `docs/GITHUB.md`  
- `ai-prompts/README.md` — authoring rules + rubric-aligned entry template  
- `cursor-workflow/{project-context,spec,task-breakdown,cursor-rules-or-instructions}.md`

### User-driven iteration signals
- Plugin verification requested explicitly (P1)  
- Hook filename noise user-identified and fixed (P8)  
- `/nohistory` user-requested for non-assessment chats (P11)  
- GitHub MCP scope user-caught and project-configured (P12)  
- PR #1 used as workflow/hook/lint integration test (P6, cross-ref `01` P10)
