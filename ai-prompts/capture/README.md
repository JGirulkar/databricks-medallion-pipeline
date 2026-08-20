# Session capture (hook drafts)

**Purpose:** Demonstrate automated AI session logging via Cursor hooks for the DE C1 assessment. These files are **raw drafts** — you refine them into the curated numbered entries in `ai-prompts/01-….md`.

## Project-only scope (assessment repo only)

Hooks are defined in **this repo only**: `.cursor/hooks.json`.

| Scope | Location | Applies to |
|-------|----------|------------|
| **This assessment** | `databricks-medallion-pipeline/.cursor/hooks.json` | Only when this folder is the Cursor workspace root |
| **Other projects** | No hooks unless they have their own `.cursor/hooks.json` | Unaffected |
| **Global** | `~/.cursor/hooks.json` | **Not used** — we do not put assessment hooks here |

**Requirement:** Open `~/Desktop/Projects/databricks-medallion-pipeline/` as the workspace (File → Open Folder). Hooks run only when this repo is the workspace root.

Scripts write only under this repo (`ai-prompts/capture/`). A workspace guard in `session_capture.py` no-ops if the active workspace is not this project.

## How it works

| Hook | Script | What it records |
|------|--------|-----------------|
| `sessionStart` | `session-start.sh` | Session start, workspace roots |
| `beforeSubmitPrompt` | `track-prompt.sh` | Each user prompt |
| `afterFileEdit` | `track-file-edit.sh` | Files the agent edited |
| `stop` | `prompt-capture.sh` | Merges state + transcript → markdown draft |

Output: `ai-prompts/capture/sessions/YYYY-MM-DD_HHMMSS_<id>_<model>_<status>_<topic>.md`  
Index: `ai-prompts/capture/INDEX.md`

Ephemeral state (not committed): `.session-state.json`

Retention:
- Hook keeps only the latest 15 session markdown files under `sessions/`.
- Older raw session files are auto-pruned.
- Session metadata also records model signals observed in transcript payloads (helps show model-switching/tooling choices across runs).

Opt-out per chat:
- Start any chat/prompt with `/nohistory` (or `#nohistory`) to skip capture for that session.
- When opt-out is used, no session markdown draft is written on stop.

## Your workflow

1. Work in Cursor Agent as usual — hooks capture in the background.
2. When a session ends, open the latest file under `sessions/`.
3. Copy the **Draft P-entry** block; edit Accepted/Changed/Rejected/Why.
4. Paste into the right numbered file (e.g. `02-tooling-rules-and-workflow.md`).
5. Optionally trim redundancy — assessors reward **editorial judgment**, not log spam.

## Why keep both?

- **Hook captures** = evidence of *process automation* and complete session record.
- **Numbered ai-prompts/** = *curated* narrative for evaluators (senior 94/100 pattern).

Redundancy is intentional: hooks ensure nothing is lost; you distill what matters for scoring.
