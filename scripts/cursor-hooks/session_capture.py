#!/usr/bin/env python3
"""Cursor hooks: capture agent session drafts for ai-prompts/ curation.

Subcommands (wired in .cursor/hooks.json):
  start   — sessionStart: init ephemeral state
  prompt  — beforeSubmitPrompt: record user prompt
  edit    — afterFileEdit: record touched files
  stop    — stop: write markdown draft from state + transcript
"""

from __future__ import annotations

import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
CAPTURE_DIR = REPO_ROOT / "ai-prompts" / "capture"
SESSIONS_DIR = CAPTURE_DIR / "sessions"
STATE_FILE = CAPTURE_DIR / ".session-state.json"

MAX_PROMPT_CHARS = 12_000
MAX_ASSISTANT_CHARS = 2_000
MAX_TRANSCRIPT_TURNS = 40
MAX_SESSION_FILES = 15
NO_HISTORY_MARKERS = ("/nohistory", "#nohistory")

# Only capture when this assessment repo is an active workspace root.
ASSESSMENT_REPO_MARKER = "databricks-medallion-pipeline"


def _workspace_is_assessment(payload: dict[str, Any]) -> bool:
    """True when Cursor workspace includes this repo (project-scoped hooks guard)."""
    roots = payload.get("workspace_roots") or []
    if not roots:
        # Some hook events omit workspace_roots; trust project .cursor/hooks.json scope.
        return True
    repo = REPO_ROOT.resolve()
    for root in roots:
        try:
            resolved = Path(root).resolve()
        except OSError:
            continue
        if resolved == repo:
            return True
        if ASSESSMENT_REPO_MARKER in resolved.parts:
            return True
    return False


def _noop_response(hook: str) -> None:
    print('{"permission":"allow"}' if hook in {"start", "prompt"} else "{}")


def _read_stdin_json() -> dict[str, Any]:
    raw = sys.stdin.read()
    if not raw.strip():
        return {}
    return json.loads(raw)


def _load_state() -> dict[str, Any]:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {}


def _save_state(state: dict[str, Any]) -> None:
    CAPTURE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _rel_path(path: str) -> str:
    p = Path(path)
    try:
        return str(p.resolve().relative_to(REPO_ROOT.resolve()))
    except ValueError:
        return path


def _strip_user_query(text: str) -> str:
    text = re.sub(r"<timestamp>.*?</timestamp>\s*", "", text, flags=re.DOTALL)
    match = re.search(r"<user_query>\s*(.*?)\s*</user_query>", text, re.DOTALL)
    if match:
        return match.group(1).strip()
    return text.strip()


def _capture_disabled_by_prompt(prompt: str) -> bool:
    normalized = prompt.strip().lower()
    return any(normalized.startswith(marker) for marker in NO_HISTORY_MARKERS)


def _truncate(text: str, limit: int) -> str:
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _slugify(text: str, max_len: int = 40) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    if not slug:
        return "no-topic"
    return slug[:max_len].strip("-") or "no-topic"


def _extract_assistant_text(content: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for block in content:
        if block.get("type") != "text":
            continue
        text = (block.get("text") or "").strip()
        if not text or text == "[REDACTED]":
            continue
        parts.append(text)
    return "\n\n".join(parts)


def _collect_model_strings(node: Any, out: set[str]) -> None:
    if isinstance(node, dict):
        for k, v in node.items():
            if k.lower() == "model" and isinstance(v, str) and v.strip():
                out.add(v.strip())
            else:
                _collect_model_strings(v, out)
    elif isinstance(node, list):
        for item in node:
            _collect_model_strings(item, out)


def _parse_transcript(transcript_path: str | None) -> dict[str, Any]:
    if not transcript_path:
        return {"user_turns": [], "assistant_turns": []}

    path = Path(transcript_path)
    if not path.exists():
        return {"user_turns": [], "assistant_turns": [], "transcript_missing": True}

    user_turns: list[str] = []
    assistant_turns: list[str] = []
    models_seen: set[str] = set()

    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        _collect_model_strings(row, models_seen)

        role = row.get("role")
        message = row.get("message") or {}
        content = message.get("content") or []

        if role == "user":
            for block in content:
                if block.get("type") == "text":
                    cleaned = _strip_user_query(block.get("text") or "")
                    if cleaned:
                        user_turns.append(cleaned)
        elif role == "assistant":
            text = _extract_assistant_text(content)
            if text:
                assistant_turns.append(_truncate(text, MAX_ASSISTANT_CHARS))

        if len(user_turns) >= MAX_TRANSCRIPT_TURNS:
            break

    return {
        "user_turns": user_turns,
        "assistant_turns": assistant_turns,
        "models_seen": sorted(models_seen),
    }


def _session_filename(
    conversation_id: str | None,
    first_prompt: str,
    model: str,
    status: str,
) -> str:
    stamp = datetime.now(UTC).strftime("%Y-%m-%d_%H%M%S")
    short_id = (conversation_id or "unknown")[:8]
    topic = _slugify(first_prompt, max_len=36)
    model_slug = _slugify(model or "unknown-model", max_len=18)
    status_slug = _slugify(status or "unknown", max_len=12)
    return f"{stamp}_{short_id}_{model_slug}_{status_slug}_{topic}.md"


def _prune_session_files() -> None:
    files = sorted(
        [p for p in SESSIONS_DIR.glob("*.md") if p.name != ".gitkeep"],
        key=lambda p: p.stat().st_mtime,
    )
    if len(files) <= MAX_SESSION_FILES:
        return
    for path in files[: len(files) - MAX_SESSION_FILES]:
        path.unlink(missing_ok=True)


def _refresh_index() -> None:
    index_path = CAPTURE_DIR / "INDEX.md"
    files = sorted([p.name for p in SESSIONS_DIR.glob("*.md")])
    lines = [
        "# Session capture index",
        "",
        "Raw hook drafts — refine into numbered `ai-prompts/0X-....md` files.",
        "",
    ]
    if not files:
        lines.append("- (no session files)")
    else:
        lines.extend(f"- [{name}](sessions/{name})" for name in files)
    lines.append("")
    index_path.write_text("\n".join(lines), encoding="utf-8")


def _write_session_markdown(
    state: dict[str, Any],
    stop_payload: dict[str, Any],
    transcript: dict[str, Any],
) -> Path:
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

    hook_prompts = [
        p.get("text", "")
        for p in state.get("prompts", [])
        if p.get("text")
    ]
    transcript_prompts = transcript.get("user_turns", [])
    all_prompts = hook_prompts or transcript_prompts

    assistant_turns = transcript.get("assistant_turns", [])
    models_seen = transcript.get("models_seen", [])
    files_edited = state.get("files_edited", [])

    first_prompt = all_prompts[0] if all_prompts else "(no prompt captured)"
    last_assistant = assistant_turns[-1] if assistant_turns else "(see transcript)"

    conversation_id = stop_payload.get("conversation_id") or state.get("conversation_id")
    out_path = SESSIONS_DIR / _session_filename(
        conversation_id=conversation_id,
        first_prompt=first_prompt,
        model=str(stop_payload.get("model", "unknown")),
        status=str(stop_payload.get("status", "unknown")),
    )

    lines = [
        "# Session capture (draft — refine into ai-prompts/)",
        "",
        "> Auto-generated by Cursor `stop` hook. Edit, then merge the useful parts",
        "> into the numbered `ai-prompts/0X-....md` files with Accepted/Changed/Rejected.",
        "",
        "## Metadata",
        "",
        f"- **Captured at:** {datetime.now(UTC).isoformat()}",
        f"- **Status:** {stop_payload.get('status', 'unknown')}",
        f"- **Model:** {stop_payload.get('model', 'unknown')}",
        f"- **Conversation ID:** `{conversation_id or 'unknown'}`",
        f"- **Loop count:** {stop_payload.get('loop_count', 0)}",
        f"- **Hooks fired:** {', '.join(state.get('hook_events', [])) or 'stop only'}",
        f"- **Transcript:** `{stop_payload.get('transcript_path') or 'n/a'}`",
        f"- **Models observed in transcript:** {', '.join(models_seen) if models_seen else 'n/a'}",
        "",
    ]

    if files_edited:
        lines.extend(["## Files touched", ""])
        lines.extend(f"- `{f}`" for f in files_edited)
        lines.append("")

    if all_prompts:
        lines.extend(["## User prompts", ""])
        for i, prompt in enumerate(all_prompts, 1):
            lines.append(f"### Prompt {i}")
            lines.append("")
            lines.append(_truncate(prompt, MAX_PROMPT_CHARS))
            lines.append("")

    if assistant_turns:
        lines.extend(["## Assistant highlights (truncated)", ""])
        for i, turn in enumerate(assistant_turns, 1):
            lines.append(f"### Response {i}")
            lines.append("")
            lines.append(turn)
            lines.append("")

    if models_seen:
        lines.extend(["## Model usage signals", ""])
        lines.extend(f"- `{m}`" for m in models_seen)
        lines.append("")

    lines.extend(
        [
            "## Draft P-entry (copy → edit → merge to ai-prompts/)",
            "",
            "```markdown",
            "## P{n} — {title}",
            "",
            f"**Prompt:** {_truncate(first_prompt, 500)}",
            "",
            f"**AI response:** {_truncate(last_assistant, 800)}",
            "",
            "**Accepted:** TBD",
            "",
            "**Changed:** TBD",
            "",
            "**Rejected:** TBD",
            "",
            "**Why:** TBD — tie to rubric / layer / decision",
            "```",
            "",
            f"_Source file: `ai-prompts/capture/sessions/{out_path.name}`_",
            "",
        ]
    )

    out_path.write_text("\n".join(lines), encoding="utf-8")

    _prune_session_files()
    _refresh_index()

    return out_path


def cmd_start() -> None:
    payload = _read_stdin_json()
    if not _workspace_is_assessment(payload):
        _noop_response("start")
        return
    state = {
        "started_at": datetime.now(UTC).isoformat(),
        "conversation_id": payload.get("conversation_id"),
        "workspace_roots": payload.get("workspace_roots", []),
        "prompts": [],
        "files_edited": [],
        "hook_events": ["sessionStart"],
        "capture_disabled": False,
    }
    _save_state(state)
    print(
        json.dumps(
            {
                "permission": "allow",
                "additional_context": (
                    "Assessment Databricks isolation: use profile de-assessment-ce only "
                    "(host https://dbc-06f970f4-0f19.cloud.databricks.com). "
                    "MCP server: databricks-de-assessment. "
                    "Run `source scripts/env.sh` before CLI; always pass "
                    "`--profile de-assessment-ce`."
                ),
            }
        )
    )


def cmd_prompt() -> None:
    payload = _read_stdin_json()
    if not _workspace_is_assessment(payload):
        _noop_response("prompt")
        return
    state = _load_state()
    if not state:
        state = {
            "started_at": datetime.now(UTC).isoformat(),
            "prompts": [],
            "files_edited": [],
            "hook_events": [],
            "capture_disabled": False,
        }

    prompt = (
        payload.get("prompt")
        or payload.get("text")
        or payload.get("user_message")
        or ""
    )
    if isinstance(prompt, str) and _capture_disabled_by_prompt(prompt):
        state["capture_disabled"] = True
        state["capture_disabled_at"] = datetime.now(UTC).isoformat()
        state["capture_disabled_reason"] = "user opt-out marker"
        events = state.setdefault("hook_events", [])
        if "beforeSubmitPrompt" not in events:
            events.append("beforeSubmitPrompt")
        _save_state(state)
        print('{"permission":"allow"}')
        return

    if state.get("capture_disabled"):
        print('{"permission":"allow"}')
        return

    if isinstance(prompt, str) and prompt.strip():
        state.setdefault("prompts", []).append(
            {
                "at": datetime.now(UTC).isoformat(),
                "text": _strip_user_query(prompt.strip()),
            }
        )

    events = state.setdefault("hook_events", [])
    if "beforeSubmitPrompt" not in events:
        events.append("beforeSubmitPrompt")
    _save_state(state)
    print('{"permission":"allow"}')


def cmd_edit() -> None:
    payload = _read_stdin_json()
    if not _workspace_is_assessment(payload):
        _noop_response("edit")
        return
    state = _load_state()
    if not state:
        state = {
            "started_at": datetime.now(UTC).isoformat(),
            "prompts": [],
            "files_edited": [],
            "hook_events": [],
            "capture_disabled": False,
        }

    if state.get("capture_disabled"):
        print("{}")
        return

    path = (
        payload.get("file_path")
        or payload.get("path")
        or payload.get("file")
        or ""
    )
    if isinstance(path, str) and path.strip():
        rel = _rel_path(path.strip())
        files = state.setdefault("files_edited", [])
        if rel not in files:
            files.append(rel)

    events = state.setdefault("hook_events", [])
    if "afterFileEdit" not in events:
        events.append("afterFileEdit")
    _save_state(state)
    print("{}")


def cmd_stop() -> None:
    payload = _read_stdin_json()
    if not _workspace_is_assessment(payload):
        _noop_response("stop")
        return
    state = _load_state()
    if not state:
        state = {
            "started_at": datetime.now(UTC).isoformat(),
            "prompts": [],
            "files_edited": [],
            "hook_events": [],
            "capture_disabled": False,
        }

    state["conversation_id"] = payload.get("conversation_id") or state.get(
        "conversation_id"
    )
    events = state.setdefault("hook_events", [])
    if "stop" not in events:
        events.append("stop")

    if state.get("capture_disabled"):
        if STATE_FILE.exists():
            STATE_FILE.unlink()
        print("{}")
        print("session capture skipped: user opted out via /nohistory", file=sys.stderr)
        return

    transcript = _parse_transcript(payload.get("transcript_path"))
    out_path = _write_session_markdown(state, payload, transcript)

    if STATE_FILE.exists():
        STATE_FILE.unlink()

    # stop hook: empty JSON = no follow-up
    print("{}")
    print(f"session capture written: {out_path}", file=sys.stderr)


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: session_capture.py {start|prompt|edit|stop}", file=sys.stderr)
        sys.exit(1)

    handlers = {
        "start": cmd_start,
        "prompt": cmd_prompt,
        "edit": cmd_edit,
        "stop": cmd_stop,
    }
    handler = handlers.get(sys.argv[1])
    if not handler:
        print(f"unknown subcommand: {sys.argv[1]}", file=sys.stderr)
        sys.exit(1)
    handler()


if __name__ == "__main__":
    main()
