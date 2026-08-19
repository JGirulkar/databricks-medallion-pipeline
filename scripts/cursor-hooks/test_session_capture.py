"""Unit tests for session capture hook logic."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

MODULE = Path(__file__).resolve().parents[1] / "session_capture.py"
spec = importlib.util.spec_from_file_location("session_capture", MODULE)
assert spec and spec.loader
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


@pytest.mark.unit
def test_strip_user_query() -> None:
    raw = (
        "<timestamp>Wed</timestamp>\n"
        "<user_query>\nhello world\n</user_query>"
    )
    assert mod._strip_user_query(raw) == "hello world"


@pytest.mark.unit
def test_workspace_guard_assessment_repo() -> None:
    payload = {
        "workspace_roots": ["/home/user/Desktop/Projects/databricks-medallion-pipeline"]
    }
    assert mod._workspace_is_assessment(payload) is True


@pytest.mark.unit
def test_workspace_guard_other_project() -> None:
    payload = {"workspace_roots": ["/home/user/Desktop/Projects/Intelo.ai/retail-agents-backend"]}
    assert mod._workspace_is_assessment(payload) is False


@pytest.mark.unit
def test_write_session_markdown(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod, "CAPTURE_DIR", tmp_path)
    monkeypatch.setattr(mod, "SESSIONS_DIR", tmp_path / "sessions")

    state = {
        "prompts": [{"text": "Build bronze layer"}],
        "files_edited": ["databricks/jobs/bronze/src/ingest_all.py"],
        "hook_events": ["sessionStart", "beforeSubmitPrompt", "afterFileEdit", "stop"],
    }
    stop_payload = {
        "status": "completed",
        "model": "test-model",
        "conversation_id": "abcd1234-efgh",
        "loop_count": 0,
        "transcript_path": None,
    }
    transcript = {"user_turns": ["Build bronze layer"], "assistant_turns": ["Here is the plan."]}

    out = mod._write_session_markdown(state, stop_payload, transcript)
    assert out.exists()
    text = out.read_text(encoding="utf-8")
    assert "Build bronze layer" in text
    assert "Draft P-entry" in text
    assert (tmp_path / "INDEX.md").exists()
