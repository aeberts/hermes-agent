"""Tests for the kanban_subscribe tool + subscribe-and-yield guidance (F13).

- Schema validates and the tool registers under the orchestrator-mode gate
  (visible to an orchestrator profile, hidden from a task-scoped worker).
- The handler writes the orchestrator-subtree subscription shape that the F07
  delivery adapter + F11 reengage-in-tick key off
  (``subscriber_kind='orchestrator'``, ``scope='subtree'``,
  ``delivery_policy='supervise'``), with a deterministic CLI-fallback target.
- The sub is reengage-pickup-able (``list_notify_subs`` filtered on
  ``subscriber_kind=='orchestrator'`` finds it on the task).
- Idempotent: a double-call yields exactly one row.
- The injected KANBAN_GUIDANCE carries the subscribe-and-yield protocol.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def orch_env(monkeypatch, tmp_path):
    """Simulate an orchestrator session: kanban toolset, NO HERMES_KANBAN_TASK,
    CLI (no channel)."""
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_PROFILE", "techlead")
    monkeypatch.delenv("HERMES_KANBAN_TASK", raising=False)
    monkeypatch.delenv("HERMES_SESSION_CHAT_ID", raising=False)
    monkeypatch.delenv("HERMES_SESSION_PLATFORM", raising=False)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)

    from hermes_cli import kanban_db as kb
    kb._INITIALIZED_PATHS.clear()
    kb.init_db()
    return home


# --- gating ----------------------------------------------------------------

def test_subscribe_visible_to_orchestrator_hidden_from_worker(monkeypatch, tmp_path):
    home = tmp_path / ".hermes"
    home.mkdir()
    (home / "config.yaml").write_text("toolsets:\n  - kanban\n")
    monkeypatch.setenv("HERMES_HOME", str(home))

    import tools.kanban_tools  # noqa: F401  (ensure registered)
    from tools.registry import invalidate_check_fn_cache, registry
    from toolsets import resolve_toolset

    # Orchestrator: no HERMES_KANBAN_TASK.
    monkeypatch.delenv("HERMES_KANBAN_TASK", raising=False)
    invalidate_check_fn_cache()
    schema = registry.get_definitions(set(resolve_toolset("hermes-cli")), quiet=True)
    names = {s["function"].get("name") for s in schema if "function" in s}
    assert "kanban_subscribe" in names

    # Worker: HERMES_KANBAN_TASK set → hidden.
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_fake")
    invalidate_check_fn_cache()
    schema = registry.get_definitions(set(resolve_toolset("hermes-cli")), quiet=True)
    names = {s["function"].get("name") for s in schema if "function" in s}
    assert "kanban_subscribe" not in names


def test_subscribe_schema_shape():
    from tools.kanban_tools import KANBAN_SUBSCRIBE_SCHEMA
    assert KANBAN_SUBSCRIBE_SCHEMA["name"] == "kanban_subscribe"
    params = KANBAN_SUBSCRIBE_SCHEMA["parameters"]
    assert params["required"] == ["task_id"]
    assert "task_id" in params["properties"]
    # No scope arg (F13: node-closure model collapses task/subtree).
    assert "scope" not in params["properties"]


# --- handler ---------------------------------------------------------------

def test_subscribe_writes_orchestrator_subtree_sub(orch_env):
    from tools import kanban_tools as kt
    from hermes_cli import kanban_db as kb

    conn = kb.connect()
    try:
        root = kb.create_task(conn, title="goal root", assignee="techlead")
    finally:
        conn.close()

    out = kt._handle_subscribe({"task_id": root})
    d = json.loads(out)
    assert d["ok"] is True
    assert d["task_id"] == root
    assert d["subscriber_kind"] == "orchestrator"
    assert d["scope"] == "subtree"
    assert d["delivery_policy"] == "supervise"

    conn = kb.connect()
    try:
        subs = kb.list_notify_subs(conn, root)
        assert len(subs) == 1
        sub = subs[0]
        assert sub["subscriber_kind"] == "orchestrator"
        assert sub["scope"] == "subtree"
        assert sub["delivery_policy"] == "supervise"
        # Deterministic CLI fallback target derived from the task_id.
        assert sub["chat_id"] == f"orch:{root}"
    finally:
        conn.close()


def test_subscribe_row_is_reengage_pickup_able(orch_env):
    """A reengage-in-tick scan filters subscriber_kind=='orchestrator' and keys
    off task_id — assert the written row matches that exact filter."""
    from tools import kanban_tools as kt
    from hermes_cli import kanban_db as kb

    conn = kb.connect()
    try:
        root = kb.create_task(conn, title="root", assignee="techlead")
    finally:
        conn.close()

    kt._handle_subscribe({"task_id": root})

    conn = kb.connect()
    try:
        orch_subs = [
            s for s in kb.list_notify_subs(conn)
            if s.get("subscriber_kind") == "orchestrator"
        ]
        assert any(s["task_id"] == root for s in orch_subs)
    finally:
        conn.close()


def test_subscribe_is_idempotent(orch_env):
    from tools import kanban_tools as kt
    from hermes_cli import kanban_db as kb

    conn = kb.connect()
    try:
        root = kb.create_task(conn, title="root", assignee="techlead")
    finally:
        conn.close()

    first = json.loads(kt._handle_subscribe({"task_id": root}))
    second = json.loads(kt._handle_subscribe({"task_id": root}))
    assert first["subscription_id"] == second["subscription_id"]

    conn = kb.connect()
    try:
        assert len(kb.list_notify_subs(conn, root)) == 1
    finally:
        conn.close()


def test_subscribe_uses_session_chat_id_when_present(orch_env, monkeypatch):
    """A real channel session subscribes back to that chat, not the fallback."""
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "chat-123")
    from tools import kanban_tools as kt
    from hermes_cli import kanban_db as kb

    conn = kb.connect()
    try:
        root = kb.create_task(conn, title="root", assignee="techlead")
    finally:
        conn.close()

    kt._handle_subscribe({"task_id": root})
    conn = kb.connect()
    try:
        sub = kb.list_notify_subs(conn, root)[0]
        assert sub["chat_id"] == "chat-123"
    finally:
        conn.close()


def test_subscribe_unknown_task_errors(orch_env):
    from tools import kanban_tools as kt
    out = kt._handle_subscribe({"task_id": "t_nope"})
    d = json.loads(out)
    assert d.get("ok") is not True
    assert "error" in d or "unknown" in out.lower()


def test_subscribe_missing_task_id_errors(orch_env):
    from tools import kanban_tools as kt
    out = kt._handle_subscribe({})
    d = json.loads(out)
    assert d.get("ok") is not True


def test_subscribe_rejected_for_worker(orch_env, monkeypatch):
    """Belt-and-suspenders runtime guard: a worker context is refused."""
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_worker")
    from tools import kanban_tools as kt
    out = kt._handle_subscribe({"task_id": "t_worker"})
    d = json.loads(out)
    assert d.get("ok") is not True
    assert "orchestrator-only" in out


# --- guidance --------------------------------------------------------------

def test_guidance_has_subscribe_and_yield_protocol():
    from agent.prompt_builder import KANBAN_GUIDANCE
    assert "kanban_subscribe" in KANBAN_GUIDANCE
    # The yield instruction (do not poll) and the link-direction warning.
    assert "kanban_list" in KANBAN_GUIDANCE
    lowered = KANBAN_GUIDANCE.lower()
    assert "child_id" in lowered and "parent_id" in lowered
