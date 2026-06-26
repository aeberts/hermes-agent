"""F15 — TUI M01 parity: surface kanban supervision notices into an idle session.

Covers the ownership hook (`_record_kanban_subscription`) + the poller-tick drain
(`_drain_kanban_tui_notices`): owned-roots filtering, the shared M01 debounce
(progress suppressed, completion wakes after fan-in), and the idle gate.
"""
import json
import threading

import pytest

from tui_gateway import server


def _payload(children, *, fan_in_ready=False, root_status=None):
    return json.dumps({
        "schema": 1, "parent_id": "t_1", "board": "default",
        "fan_in_ready": fan_in_ready, "root_status": root_status,
        "children": list(children),
    })


def _notice(task_id, message, payload):
    return {"task_id": task_id, "message": message, "payload": payload, "board": "default"}


def _session(**over):
    s = {
        "history_lock": threading.RLock(),
        "running": False,
        "kanban_roots": {"t_1"},
        "_kanban_last_check": 0.0,  # ancient → throttle never blocks the first tick
    }
    s.update(over)
    return s


@pytest.fixture
def captured(monkeypatch):
    calls = {"submits": [], "emits": []}
    monkeypatch.setattr(
        server, "_run_prompt_submit",
        lambda rid, sid, session, text: calls["submits"].append(text),
    )
    monkeypatch.setattr(server, "_emit", lambda *a, **k: calls["emits"].append(a))
    return calls


def _patch_drain(monkeypatch, fn):
    from hermes_cli import kanban_db as kb
    monkeypatch.setattr(kb, "drain_session_notices", fn)


def test_record_kanban_subscription_tracks_roots_on_success():
    session = {}
    server._record_kanban_subscription(
        session, {"task_id": "t_9"},
        json.dumps({"subscription_id": 1, "subscriber_kind": "orchestrator"}),
    )
    assert session["kanban_roots"] == {"t_9"}

    # An error result is NOT recorded.
    other = {}
    server._record_kanban_subscription(
        other, {"task_id": "t_x"}, json.dumps({"error": "kanban_subscribe: boom"}),
    )
    assert "t_x" not in other.get("kanban_roots", set())


def test_actionable_notice_wakes_idle_session(captured, monkeypatch):
    _patch_drain(monkeypatch, lambda **kw: [_notice(
        "t_1", "Kanban t_1 supervision — 1 done, 1 blocked",
        _payload([{"task_id": "c1", "kind": "completed"},
                  {"task_id": "c2", "kind": "blocked"}]),
    )])
    session = _session()
    server._drain_kanban_tui_notices("sid1", session)
    assert len(captured["submits"]) == 1
    assert "[kanban]" in captured["submits"][0]
    assert "1 blocked" in captured["submits"][0]
    # the visibility chip was emitted too
    assert any(a and a[0] == "status.update" for a in captured["emits"])


def test_progress_notice_suppressed(captured, monkeypatch):
    _patch_drain(monkeypatch, lambda **kw: [_notice(
        "t_1", "Kanban t_1 supervision — 1 done",
        _payload([{"task_id": "c1", "kind": "completed"},
                  {"task_id": "c2", "kind": None}]),
    )])
    session = _session()
    server._drain_kanban_tui_notices("sid1", session)
    assert captured["submits"] == []


def test_no_db_hit_without_owned_roots(captured, monkeypatch):
    calls = {"n": 0}

    def fake(**kw):
        calls["n"] += 1
        return []

    _patch_drain(monkeypatch, fake)
    server._drain_kanban_tui_notices("sid1", _session(kanban_roots=set()))
    assert calls["n"] == 0 and captured["submits"] == []


def test_no_drain_while_busy(captured, monkeypatch):
    calls = {"n": 0}

    def fake(**kw):
        calls["n"] += 1
        return []

    _patch_drain(monkeypatch, fake)
    # Busy session must NOT drain — the drain DELETEs, so a mid-turn drain would
    # lose the wake.
    server._drain_kanban_tui_notices("sid1", _session(running=True))
    assert calls["n"] == 0 and captured["submits"] == []


def test_drain_scoped_to_owned_roots(captured, monkeypatch):
    seen = {}

    def fake(*, subscriber_kind="orchestrator", task_ids=None):
        seen["kind"] = subscriber_kind
        seen["task_ids"] = task_ids
        return []

    _patch_drain(monkeypatch, fake)
    server._drain_kanban_tui_notices("sid1", _session(kanban_roots={"t_1", "t_2"}))
    assert seen["kind"] == "orchestrator"
    assert seen["task_ids"] == {"t_1", "t_2"}


def test_completion_wakes_after_fanin(captured, monkeypatch):
    children = [{"task_id": "c1", "kind": "completed"},
                {"task_id": "c2", "kind": "completed"}]
    queue = [
        [_notice("t_1", "Kanban t_1 supervision — fan-in ready — 2 done",
                 _payload(children, fan_in_ready=True, root_status="ready"))],
        [_notice("t_1", "Kanban t_1 supervision — goal complete — 2 done",
                 _payload(children, fan_in_ready=True, root_status="done"))],
    ]
    _patch_drain(monkeypatch, lambda **kw: queue.pop(0) if queue else [])

    session = _session()
    server._drain_kanban_tui_notices("sid1", session)   # fan-in → wake
    assert len(captured["submits"]) == 1
    assert "fan-in ready" in captured["submits"][0]

    session["running"] = False          # turn finished
    session["_kanban_last_check"] = 0.0  # bypass throttle
    server._drain_kanban_tui_notices("sid1", session)   # completion → distinct wake
    assert len(captured["submits"]) == 2
    assert "goal complete" in captured["submits"][1]
