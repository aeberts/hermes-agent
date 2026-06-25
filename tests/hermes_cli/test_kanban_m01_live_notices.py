"""M01 — live-wake surfacing of kanban supervision notices into the idle REPL.

Covers the DB drain primitive (`drain_session_notices`) and the CLI
`_drain_kanban_live_notices` boundary helper that queues a re-engage message onto
`_pending_input` (never mid-turn; the callers in `process_loop` own that gating).
"""
import json
import queue

from hermes_cli import kanban_db as kb


def _add(conn, kind, target, tid, msg, subkind="orchestrator", payload=None):
    kb.add_notice(
        conn, subscriber_kind=subkind, target_id=target,
        task_id=tid, kind=kind, message=msg, payload=payload,
    )


def _snapshot(parent_id, children, *, fan_in_ready=False, root_status=None):
    """Build an F07-shaped supervision payload (the orchestrator adapter's JSON)."""
    return json.dumps({
        "schema": 1, "parent_id": parent_id, "board": "default",
        "fan_in_ready": fan_in_ready, "root_status": root_status,
        "children": list(children),
    })


def test_drain_session_notices_one_shot_and_kind_filtered(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "kanban.db"))
    kb.init_db()
    conn = kb.connect()
    try:
        _add(conn, "supervision", "orch:t_1", "t_1", "Kanban t_1 supervision — 1 blocked")
        # a different subscriber_kind must NOT be consumed by the orchestrator drain
        _add(conn, "completed", "sess", "t_2", "t_2 done", subkind="cli")
    finally:
        conn.close()

    drained = kb.drain_session_notices(subscriber_kind="orchestrator")
    assert [d["task_id"] for d in drained] == ["t_1"]
    assert drained[0]["message"].startswith("Kanban t_1")
    assert drained[0].get("board"), "each drained notice carries its source board slug"

    # one-shot: the row was DELETEd, so a second drain finds nothing
    assert kb.drain_session_notices(subscriber_kind="orchestrator") == []

    # the cli-kind notice survived untouched (cross-kind isolation)
    conn = kb.connect()
    try:
        assert len(kb.drain_notices(conn, subscriber_kind="cli")) == 1
    finally:
        conn.close()


def test_cli_drain_queues_reengage_message(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "kanban.db"))
    kb.init_db()
    conn = kb.connect()
    try:
        _add(conn, "supervision", "orch:t_1", "t_1", "Kanban t_1 supervision — fan-in ready")
    finally:
        conn.close()

    from cli import HermesCLI

    stub = HermesCLI.__new__(HermesCLI)
    stub._pending_input = queue.Queue()

    HermesCLI._drain_kanban_live_notices(stub, force=True)
    assert stub._pending_input.qsize() == 1
    msg = stub._pending_input.get_nowait()
    assert "[kanban]" in msg
    assert "Kanban t_1 supervision" in msg
    assert "polling loop" in msg.lower()  # the do-not-poll nudge rides along

    # one-shot: the notice is gone, so a second pass queues nothing
    HermesCLI._drain_kanban_live_notices(stub, force=True)
    assert stub._pending_input.qsize() == 0


def test_cli_drain_throttled_but_force_bypasses(monkeypatch):
    calls = {"n": 0}

    def _fake_drain(*, subscriber_kind="orchestrator"):
        calls["n"] += 1
        return []

    monkeypatch.setattr(kb, "drain_session_notices", _fake_drain)

    from cli import HermesCLI

    stub = HermesCLI.__new__(HermesCLI)
    stub._pending_input = queue.Queue()

    HermesCLI._drain_kanban_live_notices(stub)          # first idle drain: runs
    HermesCLI._drain_kanban_live_notices(stub)          # immediate: throttled out
    assert calls["n"] == 1
    HermesCLI._drain_kanban_live_notices(stub, force=True)  # post-turn force: runs
    assert calls["n"] == 2


def test_signature_classifies_actionable_vs_progress():
    from cli import HermesCLI

    sig = HermesCLI._kanban_notice_signature
    # pure progress: 1 of 2 done, nothing blocked, not fan-in → NOT actionable
    progress = {"payload": _snapshot("t_1", [
        {"task_id": "c1", "kind": "completed"},
        {"task_id": "c2", "kind": None},
    ])}
    assert sig(progress)[0] is False
    # a blocked child → actionable
    blocked = {"payload": _snapshot("t_1", [
        {"task_id": "c1", "kind": "completed"},
        {"task_id": "c2", "kind": "blocked"},
    ])}
    assert sig(blocked)[0] is True
    # fan-in ready → actionable
    fan_in = {"payload": _snapshot("t_1", [
        {"task_id": "c1", "kind": "completed"},
    ], fan_in_ready=True)}
    assert sig(fan_in)[0] is True
    # all subtasks done → actionable (effective completion)
    all_done = {"payload": _snapshot("t_1", [
        {"task_id": "c1", "kind": "completed"},
        {"task_id": "c2", "kind": "completed"},
    ])}
    assert sig(all_done)[0] is True
    # no/garbage payload → None (caller surfaces unconditionally)
    assert sig({"payload": None}) is None
    assert sig({"payload": "{not json"}) is None


def test_cli_drain_suppresses_progress_and_duplicates(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "kanban.db"))
    kb.init_db()

    from cli import HermesCLI
    stub = HermesCLI.__new__(HermesCLI)
    stub._pending_input = queue.Queue()

    # 1) pure-progress snapshot → suppressed (no turn spent)
    conn = kb.connect()
    try:
        _add(conn, "supervision", "orch:t_1", "t_1",
             "Kanban t_1 supervision — 1 done", payload=_snapshot("t_1", [
                 {"task_id": "c1", "kind": "completed"},
                 {"task_id": "c2", "kind": None},
             ]))
    finally:
        conn.close()
    HermesCLI._drain_kanban_live_notices(stub, force=True)
    assert stub._pending_input.qsize() == 0

    # 2) a blocked child → surfaces exactly one re-engage
    conn = kb.connect()
    try:
        _add(conn, "supervision", "orch:t_1", "t_1",
             "Kanban t_1 supervision — 1 done, 1 blocked", payload=_snapshot("t_1", [
                 {"task_id": "c1", "kind": "completed"},
                 {"task_id": "c2", "kind": "blocked"},
             ]))
    finally:
        conn.close()
    HermesCLI._drain_kanban_live_notices(stub, force=True)
    assert stub._pending_input.qsize() == 1
    stub._pending_input.get_nowait()

    # 3) the SAME blocked state again → suppressed as a duplicate re-wake
    conn = kb.connect()
    try:
        _add(conn, "supervision", "orch:t_1", "t_1",
             "Kanban t_1 supervision — 1 done, 1 blocked", payload=_snapshot("t_1", [
                 {"task_id": "c1", "kind": "completed"},
                 {"task_id": "c2", "kind": "blocked"},
             ]))
    finally:
        conn.close()
    HermesCLI._drain_kanban_live_notices(stub, force=True)
    assert stub._pending_input.qsize() == 0


def test_cli_drain_coalesces_to_freshest_per_root(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "kanban.db"))
    kb.init_db()

    from cli import HermesCLI
    stub = HermesCLI.__new__(HermesCLI)
    stub._pending_input = queue.Queue()

    # Two notices for the SAME root land in one drain window: an intermediate
    # progress snapshot, then a fan-in snapshot. Only the freshest (fan-in,
    # actionable) survives coalescing → exactly one wake, carrying fan-in.
    conn = kb.connect()
    try:
        _add(conn, "supervision", "orch:t_1", "t_1",
             "Kanban t_1 supervision — 1 done", payload=_snapshot("t_1", [
                 {"task_id": "c1", "kind": "completed"},
                 {"task_id": "c2", "kind": None},
             ]))
        _add(conn, "supervision", "orch:t_1", "t_1",
             "Kanban t_1 supervision — fan-in ready — 2 done", payload=_snapshot("t_1", [
                 {"task_id": "c1", "kind": "completed"},
                 {"task_id": "c2", "kind": "completed"},
             ], fan_in_ready=True))
    finally:
        conn.close()
    HermesCLI._drain_kanban_live_notices(stub, force=True)
    assert stub._pending_input.qsize() == 1
    msg = stub._pending_input.get_nowait()
    assert "fan-in ready" in msg


def test_cli_drain_completion_wakes_even_after_fan_in(tmp_path, monkeypatch):
    """Regression: the root's own completion must surface as a final wake.

    The subtree closure includes the root node, so the root's `completed` event
    fires one last supervision notice — but its children + fan_in_ready are
    computed purely over the subtasks and so are byte-identical to the earlier
    fan-in-ready notice. Without the root_status field in the signature the
    debounce dedup swallowed this completion wake (observed live: board m01_1,
    root t_5ea6bf3d completed but the supervisor was never re-engaged).
    """
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "kanban.db"))
    kb.init_db()

    from cli import HermesCLI
    stub = HermesCLI.__new__(HermesCLI)
    stub._pending_input = queue.Queue()

    children = [
        {"task_id": "c1", "kind": "completed"},
        {"task_id": "c2", "kind": "completed"},
    ]

    # 1) fan-in ready (root still `ready`, not yet worked) → surfaces
    conn = kb.connect()
    try:
        _add(conn, "supervision", "orch:t_1", "t_1",
             "Kanban t_1 supervision — fan-in ready — 2 done",
             payload=_snapshot("t_1", children, fan_in_ready=True, root_status="ready"))
    finally:
        conn.close()
    HermesCLI._drain_kanban_live_notices(stub, force=True)
    assert stub._pending_input.qsize() == 1
    stub._pending_input.get_nowait()

    # 2) root now done — SAME children, SAME fan_in_ready, only root_status flips.
    #    Must still surface (goal-complete is a distinct, actionable state).
    conn = kb.connect()
    try:
        _add(conn, "supervision", "orch:t_1", "t_1",
             "Kanban t_1 supervision — goal complete — 2 done",
             payload=_snapshot("t_1", children, fan_in_ready=True, root_status="done"))
    finally:
        conn.close()
    HermesCLI._drain_kanban_live_notices(stub, force=True)
    assert stub._pending_input.qsize() == 1, "root completion must wake the supervisor"
    msg = stub._pending_input.get_nowait()
    assert "goal complete" in msg

    # 3) a duplicate completion notice → deduped (no second wake)
    conn = kb.connect()
    try:
        _add(conn, "supervision", "orch:t_1", "t_1",
             "Kanban t_1 supervision — goal complete — 2 done",
             payload=_snapshot("t_1", children, fan_in_ready=True, root_status="done"))
    finally:
        conn.close()
    HermesCLI._drain_kanban_live_notices(stub, force=True)
    assert stub._pending_input.qsize() == 0
