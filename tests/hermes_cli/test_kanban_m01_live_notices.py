"""M01 — live-wake surfacing of kanban supervision notices into the idle REPL.

Covers the DB drain primitive (`drain_session_notices`) and the CLI
`_drain_kanban_live_notices` boundary helper that queues a re-engage message onto
`_pending_input` (never mid-turn; the callers in `process_loop` own that gating).
"""
import queue

from hermes_cli import kanban_db as kb


def _add(conn, kind, target, tid, msg, subkind="orchestrator"):
    kb.add_notice(
        conn, subscriber_kind=subkind, target_id=target,
        task_id=tid, kind=kind, message=msg,
    )


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
