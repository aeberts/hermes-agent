"""Tests for the supervisor-dispatch sub-loop (event-hub F11-core, Increment 1).

The dispatcher auto-wakes a fresh ORCHESTRATOR-MODE turn when a supervised root
(one carrying an ``subscriber_kind='orchestrator'`` notify-sub) has a ``blocked``
child — turning the silent deadlock into an automatic triage re-engagement.

These tests exercise the enqueue/spawn side off-daemon: ``dispatch_once`` with a
recording stub for ``supervisor_spawn_fn`` (no real subprocess), plus a direct
test of ``_supervisor_spawn``'s argv/env with ``subprocess.Popen`` monkeypatched.
The OQ-5 guards (concurrency / cooldown / breaker) are driven by manipulating
``task_runs`` rows + ``kb._pid_alive``.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def kanban_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()
    return home


def _subscribe_orchestrator(conn, root_id: str, target_id: str = "orch-1") -> None:
    """Register an orchestrator supervision subscription on ``root_id``."""
    kb.add_notify_sub(
        conn,
        task_id=root_id,
        platform="orchestrator",
        chat_id=target_id,
        subscriber_kind="orchestrator",
        target=target_id,
        scope="subtree",
        delivery_policy="supervise",
    )


def _make_root_with_child(conn, *, assignee: str = "worker"):
    """Create a (root, child) pair wired the way decomposition links them.

    Decomposition links the root UNDER each subtask, so the subtask is the
    root's *parent* (``parent_ids(root)`` returns the subtasks). Return the ids.
    The child starts ``ready`` (no parents) so it can be transitioned to
    ``blocked`` via the public ``block_task`` (which logs the ``blocked`` event
    the breaker keys on). Linking the child as the root's parent re-gates the
    root to ``todo``.
    """
    root = kb.create_task(conn, title="root goal", assignee=assignee)
    child = kb.create_task(conn, title="subtask", assignee=assignee)
    kb.link_tasks(conn, child, root)  # child is the root's parent
    return root, child


def _block(conn, child: str, *, reason: str = "needs a decision") -> None:
    assert kb.block_task(conn, child, reason=reason) is True


class _RecordingSpawn:
    """Recording stub for ``supervisor_spawn_fn``; returns a fake pid."""

    def __init__(self, pid: int = 4321):
        self.calls: list[str] = []
        self._pid = pid

    def __call__(self, root_task, *, board=None):
        self.calls.append(root_task.id)
        return self._pid


# ---------------------------------------------------------------------------
# Core trigger: supervised root + blocked child → spawn once
# ---------------------------------------------------------------------------

def test_supervised_root_with_blocked_child_spawns_once(kanban_home, all_assignees_spawnable):
    conn = kb.connect()
    try:
        root, child = _make_root_with_child(conn)
        _subscribe_orchestrator(conn, root)
        _block(conn, child)

        stub = _RecordingSpawn()
        res = kb.dispatch_once(conn, supervisor_spawn_fn=stub)

        assert stub.calls == [root]
        assert res.supervised == [root]
    finally:
        conn.close()


def test_supervised_root_with_nonspawnable_assignee_skipped(kanban_home):
    """A supervised root with a blocked child but a non-existent assignee profile
    is bucketed nonspawnable — NOT spawned (the orchestrator-dispatches-to-a-
    missing-profile footgun) — and no supervisor run / cooldown is recorded."""
    conn = kb.connect()
    try:
        # No `all_assignees_spawnable` fixture → profile_exists("ghost") is False.
        root, child = _make_root_with_child(conn, assignee="ghost")
        _subscribe_orchestrator(conn, root)
        _block(conn, child)

        stub = _RecordingSpawn()
        res = kb.dispatch_once(conn, supervisor_spawn_fn=stub)

        assert stub.calls == []
        assert res.supervised == []
        assert root in res.skipped_nonspawnable
        # No supervisor run recorded → the cooldown/breaker did not churn.
        rows = conn.execute(
            "SELECT COUNT(*) AS n FROM task_runs "
            "WHERE task_id = ? AND step_key = 'supervisor'",
            (root,),
        ).fetchone()
        assert rows["n"] == 0
    finally:
        conn.close()


def test_blocked_child_without_orchestrator_sub_not_spawned(kanban_home):
    """A blocked child whose root has NO orchestrator subscription → no wake."""
    conn = kb.connect()
    try:
        root, child = _make_root_with_child(conn)
        _block(conn, child)  # no _subscribe_orchestrator

        stub = _RecordingSpawn()
        res = kb.dispatch_once(conn, supervisor_spawn_fn=stub)

        assert stub.calls == []
        assert res.supervised == []
    finally:
        conn.close()


def test_supervised_root_no_blocked_child_not_spawned(kanban_home):
    """Supervised root whose children are running/done → no supervisor wake."""
    conn = kb.connect()
    try:
        root, child = _make_root_with_child(conn)
        _subscribe_orchestrator(conn, root)
        # child stays 'running' (never blocked)

        stub = _RecordingSpawn()
        res = kb.dispatch_once(conn, supervisor_spawn_fn=stub)

        assert stub.calls == []
        assert res.supervised == []
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# OQ-5 guard: concurrency (a live supervisor)
# ---------------------------------------------------------------------------

def test_concurrency_guard_skips_when_supervisor_alive(kanban_home, monkeypatch, all_assignees_spawnable):
    conn = kb.connect()
    try:
        root, child = _make_root_with_child(conn)
        _subscribe_orchestrator(conn, root)
        _block(conn, child)

        # An OPEN supervisor run (ended_at IS NULL) with a live PID.
        kb._record_supervisor_run(conn, root, worker_pid=9999)
        monkeypatch.setattr(kb, "_pid_alive", lambda pid: True)

        stub = _RecordingSpawn()
        res = kb.dispatch_once(conn, supervisor_spawn_fn=stub)
        assert stub.calls == []
        assert res.supervised == []

        # Once the supervisor PID is dead, the next tick re-spawns. Age the run
        # past the cooldown so only the concurrency guard is what changes.
        conn.execute(
            "UPDATE task_runs SET started_at = ? WHERE step_key = 'supervisor'",
            (int(time.time()) - kb._SUPERVISOR_COOLDOWN_SECONDS - 5,),
        )
        conn.commit()
        monkeypatch.setattr(kb, "_pid_alive", lambda pid: False)

        stub2 = _RecordingSpawn()
        res2 = kb.dispatch_once(conn, supervisor_spawn_fn=stub2)
        assert stub2.calls == [root]
        assert res2.supervised == [root]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# OQ-5 guard: cooldown
# ---------------------------------------------------------------------------

def test_cooldown_guard_skips_recent_then_respawns_when_elapsed(kanban_home, monkeypatch, all_assignees_spawnable):
    conn = kb.connect()
    try:
        root, child = _make_root_with_child(conn)
        _subscribe_orchestrator(conn, root)
        _block(conn, child)

        # A recent, already-ended supervisor run (PID dead so only cooldown gates).
        monkeypatch.setattr(kb, "_pid_alive", lambda pid: False)
        rid = kb._record_supervisor_run(conn, root, worker_pid=None)
        conn.execute(
            "UPDATE task_runs SET ended_at = started_at WHERE id = ?", (rid,),
        )
        conn.commit()

        stub = _RecordingSpawn()
        res = kb.dispatch_once(conn, supervisor_spawn_fn=stub)
        assert stub.calls == []
        assert res.supervised == []

        # Simulate elapsed time: push started_at back beyond the cooldown.
        conn.execute(
            "UPDATE task_runs SET started_at = ? WHERE id = ?",
            (int(time.time()) - kb._SUPERVISOR_COOLDOWN_SECONDS - 5, rid),
        )
        conn.commit()

        stub2 = _RecordingSpawn()
        res2 = kb.dispatch_once(conn, supervisor_spawn_fn=stub2)
        assert stub2.calls == [root]
        assert res2.supervised == [root]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# OQ-5 guard: breaker (park after K no-resolution turns; reset on re-block)
# ---------------------------------------------------------------------------

def test_breaker_parks_after_limit_and_resets_on_reblock(kanban_home, monkeypatch, all_assignees_spawnable):
    conn = kb.connect()
    try:
        root, child = _make_root_with_child(conn)
        _subscribe_orchestrator(conn, root)
        _block(conn, child)

        monkeypatch.setattr(kb, "_pid_alive", lambda pid: False)
        # Age the original block episode well into the past so cooldown is
        # satisfied for the pre-seeded runs and the BREAKER is the gate.
        episode_start = int(time.time()) - 10_000
        conn.execute(
            "UPDATE task_events SET created_at = ? "
            "WHERE kind = 'blocked' AND task_id = ?",
            (episode_start, child),
        )
        conn.commit()

        # Pre-seed K supervisor runs in that (old) block episode — each ended
        # and aged past the cooldown so only the breaker is what gates.
        for _ in range(kb._SUPERVISOR_FAILURE_LIMIT):
            rid = kb._record_supervisor_run(conn, root, worker_pid=None)
            conn.execute(
                "UPDATE task_runs SET started_at = ?, ended_at = ? WHERE id = ?",
                (episode_start + 1, episode_start + 1, rid),
            )
        conn.commit()

        stub = _RecordingSpawn()
        res = kb.dispatch_once(conn, supervisor_spawn_fn=stub)
        assert stub.calls == [], "breaker should park after the failure limit"
        assert res.supervised == []

        # Resolution + re-block: unblock then re-block writes a NEW blocked
        # event with a newer timestamp, so the old episode's runs drop out of
        # the breaker count → it resets and a fresh wake fires.
        assert kb.unblock_task(conn, child) is True
        # Re-claim into running so block_task's running->blocked guard passes.
        conn.execute(
            "UPDATE tasks SET status = 'running' WHERE id = ?", (child,),
        )
        conn.commit()
        _block(conn, child, reason="re-blocked anew")

        stub2 = _RecordingSpawn()
        res2 = kb.dispatch_once(conn, supervisor_spawn_fn=stub2)
        assert stub2.calls == [root], "a re-block should re-engage the supervisor"
        assert res2.supervised == [root]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Orchestrator-mode env: _supervisor_spawn builds the right argv + env
# ---------------------------------------------------------------------------

def test_supervisor_spawn_env_is_orchestrator_mode(kanban_home, monkeypatch):
    conn = kb.connect()
    try:
        root = kb.create_task(conn, title="root goal", assignee="worker")
        root_task = kb.get_task(conn, root)
    finally:
        conn.close()

    captured = {}

    class _FakeProc:
        pid = 5555

    def _fake_popen(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["env"] = kwargs.get("env")
        return _FakeProc()

    import subprocess
    monkeypatch.setattr(subprocess, "Popen", _fake_popen)

    pid = kb._supervisor_spawn(root_task, board=None)
    assert pid == 5555

    env = captured["env"]
    cmd = captured["cmd"]
    # Orchestrator mode = HERMES_KANBAN_TASK absent.
    assert "HERMES_KANBAN_TASK" not in env
    # Board pin present.
    assert "HERMES_KANBAN_BOARD" in env
    assert env["HERMES_PROFILE"] == "worker"
    # Top-level one-shot `-z`, never `chat`.
    assert "-z" in cmd
    assert "chat" not in cmd


# ---------------------------------------------------------------------------
# Fan-in regression: all-children-done root → NO supervisor spawn, stock
# ready-dispatch still promotes/spawns the root unchanged.
# ---------------------------------------------------------------------------

def test_fan_in_root_no_supervisor_but_stock_dispatch_unaffected(
    kanban_home, all_assignees_spawnable,
):
    conn = kb.connect()
    try:
        root, child = _make_root_with_child(conn)
        _subscribe_orchestrator(conn, root)
        # Complete the only child → fan-in. Root has no blocked child.
        assert kb.complete_task(conn, child, result="done") is True

        worker_calls: list[str] = []

        def _worker_spawn(task, workspace, *, board=None):
            worker_calls.append(task.id)
            return 1111

        sup_stub = _RecordingSpawn()
        res = kb.dispatch_once(
            conn, spawn_fn=_worker_spawn, supervisor_spawn_fn=sup_stub,
        )

        # No supervisor wake (no blocked child).
        assert sup_stub.calls == []
        assert res.supervised == []
        # Stock ready-dispatch promoted + spawned the root as before.
        assert root in worker_calls
        assert any(s[0] == root for s in res.spawned)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Increment 2 (reengage-in-tick): the dispatch tick runs reengage_orchestrator
# once per orchestrator target each tick, BEFORE the fan-in re-promotion, so the
# F09/F10 handoff comment exists before the re-promoted root is re-spawned.
#
# Mirrors the notice-seeding helpers from tests/hermes_cli/test_kanban_reengage.py
# (F07-shaped orchestrator notices via add_notice + a fan-in snapshot payload).
# ---------------------------------------------------------------------------

def _add_orchestrator_notice(target_id: str, root_id: str, snapshot: dict) -> int:
    """Hand-build an F07-shaped orchestrator supervision notice (snapshot payload)."""
    conn = kb.connect()
    try:
        return kb.add_notice(
            conn,
            subscriber_kind="orchestrator",
            target_id=target_id,
            task_id=root_id,
            kind="supervision",
            message=f"Kanban {root_id} supervision",
            payload=json.dumps(snapshot, ensure_ascii=False),
        )
    finally:
        conn.close()


def _snapshot(root_id: str, *, fan_in_ready: bool, children=None) -> dict:
    return {
        "schema": 1,
        "parent_id": root_id,
        "board": "default",
        "fan_in_ready": fan_in_ready,
        "children": children or [],
    }


def _comment_bodies(conn, root_id: str) -> list[str]:
    return [c.body for c in kb.list_comments(conn, root_id)]


def test_reengage_in_tick_fan_in_writes_reengage_handoff(kanban_home):
    """fan_in_ready=true notice → dispatch tick writes a [kanban:reengage]
    comment on the root and result.reengaged lists it (Increment 2 fan-in)."""
    conn = kb.connect()
    try:
        root = kb.create_task(conn, title="root goal", assignee="worker")
        child = kb.create_task(conn, title="child A", assignee="worker")
        _subscribe_orchestrator(conn, root, target_id="orch-fanin")
        snap = _snapshot(
            root, fan_in_ready=True,
            children=[{"task_id": child, "title": "child A", "kind": "completed",
                       "status": "done", "assignee": "worker",
                       "summary": "A finished", "artifacts": []}],
        )
        _add_orchestrator_notice("orch-fanin", root, snap)

        res = kb.dispatch_once(conn)

        assert root in res.reengaged
        bodies = _comment_bodies(conn, root)
        assert any(b.startswith(kb.REENGAGE_PREFIX) for b in bodies)
    finally:
        conn.close()


def test_reengage_in_tick_blocked_writes_triage_handoff(kanban_home):
    """A blocked-child snapshot (fan_in_ready=false) → dispatch tick writes a
    [kanban:triage] comment on the root and result.reengaged lists it."""
    conn = kb.connect()
    try:
        root = kb.create_task(conn, title="root goal", assignee="worker")
        _subscribe_orchestrator(conn, root, target_id="orch-triage")
        snap = _snapshot(
            root, fan_in_ready=False,
            children=[{"task_id": "t_child", "title": "child", "kind": "blocked",
                       "status": "blocked", "assignee": "worker",
                       "reason": "needs a decision", "artifacts": []}],
        )
        _add_orchestrator_notice("orch-triage", root, snap)

        res = kb.dispatch_once(conn)

        assert root in res.reengaged
        bodies = _comment_bodies(conn, root)
        assert any(b.startswith(kb.TRIAGE_PREFIX) for b in bodies)
    finally:
        conn.close()


def test_reengage_in_tick_idempotent_second_tick_noop(kanban_home):
    """A second dispatch tick with no new notice → no duplicate handoff comment
    and result.reengaged is empty that tick (one-shot drain idempotency)."""
    conn = kb.connect()
    try:
        root = kb.create_task(conn, title="root goal", assignee="worker")
        _subscribe_orchestrator(conn, root, target_id="orch-idem")
        _add_orchestrator_notice(
            "orch-idem", root, _snapshot(root, fan_in_ready=True),
        )

        res1 = kb.dispatch_once(conn)
        assert root in res1.reengaged
        bodies_after_first = _comment_bodies(conn, root)
        n_reengage = sum(
            1 for b in bodies_after_first if b.startswith(kb.REENGAGE_PREFIX)
        )
        assert n_reengage == 1

        res2 = kb.dispatch_once(conn)
        assert res2.reengaged == []
        bodies_after_second = _comment_bodies(conn, root)
        assert sum(
            1 for b in bodies_after_second if b.startswith(kb.REENGAGE_PREFIX)
        ) == 1
    finally:
        conn.close()


def test_reengage_in_tick_no_orchestrator_subs_is_noop(kanban_home):
    """A board with no orchestrator subscription → result.reengaged == [] and no
    handoff comment is written."""
    conn = kb.connect()
    try:
        root = kb.create_task(conn, title="root goal", assignee="worker")
        # No _subscribe_orchestrator and no notices.

        res = kb.dispatch_once(conn)

        assert res.reengaged == []
        assert _comment_bodies(conn, root) == []
    finally:
        conn.close()


def test_reengage_in_tick_dry_run_skips_reengage(kanban_home):
    """dry_run skips the (mutating) reengage: no handoff comment, empty
    result.reengaged, and the seeded notice is NOT drained (still present)."""
    conn = kb.connect()
    try:
        root = kb.create_task(conn, title="root goal", assignee="worker")
        _subscribe_orchestrator(conn, root, target_id="orch-dry")
        _add_orchestrator_notice(
            "orch-dry", root, _snapshot(root, fan_in_ready=True),
        )

        res = kb.dispatch_once(conn, dry_run=True)

        assert res.reengaged == []
        bodies = _comment_bodies(conn, root)
        assert not any(b.startswith(kb.REENGAGE_PREFIX) for b in bodies)

        # The notice was NOT drained — still claimable on a live (non-dry) tick.
        remaining = kb.drain_notices(
            conn, subscriber_kind="orchestrator", target_id="orch-dry",
        )
        assert len(remaining) == 1
    finally:
        conn.close()
