"""Watcher-level coverage for the orchestrator subtree delivery gate (event-hub F12).

The F07 orchestrator-delivery suite drives ``adapter.deliver(...)`` **directly**,
which bypasses the notifier watcher's gate in ``_collect``. That gate claimed the
subscribed *root* task's OWN terminal events and ``continue``-d when empty — and an
orchestrator subtree root never emits ``blocked``/``completed`` (a *child* does),
so the gate was always empty and ``OrchestratorDeliveryAdapter.deliver()`` was
never invoked live. F12 fixes the gate to peek the **children's** unseen subtree
events (non-advancing) while the adapter stays the sole cursor claimer.

These tests pin the previously-dead path end-to-end through the real watcher:

- a real orchestrator(subtree) sub + a terminal CHILD → the watcher reaches
  ``deliver()`` and a supervision notice is written (the dead path);
- a second tick with no new child event → no-op (no duplicate notice, the
  subtree cursor advanced exactly once — the single-claimer invariant);
- a task-scoped (gateway) sub in the same board still delivers on its OWN
  completion (no regression to the unchanged branch).

Driven through ``runner._kanban_notifier_watcher`` (one tick) — no running
gateway, no network.
"""

import asyncio

from gateway.config import Platform
from gateway.run import GatewayRunner
from hermes_cli import kanban_db as kb


class RecordingAdapter:
    """Minimal connected messaging adapter (records gateway sends)."""

    def __init__(self):
        self.sent = []

    async def send(self, chat_id, text, metadata=None):
        self.sent.append({"chat_id": chat_id, "text": text, "metadata": metadata or {}})


async def _run_one_notifier_tick(monkeypatch, runner):
    real_sleep = asyncio.sleep

    async def fake_sleep(delay):
        # The watcher's initial wiring delay is 5s; let that pass, then stop
        # the loop after the first body so exactly one tick runs.
        if delay == 5:
            return None
        runner._running = False
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    await runner._kanban_notifier_watcher(interval=1)


def _make_runner(adapter):
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._running = True
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._kanban_sub_fail_counts = {}
    return runner


def _make_runner_no_platform():
    """A runner with NO connected messaging platform (``self.adapters == {}``).

    This is the Mini's live posture (Telegram deferred → "No messaging
    platforms enabled"). Before F14 ``_collect`` early-returned the whole tick
    for this, stranding every surface-agnostic (orchestrator/cli/tui) sub.
    """
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._running = True
    runner.adapters = {}
    runner._kanban_sub_fail_counts = {}
    return runner


def _subscribe_orchestrator(parent_id: str, target_id: str) -> None:
    conn = kb.connect()
    try:
        # F04 convention for a non-gateway sub: platform=subscriber_kind,
        # chat_id=<target-id>. _target_id() falls back to chat_id, so the
        # supervision notice lands under this target_id.
        kb.add_notify_sub(
            conn,
            task_id=parent_id,
            platform="orchestrator",
            chat_id=target_id,
            subscriber_kind="orchestrator",
            scope="subtree",
            delivery_policy="supervise",
        )
    finally:
        conn.close()


def _drain_orchestrator(target_id: str) -> list[dict]:
    conn = kb.connect()
    try:
        return kb.drain_notices(
            conn, subscriber_kind="orchestrator", target_id=target_id,
        )
    finally:
        conn.close()


def _subscribe_cli(task_id: str, target_id: str) -> None:
    """A surface-agnostic cli sub (F04 convention: platform=kind, chat_id=target)."""
    conn = kb.connect()
    try:
        kb.add_notify_sub(
            conn,
            task_id=task_id,
            platform="cli",
            chat_id=target_id,
            subscriber_kind="cli",
        )
    finally:
        conn.close()


def _drain_cli(target_id: str) -> list[dict]:
    conn = kb.connect()
    try:
        return kb.drain_notices(conn, subscriber_kind="cli", target_id=target_id)
    finally:
        conn.close()


def _sub_cursor(parent_id: str) -> int:
    conn = kb.connect()
    try:
        subs = kb.list_notify_subs(conn, parent_id)
        return int(subs[0]["last_event_id"])
    finally:
        conn.close()


def test_watcher_delivers_orchestrator_subtree_on_child_terminal(tmp_path, monkeypatch):
    """The watcher reaches deliver() for an orchestrator sub on a CHILD event.

    This is the path that was dead before F12: the root never emits a terminal
    event, so the old root-only gate always bailed and the adapter was never
    invoked live. Now the gate peeks the child's subtree event and proceeds.
    """
    db_path = tmp_path / "orch-gate.db"
    monkeypatch.setenv("HERMES_KANBAN_DB", str(db_path))
    kb.init_db()

    conn = kb.connect()
    try:
        parent = kb.create_task(conn, title="root goal", assignee="orchestrator")
        child = kb.create_task(conn, title="child A", assignee="worker")
        # Real decompose direction: the root is linked UNDER the subtask, so the
        # subtask is the root's task_links parent and is not gated by the root.
        kb.link_tasks(conn, child, parent)
        kb.complete_task(conn, child, summary="child A done")
    finally:
        conn.close()

    _subscribe_orchestrator(parent, "orch-watch-1")

    runner = _make_runner(RecordingAdapter())
    asyncio.run(_run_one_notifier_tick(monkeypatch, runner))

    notices = _drain_orchestrator("orch-watch-1")
    assert len(notices) == 1, (
        "watcher must reach OrchestratorDeliveryAdapter.deliver() and write one "
        "supervision notice for the child's terminal event (the F12 dead path)"
    )
    assert notices[0]["task_id"] == parent
    assert notices[0]["kind"] == "supervision"


def test_watcher_orchestrator_second_tick_is_noop(tmp_path, monkeypatch):
    """Second tick with no new child event → no duplicate notice (cursor once).

    Pins the single-claimer invariant: the adapter advances the subtree cursor
    exactly once; the watcher only peeked, so a re-tick claims nothing.
    """
    db_path = tmp_path / "orch-gate-noop.db"
    monkeypatch.setenv("HERMES_KANBAN_DB", str(db_path))
    kb.init_db()

    conn = kb.connect()
    try:
        parent = kb.create_task(conn, title="root goal", assignee="orchestrator")
        child = kb.create_task(conn, title="child A", assignee="worker")
        kb.link_tasks(conn, child, parent)
        kb.complete_task(conn, child, summary="child A done")
    finally:
        conn.close()

    _subscribe_orchestrator(parent, "orch-watch-2")

    # First tick: delivers once and the (adapter-owned) subtree cursor advances.
    asyncio.run(_run_one_notifier_tick(monkeypatch, _make_runner(RecordingAdapter())))
    assert len(_drain_orchestrator("orch-watch-2")) == 1
    cursor_after_first = _sub_cursor(parent)
    assert cursor_after_first > 0, "adapter advanced the subtree cursor once"

    # Second tick: no new child event → peek finds nothing → no deliver, and the
    # cursor stays put (the watcher never clobbers the adapter's claim).
    asyncio.run(_run_one_notifier_tick(monkeypatch, _make_runner(RecordingAdapter())))
    assert _drain_orchestrator("orch-watch-2") == [], "no duplicate notice on re-tick"
    assert _sub_cursor(parent) == cursor_after_first, (
        "subtree cursor must advance exactly once — the adapter is the sole "
        "claimer; the watcher only peeks (single-claimer invariant)"
    )


def test_watcher_orchestrator_picks_up_new_child_event_on_later_tick(tmp_path, monkeypatch):
    """A NEW child event after the first delivery is picked up on a later tick."""
    db_path = tmp_path / "orch-gate-new.db"
    monkeypatch.setenv("HERMES_KANBAN_DB", str(db_path))
    kb.init_db()

    conn = kb.connect()
    try:
        parent = kb.create_task(conn, title="root goal", assignee="orchestrator")
        child_a = kb.create_task(conn, title="child A", assignee="worker")
        child_b = kb.create_task(conn, title="child B", assignee="worker")
        kb.link_tasks(conn, child_a, parent)
        kb.link_tasks(conn, child_b, parent)
        kb.complete_task(conn, child_a, summary="a done")
    finally:
        conn.close()

    _subscribe_orchestrator(parent, "orch-watch-3")

    asyncio.run(_run_one_notifier_tick(monkeypatch, _make_runner(RecordingAdapter())))
    assert len(_drain_orchestrator("orch-watch-3")) == 1

    # Finish the last child → a fresh subtree event past the cursor.
    conn = kb.connect()
    try:
        kb.complete_task(conn, child_b, summary="b done")
    finally:
        conn.close()

    asyncio.run(_run_one_notifier_tick(monkeypatch, _make_runner(RecordingAdapter())))
    second = _drain_orchestrator("orch-watch-3")
    assert len(second) == 1, "a new child terminal event re-engages delivery"


def test_task_scoped_gateway_sub_unaffected_in_same_board(tmp_path, monkeypatch):
    """A cli/gateway task-scoped sub still delivers on its OWN completion.

    The unchanged branch: the root-only claim gate is correct for task-scoped
    subs, and F12 must not regress it — even alongside an orchestrator sub on
    the same board.
    """
    db_path = tmp_path / "orch-gate-mixed.db"
    monkeypatch.setenv("HERMES_KANBAN_DB", str(db_path))
    kb.init_db()

    conn = kb.connect()
    try:
        # An orchestrator subtree sub (its child completes) ...
        parent = kb.create_task(conn, title="root goal", assignee="orchestrator")
        child = kb.create_task(conn, title="child", assignee="worker")
        kb.link_tasks(conn, child, parent)
        kb.complete_task(conn, child, summary="child done")
        # ... plus an independent task-scoped gateway sub that completes itself.
        own = kb.create_task(conn, title="own task", assignee="worker")
        kb.add_notify_sub(conn, task_id=own, platform="telegram", chat_id="chat-1")
        kb.complete_task(conn, own, summary="own done")
    finally:
        conn.close()

    _subscribe_orchestrator(parent, "orch-watch-4")

    adapter = RecordingAdapter()
    asyncio.run(_run_one_notifier_tick(monkeypatch, _make_runner(adapter)))

    # The gateway sub delivered its own completion through the platform adapter.
    assert len(adapter.sent) == 1
    assert own in adapter.sent[0]["text"]
    # The orchestrator sub delivered its child notice through the notice queue.
    assert len(_drain_orchestrator("orch-watch-4")) == 1


# ---------------------------------------------------------------------------
# F14 — the notifier tick must run even with NO connected messaging platform.
#
# Before F14, ``_collect`` early-returned the whole tick when
# ``self.adapters == {}``, so on any host with no Discord/Telegram/Slack (the
# Mini, where Telegram is deferred) NO notifier-produced notice was ever
# delivered live — orchestrator/cli/tui subs were stranded even though they
# need no Platform. These pin that surface-agnostic delivery still happens with
# an empty adapter map, and that the per-sub gateway gate remains the
# authoritative connectivity filter (a gateway sub with no adapter is STILL
# skipped — the regression guard).
# ---------------------------------------------------------------------------


def test_watcher_delivers_orchestrator_subtree_with_no_platform(tmp_path, monkeypatch):
    """No messaging platform + orchestrator subtree sub + completed child →
    deliver() is still reached and a supervision notice is written (F14)."""
    db_path = tmp_path / "f14-orch-noplatform.db"
    monkeypatch.setenv("HERMES_KANBAN_DB", str(db_path))
    kb.init_db()

    conn = kb.connect()
    try:
        parent = kb.create_task(conn, title="root goal", assignee="orchestrator")
        child = kb.create_task(conn, title="child A", assignee="worker")
        kb.link_tasks(conn, child, parent)
        kb.complete_task(conn, child, summary="child A done")
    finally:
        conn.close()

    _subscribe_orchestrator(parent, "f14-orch-1")

    # No connected messaging Platform — the Mini's live posture.
    runner = _make_runner_no_platform()
    asyncio.run(_run_one_notifier_tick(monkeypatch, runner))

    notices = _drain_orchestrator("f14-orch-1")
    assert len(notices) == 1, (
        "with self.adapters == {} the tick must STILL enumerate subs and reach "
        "OrchestratorDeliveryAdapter.deliver() — surface-agnostic delivery (F14)"
    )
    assert notices[0]["task_id"] == parent
    assert notices[0]["kind"] == "supervision"
    # The subtree cursor advanced (the adapter claimed), proving the sub was
    # actually evaluated, not silently skipped.
    assert _sub_cursor(parent) > 0


def test_watcher_delivers_cli_sub_with_no_platform(tmp_path, monkeypatch):
    """No messaging platform + cli sub + completed task → a cli notice is
    persisted through the cli adapter (F14)."""
    db_path = tmp_path / "f14-cli-noplatform.db"
    monkeypatch.setenv("HERMES_KANBAN_DB", str(db_path))
    kb.init_db()

    conn = kb.connect()
    try:
        task = kb.create_task(conn, title="cli task", assignee="worker")
        kb.complete_task(conn, task, summary="cli done")
    finally:
        conn.close()

    _subscribe_cli(task, "f14-cli-1")

    runner = _make_runner_no_platform()
    asyncio.run(_run_one_notifier_tick(monkeypatch, runner))

    notices = _drain_cli("f14-cli-1")
    assert len(notices) == 1, (
        "a cli sub must deliver a notice with no connected messaging platform (F14)"
    )
    assert notices[0]["task_id"] == task
    assert notices[0]["kind"] == "completed"


def test_watcher_gateway_sub_still_skipped_with_no_platform(tmp_path, monkeypatch):
    """REGRESSION: a gateway sub whose platform is NOT in active_platforms
    (self.adapters == {}) is STILL skipped — no notice, no crash. The per-sub
    gate remains the authoritative connectivity filter (F14)."""
    db_path = tmp_path / "f14-gw-noplatform.db"
    monkeypatch.setenv("HERMES_KANBAN_DB", str(db_path))
    kb.init_db()

    conn = kb.connect()
    try:
        # A cli sub that SHOULD deliver alongside ...
        cli_task = kb.create_task(conn, title="cli task", assignee="worker")
        kb.complete_task(conn, cli_task, summary="cli done")
        # ... a gateway (telegram) sub that must STILL be skipped with no adapter.
        gw_task = kb.create_task(conn, title="gw task", assignee="worker")
        kb.add_notify_sub(conn, task_id=gw_task, platform="telegram", chat_id="chat-x")
        kb.complete_task(conn, gw_task, summary="gw done")
        gw_subs = kb.list_notify_subs(conn, gw_task)
    finally:
        conn.close()

    _subscribe_cli(cli_task, "f14-gw-1")

    runner = _make_runner_no_platform()
    asyncio.run(_run_one_notifier_tick(monkeypatch, runner))

    # The surface-agnostic cli sub delivered ...
    assert len(_drain_cli("f14-gw-1")) == 1
    # ... but the gateway sub was skipped: its cursor never advanced (no claim).
    assert int(gw_subs[0]["last_event_id"]) == 0
    conn = kb.connect()
    try:
        after = kb.list_notify_subs(conn, gw_task)
    finally:
        conn.close()
    assert int(after[0]["last_event_id"]) == 0, (
        "a gateway sub with no connected adapter must STILL be skipped at the "
        "per-sub gate — the tick-level removal must not let it deliver (F14)"
    )
