"""Tests for the TUI delivery adapter (event-hub F06).

F06 is the *second* non-gateway delivery adapter. It proves the F03 registry
generalizes to a new surface with no Kanban-core / watcher changes: registering
``DELIVERY_ADAPTERS['tui']`` is sufficient because the watcher already routes
any non-gateway ``subscriber_kind`` to its adapter (F05 made that gating
generic). Like CLI, ``subscriber_kind='tui'`` is notice-first — RFC §5.3 lists
both as "session notice"; live-turn wakeup/WS push is deferred to M01. So
"delivery" persists a plain-text notice into the shared, surface-agnostic
``kanban_notices`` store keyed by ``subscriber_kind`` + target id; ``hermes
kanban notices`` drains it.

These tests prove:

- subscribe tui → complete a task → exactly one notice; a second claim returns
  no events so no duplicate notice is written (cursor dedup);
- a ``blocked`` event delivers while a non-terminal event is ignored;
- the ``notices`` command drains+clears for a tui target;
- the adapter is registered (``TUIDeliveryAdapter``);
- cross-surface: cli + tui subscriptions to the same task each get their own
  notice, and draining one kind/target does not consume the other's (the shared
  table is correctly keyed by ``subscriber_kind`` + target).

Delivery is driven directly (claim + adapter.deliver) per the F06 gate — no
running gateway, no network, no Platform adapter, no ``tui_gateway`` push.
"""

import asyncio
import re

import pytest

from pathlib import Path

from hermes_cli import kanban as kc
from hermes_cli import kanban_db as kb

from gateway.kanban_delivery import (
    CLIDeliveryAdapter,
    DeliveryResult,
    TUIDeliveryAdapter,
    get_delivery_adapter,
)


@pytest.fixture
def kanban_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()
    return home


TERMINAL_KINDS = ("completed", "blocked", "gave_up", "crashed", "timed_out")


def _subscribe(task_id: str, kind: str, target_id: str) -> dict:
    """Declare an explicit subscription via the F04 CLI path, return its row."""
    out = kc.run_slash(
        f"notify-subscribe {task_id} --subscriber-kind {kind} --target-id {target_id}"
    )
    assert f"{kind}:{target_id}" in out
    conn = kb.connect()
    try:
        subs = kb.list_notify_subs(conn, task_id)
    finally:
        conn.close()
    return subs


def _subscribe_tui(task_id: str, target_id: str) -> dict:
    subs = _subscribe(task_id, "tui", target_id)
    assert len(subs) == 1
    return subs[0]


def _claim_and_deliver(sub: dict):
    """Claim unseen terminal events for ``sub`` and run them through its adapter."""
    conn = kb.connect()
    try:
        _old, _new, events = kb.claim_unseen_events_for_sub(
            conn,
            task_id=sub["task_id"],
            platform=sub["platform"],
            chat_id=sub["chat_id"],
            thread_id=sub.get("thread_id") or "",
            kinds=TERMINAL_KINDS,
        )
        task = kb.get_task(conn, sub["task_id"])
    finally:
        conn.close()
    delivery = {
        "sub": sub,
        "events": events,
        "task": task,
        "board": None,
        "adapter": None,
    }
    adapter = get_delivery_adapter(sub["subscriber_kind"])
    return asyncio.run(adapter.deliver(None, delivery)), events


def _drain(kind: str | None = None, target_id: str | None = None) -> list[dict]:
    conn = kb.connect()
    try:
        return kb.drain_notices(conn, subscriber_kind=kind, target_id=target_id)
    finally:
        conn.close()


def test_tui_adapter_registered(kanban_home):
    assert isinstance(get_delivery_adapter("tui"), TUIDeliveryAdapter)


def test_completed_event_delivered_once_then_cursor_dedups(kanban_home):
    """subscribe tui → complete → one notice; re-claim → nothing (cursor dedup)."""
    out = kc.run_slash("create 'tui done' --assignee worker1")
    tid = re.search(r"(t_[a-f0-9]+)", out).group(1)
    sub = _subscribe_tui(tid, "tsess-1")

    conn = kb.connect()
    try:
        kb.complete_task(conn, tid, summary="all wired up")
    finally:
        conn.close()

    result, events = _claim_and_deliver(sub)
    assert result.ok is True
    assert [e.kind for e in events] == ["completed"]

    notices = _drain("tui", "tsess-1")
    assert len(notices) == 1
    n = notices[0]
    assert n["subscriber_kind"] == "tui"
    assert n["task_id"] == tid
    assert n["kind"] == "completed"
    assert "done" in n["message"]
    assert "all wired up" in n["message"]

    # Second poll: the claim cursor was advanced, so no new events → no notice.
    result2, events2 = _claim_and_deliver(sub)
    assert result2.ok is True
    assert events2 == []
    assert _drain("tui", "tsess-1") == [], "no duplicate notice on re-claim"


def test_blocked_delivered_non_terminal_ignored(kanban_home):
    """A blocked event yields a notice; an intervening non-terminal event does not."""
    out = kc.run_slash("create 'tui block' --assignee worker1")
    tid = re.search(r"(t_[a-f0-9]+)", out).group(1)
    sub = _subscribe_tui(tid, "tsess-2")

    conn = kb.connect()
    try:
        # Non-terminal noise the adapter must ignore (not in TERMINAL_KINDS).
        with kb.write_txn(conn):
            kb._append_event(conn, tid, kind="heartbeat")
        kb.block_task(conn, tid, reason="needs input")
    finally:
        conn.close()

    result, events = _claim_and_deliver(sub)
    assert result.ok is True
    assert [e.kind for e in events] == ["blocked"], "non-terminal events filtered by claim"

    notices = _drain("tui", "tsess-2")
    assert len(notices) == 1
    assert notices[0]["kind"] == "blocked"
    assert "blocked" in notices[0]["message"]
    assert "needs input" in notices[0]["message"]


def test_notices_command_drains_and_clears_for_tui(kanban_home):
    """`kanban notices --kind tui` prints pending tui notices and clears them."""
    out = kc.run_slash("create 'tui cmd' --assignee worker1")
    tid = re.search(r"(t_[a-f0-9]+)", out).group(1)
    sub = _subscribe_tui(tid, "tsess-3")
    conn = kb.connect()
    try:
        kb.complete_task(conn, tid, summary="shipped it")
    finally:
        conn.close()
    _claim_and_deliver(sub)

    out = kc.run_slash("notices --kind tui --target-id tsess-3")
    assert "shipped it" in out
    assert tid in out

    # Drained: a second invocation shows nothing.
    out = kc.run_slash("notices --kind tui --target-id tsess-3")
    assert "(no notices)" in out


def test_cli_and_tui_share_store_but_keyed_independently(kanban_home):
    """cli + tui subs to one task each get their own notice; draining one kind/target
    does not consume the other's (shared table correctly keyed)."""
    out = kc.run_slash("create 'shared store' --assignee worker1")
    tid = re.search(r"(t_[a-f0-9]+)", out).group(1)

    # Two subscriptions to the same task, different surfaces + targets.
    subs = _subscribe(tid, "cli", "csess")
    subs = _subscribe(tid, "tui", "tsess")
    assert len(subs) == 2
    cli_sub = next(s for s in subs if s["subscriber_kind"] == "cli")
    tui_sub = next(s for s in subs if s["subscriber_kind"] == "tui")

    conn = kb.connect()
    try:
        kb.complete_task(conn, tid, summary="both notified")
    finally:
        conn.close()

    # Each adapter delivers independently (each sub has its own claim cursor).
    res_cli, ev_cli = _claim_and_deliver(cli_sub)
    res_tui, ev_tui = _claim_and_deliver(tui_sub)
    assert res_cli.ok is True and [e.kind for e in ev_cli] == ["completed"]
    assert res_tui.ok is True and [e.kind for e in ev_tui] == ["completed"]

    # Draining the tui target leaves the cli notice intact (keyed independently).
    tui_notices = _drain("tui", "tsess")
    assert len(tui_notices) == 1
    assert tui_notices[0]["subscriber_kind"] == "tui"
    assert tui_notices[0]["target_id"] == "tsess"

    # The cli notice is still there.
    cli_notices = _drain("cli", "csess")
    assert len(cli_notices) == 1
    assert cli_notices[0]["subscriber_kind"] == "cli"
    assert cli_notices[0]["target_id"] == "csess"

    # Both surfaces now drained.
    assert _drain() == []
