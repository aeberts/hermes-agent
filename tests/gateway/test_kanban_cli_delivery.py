"""Tests for the CLI delivery adapter.

This is the first non-gateway delivery adapter. A ``subscriber_kind='cli'``
subscription has no live push channel, so the adapter persists each claimed
terminal event as a plain-text notice in the shared ``kanban_notices`` store
(the TUI adapter later generalized the cli-only table); ``hermes kanban notices`` drains it.
These tests prove:

- subscribe cli → complete a task → exactly one notice is delivered; a second
  claim returns no events so no duplicate notice is written (cursor dedup);
- a ``blocked`` event delivers a notice while a non-terminal event is ignored;
- the gateway adapter still routes unchanged for gateway subscriptions.

Delivery is driven directly (claim + adapter.deliver) — no
running gateway, no network, no Platform adapter.
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
    GatewayDeliveryAdapter,
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


def _subscribe_cli(task_id: str, target_id: str) -> dict:
    """Declare an explicit cli subscription via the CLI path, return its row."""
    out = kc.run_slash(
        f"notify-subscribe {task_id} --subscriber-kind cli --target-id {target_id}"
    )
    assert f"cli:{target_id}" in out
    conn = kb.connect()
    try:
        subs = kb.list_notify_subs(conn, task_id)
    finally:
        conn.close()
    assert len(subs) == 1
    return subs[0]


def _claim_and_deliver(sub: dict) -> DeliveryResult:
    """Claim unseen terminal events for ``sub`` and run them through the adapter."""
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


def _drain(target_id: str | None = None) -> list[dict]:
    conn = kb.connect()
    try:
        return kb.drain_notices(conn, subscriber_kind="cli", target_id=target_id)
    finally:
        conn.close()


def test_cli_adapter_registered(kanban_home):
    assert isinstance(get_delivery_adapter("cli"), CLIDeliveryAdapter)


def test_completed_event_delivered_once_then_cursor_dedups(kanban_home):
    """subscribe cli → complete → one notice; re-claim → nothing (cursor dedup)."""
    out = kc.run_slash("create 'cli done' --assignee worker1")
    tid = re.search(r"(t_[a-f0-9]+)", out).group(1)
    sub = _subscribe_cli(tid, "sess-1")

    conn = kb.connect()
    try:
        kb.complete_task(conn, tid, summary="all wired up")
    finally:
        conn.close()

    result, events = _claim_and_deliver(sub)
    assert result.ok is True
    assert [e.kind for e in events] == ["completed"]

    notices = _drain("sess-1")
    assert len(notices) == 1
    n = notices[0]
    assert n["task_id"] == tid
    assert n["kind"] == "completed"
    assert "done" in n["message"]
    assert "all wired up" in n["message"]

    # Second poll: the claim cursor was advanced, so no new events → no notice.
    result2, events2 = _claim_and_deliver(sub)
    assert result2.ok is True
    assert events2 == []
    assert _drain("sess-1") == [], "no duplicate notice on re-claim"


def test_blocked_delivered_non_terminal_ignored(kanban_home):
    """A blocked event yields a notice; an intervening non-terminal event does not."""
    out = kc.run_slash("create 'cli block' --assignee worker1")
    tid = re.search(r"(t_[a-f0-9]+)", out).group(1)
    sub = _subscribe_cli(tid, "sess-2")

    conn = kb.connect()
    try:
        # Non-terminal noise the adapter must ignore (not in TERMINAL_KINDS).
        with kb.write_txn(conn):
            kb._append_event(conn, tid, kind="heartbeat")
        # Fresh task is 'ready'; block_task accepts ready|running.
        kb.block_task(conn, tid, reason="needs input")
    finally:
        conn.close()

    result, events = _claim_and_deliver(sub)
    assert result.ok is True
    assert [e.kind for e in events] == ["blocked"], "non-terminal events filtered by claim"

    notices = _drain("sess-2")
    assert len(notices) == 1
    assert notices[0]["kind"] == "blocked"
    assert "blocked" in notices[0]["message"]
    assert "needs input" in notices[0]["message"]


def test_drain_is_one_shot(kanban_home):
    """Reading notices clears them (notice-first display)."""
    out = kc.run_slash("create 'cli drain' --assignee worker1")
    tid = re.search(r"(t_[a-f0-9]+)", out).group(1)
    sub = _subscribe_cli(tid, "sess-3")
    conn = kb.connect()
    try:
        kb.complete_task(conn, tid, summary="done")
    finally:
        conn.close()
    _claim_and_deliver(sub)

    assert len(_drain("sess-3")) == 1
    assert _drain("sess-3") == [], "second drain is empty"


def test_notices_command_drains_and_clears(kanban_home):
    """`kanban notices` prints pending notices and clears them."""
    out = kc.run_slash("create 'cli cmd' --assignee worker1")
    tid = re.search(r"(t_[a-f0-9]+)", out).group(1)
    sub = _subscribe_cli(tid, "sess-4")
    conn = kb.connect()
    try:
        kb.complete_task(conn, tid, summary="shipped it")
    finally:
        conn.close()
    _claim_and_deliver(sub)

    out = kc.run_slash("notices --target-id sess-4")
    assert "shipped it" in out
    assert tid in out

    # Drained: a second invocation shows nothing.
    out = kc.run_slash("notices --target-id sess-4")
    assert "(no notices)" in out


def test_gateway_adapter_unchanged_for_gateway_subs(kanban_home):
    """The gateway adapter still routes a gateway subscription (left intact)."""
    out = kc.run_slash("create 'gw still' --assignee worker1")
    tid = re.search(r"(t_[a-f0-9]+)", out).group(1)
    kc.run_slash(f"notify-subscribe {tid} --platform telegram --chat-id chat-9")
    conn = kb.connect()
    try:
        kb.complete_task(conn, tid, summary="gw done")
        subs = kb.list_notify_subs(conn, tid)
    finally:
        conn.close()
    sub = subs[0]
    assert (sub.get("subscriber_kind") or "gateway") == "gateway"
    assert isinstance(get_delivery_adapter(sub.get("subscriber_kind")), GatewayDeliveryAdapter)

    sent = []

    class RecordingAdapter:
        async def send(self, chat_id, text, metadata=None):
            sent.append((chat_id, text))

    conn = kb.connect()
    try:
        _o, _n, events = kb.claim_unseen_events_for_sub(
            conn,
            task_id=sub["task_id"],
            platform=sub["platform"],
            chat_id=sub["chat_id"],
            thread_id=sub.get("thread_id") or "",
            kinds=TERMINAL_KINDS,
        )
        task = kb.get_task(conn, tid)
    finally:
        conn.close()
    delivery = {
        "sub": sub, "events": events, "task": task,
        "board": None, "adapter": RecordingAdapter(),
    }
    result = asyncio.run(GatewayDeliveryAdapter().deliver(None, delivery))
    assert result.ok is True
    assert len(sent) == 1
    assert sent[0][0] == "chat-9"
    assert "done" in sent[0][1]
    # No CLI notice was written for a gateway sub.
    assert _drain() == []
