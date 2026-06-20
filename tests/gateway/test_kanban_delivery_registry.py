"""Tests for the delivery-adapter registry (event-hub F03).

The notifier watcher used to be the only delivery path. F03 routes each claimed
batch through a registry keyed by ``subscriber_kind``; the gateway send logic is
the first registered adapter. These tests prove:

- a claimed terminal event is dispatched to the adapter registered for its
  ``subscriber_kind`` (using an in-test fake adapter for a non-gateway kind —
  no real non-gateway adapter ships in F03);
- an unknown/unregistered kind is skipped without error;
- an adapter delivery failure increments the per-sub failure counter and drops
  the subscription after MAX_SEND_FAILURES, going through the adapter boundary.
"""

import asyncio

from gateway.config import Platform
from gateway.run import GatewayRunner
from hermes_cli import kanban_db as kb

from gateway import kanban_delivery
from gateway.kanban_delivery import DeliveryResult


async def _run_one_notifier_tick(monkeypatch, runner):
    real_sleep = asyncio.sleep
    runner._running = True

    async def fake_sleep(delay):
        if delay == 5:
            return None
        runner._running = False
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    await runner._kanban_notifier_watcher(interval=1)


class RecordingAdapter:
    def __init__(self):
        self.sent = []

    async def send(self, chat_id, text, metadata=None):
        self.sent.append({"chat_id": chat_id, "text": text, "metadata": metadata or {}})


def _make_runner(adapter):
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._running = True
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._kanban_sub_fail_counts = {}
    return runner


def _set_subscriber_kind(tid, kind):
    conn = kb.connect()
    try:
        with kb.write_txn(conn):
            conn.execute(
                "UPDATE kanban_notify_subs SET subscriber_kind = ? WHERE task_id = ?",
                (kind, tid),
            )
    finally:
        conn.close()


def _create_completed_subscription(summary="done once"):
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="notify once", assignee="worker")
        kb.add_notify_sub(conn, task_id=tid, platform="telegram", chat_id="chat-1")
        kb.complete_task(conn, tid, summary=summary)
        return tid
    finally:
        conn.close()


def test_registry_dispatches_to_adapter_for_subscriber_kind(tmp_path, monkeypatch):
    """A claimed terminal event routes to the adapter registered for its kind."""
    db_path = tmp_path / "kind-dispatch.db"
    monkeypatch.setenv("HERMES_KANBAN_DB", str(db_path))
    kb.init_db()

    tid = _create_completed_subscription()
    _set_subscriber_kind(tid, "fake")

    received = []

    class FakeAdapter:
        async def deliver(self, runner, delivery):
            received.append(delivery)
            return DeliveryResult(ok=True)

    monkeypatch.setitem(kanban_delivery.DELIVERY_ADAPTERS, "fake", FakeAdapter())

    # The gateway send adapter must NOT fire for a 'fake' sub.
    gateway_adapter = RecordingAdapter()
    runner = _make_runner(gateway_adapter)

    asyncio.run(_run_one_notifier_tick(monkeypatch, runner))

    assert len(received) == 1, "fake adapter should receive exactly one claimed batch"
    batch = received[0]
    assert batch["sub"]["task_id"] == tid
    assert [ev.kind for ev in batch["events"]] == ["completed"]
    # Routing by kind: the gateway send path was not used for a 'fake' sub.
    assert gateway_adapter.sent == []


def test_unknown_subscriber_kind_skipped_without_error(tmp_path, monkeypatch):
    """An unregistered subscriber_kind is skipped; the claim is not advanced."""
    db_path = tmp_path / "unknown-kind.db"
    monkeypatch.setenv("HERMES_KANBAN_DB", str(db_path))
    kb.init_db()

    tid = _create_completed_subscription()
    _set_subscriber_kind(tid, "no-such-kind")

    adapter = RecordingAdapter()
    runner = _make_runner(adapter)

    # Must not raise.
    asyncio.run(_run_one_notifier_tick(monkeypatch, runner))

    assert adapter.sent == [], "no delivery for an unregistered subscriber_kind"
    # Skipped before claim handling, so the subscription survives untouched.
    conn = kb.connect()
    try:
        subs = kb.list_notify_subs(conn, tid)
        assert len(subs) == 1
    finally:
        conn.close()


def test_adapter_failure_increments_counter_and_drops_after_n(tmp_path, monkeypatch):
    """Adapter delivery failure increments the sub failure counter; drop after N."""
    db_path = tmp_path / "fail-drop.db"
    monkeypatch.setenv("HERMES_KANBAN_DB", str(db_path))
    kb.init_db()

    tid = _create_completed_subscription()
    _set_subscriber_kind(tid, "fake")

    class FailingAdapter:
        def __init__(self):
            self.calls = 0

        async def deliver(self, runner, delivery):
            self.calls += 1
            return DeliveryResult(ok=False)

    failing = FailingAdapter()
    monkeypatch.setitem(kanban_delivery.DELIVERY_ADAPTERS, "fake", failing)

    adapter = RecordingAdapter()
    runner = _make_runner(adapter)

    sub_key = (tid, "telegram", "chat-1", "")

    # First two failures rewind the claim (sub survives); the event is still
    # claimable each tick so the counter climbs.
    for expected in (1, 2):
        asyncio.run(_run_one_notifier_tick(monkeypatch, runner))
        assert runner._kanban_sub_fail_counts.get(sub_key) == expected
        conn = kb.connect()
        try:
            assert len(kb.list_notify_subs(conn, tid)) == 1
        finally:
            conn.close()

    # Third failure hits MAX_SEND_FAILURES (3) and drops the subscription.
    asyncio.run(_run_one_notifier_tick(monkeypatch, runner))
    assert failing.calls == 3
    assert sub_key not in runner._kanban_sub_fail_counts
    conn = kb.connect()
    try:
        assert kb.list_notify_subs(conn, tid) == [], "sub dropped after N failures"
    finally:
        conn.close()
