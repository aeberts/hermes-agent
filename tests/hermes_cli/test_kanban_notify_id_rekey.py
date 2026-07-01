"""Tests for the id-keyed subscription claim/cursor API.

A surrogate ``id`` was added to ``kanban_notify_subs``; this re-keys the
claim/cursor API onto that ``id`` while keeping the legacy gateway
``(task_id, platform, chat_id, thread_id)`` tuple call paths working through a
resolver shim. These tests pin both the id-keyed API and the tuple shim, plus
the single-owner cursor-CAS under concurrent claimers.
"""

from __future__ import annotations

import concurrent.futures
import threading
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb


@pytest.fixture
def kanban_home(tmp_path, monkeypatch):
    """Isolated HERMES_HOME with an empty kanban DB."""
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()
    return home


def test_add_notify_sub_returns_id(kanban_home):
    """Subscribe returns the surrogate id; re-subscribe returns the same id."""
    with kb.connect() as conn:
        tid = kb.create_task(conn, title="t", assignee="w1")
        sid = kb.add_notify_sub(conn, task_id=tid, platform="telegram", chat_id="c1")
        assert isinstance(sid, int) and sid > 0
        # Idempotent on the tuple → same id.
        sid2 = kb.add_notify_sub(conn, task_id=tid, platform="telegram", chat_id="c1")
        assert sid2 == sid
        # Distinct tuple → distinct id.
        sid3 = kb.add_notify_sub(conn, task_id=tid, platform="telegram", chat_id="c2")
        assert sid3 != sid


def test_claim_and_advance_by_id(kanban_home):
    """unseen / claim / advance all operate by id."""
    with kb.connect() as conn:
        tid = kb.create_task(conn, title="t", assignee="w1")
        sid = kb.add_notify_sub(conn, task_id=tid, platform="telegram", chat_id="c1")
        kb.complete_task(conn, tid, result="done")

        # unseen by id
        new_cursor, events = kb.unseen_events_for_sub(conn, sub_id=sid)
        assert events and new_cursor > 0

        # claim by id advances the cursor atomically
        old_c, new_c, claimed = kb.claim_unseen_events_for_sub(conn, sub_id=sid)
        assert old_c == 0
        assert new_c == new_cursor
        assert [e.id for e in claimed] == [e.id for e in events]

        # second claim by id sees nothing new
        old_c2, new_c2, claimed2 = kb.claim_unseen_events_for_sub(conn, sub_id=sid)
        assert claimed2 == []
        assert old_c2 == new_c2 == new_c


def test_advance_and_remove_by_id(kanban_home):
    """advance_notify_cursor and remove_notify_sub key on id."""
    with kb.connect() as conn:
        tid = kb.create_task(conn, title="t", assignee="w1")
        sid = kb.add_notify_sub(conn, task_id=tid, platform="telegram", chat_id="c1")
        kb.complete_task(conn, tid, result="done")
        _, ev = kb.unseen_events_for_sub(conn, sub_id=sid)
        target = max(e.id for e in ev)

        kb.advance_notify_cursor(conn, sub_id=sid, new_cursor=target)
        subs = kb.list_notify_subs(conn, tid)
        assert int(subs[0]["last_event_id"]) == target

        assert kb.remove_notify_sub(conn, sub_id=sid) is True
        assert kb.list_notify_subs(conn, tid) == []
        # Removing an unknown id is a no-op.
        assert kb.remove_notify_sub(conn, sub_id=sid) is False


def test_tuple_shim_resolves_to_same_row(kanban_home):
    """The gateway tuple path and the id path hit the same row."""
    with kb.connect() as conn:
        tid = kb.create_task(conn, title="t", assignee="w1")
        sid = kb.add_notify_sub(
            conn, task_id=tid, platform="telegram", chat_id="c1", thread_id="th1",
        )
        resolved = kb._resolve_notify_sub_id(
            conn, task_id=tid, platform="telegram", chat_id="c1", thread_id="th1",
        )
        assert resolved == sid

        kb.complete_task(conn, tid, result="done")

        # Claim via the legacy tuple (exactly how gateway/kanban_watchers calls it).
        old_c, new_c, events = kb.claim_unseen_events_for_sub(
            conn,
            task_id=tid,
            platform="telegram",
            chat_id="c1",
            thread_id="th1",
        )
        assert events
        # The id-keyed row reflects the tuple-path claim.
        subs = kb.list_notify_subs(conn, tid)
        assert int(subs[0]["id"]) == sid
        assert int(subs[0]["last_event_id"]) == new_c


def test_concurrent_claim_single_owner(kanban_home):
    """Two concurrent claims on one id: only one claims the event range."""
    with kb.connect() as conn:
        tid = kb.create_task(conn, title="t", assignee="w1")
        sid = kb.add_notify_sub(conn, task_id=tid, platform="telegram", chat_id="c1")
        kb.complete_task(conn, tid, result="done")

    barrier = threading.Barrier(2)

    def _claim() -> int:
        # Each thread gets its own connection (SQLite connections are not
        # shareable across threads). The cursor-CAS inside BEGIN IMMEDIATE
        # serializes them so only one observes the unclaimed range.
        conn = kb.connect()
        try:
            barrier.wait(timeout=10)
            _old, _new, events = kb.claim_unseen_events_for_sub(conn, sub_id=sid)
            return len(events)
        finally:
            conn.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        results = [f.result() for f in [ex.submit(_claim), ex.submit(_claim)]]

    # Exactly one claimer got the (non-empty) event range; the other saw an
    # empty range. The completed transition emits >1 event, so assert on the
    # single-owner split rather than an exact count.
    claimed = [n for n in results if n > 0]
    empty = [n for n in results if n == 0]
    assert len(claimed) == 1 and len(empty) == 1, (
        f"cursor-CAS must make claim single-owner, got {results!r}"
    )

    # Cursor advanced exactly once, past the completed event.
    with kb.connect() as conn:
        subs = kb.list_notify_subs(conn, tid)
    assert int(subs[0]["last_event_id"]) >= 1
