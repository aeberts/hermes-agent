"""Inclusive subtree-closure tests for the orchestrator claim + peek.

event-hub F13: the orchestrator-subtree observed set is the node's CLOSURE =
``{node} ∪ {its direct subtasks}`` (transitive descendants stay F08). Both the
authoritative claim (``claim_unseen_subtree_events_for_sub``) and the read-only
watcher-gate peek (``subtree_has_unseen_events_for_sub``) share one helper
(``_subtree_closure_ids``) so they can never diverge.

Coverage:
- n=1 (childless card): the node's OWN terminal event fires through claim AND
  peek (the old ``(cursor, cursor, [])`` never-fires case is fixed).
- n>1 (root + subtasks): behavior unchanged — a live/todo decompose root emits no
  terminal/blocker event, so adding it to the set is a no-op for fan-in/blocker
  delivery.
- cursor/dedup CAS still correct: a re-claim returns nothing.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb


@pytest.fixture
def kanban_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()
    return home


def _orch_sub(conn, parent_id, target_id="orch"):
    return kb.add_notify_sub(
        conn, task_id=parent_id, platform="orchestrator", chat_id=target_id,
        subscriber_kind="orchestrator", scope="subtree",
        delivery_policy="supervise",
    )


# --- n = 1 (childless card) ------------------------------------------------

def test_n1_childless_card_own_terminal_event_is_claimed(kanban_home):
    """A single subscribed card with NO subtasks fires on its own completion."""
    conn = kb.connect()
    try:
        card = kb.create_task(conn, title="lone card")
        sub_id = _orch_sub(conn, card, "orch-card")
        kb.complete_task(conn, card, summary="done")

        old, new, events = kb.claim_unseen_subtree_events_for_sub(conn, sub_id=sub_id)
        assert [e.kind for e in events] == ["completed"]
        assert {e.task_id for e in events} == {card}
        assert new > old
    finally:
        conn.close()


def test_n1_childless_card_peek_fires(kanban_home):
    """The read-only peek must agree with the claim for the n=1 case."""
    conn = kb.connect()
    try:
        card = kb.create_task(conn, title="lone card")
        sub_id = _orch_sub(conn, card, "orch-card")
        # No terminal event yet → peek is False.
        assert kb.subtree_has_unseen_events_for_sub(conn, sub_id=sub_id) is False
        kb.block_task(conn, card, reason="stuck")
        # Own blocker event now visible to the peek (without advancing cursor).
        assert kb.subtree_has_unseen_events_for_sub(conn, sub_id=sub_id) is True
    finally:
        conn.close()


def test_n1_claim_and_peek_identical_selection(kanban_home):
    """Peek True iff claim would deliver — the load-bearing identity."""
    conn = kb.connect()
    try:
        card = kb.create_task(conn, title="card")
        sub_id = _orch_sub(conn, card, "orch-x")
        kb.complete_task(conn, card, summary="ok")

        assert kb.subtree_has_unseen_events_for_sub(conn, sub_id=sub_id) is True
        _o, _n, events = kb.claim_unseen_subtree_events_for_sub(conn, sub_id=sub_id)
        assert len(events) == 1
        # After the claim advances the cursor, the peek must say "nothing".
        assert kb.subtree_has_unseen_events_for_sub(conn, sub_id=sub_id) is False
    finally:
        conn.close()


def test_n1_recclaim_is_empty_cursor_unchanged(kanban_home):
    conn = kb.connect()
    try:
        card = kb.create_task(conn, title="card")
        sub_id = _orch_sub(conn, card, "orch-y")
        kb.complete_task(conn, card, summary="ok")

        _o1, _n1, first = kb.claim_unseen_subtree_events_for_sub(conn, sub_id=sub_id)
        assert len(first) == 1
        o2, n2, second = kb.claim_unseen_subtree_events_for_sub(conn, sub_id=sub_id)
        assert second == []
        assert n2 == o2, "cursor unchanged on an empty re-claim"
    finally:
        conn.close()


# --- n > 1 (root + subtasks): behavior UNCHANGED ---------------------------

def test_n_gt_1_root_alive_no_op_only_subtasks_fire(kanban_home):
    """A live (todo) decompose root emits no terminal event, so including it in
    the closure is a no-op: only the subtasks' events are claimed."""
    conn = kb.connect()
    try:
        root = kb.create_task(conn, title="root")
        a = kb.create_task(conn, title="a")
        b = kb.create_task(conn, title="b")
        kb.complete_task(conn, a, summary="a done")
        kb.block_task(conn, b, reason="stuck")
        # Decompose direction: each subtask is the PARENT of the root.
        kb.link_tasks(conn, a, root)
        kb.link_tasks(conn, b, root)
        sub_id = _orch_sub(conn, root, "orch-root")

        old, new, events = kb.claim_unseen_subtree_events_for_sub(conn, sub_id=sub_id)
        assert sorted(e.kind for e in events) == ["blocked", "completed"]
        # The root itself (alive/todo) contributes nothing — only subtasks fire.
        assert {e.task_id for e in events} == {a, b}
        assert new > old
    finally:
        conn.close()


def test_n_gt_1_peek_matches_claim(kanban_home):
    conn = kb.connect()
    try:
        root = kb.create_task(conn, title="root")
        a = kb.create_task(conn, title="a")
        kb.link_tasks(conn, a, root)
        sub_id = _orch_sub(conn, root, "orch-root2")
        assert kb.subtree_has_unseen_events_for_sub(conn, sub_id=sub_id) is False
        kb.complete_task(conn, a, summary="a done")
        assert kb.subtree_has_unseen_events_for_sub(conn, sub_id=sub_id) is True
        kb.claim_unseen_subtree_events_for_sub(conn, sub_id=sub_id)
        assert kb.subtree_has_unseen_events_for_sub(conn, sub_id=sub_id) is False
    finally:
        conn.close()


def test_n_gt_1_recclaim_dedups(kanban_home):
    conn = kb.connect()
    try:
        root = kb.create_task(conn, title="root")
        a = kb.create_task(conn, title="a")
        kb.complete_task(conn, a, summary="x")
        kb.link_tasks(conn, a, root)
        sub_id = _orch_sub(conn, root, "orch-root3")

        _o1, _n1, first = kb.claim_unseen_subtree_events_for_sub(conn, sub_id=sub_id)
        assert len(first) == 1
        o2, n2, second = kb.claim_unseen_subtree_events_for_sub(conn, sub_id=sub_id)
        assert second == []
        assert n2 == o2
    finally:
        conn.close()


def test_closure_ids_helper_is_inclusive(kanban_home):
    """The shared helper returns {node} ∪ direct subtasks — the single source of
    truth that keeps claim and peek identical."""
    conn = kb.connect()
    try:
        root = kb.create_task(conn, title="root")
        # n=1: closure is just the node.
        assert kb._subtree_closure_ids(conn, root) == [root]
        a = kb.create_task(conn, title="a")
        b = kb.create_task(conn, title="b")
        kb.link_tasks(conn, a, root)
        kb.link_tasks(conn, b, root)
        ids = kb._subtree_closure_ids(conn, root)
        assert ids[0] == root
        assert set(ids) == {root, a, b}
    finally:
        conn.close()
