"""Tests for orchestrator re-engagement / close-the-loop (event-hub F09 + F10).

F10 extends F09's single consumer (``reengage_orchestrator``) to BRANCH on the
drained F07 snapshot: a ``fan_in_ready`` snapshot still writes the F09
``[kanban:reengage]`` comment (judge), while a snapshot carrying a blocked child
(``kind=="blocked"``) writes a new ``[kanban:triage]`` handoff (answer / unblock
/ escalate) carrying ``{trigger:"blocked", child id, reason}`` + the snapshot.
The two branches are mutually exclusive per snapshot (a blocked child is never
terminal, so a snapshot is never both blocked and fan-in-ready), and idempotency
is still the F07 one-shot drain — a re-block after unblock is a new claimed event
→ a new triage handoff. F10 is enqueue-only (it writes the handoff; it does NOT
wake the supervisor — that's F11).

The original F09 docstring follows.

Tests for orchestrator re-engagement / close-the-loop (event-hub F09).

F09 is the consumer of F07's orchestrator supervision notices: it drains those
notices for a target, groups them by ``parent_id`` (root), and for each root
whose latest drained notice is ``fan_in_ready=true`` appends ONE structured
``[kanban:reengage]`` comment carrying the aggregate snapshot to that root. That
comment lands in the root's comment thread, which ``build_worker_context``
surfaces to the re-spawned orchestrator turn — the curated fan-in handoff the
fresh turn reads to judge the goal, route more work, or finish.

These tests prove (per the spec's "Tests Needed"):

- a ``fan_in_ready=true`` notice → one re-engagement comment on the root whose
  body contains the snapshot, visible in ``build_worker_context``;
- a ``fan_in_ready=false`` (partial) notice → no comment, zero re-engagements;
- a second pass with the store drained → no-op (idempotent via one-shot drain);
- multi-round: a second F07 fan-in notice → a second re-engagement comment;
- observational: task count + root status unchanged across the pass;
- CLI ``kanban reengage --target-id X`` (+ ``--json``) round-trips and reports
  the re-engaged root once, nothing on the second call;
- end-to-end (no dispatcher): real ``decompose_triage_task`` → subscribe
  orchestrator(subtree) → complete all subtasks → drive F07 delivery →
  ``reengage`` writes the snapshot comment that ``build_worker_context`` shows.

Delivery is driven directly (adapter.deliver) per the F05/F06/F07 gate — no
running gateway, no network, no Platform adapter.
"""

import asyncio
import json
import re

from pathlib import Path

import pytest

from hermes_cli import kanban as kc
from hermes_cli import kanban_db as kb

from gateway.kanban_delivery import get_delivery_adapter


@pytest.fixture
def kanban_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()
    return home


# --- helpers (mirror the F07 orchestrator-delivery test patterns) -----------

def _create(title: str, *, assignee: str = "worker1") -> str:
    out = kc.run_slash(f"create '{title}' --assignee {assignee}")
    return re.search(r"(t_[a-f0-9]+)", out).group(1)


def _subscribe_orchestrator(parent_id: str, target_id: str) -> dict:
    out = kc.run_slash(
        f"notify-subscribe {parent_id} --subscriber-kind orchestrator "
        f"--target-id {target_id} --scope subtree --delivery-policy supervise"
    )
    assert f"orchestrator:{target_id}" in out
    conn = kb.connect()
    try:
        subs = kb.list_notify_subs(conn, parent_id)
    finally:
        conn.close()
    assert len(subs) == 1
    return subs[0]


def _deliver(sub: dict):
    """Drive the orchestrator adapter directly (it does its own subtree claim)."""
    delivery = {"sub": sub, "board": None}
    adapter = get_delivery_adapter(sub["subscriber_kind"])
    return asyncio.run(adapter.deliver(None, delivery))


def _complete(conn, task_id: str, **kw) -> None:
    assert kb.complete_task(conn, task_id, **kw) is True


def _block(conn, task_id: str, **kw) -> None:
    assert kb.block_task(conn, task_id, **kw) is True


def _unblock(conn, task_id: str) -> None:
    assert kb.unblock_task(conn, task_id) is True


def _blocked_child(task_id: str, *, reason: str, title: str = "child") -> dict:
    """An F07-shaped blocked child row (mirrors the orchestrator adapter)."""
    return {
        "task_id": task_id,
        "title": title,
        "kind": "blocked",
        "status": "blocked",
        "assignee": "worker1",
        "reason": reason,
        "artifacts": [],
    }


def _link(conn, root_id: str, subtask_id: str) -> None:
    """Wire a supervision subtree edge the way decomposition does (root under subtask)."""
    kb.link_tasks(conn, subtask_id, root_id)


def _add_orchestrator_notice(target_id: str, root_id: str, snapshot: dict) -> int:
    """Hand-build an F07-shaped orchestrator notice (a fan-in snapshot payload)."""
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


def _comments(root_id: str) -> list:
    conn = kb.connect()
    try:
        return kb.list_comments(conn, root_id)
    finally:
        conn.close()


def _reengage(target_id: str):
    conn = kb.connect()
    try:
        return kb.reengage_orchestrator(conn, target_id=target_id)
    finally:
        conn.close()


# --- DB-layer unit tests ----------------------------------------------------

def test_fan_in_true_writes_one_snapshot_comment_visible_in_context(kanban_home):
    """fan_in_ready=true → one [kanban:reengage] comment carrying the snapshot."""
    root = _create("root goal")
    child = _create("child A")
    snap = _snapshot(
        root, fan_in_ready=True,
        children=[{"task_id": child, "title": "child A", "kind": "completed",
                   "status": "done", "assignee": "worker1",
                   "summary": "A finished", "artifacts": []}],
    )
    _add_orchestrator_notice("orch-1", root, snap)

    results = _reengage("orch-1")
    assert len(results) == 1
    assert results[0].root_id == root
    assert results[0].comment_id > 0

    comments = _comments(root)
    assert len(comments) == 1
    body = comments[0].body
    assert body.startswith(kb.REENGAGE_PREFIX)
    # The snapshot block is embedded and parseable.
    assert root in body
    assert child in body
    assert "fan_in_ready" in body
    json_block = body[body.index("{"):]
    parsed = json.loads(json_block)
    assert parsed["parent_id"] == root
    assert parsed["fan_in_ready"] is True

    # The comment is surfaced to the re-spawned orchestrator turn.
    conn = kb.connect()
    try:
        ctx = kb.build_worker_context(conn, root)
    finally:
        conn.close()
    assert kb.REENGAGE_PREFIX in ctx
    assert child in ctx


def test_fan_in_false_writes_no_comment(kanban_home):
    """A partial (fan_in_ready=false) notice is passive — no re-engagement."""
    root = _create("root")
    _add_orchestrator_notice("orch-2", root, _snapshot(root, fan_in_ready=False))

    results = _reengage("orch-2")
    assert results == []
    assert _comments(root) == []

    # The partial notice was consumed by the drain (not left re-deliverable).
    conn = kb.connect()
    try:
        assert kb.drain_notices(
            conn, subscriber_kind="orchestrator", target_id="orch-2",
        ) == []
    finally:
        conn.close()


def test_second_pass_with_drained_store_is_noop(kanban_home):
    """Idempotent via the one-shot drain: a re-run with no new notice is a no-op."""
    root = _create("root")
    _add_orchestrator_notice("orch-3", root, _snapshot(root, fan_in_ready=True))

    assert len(_reengage("orch-3")) == 1
    assert len(_comments(root)) == 1

    # Nothing new in the store → no duplicate comment.
    assert _reengage("orch-3") == []
    assert len(_comments(root)) == 1


def test_multi_round_second_fan_in_yields_second_comment(kanban_home):
    """A second decompose round's fan-in notice re-engages again (no marker block)."""
    root = _create("root")
    _add_orchestrator_notice("orch-4", root, _snapshot(root, fan_in_ready=True))
    assert len(_reengage("orch-4")) == 1
    assert len(_comments(root)) == 1

    # Round 2: a fresh F07 fan-in notice for the same root.
    _add_orchestrator_notice("orch-4", root, _snapshot(root, fan_in_ready=True))
    results = _reengage("orch-4")
    assert len(results) == 1
    assert results[0].root_id == root
    assert len(_comments(root)) == 2


def test_at_most_one_comment_per_root_per_pass_uses_latest(kanban_home):
    """Multiple notices for one root in a single drain → one comment (latest wins)."""
    root = _create("root")
    # An earlier partial, then a later fan-in — both for the same root, drained
    # together. Latest (fan_in_ready=true) wins; exactly one comment.
    _add_orchestrator_notice("orch-5", root, _snapshot(root, fan_in_ready=False))
    later = _snapshot(
        root, fan_in_ready=True,
        children=[{"task_id": "t_xyz", "kind": "completed"}],
    )
    _add_orchestrator_notice("orch-5", root, later)

    results = _reengage("orch-5")
    assert len(results) == 1
    comments = _comments(root)
    assert len(comments) == 1
    parsed = json.loads(comments[0].body[comments[0].body.index("{"):])
    assert parsed["fan_in_ready"] is True
    assert parsed["children"][0]["task_id"] == "t_xyz"


def test_observational_no_task_or_status_change(kanban_home):
    """reengage adds only a comment (+ its commented event); no task/status change."""
    root = _create("root")
    _add_orchestrator_notice("orch-6", root, _snapshot(root, fan_in_ready=True))

    conn = kb.connect()
    try:
        before_count = len(kb.list_tasks(conn, include_archived=True))
        before_status = kb.get_task(conn, root).status
    finally:
        conn.close()

    _reengage("orch-6")

    conn = kb.connect()
    try:
        after_count = len(kb.list_tasks(conn, include_archived=True))
        after_status = kb.get_task(conn, root).status
    finally:
        conn.close()

    assert after_count == before_count, "reengage created no tasks"
    assert after_status == before_status, "reengage did not change promotion state"


# --- CLI tests --------------------------------------------------------------

def test_cli_reengage_reports_root_then_empty(kanban_home):
    """`kanban reengage --target-id X` reports the root once; empty on re-run."""
    root = _create("root")
    _add_orchestrator_notice("orch-7", root, _snapshot(root, fan_in_ready=True))

    out = kc.run_slash("reengage --target-id orch-7")
    assert root in out
    assert "(no roots re-engaged)" not in out

    out2 = kc.run_slash("reengage --target-id orch-7")
    assert "(no roots re-engaged)" in out2


def test_cli_reengage_json_round_trip(kanban_home):
    """`--json` emits a machine-readable list of re-engaged roots; empty on re-run."""
    root = _create("root")
    _add_orchestrator_notice("orch-8", root, _snapshot(root, fan_in_ready=True))

    out = kc.run_slash("reengage --target-id orch-8 --json")
    data = json.loads(out)
    assert len(data) == 1
    assert data[0]["root_id"] == root
    assert isinstance(data[0]["comment_id"], int)

    out2 = kc.run_slash("reengage --target-id orch-8 --json")
    assert json.loads(out2) == []


def test_cli_reengage_partial_reports_nothing(kanban_home):
    """A drained partial notice writes nothing and reports no re-engaged roots."""
    root = _create("root")
    _add_orchestrator_notice("orch-9", root, _snapshot(root, fan_in_ready=False))

    out = kc.run_slash("reengage --target-id orch-9 --json")
    assert json.loads(out) == []
    assert _comments(root) == []


# --- End-to-end (no dispatcher): the close-the-loop unit proof --------------

def test_end_to_end_real_decompose_reengages_root(kanban_home):
    """Real decompose → F07 delivery → reengage writes the handoff onto the root.

    The MBP analogue of M07's live proof: everything up to, but not including,
    the actual dispatcher re-spawn. After reengage, build_worker_context(root)
    contains the fan-in handoff the re-spawned orchestrator would read.
    """
    conn = kb.connect()
    try:
        root = kb.create_task(conn, title="orchestrated goal", triage=True)
        child_ids = kb.decompose_triage_task(
            conn, root, root_assignee="orchestrator",
            children=[{"title": "subtask A"}, {"title": "subtask B"}],
        )
    finally:
        conn.close()
    assert child_ids is not None and len(child_ids) == 2
    sub_a, sub_b = child_ids

    sub = _subscribe_orchestrator(root, "orch-e2e")

    # Complete the first subtask → partial fan-in notice (no re-engagement).
    conn = kb.connect()
    try:
        _complete(conn, sub_a, summary="A done")
    finally:
        conn.close()
    _deliver(sub)
    assert _reengage("orch-e2e") == [], "partial fan-in does not re-engage"
    assert _comments(root) == []

    # Complete the last subtask → fan-in flips true → F07 emits a fan-in notice.
    conn = kb.connect()
    try:
        _complete(conn, sub_b, summary="B done")
    finally:
        conn.close()
    _deliver(sub)

    results = _reengage("orch-e2e")
    assert len(results) == 1
    assert results[0].root_id == root

    # The re-spawned orchestrator turn reads the curated fan-in handoff.
    conn = kb.connect()
    try:
        ctx = kb.build_worker_context(conn, root)
    finally:
        conn.close()
    assert kb.REENGAGE_PREFIX in ctx
    assert sub_a in ctx and sub_b in ctx
    assert '"fan_in_ready": true' in ctx


# --- F10: blocker-triggered triage handoff ----------------------------------

def test_blocked_child_writes_one_triage_handoff_visible_in_context(kanban_home):
    """Tests Needed #1: a blocked child → exactly one [kanban:triage] handoff on
    the root carrying child id + reason; visible in build_worker_context."""
    root = _create("root goal")
    child = _create("child A")
    snap = _snapshot(
        root, fan_in_ready=False,
        children=[_blocked_child(child, reason="needs API key", title="child A")],
    )
    _add_orchestrator_notice("orch-b1", root, snap)

    results = _reengage("orch-b1")
    assert len(results) == 1
    assert results[0].root_id == root
    assert results[0].comment_id > 0
    assert results[0].trigger == "blocked"

    comments = _comments(root)
    assert len(comments) == 1
    body = comments[0].body
    assert body.startswith(kb.TRIAGE_PREFIX)
    assert child in body
    assert "needs API key" in body
    # The triage block is machine-parseable and carries the resolved fields.
    parsed = json.loads(body[body.index("{"):])
    assert parsed["trigger"] == "blocked"
    assert parsed["child"] == child
    assert parsed["reason"] == "needs API key"
    assert parsed["snapshot"]["parent_id"] == root

    # The handoff is surfaced to the re-spawned orchestrator turn.
    conn = kb.connect()
    try:
        ctx = kb.build_worker_context(conn, root)
    finally:
        conn.close()
    assert kb.TRIAGE_PREFIX in ctx
    assert child in ctx
    assert "needs API key" in ctx


def test_partial_with_no_block_writes_no_triage_and_is_idempotent(kanban_home):
    """Tests Needed #2: a partial with no blocked child → no triage; re-run with
    nothing new is a no-op (idempotent via the one-shot drain)."""
    root = _create("root")
    child = _create("child A")
    # A partial snapshot: one completed child, none blocked, not fan-in-ready.
    snap = _snapshot(
        root, fan_in_ready=False,
        children=[{"task_id": child, "title": "child A", "kind": "completed",
                   "status": "running", "assignee": "worker1",
                   "summary": "A done", "artifacts": []}],
    )
    _add_orchestrator_notice("orch-b2", root, snap)

    assert _reengage("orch-b2") == []
    assert _comments(root) == []

    # Re-run with nothing new in the drained store → still a no-op.
    assert _reengage("orch-b2") == []
    assert _comments(root) == []


def test_block_unblock_block_yields_second_triage_handoff(kanban_home):
    """Tests Needed #3: block → unblock → block again → a SECOND triage handoff
    (multi-round; a re-block is a new claimed event, no separate marker)."""
    root = _create("root")
    child = _create("child A")

    # Round 1: blocked → triage.
    _add_orchestrator_notice(
        "orch-b3", root,
        _snapshot(root, fan_in_ready=False,
                  children=[_blocked_child(child, reason="round-1 question")]),
    )
    r1 = _reengage("orch-b3")
    assert len(r1) == 1 and r1[0].trigger == "blocked"
    assert len(_comments(root)) == 1

    # Unblock (and re-block): a fresh F07 notice for the same root/child.
    _add_orchestrator_notice(
        "orch-b3", root,
        _snapshot(root, fan_in_ready=False,
                  children=[_blocked_child(child, reason="round-2 question")]),
    )
    r2 = _reengage("orch-b3")
    assert len(r2) == 1 and r2[0].trigger == "blocked"
    comments = _comments(root)
    assert len(comments) == 2
    # The second handoff carries the new reason.
    parsed = json.loads(comments[1].body[comments[1].body.index("{"):])
    assert parsed["reason"] == "round-2 question"


def test_composition_block_then_fan_in_each_fire_once(kanban_home):
    """Tests Needed #4: block (→triage) then later complete-all (→fan-in) both
    fire once each; no masking. Driven through real decompose + F07 delivery."""
    conn = kb.connect()
    try:
        root = kb.create_task(conn, title="orchestrated goal", triage=True)
        child_ids = kb.decompose_triage_task(
            conn, root, root_assignee="orchestrator",
            children=[{"title": "subtask A"}, {"title": "subtask B"}],
        )
    finally:
        conn.close()
    assert child_ids is not None and len(child_ids) == 2
    sub_a, sub_b = child_ids

    sub = _subscribe_orchestrator(root, "orch-b4")

    # Phase 1: subtask A blocks → F07 emits a snapshot with a blocked child →
    # one triage handoff. (block requires a running task; claim it first.)
    conn = kb.connect()
    try:
        assert kb.claim_task(conn, sub_a, claimer="worker1") is not None
        _block(conn, sub_a, reason="A is stuck")
    finally:
        conn.close()
    _deliver(sub)
    r_block = _reengage("orch-b4")
    assert len(r_block) == 1
    assert r_block[0].trigger == "blocked"
    assert len(_comments(root)) == 1
    assert _comments(root)[0].body.startswith(kb.TRIAGE_PREFIX)

    # Phase 2: unblock A, complete both → fan-in flips true → one reengage.
    conn = kb.connect()
    try:
        _unblock(conn, sub_a)
        _complete(conn, sub_a, summary="A done")
        _complete(conn, sub_b, summary="B done")
    finally:
        conn.close()
    _deliver(sub)
    r_fanin = _reengage("orch-b4")
    assert len(r_fanin) == 1
    assert r_fanin[0].trigger == "fan_in"
    comments = _comments(root)
    # Exactly one of each: the triage (phase 1) and the reengage (phase 2).
    assert len(comments) == 2
    assert comments[0].body.startswith(kb.TRIAGE_PREFIX)
    assert comments[1].body.startswith(kb.REENGAGE_PREFIX)

    # Both handoffs are surfaced; no masking, no double-fire.
    conn = kb.connect()
    try:
        ctx = kb.build_worker_context(conn, root)
    finally:
        conn.close()
    assert kb.TRIAGE_PREFIX in ctx
    assert kb.REENGAGE_PREFIX in ctx


def test_cli_reengage_triage_reports_trigger(kanban_home):
    """CLI: a blocked-child snapshot → `triaged <root>` human line + trigger in
    --json (backward-compatible root_id/comment_id retained)."""
    root = _create("root")
    child = _create("child A")
    _add_orchestrator_notice(
        "orch-b5", root,
        _snapshot(root, fan_in_ready=False,
                  children=[_blocked_child(child, reason="blocked-cli")]),
    )

    out = kc.run_slash("reengage --target-id orch-b5")
    assert f"triaged {root}" in out

    # Re-run drains nothing → empty.
    out2 = kc.run_slash("reengage --target-id orch-b5")
    assert "(no roots re-engaged)" in out2


def test_cli_reengage_json_includes_trigger(kanban_home):
    """CLI --json: triage result carries trigger=blocked alongside root_id +
    comment_id; fan-in carries trigger=fan_in."""
    root_b = _create("root blocked")
    child = _create("child A")
    _add_orchestrator_notice(
        "orch-b6", root_b,
        _snapshot(root_b, fan_in_ready=False,
                  children=[_blocked_child(child, reason="q")]),
    )
    out = kc.run_slash("reengage --target-id orch-b6 --json")
    data = json.loads(out)
    assert len(data) == 1
    assert data[0]["root_id"] == root_b
    assert isinstance(data[0]["comment_id"], int)
    assert data[0]["trigger"] == "blocked"

    root_f = _create("root fanin")
    _add_orchestrator_notice("orch-b7", root_f,
                             _snapshot(root_f, fan_in_ready=True))
    out_f = kc.run_slash("reengage --target-id orch-b7 --json")
    data_f = json.loads(out_f)
    assert len(data_f) == 1
    assert data_f[0]["trigger"] == "fan_in"
