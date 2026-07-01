"""Tests for the orchestrator supervision delivery adapter.

This is the third non-gateway delivery adapter, after the CLI and TUI adapters.
A ``subscriber_kind='orchestrator'`` subscription is created with
``scope='subtree'`` + ``delivery_policy='supervise'``; on delivery the adapter
observes the subscribed (root) task's dependency parents (its subtasks) — the
direction real decomposition wires (the root is linked *under* every subtask) —
claims their terminal/blocker events (its own subtree claim), computes fan-in in
code, and persists ONE structured supervision notice (an aggregate snapshot of
the whole subtask set, claim-once). These tests prove:

- subscribe orchestrator(subtree) → complete child A + block child B → one
  supervision notice; payload ``children[]`` shows both with correct
  kind/status/summary|reason; ``fan_in_ready`` is False while a child is active;
- completing the LAST child flips ``fan_in_ready`` true exactly once; re-claim
  yields nothing (cursor dedup);
- non-terminal child events are ignored; a parent with zero children doesn't
  crash and writes no notice;
- ``artifacts`` from a completion event surface in the snapshot;
- ``kanban supervise --target-id … --json`` drains + clears and prints payload;
- supervision is observational: it does not create tasks or change promotion;
- the gateway/cli/tui adapters still register and the orchestrator adapter is
  registered.

Delivery is driven directly (adapter.deliver) — no running
gateway, no network, no Platform adapter.
"""

import asyncio
import json
import re

from pathlib import Path

import pytest

from hermes_cli import kanban as kc
from hermes_cli import kanban_db as kb

from gateway.kanban_delivery import (
    CLIDeliveryAdapter,
    OrchestratorDeliveryAdapter,
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


def _create(title: str, *, assignee: str = "worker1") -> str:
    out = kc.run_slash(f"create '{title}' --assignee {assignee}")
    return re.search(r"(t_[a-f0-9]+)", out).group(1)


def _link(conn, root_id: str, subtask_id: str) -> None:
    """Wire a supervision subtree edge the way real decomposition does.

    Per ``decompose_triage_task`` the root is linked *under* every subtask
    (``parent_id = subtask, child_id = root``), so the root is the dependency
    *child* and its subtasks are its dependency *parents*. With this direction
    the subtasks are NOT gated by the unfinished root, so a test can create the
    subtask and drive it terminal directly — no unlink/relink gymnastics. The
    orchestrator adapter then observes the root's ``task_links`` parents.
    """
    kb.link_tasks(conn, subtask_id, root_id)


def _subscribe_orchestrator(parent_id: str, target_id: str) -> dict:
    """Subscribe an orchestrator(subtree, supervise) via the CLI path."""
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
    sub = subs[0]
    assert sub["scope"] == "subtree"
    assert sub["delivery_policy"] == "supervise"
    return sub


def _deliver(sub: dict):
    """Drive the orchestrator adapter directly (it does its own subtree claim)."""
    delivery = {"sub": sub, "board": None}
    adapter = get_delivery_adapter(sub["subscriber_kind"])
    return asyncio.run(adapter.deliver(None, delivery))


def _complete(conn, task_id: str, **kw) -> None:
    assert kb.complete_task(conn, task_id, **kw) is True


def _block(conn, task_id: str, **kw) -> None:
    assert kb.block_task(conn, task_id, **kw) is True


def _drain(target_id: str) -> list[dict]:
    conn = kb.connect()
    try:
        return kb.drain_notices(
            conn, subscriber_kind="orchestrator", target_id=target_id,
        )
    finally:
        conn.close()


def test_orchestrator_adapter_registered(kanban_home):
    assert isinstance(get_delivery_adapter("orchestrator"), OrchestratorDeliveryAdapter)


def test_complete_and_block_yields_one_snapshot_notice(kanban_home):
    """Complete child A + block child B → one notice; children[] shows both."""
    parent = _create("root goal")
    child_a = _create("child A")
    child_b = _create("child B")
    sub = _subscribe_orchestrator(parent, "orch-1")

    conn = kb.connect()
    try:
        # Drive each subtask terminal while independent, then attach it under
        # the orchestrator parent (the supervision subtree edge).
        _complete(conn, child_a, summary="A finished cleanly")
        _block(conn, child_b, reason="missing creds")
        _link(conn, parent, child_a)
        _link(conn, parent, child_b)
    finally:
        conn.close()

    result = _deliver(sub)
    assert result.ok is True

    notices = _drain("orch-1")
    assert len(notices) == 1, "exactly one supervision notice (snapshot, not per-child)"
    n = notices[0]
    assert n["task_id"] == parent
    assert n["kind"] == "supervision"
    payload = json.loads(n["payload"])
    assert payload["schema"] == 1
    assert payload["parent_id"] == parent
    # One child still blocked → not all-terminal → not fan-in ready.
    assert payload["fan_in_ready"] is False

    by_id = {c["task_id"]: c for c in payload["children"]}
    assert set(by_id) == {child_a, child_b}
    assert by_id[child_a]["kind"] == "completed"
    assert by_id[child_a]["status"] == "done"
    assert by_id[child_a]["summary"] == "A finished cleanly"
    assert by_id[child_b]["kind"] == "blocked"
    assert by_id[child_b]["status"] == "blocked"
    assert by_id[child_b]["reason"] == "missing creds"

    # message line stays human-readable.
    assert parent in n["message"]


def test_fan_in_ready_once_then_cursor_dedups(kanban_home):
    """Completing the last child flips fan_in_ready true; re-claim → nothing."""
    parent = _create("root")
    child_a = _create("child A")
    child_b = _create("child B")
    sub = _subscribe_orchestrator(parent, "orch-2")

    # child A terminal + linked; child B still active + linked (so the subtree
    # has an outstanding subtask → fan-in not yet ready).
    conn = kb.connect()
    try:
        _complete(conn, child_a, summary="a done")
        _link(conn, parent, child_a)
        _link(conn, parent, child_b)
    finally:
        conn.close()

    _deliver(sub)
    first = _drain("orch-2")
    assert len(first) == 1
    assert json.loads(first[0]["payload"])["fan_in_ready"] is False

    # Finish the last outstanding subtask. With the real link direction the
    # subtask is the dependency *parent* of the root, so it is not gated by the
    # unfinished root and can be driven terminal directly — no unlink/relink.
    conn = kb.connect()
    try:
        _complete(conn, child_b, summary="b done")
    finally:
        conn.close()

    _deliver(sub)
    second = _drain("orch-2")
    assert len(second) == 1
    assert json.loads(second[0]["payload"])["fan_in_ready"] is True

    # Re-claim with no new child events → no notice (cursor dedup).
    _deliver(sub)
    assert _drain("orch-2") == [], "no duplicate notice on re-claim"


def test_root_completion_snapshot_carries_root_status_and_goal_complete(kanban_home):
    """The root's OWN completion fires a final notice tagged root_status=done.

    The subtree closure includes the root node, so when the root itself reaches
    `done` a last supervision notice is written. Its children + fan_in_ready are
    identical to the earlier fan-in notice, so the snapshot must carry
    ``root_status`` (and the message say "goal complete") for the live-wake
    debounce to tell the two apart and surface the final wake.
    """
    parent = _create("root")
    child = _create("child")
    sub = _subscribe_orchestrator(parent, "orch-rootdone")

    conn = kb.connect()
    try:
        _link(conn, parent, child)
        _complete(conn, child, summary="child done")
    finally:
        conn.close()

    # fan-in notice: root still pending, not yet terminal.
    _deliver(sub)
    fan_in = _drain("orch-rootdone")
    assert len(fan_in) == 1
    fi_payload = json.loads(fan_in[0]["payload"])
    assert fi_payload["fan_in_ready"] is True
    assert fi_payload["root_status"] not in ("done", "archived")
    assert "goal complete" not in fan_in[0]["message"]

    # Now drive the ROOT itself terminal (its subtask is done, so it's completable).
    conn = kb.connect()
    try:
        _complete(conn, parent, summary="goal brief")
    finally:
        conn.close()

    _deliver(sub)
    done = _drain("orch-rootdone")
    assert len(done) == 1, "root's own completion fires a final supervision notice"
    payload = json.loads(done[0]["payload"])
    assert payload["root_status"] == "done"
    assert payload["fan_in_ready"] is True  # children unchanged → identical but-for root_status
    assert "goal complete" in done[0]["message"]


def test_non_terminal_events_ignored(kanban_home):
    """A non-terminal child event alone produces no supervision notice."""
    parent = _create("root")
    child = _create("child")
    conn = kb.connect()
    try:
        _link(conn, parent, child)
        with kb.write_txn(conn):
            kb._append_event(conn, child, kind="heartbeat")
    finally:
        conn.close()
    sub = _subscribe_orchestrator(parent, "orch-3")

    result = _deliver(sub)
    assert result.ok is True
    assert _drain("orch-3") == [], "non-terminal child events claim nothing"


def test_parent_with_zero_children_does_not_crash(kanban_home):
    """A childless parent subscription delivers cleanly and writes no notice."""
    parent = _create("lonely root")
    sub = _subscribe_orchestrator(parent, "orch-4")

    result = _deliver(sub)
    assert result.ok is True
    assert _drain("orch-4") == []


def test_artifacts_surface_in_snapshot(kanban_home):
    """artifacts promoted onto a completion event appear in the child row."""
    parent = _create("root")
    child = _create("child")
    sub = _subscribe_orchestrator(parent, "orch-5")

    conn = kb.connect()
    try:
        _complete(
            conn, child, summary="produced rows",
            metadata={"artifacts": ["/work/child/rows.csv"]},
        )
        _link(conn, parent, child)
    finally:
        conn.close()

    _deliver(sub)
    notices = _drain("orch-5")
    assert len(notices) == 1
    payload = json.loads(notices[0]["payload"])
    child_row = payload["children"][0]
    assert child_row["artifacts"] == ["/work/child/rows.csv"]
    assert payload["fan_in_ready"] is True


def test_supervise_command_drains_and_prints_payload(kanban_home):
    """`kanban supervise --target-id … --json` drains + clears, prints payload."""
    parent = _create("root")
    child = _create("child")
    sub = _subscribe_orchestrator(parent, "orch-6")

    conn = kb.connect()
    try:
        _complete(conn, child, summary="shipped it")
        _link(conn, parent, child)
    finally:
        conn.close()
    _deliver(sub)

    out = kc.run_slash("supervise --target-id orch-6 --json")
    data = json.loads(out)
    assert len(data) == 1
    assert data[0]["parent_id"] == parent
    assert data[0]["fan_in_ready"] is True
    assert data[0]["children"][0]["summary"] == "shipped it"

    # Drained: a second invocation shows nothing.
    out2 = kc.run_slash("supervise --target-id orch-6 --json")
    assert json.loads(out2) == []


def test_supervise_command_plain_prints_message(kanban_home):
    """Plain `kanban supervise` prints the human-readable message line."""
    parent = _create("root")
    child = _create("child")
    sub = _subscribe_orchestrator(parent, "orch-7")
    conn = kb.connect()
    try:
        _complete(conn, child, summary="done")
        _link(conn, parent, child)
    finally:
        conn.close()
    _deliver(sub)

    out = kc.run_slash("supervise --target-id orch-7")
    assert parent in out
    assert "(no supervision notices)" not in out

    out2 = kc.run_slash("supervise --target-id orch-7")
    assert "(no supervision notices)" in out2


def test_observational_only_no_task_creation_or_promotion_change(kanban_home):
    """Subscribing + draining changes neither task count nor promotion state."""
    parent = _create("root")
    child_a = _create("child A")
    child_b = _create("child B")
    sub = _subscribe_orchestrator(parent, "orch-8")

    conn = kb.connect()
    try:
        before = len(kb.list_tasks(conn, include_archived=True))
        _complete(conn, child_a, summary="a")
        _complete(conn, child_b, summary="b")
        _link(conn, parent, child_a)
        _link(conn, parent, child_b)
        # All children done → recompute_ready (the EXISTING scheduler) is what
        # owns promotion. Supervision must neither add to nor duplicate it.
        kb.recompute_ready(conn)
        parent_status_after_complete = kb.get_task(conn, parent).status
    finally:
        conn.close()

    _deliver(sub)
    _drain("orch-8")

    conn = kb.connect()
    try:
        after = len(kb.list_tasks(conn, include_archived=True))
        parent_status_after_deliver = kb.get_task(conn, parent).status
    finally:
        conn.close()

    assert after == before, "supervision created no tasks"
    assert parent_status_after_deliver == parent_status_after_complete, (
        "supervision did not change promotion state"
    )


def test_real_decompose_fan_out_is_observed(kanban_home):
    """The bug-catching test: drive the REAL decompose fan-out, not a hand-built inverse.

    Create a triage task, decompose it into two children via
    ``decompose_triage_task`` (which links the root *under* every child), then
    subscribe orchestrator(subtree) to the root. The snapshot must list exactly
    the decomposed subtasks, and ``fan_in_ready`` must flip true only once every
    subtask is done — proving the adapter observes the real fan-out mechanism rather than
    the inverted topology the old tests hand-built.
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

    sub = _subscribe_orchestrator(root, "orch-decompose")

    # The root's task_links parents are exactly the decomposed subtasks.
    conn = kb.connect()
    try:
        assert set(kb.parent_ids(conn, root)) == {sub_a, sub_b}
        _complete(conn, sub_a, summary="A done")
    finally:
        conn.close()

    _deliver(sub)
    first = _drain("orch-decompose")
    assert len(first) == 1
    payload = json.loads(first[0]["payload"])
    assert payload["parent_id"] == root
    assert {c["task_id"] for c in payload["children"]} == {sub_a, sub_b}
    # One subtask still outstanding → not fan-in ready.
    assert payload["fan_in_ready"] is False

    # Finish the last subtask → fan-in flips true exactly once.
    conn = kb.connect()
    try:
        _complete(conn, sub_b, summary="B done")
    finally:
        conn.close()

    _deliver(sub)
    second = _drain("orch-decompose")
    assert len(second) == 1
    assert json.loads(second[0]["payload"])["fan_in_ready"] is True


def test_cli_and_tui_adapters_still_registered(kanban_home):
    """Regression: the other non-gateway adapters still register cleanly."""
    assert isinstance(get_delivery_adapter("cli"), CLIDeliveryAdapter)
    assert isinstance(get_delivery_adapter("tui"), TUIDeliveryAdapter)
