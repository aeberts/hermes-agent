"""Delivery-adapter registry for kanban terminal-event notifications.

The kanban notifier watcher (``gateway/kanban_watchers.py``) used to be the
*only* delivery path: it polled ``kanban_notify_subs``, claimed unseen terminal
events, and sent them straight through ``self.adapters[platform]``. The event
substrate (#49190) generalizes that into a small registry keyed by
``subscriber_kind`` so any surface — gateway today, CLI/TUI later (F05/F06) —
can consume the same claimed events through one shared dispatch path.

F03 introduces the registry and re-homes the existing gateway send logic as the
first registered adapter (``subscriber_kind='gateway'``). Behavior for gateway
subscriptions (all current rows) is byte-for-byte identical to the inlined
loop it replaced — the watcher still owns claiming, the per-sub failure
accounting / dead-channel drop, the keep-sub-until-final-status rule, the
multi-board fan-out, ``notifier_profile`` gating, and the ``asyncio.to_thread``
DB offload. ONLY the "send each claimed event" step moves behind this
interface.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

logger = logging.getLogger("gateway.run")


class DeliveryResult:
    """Outcome of an adapter delivering one claimed batch for one subscription.

    ``ok`` is True only when *every* event in the batch was delivered. On the
    first failed event the adapter stops (mirroring the original break-on-send-
    failure behavior) and reports ``ok=False`` so the watcher can run the
    per-sub failure accounting (increment counter, drop after N, else rewind).
    """

    __slots__ = ("ok",)

    def __init__(self, ok: bool) -> None:
        self.ok = ok


class GatewayDeliveryAdapter:
    """Deliver claimed terminal events to a gateway chat (``subscriber_kind='gateway'``).

    Wraps the original ``self.adapters[platform].send(...)`` send path verbatim:
    same message formatting per kind, same artifact upload on ``completed``,
    same stop-on-first-failure. It does NOT touch the cursor or the failure
    counter — those stay in the watcher so the existing safety semantics are
    unchanged.
    """

    async def deliver(self, runner, delivery: dict) -> DeliveryResult:
        sub = delivery["sub"]
        task = delivery["task"]
        board_slug = delivery.get("board")
        platform_str = (sub["platform"] or "").lower()
        adapter = delivery["adapter"]
        title = (task.title if task else sub["task_id"])[:120]

        for ev in delivery["events"]:
            kind = ev.kind
            # Identity prefix: attribute terminal pings to the worker that did
            # the work. Makes fleets (where one chat subscribes to many tasks)
            # legible at a glance.
            who = (task.assignee if task and task.assignee else None)
            tag = f"@{who} " if who else ""
            if kind == "completed":
                # Prefer the run's summary (the worker's intentional
                # human-facing handoff, carried in the event payload), then
                # fall back to task.result for legacy rows written before runs
                # shipped.
                handoff = ""
                payload_summary = None
                if ev.payload and ev.payload.get("summary"):
                    payload_summary = str(ev.payload["summary"])
                if payload_summary:
                    lines = payload_summary.strip().splitlines()
                    h = lines[0][:200] if lines else payload_summary[:200]
                    handoff = f"\n{h}"
                elif task and task.result:
                    lines = task.result.strip().splitlines()
                    r = lines[0][:160] if lines else task.result[:160]
                    handoff = f"\n{r}"
                msg = (
                    f"✔ {tag}Kanban {sub['task_id']} done"
                    f" — {title}{handoff}"
                )
            elif kind == "blocked":
                reason = ""
                if ev.payload and ev.payload.get("reason"):
                    reason = f": {str(ev.payload['reason'])[:160]}"
                msg = f"⏸ {tag}Kanban {sub['task_id']} blocked{reason}"
            elif kind == "gave_up":
                err = ""
                if ev.payload and ev.payload.get("error"):
                    err = f"\n{str(ev.payload['error'])[:200]}"
                msg = (
                    f"✖ {tag}Kanban {sub['task_id']} gave up "
                    f"after repeated spawn failures{err}"
                )
            elif kind == "crashed":
                msg = (
                    f"✖ {tag}Kanban {sub['task_id']} worker crashed "
                    f"(pid gone); dispatcher will retry"
                )
            elif kind == "timed_out":
                limit = 0
                if ev.payload and ev.payload.get("limit_seconds"):
                    limit = int(ev.payload["limit_seconds"])
                msg = (
                    f"⏱ {tag}Kanban {sub['task_id']} timed out "
                    f"(max_runtime={limit}s); will retry"
                )
            else:
                continue
            metadata: dict[str, Any] = {}
            if sub.get("thread_id"):
                metadata["thread_id"] = sub["thread_id"]
            try:
                await adapter.send(
                    sub["chat_id"], msg, metadata=metadata,
                )
                logger.debug(
                    "kanban notifier: delivered %s event for %s to %s/%s on board %s",
                    kind, sub["task_id"], platform_str, sub["chat_id"], board_slug,
                )
                # After delivering the text notification, surface any artifact
                # paths the worker referenced in ``kanban_complete(summary=...,
                # artifacts=[...])`` (or the legacy ``result`` field) as native
                # uploads. ``extract_local_files`` finds bare absolute paths in
                # the summary; ``send_document`` / ``send_image_file`` uploads
                # them. Only fires on the ``completed`` event so we never spam
                # attachments on retries.
                if kind == "completed":
                    try:
                        await runner._deliver_kanban_artifacts(
                            adapter=adapter,
                            chat_id=sub["chat_id"],
                            metadata=metadata,
                            event_payload=getattr(ev, "payload", None),
                            task=task,
                        )
                    except Exception as art_exc:
                        logger.debug(
                            "kanban notifier: artifact delivery for %s failed: %s",
                            sub["task_id"], art_exc,
                        )
            except Exception as exc:
                logger.warning(
                    "kanban notifier: send failed for %s on %s: %s",
                    sub["task_id"], platform_str, exc,
                )
                return DeliveryResult(ok=False)
        return DeliveryResult(ok=True)


def _format_notice(sub: dict, task, ev) -> Optional[str]:
    """Render one terminal event as a plain-text notice line.

    Mirrors the gateway adapter's per-kind phrasing (minus emoji/metadata) so a
    non-gateway subscriber (CLI/TUI) reads the same handoff the gateway would
    have pushed. The phrasing is surface-agnostic, so CLI (F05) and TUI (F06)
    share it. Returns ``None`` for non-terminal kinds so the caller skips them.
    """
    kind = ev.kind
    title = (task.title if task else sub["task_id"])[:120]
    who = (task.assignee if task and task.assignee else None)
    tag = f"@{who} " if who else ""
    if kind == "completed":
        handoff = ""
        payload_summary = None
        if ev.payload and ev.payload.get("summary"):
            payload_summary = str(ev.payload["summary"])
        if payload_summary:
            lines = payload_summary.strip().splitlines()
            h = lines[0][:200] if lines else payload_summary[:200]
            handoff = f"\n{h}"
        elif task and task.result:
            lines = task.result.strip().splitlines()
            r = lines[0][:160] if lines else task.result[:160]
            handoff = f"\n{r}"
        return f"{tag}Kanban {sub['task_id']} done — {title}{handoff}"
    if kind == "blocked":
        reason = ""
        if ev.payload and ev.payload.get("reason"):
            reason = f": {str(ev.payload['reason'])[:160]}"
        return f"{tag}Kanban {sub['task_id']} blocked{reason}"
    if kind == "gave_up":
        err = ""
        if ev.payload and ev.payload.get("error"):
            err = f"\n{str(ev.payload['error'])[:200]}"
        return (
            f"{tag}Kanban {sub['task_id']} gave up "
            f"after repeated spawn failures{err}"
        )
    if kind == "crashed":
        return (
            f"{tag}Kanban {sub['task_id']} worker crashed "
            f"(pid gone); dispatcher will retry"
        )
    if kind == "timed_out":
        limit = 0
        if ev.payload and ev.payload.get("limit_seconds"):
            limit = int(ev.payload["limit_seconds"])
        return (
            f"{tag}Kanban {sub['task_id']} timed out "
            f"(max_runtime={limit}s); will retry"
        )
    return None


def _target_id(sub: dict) -> str:
    """Resolve the target id for a non-gateway subscription (event-hub F04 convention).

    F04 persists a non-gateway subscription with ``chat_id=<target-id>`` and a
    ``target`` JSON carrying ``{"subscriber_kind": <kind>, "target_id": ...}``.
    The convention is the same for cli and tui, so this is surface-agnostic:
    prefer the structured ``target`` id, fall back to ``chat_id``.
    """
    raw = sub.get("target")
    if raw:
        try:
            data = json.loads(raw)
        except (TypeError, ValueError):
            data = None
        if isinstance(data, dict) and data.get("target_id"):
            return str(data["target_id"])
    return str(sub.get("chat_id") or "")


class _NoticeDeliveryAdapter:
    """Base for non-gateway adapters that persist a notice instead of pushing.

    CLI (F05) and TUI (F06) have no live push channel, so "delivery" persists a
    plain-text notice to the shared, surface-agnostic ``kanban_notices`` table
    keyed by ``subscriber_kind`` + target id; ``hermes kanban notices`` drains
    it. No gateway, no network, no ``Platform`` adapter — the watcher sets
    ``delivery['adapter']`` to ``None`` for non-gateway kinds. Dedup is owned by
    the shared claim cursor: a re-claim returns no events, so no duplicate
    notice is written. Subclasses differ only by ``_kind`` (the literal
    ``subscriber_kind`` stamped on each notice row).
    """

    _kind: str = ""

    async def deliver(self, runner, delivery: dict) -> DeliveryResult:
        from hermes_cli import kanban_db as _kb

        sub = delivery["sub"]
        task = delivery["task"]
        board_slug = delivery.get("board")
        target_id = _target_id(sub)

        def _persist() -> None:
            conn = _kb.connect(board=board_slug)
            try:
                for ev in delivery["events"]:
                    message = _format_notice(sub, task, ev)
                    if message is None:
                        continue
                    _kb.add_notice(
                        conn,
                        subscriber_kind=self._kind,
                        target_id=target_id,
                        task_id=sub["task_id"],
                        kind=ev.kind,
                        message=message,
                    )
            finally:
                conn.close()

        try:
            import asyncio

            await asyncio.to_thread(_persist)
        except Exception as exc:
            logger.warning(
                "kanban notifier: %s notice persist failed for %s: %s",
                self._kind, sub.get("task_id"), exc,
            )
            return DeliveryResult(ok=False)
        logger.debug(
            "kanban notifier: queued %s notice(s) for %s to target %s on board %s",
            self._kind, sub["task_id"], target_id, board_slug,
        )
        return DeliveryResult(ok=True)


def _truncate_line(text: Optional[str], limit: int = 200) -> Optional[str]:
    """First non-empty line of ``text``, truncated to ``limit`` chars.

    Mirrors the size guard ``_format_notice`` already applies (OQ5) so an inline
    ``summary``/``reason`` in the supervision payload can't balloon a notice row.
    """
    if not text:
        return None
    s = str(text).strip()
    if not s:
        return None
    line = s.splitlines()[0]
    if len(line) <= limit:
        return line
    # Break on the last word boundary inside the budget (falling back to a hard
    # cut for a single over-long token) and mark the elision, so a notice never
    # ends mid-word like "…SMS can be mob".
    head = line[:limit]
    cut = head.rfind(" ")
    if cut >= limit // 2:
        head = head[:cut]
    return head.rstrip() + "…"


class OrchestratorDeliveryAdapter:
    """Deliver a root task's subtask terminal/blocker events to an orchestrator.

    The third non-gateway adapter (event-hub F07), after cli (F05) and tui
    (F06). A ``subscriber_kind='orchestrator'`` subscription is created with
    ``scope='subtree'`` + ``delivery_policy='supervise'``; on delivery this
    adapter observes the subscribed (root) task's dependency parents (its
    subtasks). NOTE on terminology: the colloquial "child task / subtask" is the
    root's ``task_links`` **parent** in hermes' dependency model — decompose
    links the root *under* every subtask, so the root waits for its subtasks. On
    delivery this adapter:

    1. **Claims** its subtasks' terminal+blocker events via
       :func:`kanban_db.claim_unseen_subtree_events_for_sub` — claim-once, the
       cursor is the dedup (a re-claim returns nothing, so no duplicate notice).
       It does its **own** subtree claim rather than consuming the watcher's
       per-task claim, so the watcher needs no orchestrator-specific branch and
       gateway/cli/tui dispatch stays byte-for-byte unchanged.
    2. **Computes fan-in in code** (OQ4) — all subtasks terminal — via
       :func:`kanban_db.subtree_fan_in_ready`; no new ``task_events`` kind.
    3. **Persists ONE** supervision notice per delivery (not one row per subtask
       — OQ3): a human-readable ``message`` line for cli/tui parity plus a
       structured aggregate-snapshot ``payload`` JSON (OQ5) the orchestrator /
       F09 re-engagement loop reads to judge the whole subtask set in one turn.

    Observational only: it never creates tasks, judges completion, or changes
    scheduling/promotion (that stays with ``recompute_ready``); re-engagement
    (waking the orchestrator) is F09, live wake is M01.
    """

    _kind = "orchestrator"

    async def deliver(self, runner, delivery: dict) -> DeliveryResult:
        sub = delivery["sub"]
        board_slug = delivery.get("board")
        target_id = _target_id(sub)
        root_id = sub["task_id"]

        def _persist() -> Optional[bool]:
            from hermes_cli import kanban_db as _kb

            conn = _kb.connect(board=board_slug)
            try:
                _old, _new, events = _kb.claim_unseen_subtree_events_for_sub(
                    conn, task_id=root_id, platform=sub["platform"],
                    chat_id=sub["chat_id"], thread_id=sub.get("thread_id") or "",
                )
                if not events:
                    return None  # nothing newly claimed → no notice (dedup)
                snapshot = self._build_snapshot(conn, root_id, board_slug)
                message = self._build_message(snapshot)
                _kb.add_notice(
                    conn,
                    subscriber_kind=self._kind,
                    target_id=target_id,
                    task_id=root_id,
                    kind="supervision",
                    message=message,
                    payload=json.dumps(snapshot, ensure_ascii=False),
                )
                return True
            finally:
                conn.close()

        try:
            import asyncio

            wrote = await asyncio.to_thread(_persist)
        except Exception as exc:
            logger.warning(
                "kanban notifier: orchestrator notice persist failed for %s: %s",
                root_id, exc,
            )
            return DeliveryResult(ok=False)
        if wrote:
            logger.debug(
                "kanban notifier: queued orchestrator supervision notice for "
                "root %s to target %s on board %s",
                root_id, target_id, board_slug,
            )
        return DeliveryResult(ok=True)

    @staticmethod
    def _latest_terminal_event(conn, child_id: str):
        """Most recent terminal/blocker event for ``child_id`` (or None)."""
        from hermes_cli import kanban_db as _kb

        marks = ",".join("?" * len(_kb.ORCHESTRATOR_CLAIM_KINDS))
        row = conn.execute(
            "SELECT kind, payload FROM task_events "
            "WHERE task_id = ? AND kind IN (" + marks + ") "
            "ORDER BY id DESC LIMIT 1",
            (child_id, *_kb.ORCHESTRATOR_CLAIM_KINDS),
        ).fetchone()
        return row

    def _build_snapshot(self, conn, root_id: str, board_slug) -> dict:
        """Aggregate-snapshot payload over the root's whole subtask set (OQ5).

        Iterates the subscribed (root) task's dependency parents (its subtasks)
        — colloquial "child task / subtask" = the root's ``task_links`` parent
        in hermes' dependency model (the root waits for its subtasks). Carries
        one row per subtask (never per event — OQ3), each with
        ``{task_id, title, kind, status, assignee, summary|reason, artifacts}``,
        plus the ``fan_in_ready`` flag computed in code (OQ4). Inline one-liners
        (truncated ~200 chars) + ids + artifact paths only — never full bodies.
        """
        from hermes_cli import kanban_db as _kb

        children = _kb.parent_ids(conn, root_id)
        fan_in_ready = _kb.subtree_fan_in_ready(conn, root_id)
        rows: list[dict] = []
        for cid in children:
            task = _kb.get_task(conn, cid)
            ev = self._latest_terminal_event(conn, cid)
            kind = ev["kind"] if ev else None
            payload = {}
            if ev and ev["payload"]:
                try:
                    payload = json.loads(ev["payload"]) or {}
                except (TypeError, ValueError):
                    payload = {}
            child: dict[str, Any] = {
                "task_id": cid,
                "title": (task.title if task else None),
                "kind": kind,
                "status": (task.status if task else None),
                "assignee": (task.assignee if task else None),
            }
            # blocker → reason; everything else → summary (one-line, truncated).
            if kind == "blocked":
                child["reason"] = _truncate_line(payload.get("reason"))
            else:
                summary = payload.get("summary")
                if not summary and task is not None:
                    summary = task.result
                child["summary"] = _truncate_line(summary)
            artifacts = payload.get("artifacts")
            child["artifacts"] = (
                [str(a) for a in artifacts]
                if isinstance(artifacts, (list, tuple))
                else []
            )
            rows.append(child)
        return {
            "schema": 1,
            "parent_id": root_id,
            "board": board_slug or "default",
            "fan_in_ready": fan_in_ready,
            "children": rows,
        }

    @staticmethod
    def _build_message(snapshot: dict) -> str:
        """Human-readable parity line for the structured supervision snapshot."""
        children = snapshot.get("children", [])
        done = sum(1 for c in children if c.get("kind") == "completed")
        blocked = [c for c in children if c.get("kind") == "blocked"]
        parts = [f"Kanban {snapshot['parent_id']} supervision"]
        if snapshot.get("fan_in_ready"):
            parts.append("fan-in ready")
        counts = f"{done} done"
        if blocked:
            counts += f", {len(blocked)} blocked"
        parts.append(counts)
        msg = " — ".join(parts)
        if blocked:
            first = blocked[0]
            reason = first.get("reason")
            tail = f" — {first['task_id']}" + (f": {reason}" if reason else "")
            msg += tail
        return msg


class CLIDeliveryAdapter(_NoticeDeliveryAdapter):
    """Persist claimed terminal events as CLI notices (``subscriber_kind='cli'``)."""

    _kind = "cli"


class TUIDeliveryAdapter(_NoticeDeliveryAdapter):
    """Persist claimed terminal events as TUI notices (``subscriber_kind='tui'``).

    F06's whole proof: this is the *second* non-gateway surface, and it reaches
    the substrate purely by registering here. RFC §5.3 lists both ``tui`` and
    ``cli`` as "session notice", so TUI is notice-first exactly like CLI —
    live-turn wakeup/WS push is deferred to M01. No ``tui_gateway`` push, no
    event_publisher, no running server; just a durable notice in the shared
    store. The watcher already routes any non-gateway ``subscriber_kind`` to its
    registered adapter (F05 generalized the gating), so no watcher change was
    needed.
    """

    _kind = "tui"


# Registry keyed by ``subscriber_kind``. Production ships the ``gateway`` adapter
# (all current rows) plus the F05 ``cli`` adapter; F06 registers the ``tui``
# adapter here without touching the watcher's dispatch logic. Legacy rows have
# NULL ``subscriber_kind`` — the watcher normalizes that to ``'gateway'`` before
# lookup.
DELIVERY_ADAPTERS: dict[str, Any] = {
    "gateway": GatewayDeliveryAdapter(),
    "cli": CLIDeliveryAdapter(),
    "tui": TUIDeliveryAdapter(),
    "orchestrator": OrchestratorDeliveryAdapter(),
}


def get_delivery_adapter(subscriber_kind: Optional[str]):
    """Return the registered adapter for ``subscriber_kind`` (NULL → gateway).

    Unknown/unregistered kinds return ``None`` so the watcher can skip them
    with a debug log — same posture as an unconnected platform adapter.
    """
    kind = (subscriber_kind or "gateway").lower()
    return DELIVERY_ADAPTERS.get(kind)
