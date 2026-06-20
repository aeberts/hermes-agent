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


# Registry keyed by ``subscriber_kind``. Production ships exactly one adapter
# (``gateway``); F05/F06 register CLI/TUI adapters here without touching the
# watcher's dispatch logic. Legacy rows have NULL ``subscriber_kind`` — the
# watcher normalizes that to ``'gateway'`` before lookup.
DELIVERY_ADAPTERS: dict[str, GatewayDeliveryAdapter] = {
    "gateway": GatewayDeliveryAdapter(),
}


def get_delivery_adapter(subscriber_kind: Optional[str]):
    """Return the registered adapter for ``subscriber_kind`` (NULL → gateway).

    Unknown/unregistered kinds return ``None`` so the watcher can skip them
    with a debug log — same posture as an unconnected platform adapter.
    """
    kind = (subscriber_kind or "gateway").lower()
    return DELIVERY_ADAPTERS.get(kind)
