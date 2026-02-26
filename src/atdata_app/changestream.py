"""In-memory broadcast channel for real-time change events.

Provides a pub/sub mechanism that the ingestion processor publishes to
and WebSocket subscribers consume from. Maintains a bounded buffer of
recent events for cursor-based replay.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_BUFFER_SIZE = 1000
DEFAULT_SUBSCRIBER_QUEUE_SIZE = 256
DEFAULT_MAX_SUBSCRIBERS = 1000


@dataclass
class ChangeEvent:
    """A single change event in the stream."""

    seq: int
    type: str  # "create", "update", or "delete"
    collection: str
    did: str
    rkey: str
    timestamp: str
    record: dict[str, Any] | None = None
    cid: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "seq": self.seq,
            "type": self.type,
            "collection": self.collection,
            "did": self.did,
            "rkey": self.rkey,
            "timestamp": self.timestamp,
        }
        if self.record is not None:
            d["record"] = self.record
        if self.cid is not None:
            d["cid"] = self.cid
        return d


@dataclass
class ChangeStream:
    """Broadcast channel with bounded replay buffer.

    Thread-safe for asyncio: all mutations happen in the event loop.
    """

    buffer_size: int = DEFAULT_BUFFER_SIZE
    subscriber_queue_size: int = DEFAULT_SUBSCRIBER_QUEUE_SIZE
    max_subscribers: int = DEFAULT_MAX_SUBSCRIBERS
    _seq: int = field(default=0, init=False)
    _buffer: deque[ChangeEvent] = field(init=False)
    _subscribers: dict[int, asyncio.Queue[ChangeEvent]] = field(
        default_factory=dict, init=False
    )
    _dropped_subs: set[int] = field(default_factory=set, init=False)
    _next_sub_id: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        self._buffer = deque(maxlen=self.buffer_size)

    def publish(self, event: ChangeEvent) -> None:
        """Publish an event to all subscribers and the replay buffer.

        Non-blocking. If a subscriber's queue is full, the subscriber is
        marked as dropped so the WebSocket handler can close the connection.
        """
        self._seq += 1
        event.seq = self._seq
        self._buffer.append(event)

        for sub_id, queue in list(self._subscribers.items()):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning(
                    "Subscriber %d queue full at seq=%d — marking for disconnect",
                    sub_id,
                    event.seq,
                )
                self._dropped_subs.add(sub_id)

    def subscribe(self) -> tuple[int, asyncio.Queue[ChangeEvent]]:
        """Create a new subscriber. Returns (subscriber_id, queue).

        Raises ``RuntimeError`` if the maximum subscriber count is reached.
        """
        if len(self._subscribers) >= self.max_subscribers:
            raise RuntimeError(
                f"Maximum subscriber count ({self.max_subscribers}) reached"
            )
        sub_id = self._next_sub_id
        self._next_sub_id += 1
        queue: asyncio.Queue[ChangeEvent] = asyncio.Queue(
            maxsize=self.subscriber_queue_size
        )
        self._subscribers[sub_id] = queue
        logger.debug("Subscriber %d connected (total: %d)", sub_id, len(self._subscribers))
        return sub_id, queue

    def unsubscribe(self, sub_id: int) -> None:
        """Remove a subscriber."""
        self._subscribers.pop(sub_id, None)
        self._dropped_subs.discard(sub_id)
        logger.debug("Subscriber %d disconnected (total: %d)", sub_id, len(self._subscribers))

    def is_dropped(self, sub_id: int) -> bool:
        """Return True if the subscriber was dropped due to backpressure."""
        return sub_id in self._dropped_subs

    def replay_from(self, cursor: int) -> list[ChangeEvent]:
        """Return buffered events with seq > cursor.

        Returns an empty list if the cursor is outside the buffer window.
        """
        if not self._buffer:
            return []

        oldest_seq = self._buffer[0].seq
        if cursor < oldest_seq - 1:
            # Cursor is too old — events between cursor and buffer start were lost
            return []

        return [ev for ev in self._buffer if ev.seq > cursor]

    @property
    def current_seq(self) -> int:
        return self._seq

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)


def make_change_event(
    *,
    event_type: str,
    collection: str,
    did: str,
    rkey: str,
    record: dict[str, Any] | None = None,
    cid: str | None = None,
) -> ChangeEvent:
    """Factory for creating change events with current timestamp."""
    from datetime import datetime, timezone

    return ChangeEvent(
        seq=0,  # Assigned by ChangeStream.publish()
        type=event_type,
        collection=collection,
        did=did,
        rkey=rkey,
        timestamp=datetime.now(timezone.utc).isoformat(),
        record=record,
        cid=cid,
    )
