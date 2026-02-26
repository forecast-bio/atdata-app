"""WebSocket subscription endpoints for real-time change streaming."""

from __future__ import annotations

import asyncio
import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from atdata_app.changestream import ChangeStream

logger = logging.getLogger(__name__)

router = APIRouter()


@router.websocket("/science.alt.dataset.subscribeChanges")
async def subscribe_changes(websocket: WebSocket) -> None:
    """Stream real-time change events over WebSocket.

    Query parameters:
        cursor: Optional sequence number to replay from.
    """
    change_stream: ChangeStream = websocket.app.state.change_stream

    await websocket.accept()

    cursor_param = websocket.query_params.get("cursor")

    try:
        sub_id, queue = change_stream.subscribe()
    except RuntimeError:
        await websocket.close(code=1013, reason="Too many subscribers")
        return

    try:
        # Replay buffered events if cursor provided, tracking last seq
        # to deduplicate against events that also landed in the live queue.
        last_replayed_seq = 0
        if cursor_param is not None:
            try:
                cursor = int(cursor_param)
            except (ValueError, TypeError):
                await websocket.close(code=1008, reason="Invalid cursor value")
                return
            missed = change_stream.replay_from(cursor)
            for event in missed:
                await websocket.send_text(json.dumps(event.to_dict()))
                last_replayed_seq = event.seq

        # Stream live events with periodic keepalive on idle
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=30.0)
            except asyncio.TimeoutError:
                # No events for 30s — send keepalive to detect dead connections
                await websocket.send_text(json.dumps({"type": "keepalive"}))
                continue

            # Deduplicate events already sent during replay
            if event.seq <= last_replayed_seq:
                continue
            await websocket.send_text(json.dumps(event.to_dict()))
            # Check if we were marked as dropped due to backpressure
            if change_stream.is_dropped(sub_id):
                await websocket.close(
                    code=4000, reason="Backpressure: events were dropped"
                )
                return

    except WebSocketDisconnect:
        logger.debug("Subscriber %d disconnected", sub_id)
    except Exception:
        logger.exception("Error in subscriber %d", sub_id)
    finally:
        change_stream.unsubscribe(sub_id)
