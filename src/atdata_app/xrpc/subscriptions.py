"""WebSocket subscription endpoints for real-time change streaming."""

from __future__ import annotations

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
    sub_id, queue = change_stream.subscribe()

    try:
        # Replay buffered events if cursor provided
        if cursor_param is not None:
            try:
                cursor = int(cursor_param)
            except (ValueError, TypeError):
                await websocket.close(code=1008, reason="Invalid cursor value")
                return
            missed = change_stream.replay_from(cursor)
            for event in missed:
                await websocket.send_text(json.dumps(event.to_dict()))

        # Stream live events
        while True:
            event = await queue.get()
            await websocket.send_text(json.dumps(event.to_dict()))

    except WebSocketDisconnect:
        logger.debug("Subscriber %d disconnected", sub_id)
    except Exception:
        logger.exception("Error in subscriber %d", sub_id)
    finally:
        change_stream.unsubscribe(sub_id)
