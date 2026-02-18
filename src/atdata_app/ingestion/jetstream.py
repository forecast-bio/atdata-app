"""Jetstream firehose consumer for real-time record ingestion."""

from __future__ import annotations

import asyncio
import json
import logging
import time

import websockets
from fastapi import FastAPI

from atdata_app.database import get_cursor, set_cursor
from atdata_app.ingestion.processor import process_commit

logger = logging.getLogger(__name__)

CURSOR_FLUSH_INTERVAL = 5.0  # seconds
CURSOR_FLUSH_COUNT = 100  # messages


def _build_url(config, cursor: int | None) -> str:
    url = f"{config.jetstream_url}?wantedCollections={config.jetstream_collections}"
    if cursor is not None:
        url += f"&cursor={cursor}"
    return url


async def jetstream_consumer(app: FastAPI) -> None:
    """Long-running task that consumes Jetstream and writes to the database."""
    pool = app.state.db_pool
    config = app.state.config
    backoff = 1.0

    last_time_us: int | None = None

    while True:
        try:
            cursor = await get_cursor(pool)
            url = _build_url(config, cursor)
            logger.info("Connecting to Jetstream: %s", url)

            async with websockets.connect(url) as ws:
                backoff = 1.0
                msg_count = 0
                last_flush = time.monotonic()
                last_time_us: int | None = None

                async for raw_msg in ws:
                    event = json.loads(raw_msg)

                    if event.get("kind") != "commit":
                        continue

                    await process_commit(pool, event)

                    last_time_us = event.get("time_us")
                    msg_count += 1

                    # Periodically persist cursor
                    now = time.monotonic()
                    if last_time_us and (
                        msg_count % CURSOR_FLUSH_COUNT == 0
                        or now - last_flush >= CURSOR_FLUSH_INTERVAL
                    ):
                        await set_cursor(pool, last_time_us)
                        last_flush = now

                # Connection closed normally — flush cursor and reconnect
                if last_time_us:
                    await set_cursor(pool, last_time_us)

        except asyncio.CancelledError:
            logger.info("Jetstream consumer cancelled")
            if last_time_us:
                await set_cursor(pool, last_time_us)
            return
        except Exception as e:
            logger.warning(
                "Jetstream disconnected: %s, reconnecting in %.1fs", e, backoff
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)
