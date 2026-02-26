"""Shared record processing for firehose and backfill ingestion."""

from __future__ import annotations

import logging
from typing import Any

import asyncpg

from atdata_app import database as db
from atdata_app.changestream import ChangeStream, make_change_event

logger = logging.getLogger(__name__)


async def process_commit(
    pool: asyncpg.Pool,
    event: dict[str, Any],
    change_stream: ChangeStream | None = None,
) -> None:
    """Process a Jetstream commit event.

    Expected event format::

        {
            "did": "did:plc:...",
            "time_us": 1725911162329308,
            "kind": "commit",
            "commit": {
                "rev": "...",
                "operation": "create" | "update" | "delete",
                "collection": "science.alt.dataset.entry",
                "rkey": "...",
                "record": { ... },
                "cid": "..."
            }
        }
    """
    commit = event["commit"]
    collection = commit["collection"]
    table = db.COLLECTION_TABLE_MAP.get(collection)
    if table is None:
        return

    did = event["did"]
    rkey = commit["rkey"]
    operation = commit["operation"]

    if operation == "delete":
        await db.delete_record(pool, table, did, rkey)
        logger.debug("Deleted %s %s/%s", collection, did, rkey)
        if change_stream is not None:
            change_stream.publish(
                make_change_event(
                    event_type="delete",
                    collection=collection,
                    did=did,
                    rkey=rkey,
                )
            )
    elif operation in ("create", "update"):
        record = commit["record"]
        cid = commit.get("cid")
        try:
            if table == "schemas":
                await db.upsert_schema(pool, did, rkey, cid, record)
            elif table == "entries":
                await db.upsert_entry(pool, did, rkey, cid, record)
            elif table == "labels":
                await db.upsert_label(pool, did, rkey, cid, record)
            elif table == "lenses":
                await db.upsert_lens(pool, did, rkey, cid, record)
            logger.debug("Upserted %s %s/%s", collection, did, rkey)
            if change_stream is not None:
                change_stream.publish(
                    make_change_event(
                        event_type=operation,
                        collection=collection,
                        did=did,
                        rkey=rkey,
                        record=record,
                        cid=cid,
                    )
                )
        except Exception:
            logger.exception("Failed to upsert %s %s/%s", collection, did, rkey)
