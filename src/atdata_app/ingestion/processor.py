"""Shared record processing for firehose and backfill ingestion."""

from __future__ import annotations

import logging
from typing import Any

import asyncpg

from atdata_app import database as db

logger = logging.getLogger(__name__)


async def process_commit(pool: asyncpg.Pool, event: dict[str, Any]) -> None:
    """Process a Jetstream commit event.

    Expected event format::

        {
            "did": "did:plc:...",
            "time_us": 1725911162329308,
            "kind": "commit",
            "commit": {
                "rev": "...",
                "operation": "create" | "update" | "delete",
                "collection": "science.alt.dataset.record",
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
        except Exception:
            logger.exception("Failed to upsert %s %s/%s", collection, did, rkey)
