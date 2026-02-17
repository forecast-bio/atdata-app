"""Backfill historical records from the ATProto network."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx
from atproto_identity.resolver import AsyncIdResolver
from fastapi import FastAPI

from atdata_app import database as db

logger = logging.getLogger(__name__)

BACKFILL_COLLECTIONS = list(db.COLLECTION_TABLE_MAP.keys())

_UPSERT_FNS: dict[str, Any] = {
    "schemas": db.upsert_schema,
    "entries": db.upsert_entry,
    "labels": db.upsert_label,
    "lenses": db.upsert_lens,
}

_id_resolver: AsyncIdResolver | None = None


def _get_resolver() -> AsyncIdResolver:
    global _id_resolver  # noqa: PLW0603
    if _id_resolver is None:
        _id_resolver = AsyncIdResolver()
    return _id_resolver


async def _resolve_pds(did: str) -> str | None:
    """Resolve a DID to its PDS endpoint URL."""
    try:
        resolver = _get_resolver()
        data = await resolver.did.resolve_atproto_data(did)
        return data.pds if data else None
    except Exception:
        logger.debug("Could not resolve PDS for %s", did)
        return None


async def _discover_dids(
    http: httpx.AsyncClient, relay_host: str, collection: str
) -> list[str]:
    """Discover all DIDs that have records in the given collection."""
    dids: list[str] = []
    cursor: str | None = None

    while True:
        params: dict[str, Any] = {"collection": collection, "limit": 500}
        if cursor:
            params["cursor"] = cursor

        try:
            resp = await http.get(
                f"{relay_host}/xrpc/com.atproto.sync.listReposByCollection",
                params=params,
            )
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.warning("listReposByCollection failed for %s: %s", collection, e)
            break

        data = resp.json()
        for repo in data.get("repos", []):
            dids.append(repo["did"])

        cursor = data.get("cursor")
        if not cursor:
            break

    logger.info("Discovered %d DIDs for %s", len(dids), collection)
    return dids


async def _backfill_repo(
    http: httpx.AsyncClient,
    pool,
    sem: asyncio.Semaphore,
    did: str,
    collection: str,
) -> None:
    """Fetch all records for a DID+collection from its PDS and upsert them."""
    table = db.COLLECTION_TABLE_MAP[collection]
    upsert_fn = _UPSERT_FNS[table]

    async with sem:
        pds = await _resolve_pds(did)
        if not pds:
            return

        cursor: str | None = None
        total = 0

        while True:
            params: dict[str, Any] = {
                "repo": did,
                "collection": collection,
                "limit": 100,
            }
            if cursor:
                params["cursor"] = cursor

            try:
                resp = await http.get(
                    f"{pds}/xrpc/com.atproto.repo.listRecords", params=params
                )
                resp.raise_for_status()
            except httpx.HTTPError as e:
                logger.debug("listRecords failed for %s on %s: %s", did, pds, e)
                break

            data = resp.json()
            for rec in data.get("records", []):
                uri_parts = rec["uri"].split("/")
                rkey = uri_parts[-1]
                try:
                    await upsert_fn(pool, did, rkey, rec.get("cid"), rec["value"])
                    total += 1
                except Exception:
                    logger.debug("Failed to upsert backfill record %s", rec["uri"])

            cursor = data.get("cursor")
            if not cursor:
                break

        if total > 0:
            logger.debug("Backfilled %d %s records for %s", total, collection, did)


async def backfill_runner(app: FastAPI) -> None:
    """One-shot backfill of all known records from the network."""
    pool = app.state.db_pool
    config = app.state.config
    sem = asyncio.Semaphore(10)

    logger.info("Starting backfill from %s", config.relay_host)

    try:
        async with httpx.AsyncClient(timeout=30.0) as http:
            for collection in BACKFILL_COLLECTIONS:
                dids = await _discover_dids(http, config.relay_host, collection)
                tasks = [
                    _backfill_repo(http, pool, sem, did, collection) for did in dids
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                errors = sum(1 for r in results if isinstance(r, Exception))
                if errors:
                    logger.warning(
                        "Backfill %s: %d/%d repos had errors",
                        collection,
                        errors,
                        len(dids),
                    )

        logger.info("Backfill complete")
    except asyncio.CancelledError:
        logger.info("Backfill cancelled")
    except Exception:
        logger.exception("Backfill failed")
