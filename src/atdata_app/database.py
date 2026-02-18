"""PostgreSQL connection pool and database operations."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta
from importlib import resources
from typing import Any

import asyncpg

logger = logging.getLogger(__name__)

LEXICON_NAMESPACE = "ac.foundation.dataset"

COLLECTION_TABLE_MAP: dict[str, str] = {
    f"{LEXICON_NAMESPACE}.schema": "schemas",
    f"{LEXICON_NAMESPACE}.record": "entries",
    f"{LEXICON_NAMESPACE}.label": "labels",
    f"{LEXICON_NAMESPACE}.lens": "lenses",
}


async def create_pool(dsn: str) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
    logger.info("Database pool created")
    return pool


async def run_migrations(pool: asyncpg.Pool) -> None:
    sql_path = resources.files("atdata_app") / "sql" / "schema.sql"
    schema_sql = sql_path.read_text(encoding="utf-8")
    async with pool.acquire() as conn:
        await conn.execute(schema_sql)
    logger.info("Database migrations applied")


# ---------------------------------------------------------------------------
# Record upserts
# ---------------------------------------------------------------------------


async def upsert_schema(
    pool: asyncpg.Pool,
    did: str,
    rkey: str,
    cid: str | None,
    record: dict[str, Any],
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO schemas (did, rkey, cid, name, version, schema_type, schema_body,
                                 description, metadata, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9::jsonb, $10)
            ON CONFLICT (did, rkey) DO UPDATE SET
                cid = EXCLUDED.cid,
                name = EXCLUDED.name,
                version = EXCLUDED.version,
                schema_type = EXCLUDED.schema_type,
                schema_body = EXCLUDED.schema_body,
                description = EXCLUDED.description,
                metadata = EXCLUDED.metadata,
                indexed_at = NOW()
            """,
            did,
            rkey,
            cid,
            record["name"],
            record["version"],
            record.get("schemaType", "jsonSchema"),
            json.dumps(record.get("schema", {})),
            record.get("description"),
            json.dumps(record["metadata"]) if record.get("metadata") else None,
            record.get("createdAt", ""),
        )


async def upsert_entry(
    pool: asyncpg.Pool,
    did: str,
    rkey: str,
    cid: str | None,
    record: dict[str, Any],
) -> None:
    size = record.get("size") or {}
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO entries (did, rkey, cid, name, schema_ref, storage,
                                 description, tags, license, size_samples, size_bytes,
                                 size_shards, metadata_schema_ref, content_metadata,
                                 created_at)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10, $11, $12, $13,
                    $14::jsonb, $15)
            ON CONFLICT (did, rkey) DO UPDATE SET
                cid = EXCLUDED.cid,
                name = EXCLUDED.name,
                schema_ref = EXCLUDED.schema_ref,
                storage = EXCLUDED.storage,
                description = EXCLUDED.description,
                tags = EXCLUDED.tags,
                license = EXCLUDED.license,
                size_samples = EXCLUDED.size_samples,
                size_bytes = EXCLUDED.size_bytes,
                size_shards = EXCLUDED.size_shards,
                metadata_schema_ref = EXCLUDED.metadata_schema_ref,
                content_metadata = EXCLUDED.content_metadata,
                indexed_at = NOW()
            """,
            did,
            rkey,
            cid,
            record["name"],
            record["schemaRef"],
            json.dumps(record.get("storage", {})),
            record.get("description"),
            record.get("tags"),
            record.get("license"),
            size.get("samples"),
            size.get("bytes"),
            size.get("shards"),
            record.get("metadataSchemaRef"),
            json.dumps(record["contentMetadata"])
            if record.get("contentMetadata")
            else None,
            record.get("createdAt", ""),
        )


async def upsert_label(
    pool: asyncpg.Pool,
    did: str,
    rkey: str,
    cid: str | None,
    record: dict[str, Any],
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO labels (did, rkey, cid, name, dataset_uri, version, description,
                                created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (did, rkey) DO UPDATE SET
                cid = EXCLUDED.cid,
                name = EXCLUDED.name,
                dataset_uri = EXCLUDED.dataset_uri,
                version = EXCLUDED.version,
                description = EXCLUDED.description,
                indexed_at = NOW()
            """,
            did,
            rkey,
            cid,
            record["name"],
            record["datasetUri"],
            record.get("version"),
            record.get("description"),
            record.get("createdAt", ""),
        )


async def upsert_lens(
    pool: asyncpg.Pool,
    did: str,
    rkey: str,
    cid: str | None,
    record: dict[str, Any],
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO lenses (did, rkey, cid, name, source_schema, target_schema,
                                getter_code, putter_code, description, language,
                                metadata, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10, $11::jsonb, $12)
            ON CONFLICT (did, rkey) DO UPDATE SET
                cid = EXCLUDED.cid,
                name = EXCLUDED.name,
                source_schema = EXCLUDED.source_schema,
                target_schema = EXCLUDED.target_schema,
                getter_code = EXCLUDED.getter_code,
                putter_code = EXCLUDED.putter_code,
                description = EXCLUDED.description,
                language = EXCLUDED.language,
                metadata = EXCLUDED.metadata,
                indexed_at = NOW()
            """,
            did,
            rkey,
            cid,
            record["name"],
            record["sourceSchema"],
            record["targetSchema"],
            json.dumps(record.get("getterCode", {})),
            json.dumps(record.get("putterCode", {})),
            record.get("description"),
            record.get("language"),
            json.dumps(record["metadata"]) if record.get("metadata") else None,
            record.get("createdAt", ""),
        )


async def delete_record(pool: asyncpg.Pool, table: str, did: str, rkey: str) -> None:
    if table not in COLLECTION_TABLE_MAP.values():
        return
    async with pool.acquire() as conn:
        await conn.execute(
            f"DELETE FROM {table} WHERE did = $1 AND rkey = $2",  # noqa: S608
            did,
            rkey,
        )


UPSERT_FNS = {
    "schemas": upsert_schema,
    "entries": upsert_entry,
    "labels": upsert_label,
    "lenses": upsert_lens,
}


# ---------------------------------------------------------------------------
# Cursor state
# ---------------------------------------------------------------------------


async def get_cursor(pool: asyncpg.Pool) -> int | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT cursor FROM cursor_state WHERE service = 'jetstream'"
        )
        return row["cursor"] if row else None


async def set_cursor(pool: asyncpg.Pool, cursor: int) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO cursor_state (service, cursor) VALUES ('jetstream', $1)
            ON CONFLICT (service) DO UPDATE SET cursor = $1, updated_at = NOW()
            """,
            cursor,
        )


# ---------------------------------------------------------------------------
# Query helpers (used by XRPC endpoints)
# ---------------------------------------------------------------------------


async def query_resolve_label(
    pool: asyncpg.Pool,
    did: str,
    name: str,
    version: str | None = None,
) -> asyncpg.Record | None:
    async with pool.acquire() as conn:
        if version:
            return await conn.fetchrow(
                """
                SELECT did, rkey, cid, name, dataset_uri, version, description, created_at
                FROM labels WHERE did = $1 AND name = $2 AND version = $3
                ORDER BY created_at DESC LIMIT 1
                """,
                did,
                name,
                version,
            )
        return await conn.fetchrow(
            """
            SELECT did, rkey, cid, name, dataset_uri, version, description, created_at
            FROM labels WHERE did = $1 AND name = $2
            ORDER BY created_at DESC LIMIT 1
            """,
            did,
            name,
        )


async def query_resolve_schema(
    pool: asyncpg.Pool,
    did: str,
    schema_id: str,
    version: str | None = None,
) -> asyncpg.Record | None:
    async with pool.acquire() as conn:
        if version:
            rkey = f"{schema_id}@{version}"
            return await conn.fetchrow(
                "SELECT * FROM schemas WHERE did = $1 AND rkey = $2",
                did,
                rkey,
            )
        return await conn.fetchrow(
            """
            SELECT * FROM schemas WHERE did = $1 AND rkey LIKE $2
            ORDER BY rkey DESC LIMIT 1
            """,
            did,
            f"{schema_id}@%",
        )


async def query_get_entry(
    pool: asyncpg.Pool, did: str, rkey: str
) -> asyncpg.Record | None:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM entries WHERE did = $1 AND rkey = $2", did, rkey
        )


async def query_get_entries(
    pool: asyncpg.Pool, keys: list[tuple[str, str]]
) -> list[asyncpg.Record]:
    if not keys:
        return []
    conditions = " OR ".join(
        f"(did = ${i * 2 + 1} AND rkey = ${i * 2 + 2})" for i in range(len(keys))
    )
    params = [v for pair in keys for v in pair]
    async with pool.acquire() as conn:
        return await conn.fetch(f"SELECT * FROM entries WHERE {conditions}", *params)


async def query_get_schema(
    pool: asyncpg.Pool, did: str, rkey: str
) -> asyncpg.Record | None:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM schemas WHERE did = $1 AND rkey = $2", did, rkey
        )


async def query_list_entries(
    pool: asyncpg.Pool,
    repo: str | None = None,
    limit: int = 50,
    cursor_did: str | None = None,
    cursor_rkey: str | None = None,
    cursor_indexed_at: str | None = None,
) -> list[asyncpg.Record]:
    conditions: list[str] = []
    params: list[Any] = []
    idx = 1

    if repo:
        conditions.append(f"did = ${idx}")
        params.append(repo)
        idx += 1

    if cursor_indexed_at and cursor_did and cursor_rkey:
        conditions.append(
            f"(indexed_at, did, rkey) < (${idx}, ${idx + 1}, ${idx + 2})"
        )
        params.extend([datetime.fromisoformat(cursor_indexed_at), cursor_did, cursor_rkey])
        idx += 3

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.append(limit)

    async with pool.acquire() as conn:
        return await conn.fetch(
            f"SELECT * FROM entries {where} ORDER BY indexed_at DESC, did DESC, rkey DESC LIMIT ${idx}",
            *params,
        )


async def query_list_schemas(
    pool: asyncpg.Pool,
    repo: str | None = None,
    limit: int = 50,
    cursor_did: str | None = None,
    cursor_rkey: str | None = None,
    cursor_indexed_at: str | None = None,
) -> list[asyncpg.Record]:
    conditions: list[str] = []
    params: list[Any] = []
    idx = 1

    if repo:
        conditions.append(f"did = ${idx}")
        params.append(repo)
        idx += 1

    if cursor_indexed_at and cursor_did and cursor_rkey:
        conditions.append(
            f"(indexed_at, did, rkey) < (${idx}, ${idx + 1}, ${idx + 2})"
        )
        params.extend([datetime.fromisoformat(cursor_indexed_at), cursor_did, cursor_rkey])
        idx += 3

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.append(limit)

    async with pool.acquire() as conn:
        return await conn.fetch(
            f"SELECT * FROM schemas {where} ORDER BY indexed_at DESC, did DESC, rkey DESC LIMIT ${idx}",
            *params,
        )


async def query_list_lenses(
    pool: asyncpg.Pool,
    repo: str | None = None,
    source_schema: str | None = None,
    target_schema: str | None = None,
    limit: int = 50,
    cursor_did: str | None = None,
    cursor_rkey: str | None = None,
    cursor_indexed_at: str | None = None,
) -> list[asyncpg.Record]:
    conditions: list[str] = []
    params: list[Any] = []
    idx = 1

    if repo:
        conditions.append(f"did = ${idx}")
        params.append(repo)
        idx += 1

    if source_schema:
        conditions.append(f"source_schema = ${idx}")
        params.append(source_schema)
        idx += 1

    if target_schema:
        conditions.append(f"target_schema = ${idx}")
        params.append(target_schema)
        idx += 1

    if cursor_indexed_at and cursor_did and cursor_rkey:
        conditions.append(
            f"(indexed_at, did, rkey) < (${idx}, ${idx + 1}, ${idx + 2})"
        )
        params.extend([datetime.fromisoformat(cursor_indexed_at), cursor_did, cursor_rkey])
        idx += 3

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.append(limit)

    async with pool.acquire() as conn:
        return await conn.fetch(
            f"SELECT * FROM lenses {where} ORDER BY indexed_at DESC, did DESC, rkey DESC LIMIT ${idx}",
            *params,
        )


async def query_search_datasets(
    pool: asyncpg.Pool,
    q: str,
    tags: list[str] | None = None,
    schema_ref: str | None = None,
    repo: str | None = None,
    limit: int = 25,
    cursor_did: str | None = None,
    cursor_rkey: str | None = None,
    cursor_indexed_at: str | None = None,
) -> list[asyncpg.Record]:
    conditions: list[str] = ["search_tsv @@ plainto_tsquery('english'::regconfig, $1)"]
    params: list[Any] = [q]
    idx = 2

    if tags:
        conditions.append(f"tags @> ${idx}")
        params.append(tags)
        idx += 1

    if schema_ref:
        conditions.append(f"schema_ref = ${idx}")
        params.append(schema_ref)
        idx += 1

    if repo:
        conditions.append(f"did = ${idx}")
        params.append(repo)
        idx += 1

    if cursor_indexed_at and cursor_did and cursor_rkey:
        conditions.append(
            f"(indexed_at, did, rkey) < (${idx}, ${idx + 1}, ${idx + 2})"
        )
        params.extend([datetime.fromisoformat(cursor_indexed_at), cursor_did, cursor_rkey])
        idx += 3

    where = f"WHERE {' AND '.join(conditions)}"
    params.append(limit)

    async with pool.acquire() as conn:
        return await conn.fetch(
            f"""
            SELECT *, ts_rank(search_tsv, plainto_tsquery('english'::regconfig, $1)) AS rank
            FROM entries {where}
            ORDER BY rank DESC, indexed_at DESC, did DESC, rkey DESC
            LIMIT ${idx}
            """,
            *params,
        )


async def query_search_lenses(
    pool: asyncpg.Pool,
    source_schema: str | None = None,
    target_schema: str | None = None,
    limit: int = 25,
    cursor_did: str | None = None,
    cursor_rkey: str | None = None,
    cursor_indexed_at: str | None = None,
) -> list[asyncpg.Record]:
    conditions: list[str] = []
    params: list[Any] = []
    idx = 1

    if source_schema and target_schema:
        conditions.append(f"(source_schema = ${idx} OR target_schema = ${idx + 1})")
        params.extend([source_schema, target_schema])
        idx += 2
    elif source_schema:
        conditions.append(f"source_schema = ${idx}")
        params.append(source_schema)
        idx += 1
    elif target_schema:
        conditions.append(f"target_schema = ${idx}")
        params.append(target_schema)
        idx += 1

    if cursor_indexed_at and cursor_did and cursor_rkey:
        conditions.append(
            f"(indexed_at, did, rkey) < (${idx}, ${idx + 1}, ${idx + 2})"
        )
        params.extend([datetime.fromisoformat(cursor_indexed_at), cursor_did, cursor_rkey])
        idx += 3

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.append(limit)

    async with pool.acquire() as conn:
        return await conn.fetch(
            f"SELECT * FROM lenses {where} ORDER BY indexed_at DESC, did DESC, rkey DESC LIMIT ${idx}",
            *params,
        )


async def query_record_counts(pool: asyncpg.Pool) -> dict[str, int]:
    async with pool.acquire() as conn:
        counts = {}
        for collection, table in COLLECTION_TABLE_MAP.items():
            row = await conn.fetchrow(f"SELECT COUNT(*) as cnt FROM {table}")  # noqa: S608
            counts[collection] = row["cnt"]
        return counts


async def query_labels_for_dataset(
    pool: asyncpg.Pool, dataset_uri: str, limit: int = 50
) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM labels WHERE dataset_uri = $1 ORDER BY created_at DESC LIMIT $2",
            dataset_uri,
            limit,
        )


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------


async def record_analytics_event(
    pool: asyncpg.Pool,
    event_type: str,
    target_did: str | None = None,
    target_rkey: str | None = None,
    query_params: dict[str, Any] | None = None,
) -> None:
    """Insert an analytics event. Designed to be called via asyncio.create_task()."""
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO analytics_events (event_type, target_did, target_rkey, query_params)
                VALUES ($1, $2, $3, $4::jsonb)
                """,
                event_type,
                target_did,
                target_rkey,
                json.dumps(query_params) if query_params else None,
            )
            # Bump the pre-aggregated counter if we have a target
            if target_did and target_rkey:
                await conn.execute(
                    """
                    INSERT INTO analytics_counters (target_did, target_rkey, event_type, count, last_updated)
                    VALUES ($1, $2, $3, 1, NOW())
                    ON CONFLICT (target_did, target_rkey, event_type) DO UPDATE SET
                        count = analytics_counters.count + 1,
                        last_updated = NOW()
                    """,
                    target_did,
                    target_rkey,
                    event_type,
                )
    except Exception:
        logger.warning("Failed to record analytics event %s", event_type, exc_info=True)


def fire_analytics_event(
    pool: asyncpg.Pool,
    event_type: str,
    target_did: str | None = None,
    target_rkey: str | None = None,
    query_params: dict[str, Any] | None = None,
) -> None:
    """Fire-and-forget analytics recording. Does not block the caller."""
    asyncio.create_task(
        record_analytics_event(pool, event_type, target_did, target_rkey, query_params)
    )


PERIOD_INTERVALS: dict[str, timedelta] = {
    "day": timedelta(days=1),
    "week": timedelta(days=7),
    "month": timedelta(days=30),
}


async def query_analytics_summary(
    pool: asyncpg.Pool,
    period: str = "week",
) -> dict[str, Any]:
    """Aggregate analytics for the getAnalytics endpoint."""
    interval = PERIOD_INTERVALS.get(period, timedelta(days=7))
    async with pool.acquire() as conn:
        # Total views (view_entry + view_schema events)
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE event_type LIKE 'view_%') AS total_views,
                COUNT(*) FILTER (WHERE event_type = 'search') AS total_searches
            FROM analytics_events
            WHERE created_at >= NOW() - $1::interval
            """,
            interval,
        )
        total_views = row["total_views"]
        total_searches = row["total_searches"]

        # Top datasets by view count in period
        top_rows = await conn.fetch(
            """
            SELECT e.target_did, e.target_rkey, ent.name, COUNT(*) AS views
            FROM analytics_events e
            LEFT JOIN entries ent ON ent.did = e.target_did AND ent.rkey = e.target_rkey
            WHERE e.event_type = 'view_entry'
              AND e.created_at >= NOW() - $1::interval
              AND e.target_did IS NOT NULL
            GROUP BY e.target_did, e.target_rkey, ent.name
            ORDER BY views DESC
            LIMIT 10
            """,
            interval,
        )
        top_datasets = [
            {
                "uri": f"at://{r['target_did']}/ac.foundation.dataset.record/{r['target_rkey']}",
                "did": r["target_did"],
                "rkey": r["target_rkey"],
                "name": r["name"] or "",
                "views": r["views"],
            }
            for r in top_rows
        ]

        # Top search terms
        term_rows = await conn.fetch(
            """
            SELECT query_params->>'q' AS term, COUNT(*) AS count
            FROM analytics_events
            WHERE event_type = 'search'
              AND query_params->>'q' IS NOT NULL
              AND created_at >= NOW() - $1::interval
            GROUP BY term
            ORDER BY count DESC
            LIMIT 10
            """,
            interval,
        )
        top_search_terms = [
            {"term": r["term"], "count": r["count"]} for r in term_rows
        ]

        # Record counts
        counts = {}
        for collection, table in COLLECTION_TABLE_MAP.items():
            c = await conn.fetchrow(f"SELECT COUNT(*) AS cnt FROM {table}")  # noqa: S608
            counts[collection] = c["cnt"]

        return {
            "totalViews": total_views,
            "totalSearches": total_searches,
            "topDatasets": top_datasets,
            "topSearchTerms": top_search_terms,
            "recordCounts": counts,
        }


async def query_entry_stats(
    pool: asyncpg.Pool,
    did: str,
    rkey: str,
    period: str = "week",
) -> dict[str, Any]:
    """Get analytics stats for a specific dataset entry."""
    interval = PERIOD_INTERVALS.get(period, timedelta(days=7))
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE event_type = 'view_entry') AS views,
                COUNT(*) FILTER (WHERE event_type = 'search') AS search_appearances
            FROM analytics_events
            WHERE target_did = $1 AND target_rkey = $2
              AND created_at >= NOW() - $3::interval
            """,
            did,
            rkey,
            interval,
        )
        return {
            "views": row["views"],
            "searchAppearances": row["search_appearances"],
            "period": period,
        }


async def query_active_publishers(pool: asyncpg.Pool, days: int = 30) -> int:
    """Count distinct publishers with records indexed in the last N days."""
    interval = timedelta(days=days)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT COUNT(DISTINCT did) AS cnt FROM (
                SELECT did FROM entries WHERE indexed_at >= NOW() - $1::interval
                UNION
                SELECT did FROM schemas WHERE indexed_at >= NOW() - $1::interval
                UNION
                SELECT did FROM labels WHERE indexed_at >= NOW() - $1::interval
                UNION
                SELECT did FROM lenses WHERE indexed_at >= NOW() - $1::interval
            ) sub
            """,
            interval,
        )
        return row["cnt"]


async def query_record_exists(
    pool: asyncpg.Pool, table: str, did: str, rkey: str
) -> bool:
    if table not in COLLECTION_TABLE_MAP.values():
        return False
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT 1 FROM {table} WHERE did = $1 AND rkey = $2",  # noqa: S608
            did,
            rkey,
        )
        return row is not None
