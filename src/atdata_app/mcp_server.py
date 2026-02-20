"""MCP server exposing dataset search and lookup tools for AI agents."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import asyncpg
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.session import ServerSession

from atdata_app.config import AppConfig
from atdata_app.database import (
    COLLECTION_TABLE_MAP,
    create_pool,
    query_get_entry,
    query_get_schema,
    query_list_schemas,
    query_record_counts,
    query_search_datasets,
    query_search_lenses,
)
from atdata_app.models import (
    parse_at_uri,
    row_to_entry,
    row_to_lens,
    row_to_schema,
)

logger = logging.getLogger(__name__)


@dataclass
class ServerContext:
    """Holds shared resources for the MCP server lifetime."""

    pool: asyncpg.Pool
    config: AppConfig


@asynccontextmanager
async def server_lifespan(server: FastMCP) -> AsyncIterator[ServerContext]:
    config = AppConfig()
    pool = await create_pool(config.database_url)
    try:
        yield ServerContext(pool=pool, config=config)
    finally:
        await pool.close()


mcp_server = FastMCP(
    "atdata",
    instructions=(
        "ATProto AppView for the science.alt.dataset namespace. "
        "Use these tools to discover and query scientific datasets, "
        "schemas, and lenses (bidirectional schema transforms) published "
        "on the AT Protocol network."
    ),
    lifespan=server_lifespan,
)

Ctx = Context[ServerSession, ServerContext]


def _get_ctx(ctx: Ctx) -> ServerContext:
    return ctx.request_context.lifespan_context


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp_server.tool()
async def search_datasets(
    ctx: Ctx,
    query: str,
    tags: list[str] | None = None,
    schema_ref: str | None = None,
    repo: str | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Search for datasets by text query, tags, schema, or author.

    Args:
        query: Full-text search query over dataset names and descriptions.
        tags: Optional list of tags to filter by (all must match).
        schema_ref: Optional AT-URI of a schema to filter by.
        repo: Optional DID or handle of the dataset author.
        limit: Maximum number of results (default 10, max 50).

    Returns:
        List of dataset entries with name, description, tags, schema ref, AT-URI, and author DID.
    """
    limit = max(1, min(limit, 50))
    sc = _get_ctx(ctx)
    rows = await query_search_datasets(sc.pool, query, tags, schema_ref, repo, limit)
    return [row_to_entry(r) for r in rows]


@mcp_server.tool()
async def get_dataset(ctx: Ctx, uri: str) -> dict[str, Any]:
    """Fetch a single dataset entry by its AT-URI.

    Args:
        uri: AT-URI of the dataset (e.g. at://did:plc:abc/science.alt.dataset.record/3xyz).

    Returns:
        Full dataset metadata including name, description, schema ref, storage, tags, and size.
    """
    sc = _get_ctx(ctx)
    did, _collection, rkey = parse_at_uri(uri)
    row = await query_get_entry(sc.pool, did, rkey)
    if row is None:
        return {"error": "Dataset not found", "uri": uri}
    return row_to_entry(row)


@mcp_server.tool()
async def get_schema(ctx: Ctx, uri: str) -> dict[str, Any]:
    """Fetch a schema definition by its AT-URI.

    Args:
        uri: AT-URI of the schema (e.g. at://did:plc:abc/science.alt.dataset.schema/my.schema@1.0.0).

    Returns:
        Full schema record including name, version, type, schema body, and description.
    """
    sc = _get_ctx(ctx)
    did, _collection, rkey = parse_at_uri(uri)
    row = await query_get_schema(sc.pool, did, rkey)
    if row is None:
        return {"error": "Schema not found", "uri": uri}
    return row_to_schema(row)


@mcp_server.tool()
async def list_schemas(
    ctx: Ctx,
    repo: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Browse available dataset schemas with optional filtering by author.

    Args:
        repo: Optional DID of the schema author to filter by.
        limit: Maximum number of results (default 20, max 100).

    Returns:
        List of schemas with name, version, type, AT-URI, and description.
    """
    limit = max(1, min(limit, 100))
    sc = _get_ctx(ctx)
    rows = await query_list_schemas(sc.pool, repo, limit)
    return [row_to_schema(r) for r in rows]


@mcp_server.tool()
async def search_lenses(
    ctx: Ctx,
    source_schema: str | None = None,
    target_schema: str | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Find lenses (bidirectional schema transforms) between schemas.

    Args:
        source_schema: Optional AT-URI of the source schema to filter by.
        target_schema: Optional AT-URI of the target schema to filter by.
        limit: Maximum number of results (default 10, max 50).

    Returns:
        List of lenses with name, source/target schemas, code references, and AT-URI.
    """
    limit = max(1, min(limit, 50))
    sc = _get_ctx(ctx)
    rows = await query_search_lenses(sc.pool, source_schema, target_schema, limit)
    return [row_to_lens(r) for r in rows]


@mcp_server.tool()
async def describe_service(ctx: Ctx) -> dict[str, Any]:
    """Get information about this AppView service.

    Returns:
        Service DID, list of supported AT Protocol collections, and record counts per collection.
    """
    sc = _get_ctx(ctx)
    counts = await query_record_counts(sc.pool)
    return {
        "did": sc.config.service_did,
        "availableCollections": list(COLLECTION_TABLE_MAP.keys()),
        "recordCount": counts,
    }


def main() -> None:
    """Entry point for the ``atdata-mcp`` CLI command."""
    mcp_server.run(transport="stdio")


if __name__ == "__main__":
    main()
