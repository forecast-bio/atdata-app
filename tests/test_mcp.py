"""Tests for MCP server tools."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from atdata_app.mcp_server import (
    Ctx,
    ServerContext,
    describe_service,
    get_dataset,
    get_schema,
    list_schemas,
    search_datasets,
    search_lenses,
)

_DB = "atdata_app.mcp_server"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_ENTRY_ROW = {
    "did": "did:plc:abc",
    "rkey": "3xyz",
    "cid": "bafyentry",
    "name": "test-dataset",
    "schema_ref": "at://did:plc:abc/ac.foundation.dataset.schema/s@1.0.0",
    "storage": {"$type": "ac.foundation.dataset.storageHttp", "url": "https://example.com"},
    "description": "A test dataset",
    "tags": ["ml", "nlp"],
    "license": "MIT",
    "size_samples": 1000,
    "size_bytes": 5000000,
    "size_shards": 4,
    "created_at": "2025-01-01T00:00:00Z",
}

_SCHEMA_ROW = {
    "did": "did:plc:abc",
    "rkey": "my.schema@1.0.0",
    "cid": "bafyschema",
    "name": "my.schema",
    "version": "1.0.0",
    "schema_type": "jsonSchema",
    "schema_body": {"type": "object", "properties": {}},
    "description": "A test schema",
    "created_at": "2025-01-01T00:00:00Z",
}

_LENS_ROW = {
    "did": "did:plc:abc",
    "rkey": "3lens",
    "cid": "bafylens",
    "name": "a-to-b",
    "source_schema": "at://did:plc:abc/ac.foundation.dataset.schema/a@1.0.0",
    "target_schema": "at://did:plc:abc/ac.foundation.dataset.schema/b@1.0.0",
    "getter_code": {"repo": "https://github.com/test/repo", "path": "get.py"},
    "putter_code": {"repo": "https://github.com/test/repo", "path": "put.py"},
    "description": "Transforms A to B",
    "language": "python",
    "created_at": "2025-01-01T00:00:00Z",
}


def _make_ctx(pool: AsyncMock) -> Ctx:
    """Build a mock MCP Context that provides a ServerContext."""
    from atdata_app.config import AppConfig

    config = AppConfig(dev_mode=True, hostname="localhost", port=8000)
    sc = ServerContext(pool=pool, config=config)

    ctx = AsyncMock(spec=Ctx)
    ctx.request_context = AsyncMock()
    ctx.request_context.lifespan_context = sc
    return ctx


# ---------------------------------------------------------------------------
# search_datasets
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch(f"{_DB}.query_search_datasets", new_callable=AsyncMock)
async def test_search_datasets_returns_entries(mock_query):
    mock_query.return_value = [_ENTRY_ROW]
    pool = AsyncMock()
    ctx = _make_ctx(pool)

    result = await search_datasets(ctx, query="test")

    mock_query.assert_called_once_with(pool, "test", None, None, None, 10)
    assert len(result) == 1
    assert result[0]["name"] == "test-dataset"
    assert result[0]["uri"] == "at://did:plc:abc/ac.foundation.dataset.record/3xyz"


@pytest.mark.asyncio
@patch(f"{_DB}.query_search_datasets", new_callable=AsyncMock)
async def test_search_datasets_with_filters(mock_query):
    mock_query.return_value = []
    pool = AsyncMock()
    ctx = _make_ctx(pool)

    result = await search_datasets(
        ctx,
        query="genomics",
        tags=["bio"],
        schema_ref="at://did:plc:x/ac.foundation.dataset.schema/s@1.0.0",
        repo="did:plc:x",
        limit=5,
    )

    mock_query.assert_called_once_with(
        pool,
        "genomics",
        ["bio"],
        "at://did:plc:x/ac.foundation.dataset.schema/s@1.0.0",
        "did:plc:x",
        5,
    )
    assert result == []


@pytest.mark.asyncio
@patch(f"{_DB}.query_search_datasets", new_callable=AsyncMock)
async def test_search_datasets_clamps_limit(mock_query):
    mock_query.return_value = []
    pool = AsyncMock()
    ctx = _make_ctx(pool)

    await search_datasets(ctx, query="x", limit=999)
    assert mock_query.call_args[0][5] == 50

    await search_datasets(ctx, query="x", limit=-5)
    assert mock_query.call_args[0][5] == 1


# ---------------------------------------------------------------------------
# get_dataset
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch(f"{_DB}.query_get_entry", new_callable=AsyncMock)
async def test_get_dataset_found(mock_query):
    mock_query.return_value = _ENTRY_ROW
    pool = AsyncMock()
    ctx = _make_ctx(pool)

    result = await get_dataset(
        ctx, uri="at://did:plc:abc/ac.foundation.dataset.record/3xyz"
    )

    mock_query.assert_called_once_with(pool, "did:plc:abc", "3xyz")
    assert result["name"] == "test-dataset"
    assert result["tags"] == ["ml", "nlp"]


@pytest.mark.asyncio
@patch(f"{_DB}.query_get_entry", new_callable=AsyncMock)
async def test_get_dataset_not_found(mock_query):
    mock_query.return_value = None
    pool = AsyncMock()
    ctx = _make_ctx(pool)

    result = await get_dataset(
        ctx, uri="at://did:plc:abc/ac.foundation.dataset.record/missing"
    )

    assert result["error"] == "Dataset not found"


# ---------------------------------------------------------------------------
# get_schema
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch(f"{_DB}.query_get_schema", new_callable=AsyncMock)
async def test_get_schema_found(mock_query):
    mock_query.return_value = _SCHEMA_ROW
    pool = AsyncMock()
    ctx = _make_ctx(pool)

    result = await get_schema(
        ctx, uri="at://did:plc:abc/ac.foundation.dataset.schema/my.schema@1.0.0"
    )

    mock_query.assert_called_once_with(pool, "did:plc:abc", "my.schema@1.0.0")
    assert result["name"] == "my.schema"
    assert result["version"] == "1.0.0"


@pytest.mark.asyncio
@patch(f"{_DB}.query_get_schema", new_callable=AsyncMock)
async def test_get_schema_not_found(mock_query):
    mock_query.return_value = None
    pool = AsyncMock()
    ctx = _make_ctx(pool)

    result = await get_schema(
        ctx, uri="at://did:plc:abc/ac.foundation.dataset.schema/missing@1.0.0"
    )

    assert result["error"] == "Schema not found"


# ---------------------------------------------------------------------------
# list_schemas
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch(f"{_DB}.query_list_schemas", new_callable=AsyncMock)
async def test_list_schemas_default(mock_query):
    mock_query.return_value = [_SCHEMA_ROW]
    pool = AsyncMock()
    ctx = _make_ctx(pool)

    result = await list_schemas(ctx)

    mock_query.assert_called_once_with(pool, None, 20)
    assert len(result) == 1
    assert result[0]["name"] == "my.schema"


@pytest.mark.asyncio
@patch(f"{_DB}.query_list_schemas", new_callable=AsyncMock)
async def test_list_schemas_with_repo(mock_query):
    mock_query.return_value = []
    pool = AsyncMock()
    ctx = _make_ctx(pool)

    await list_schemas(ctx, repo="did:plc:abc", limit=5)

    mock_query.assert_called_once_with(pool, "did:plc:abc", 5)


@pytest.mark.asyncio
@patch(f"{_DB}.query_list_schemas", new_callable=AsyncMock)
async def test_list_schemas_clamps_limit(mock_query):
    mock_query.return_value = []
    pool = AsyncMock()
    ctx = _make_ctx(pool)

    await list_schemas(ctx, limit=500)
    assert mock_query.call_args[0][2] == 100


# ---------------------------------------------------------------------------
# search_lenses
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch(f"{_DB}.query_search_lenses", new_callable=AsyncMock)
async def test_search_lenses_default(mock_query):
    mock_query.return_value = [_LENS_ROW]
    pool = AsyncMock()
    ctx = _make_ctx(pool)

    result = await search_lenses(ctx)

    mock_query.assert_called_once_with(pool, None, None, 10)
    assert len(result) == 1
    assert result[0]["name"] == "a-to-b"
    assert result[0]["sourceSchema"] == _LENS_ROW["source_schema"]


@pytest.mark.asyncio
@patch(f"{_DB}.query_search_lenses", new_callable=AsyncMock)
async def test_search_lenses_with_filters(mock_query):
    mock_query.return_value = []
    pool = AsyncMock()
    ctx = _make_ctx(pool)

    await search_lenses(
        ctx,
        source_schema="at://did:plc:abc/ac.foundation.dataset.schema/a@1.0.0",
        target_schema="at://did:plc:abc/ac.foundation.dataset.schema/b@1.0.0",
        limit=5,
    )

    mock_query.assert_called_once_with(
        pool,
        "at://did:plc:abc/ac.foundation.dataset.schema/a@1.0.0",
        "at://did:plc:abc/ac.foundation.dataset.schema/b@1.0.0",
        5,
    )


@pytest.mark.asyncio
@patch(f"{_DB}.query_search_lenses", new_callable=AsyncMock)
async def test_search_lenses_clamps_limit(mock_query):
    mock_query.return_value = []
    pool = AsyncMock()
    ctx = _make_ctx(pool)

    await search_lenses(ctx, limit=999)
    assert mock_query.call_args[0][3] == 50


# ---------------------------------------------------------------------------
# describe_service
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch(f"{_DB}.query_record_counts", new_callable=AsyncMock)
async def test_describe_service(mock_counts):
    mock_counts.return_value = {
        "ac.foundation.dataset.schema": 10,
        "ac.foundation.dataset.record": 50,
        "ac.foundation.dataset.label": 30,
        "ac.foundation.dataset.lens": 5,
    }
    pool = AsyncMock()
    ctx = _make_ctx(pool)

    result = await describe_service(ctx)

    assert result["did"].startswith("did:web:")
    assert "ac.foundation.dataset.record" in result["availableCollections"]
    assert result["recordCount"]["ac.foundation.dataset.record"] == 50
