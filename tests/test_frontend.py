"""Tests for the dataset browser frontend routes."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from atdata_app.config import AppConfig


def _make_entry_row(
    did: str = "did:plc:test123",
    rkey: str = "3xyz",
    name: str = "test-dataset",
    description: str = "A test dataset",
    tags: list[str] | None = None,
) -> dict:
    return {
        "did": did,
        "rkey": rkey,
        "cid": "bafytest",
        "name": name,
        "schema_ref": "at://did:plc:test/science.alt.dataset.schema/test@1.0.0",
        "storage": '{"$type": "science.alt.dataset.storageHttp", "shards": []}',
        "description": description,
        "tags": tags or ["ml", "test"],
        "license": "MIT",
        "size_samples": 1000,
        "size_bytes": 5000000,
        "size_shards": 2,
        "metadata_schema_ref": None,
        "content_metadata": None,
        "created_at": "2025-01-01T00:00:00Z",
        "indexed_at": "2025-01-02T00:00:00Z",
    }


def _make_schema_row(
    did: str = "did:plc:test123",
    rkey: str = "test@1.0.0",
    name: str = "TestSchema",
) -> dict:
    return {
        "did": did,
        "rkey": rkey,
        "cid": "bafyschema",
        "name": name,
        "version": "1.0.0",
        "schema_type": "jsonSchema",
        "schema_body": '{"type": "object"}',
        "description": "A test schema",
        "metadata": None,
        "created_at": "2025-01-01T00:00:00Z",
        "indexed_at": "2025-01-02T00:00:00Z",
    }


def _make_label_row(
    did: str = "did:plc:test123",
    rkey: str = "3abc",
    name: str = "v1",
) -> dict:
    return {
        "did": did,
        "rkey": rkey,
        "cid": "bafylabel",
        "name": name,
        "dataset_uri": "at://did:plc:test123/science.alt.dataset.entry/3xyz",
        "version": "1.0",
        "description": "First version",
        "created_at": "2025-01-01T00:00:00Z",
        "indexed_at": "2025-01-02T00:00:00Z",
    }


def _mock_pool():
    """Create a mock pool with an acquire context manager."""
    pool = AsyncMock()
    conn = AsyncMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = ctx
    return pool, conn


def _make_app(pool):
    """Build a minimal FastAPI app with frontend routes mounted (no lifespan)."""
    from fastapi import FastAPI
    from fastapi.staticfiles import StaticFiles

    from atdata_app.frontend import router as frontend_router
    from atdata_app.frontend.routes import _FRONTEND_DIR

    config = AppConfig(dev_mode=True, hostname="localhost", port=8000)
    app = FastAPI()
    app.state.config = config
    app.state.db_pool = pool
    app.include_router(frontend_router)
    app.mount("/static", StaticFiles(directory=str(_FRONTEND_DIR / "static")), name="static")
    return app


# ---------------------------------------------------------------------------
# Home / Search
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_home_empty():
    pool, _conn = _mock_pool()
    app = _make_app(pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/")
    assert resp.status_code == 200
    assert "Dataset Browser" in resp.text


@pytest.mark.asyncio
@patch("atdata_app.frontend.routes.query_search_datasets", new_callable=AsyncMock)
async def test_home_search(mock_search):
    pool, _conn = _mock_pool()
    mock_search.return_value = [_make_entry_row()]
    app = _make_app(pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/?q=test")
    assert resp.status_code == 200
    assert "test-dataset" in resp.text
    mock_search.assert_called_once()


# ---------------------------------------------------------------------------
# Dataset detail
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("atdata_app.frontend.routes.query_labels_for_dataset", new_callable=AsyncMock)
@patch("atdata_app.frontend.routes.query_get_entry", new_callable=AsyncMock)
async def test_dataset_detail(mock_get, mock_labels):
    pool, _conn = _mock_pool()
    mock_get.return_value = _make_entry_row()
    mock_labels.return_value = [_make_label_row()]
    app = _make_app(pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/dataset/did:plc:test123/3xyz")
    assert resp.status_code == 200
    assert "test-dataset" in resp.text
    assert "Labels" in resp.text


@pytest.mark.asyncio
@patch("atdata_app.frontend.routes.query_get_entry", new_callable=AsyncMock)
async def test_dataset_detail_not_found(mock_get):
    pool, _conn = _mock_pool()
    mock_get.return_value = None
    app = _make_app(pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/dataset/did:plc:test123/missing")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Schema detail
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("atdata_app.frontend.routes.query_get_schema", new_callable=AsyncMock)
async def test_schema_detail(mock_get):
    pool, _conn = _mock_pool()
    mock_get.return_value = _make_schema_row()
    app = _make_app(pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/schema/did:plc:test123/test@1.0.0")
    assert resp.status_code == 200
    assert "TestSchema" in resp.text
    assert "Schema Body" in resp.text


@pytest.mark.asyncio
@patch("atdata_app.frontend.routes.query_get_schema", new_callable=AsyncMock)
async def test_schema_detail_not_found(mock_get):
    pool, _conn = _mock_pool()
    mock_get.return_value = None
    app = _make_app(pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/schema/did:plc:test123/missing@1.0.0")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Schemas list
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("atdata_app.frontend.routes.query_list_schemas", new_callable=AsyncMock)
async def test_schemas_list(mock_list):
    pool, _conn = _mock_pool()
    mock_list.return_value = [_make_schema_row()]
    app = _make_app(pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/schemas")
    assert resp.status_code == 200
    assert "TestSchema" in resp.text


# ---------------------------------------------------------------------------
# Profile
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("atdata_app.frontend.routes.query_list_schemas", new_callable=AsyncMock)
@patch("atdata_app.frontend.routes.query_list_entries", new_callable=AsyncMock)
async def test_profile(mock_entries, mock_schemas):
    pool, _conn = _mock_pool()
    mock_entries.return_value = [_make_entry_row()]
    mock_schemas.return_value = [_make_schema_row()]
    app = _make_app(pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/profile/did:plc:test123")
    assert resp.status_code == 200
    assert "test-dataset" in resp.text
    assert "TestSchema" in resp.text


# ---------------------------------------------------------------------------
# About
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("atdata_app.frontend.routes.query_record_counts", new_callable=AsyncMock)
async def test_about(mock_counts):
    pool, _conn = _mock_pool()
    mock_counts.return_value = {
        "science.alt.dataset.schema": 5,
        "science.alt.dataset.entry": 10,
        "science.alt.dataset.label": 3,
        "science.alt.dataset.lens": 1,
    }
    app = _make_app(pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/about")
    assert resp.status_code == 200
    assert "About This Service" in resp.text
    assert "did:web:localhost%3A8000" in resp.text


# ---------------------------------------------------------------------------
# Static files
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_static_css():
    pool, _conn = _mock_pool()
    app = _make_app(pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/static/style.css")
    assert resp.status_code == 200
    assert "text/css" in resp.headers["content-type"]
