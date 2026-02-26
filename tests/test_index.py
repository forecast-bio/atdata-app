"""Tests for index provider endpoints (skeleton/hydration pattern)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from httpx import ASGITransport, AsyncClient

from atdata_app.config import AppConfig
from atdata_app.ingestion.processor import process_commit
from atdata_app.main import create_app

_DB = "atdata_app.database"
_QUERIES = "atdata_app.xrpc.queries"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_app() -> tuple:
    config = AppConfig(dev_mode=True, hostname="localhost", port=8000)
    app = create_app(config)
    pool = AsyncMock()
    app.state.db_pool = pool
    return app, pool


def _index_provider_row(
    did: str = "did:plc:provider1",
    rkey: str = "3abc",
    endpoint_url: str = "https://example.com/skeleton",
    name: str = "Genomics Index",
    description: str = "Curated genomics datasets",
) -> dict:
    """Simulate an asyncpg Record as a dict-like object."""
    return MagicMock(
        **{
            "__getitem__": lambda self, key: {
                "did": did,
                "rkey": rkey,
                "cid": "bafyindex",
                "name": name,
                "description": description,
                "endpoint_url": endpoint_url,
                "created_at": "2025-01-01T00:00:00Z",
                "indexed_at": "2025-01-01T00:00:00+00:00",
            }[key],
        }
    )


def _entry_row(did: str = "did:plc:author1", rkey: str = "3xyz") -> MagicMock:
    return MagicMock(
        **{
            "__getitem__": lambda self, key: {
                "did": did,
                "rkey": rkey,
                "cid": "bafyentry",
                "name": "test-dataset",
                "schema_ref": "at://did:plc:test/science.alt.dataset.schema/test@1.0.0",
                "storage": '{"$type": "science.alt.dataset.storageHttp", "shards": []}',
                "description": None,
                "tags": None,
                "license": None,
                "size_samples": None,
                "size_bytes": None,
                "size_shards": None,
                "created_at": "2025-01-01T00:00:00Z",
                "indexed_at": "2025-01-01T00:00:00+00:00",
            }[key],
        }
    )


# ---------------------------------------------------------------------------
# getIndexSkeleton
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch(f"{_QUERIES}.query_get_index_provider", new_callable=AsyncMock)
async def test_get_index_skeleton_success(mock_get_provider):
    app, pool = _make_app()
    mock_get_provider.return_value = _index_provider_row()

    skeleton_response = {
        "items": [{"uri": "at://did:plc:a/science.alt.dataset.entry/3xyz"}],
        "cursor": "next123",
    }

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = skeleton_response

    with patch("atdata_app.xrpc.queries.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_resp
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/xrpc/science.alt.dataset.getIndexSkeleton",
                params={"index": "at://did:plc:provider1/science.alt.dataset.index/3abc"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert len(data["items"]) == 1
            assert data["items"][0]["uri"] == "at://did:plc:a/science.alt.dataset.entry/3xyz"
            assert data["cursor"] == "next123"


@pytest.mark.asyncio
@patch(f"{_QUERIES}.query_get_index_provider", new_callable=AsyncMock)
async def test_get_index_skeleton_not_found(mock_get_provider):
    app, pool = _make_app()
    mock_get_provider.return_value = None

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/xrpc/science.alt.dataset.getIndexSkeleton",
            params={"index": "at://did:plc:missing/science.alt.dataset.index/3abc"},
        )
        assert resp.status_code == 404


@pytest.mark.asyncio
@patch(f"{_QUERIES}.query_get_index_provider", new_callable=AsyncMock)
async def test_get_index_skeleton_invalid_uri(mock_get_provider):
    app, pool = _make_app()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/xrpc/science.alt.dataset.getIndexSkeleton",
            params={"index": "not-a-uri"},
        )
        assert resp.status_code == 400


@pytest.mark.asyncio
@patch(f"{_QUERIES}.query_get_index_provider", new_callable=AsyncMock)
async def test_get_index_skeleton_upstream_error(mock_get_provider):
    app, pool = _make_app()
    mock_get_provider.return_value = _index_provider_row()

    mock_resp = MagicMock()
    mock_resp.status_code = 500

    with patch("atdata_app.xrpc.queries.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_resp
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/xrpc/science.alt.dataset.getIndexSkeleton",
                params={"index": "at://did:plc:provider1/science.alt.dataset.index/3abc"},
            )
            assert resp.status_code == 502


@pytest.mark.asyncio
@patch(f"{_QUERIES}.query_get_index_provider", new_callable=AsyncMock)
async def test_get_index_skeleton_upstream_unreachable(mock_get_provider):
    app, pool = _make_app()
    mock_get_provider.return_value = _index_provider_row()

    with patch("atdata_app.xrpc.queries.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.ConnectError("Connection refused")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/xrpc/science.alt.dataset.getIndexSkeleton",
                params={"index": "at://did:plc:provider1/science.alt.dataset.index/3abc"},
            )
            assert resp.status_code == 502


@pytest.mark.asyncio
@patch(f"{_QUERIES}.query_get_index_provider", new_callable=AsyncMock)
async def test_get_index_skeleton_invalid_response(mock_get_provider):
    """Upstream returns JSON without 'items' array."""
    app, pool = _make_app()
    mock_get_provider.return_value = _index_provider_row()

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"bad": "data"}

    with patch("atdata_app.xrpc.queries.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_resp
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/xrpc/science.alt.dataset.getIndexSkeleton",
                params={"index": "at://did:plc:provider1/science.alt.dataset.index/3abc"},
            )
            assert resp.status_code == 502


# ---------------------------------------------------------------------------
# getIndex (hydrated)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch(f"{_QUERIES}.query_get_entries", new_callable=AsyncMock)
@patch(f"{_QUERIES}.query_get_index_provider", new_callable=AsyncMock)
async def test_get_index_hydrated(mock_get_provider, mock_get_entries):
    app, pool = _make_app()
    mock_get_provider.return_value = _index_provider_row()
    mock_get_entries.return_value = [_entry_row("did:plc:a", "3xyz")]

    skeleton_response = {
        "items": [
            {"uri": "at://did:plc:a/science.alt.dataset.entry/3xyz"},
        ],
        "cursor": "next456",
    }

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = skeleton_response

    with patch("atdata_app.xrpc.queries.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_resp
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/xrpc/science.alt.dataset.getIndex",
                params={"index": "at://did:plc:provider1/science.alt.dataset.index/3abc"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert len(data["items"]) == 1
            assert data["items"][0]["name"] == "test-dataset"
            assert data["cursor"] == "next456"


@pytest.mark.asyncio
@patch(f"{_QUERIES}.query_get_entries", new_callable=AsyncMock)
@patch(f"{_QUERIES}.query_get_index_provider", new_callable=AsyncMock)
async def test_get_index_omits_missing_entries(mock_get_provider, mock_get_entries):
    """Entries not in the DB should be silently omitted."""
    app, pool = _make_app()
    mock_get_provider.return_value = _index_provider_row()
    # Return only one of two requested entries
    mock_get_entries.return_value = [_entry_row("did:plc:a", "3xyz")]

    skeleton_response = {
        "items": [
            {"uri": "at://did:plc:a/science.alt.dataset.entry/3xyz"},
            {"uri": "at://did:plc:b/science.alt.dataset.entry/3deleted"},
        ],
    }

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = skeleton_response

    with patch("atdata_app.xrpc.queries.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_resp
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/xrpc/science.alt.dataset.getIndex",
                params={"index": "at://did:plc:provider1/science.alt.dataset.index/3abc"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert len(data["items"]) == 1


@pytest.mark.asyncio
@patch(f"{_QUERIES}.query_get_index_provider", new_callable=AsyncMock)
async def test_get_index_not_found(mock_get_provider):
    app, pool = _make_app()
    mock_get_provider.return_value = None

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/xrpc/science.alt.dataset.getIndex",
            params={"index": "at://did:plc:missing/science.alt.dataset.index/3abc"},
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# listIndexes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch(f"{_QUERIES}.query_list_index_providers", new_callable=AsyncMock)
async def test_list_indexes(mock_list):
    app, pool = _make_app()
    mock_list.return_value = [_index_provider_row()]

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/xrpc/science.alt.dataset.listIndexes")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["indexes"]) == 1
        assert data["indexes"][0]["name"] == "Genomics Index"
        assert data["indexes"][0]["endpointUrl"] == "https://example.com/skeleton"


@pytest.mark.asyncio
@patch(f"{_QUERIES}.query_list_index_providers", new_callable=AsyncMock)
async def test_list_indexes_empty(mock_list):
    app, pool = _make_app()
    mock_list.return_value = []

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/xrpc/science.alt.dataset.listIndexes")
        assert resp.status_code == 200
        data = resp.json()
        assert data["indexes"] == []
        assert data["cursor"] is None


@pytest.mark.asyncio
@patch(f"{_QUERIES}.query_list_index_providers", new_callable=AsyncMock)
async def test_list_indexes_with_repo_filter(mock_list):
    app, pool = _make_app()
    mock_list.return_value = []

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/xrpc/science.alt.dataset.listIndexes",
            params={"repo": "did:plc:provider1"},
        )
        assert resp.status_code == 200
        mock_list.assert_called_once_with(pool, "did:plc:provider1", 50, None, None, None)


# ---------------------------------------------------------------------------
# publishIndex
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("atdata_app.xrpc.procedures.verify_service_auth", new_callable=AsyncMock)
@patch("atdata_app.xrpc.procedures._resolve_pds", new_callable=AsyncMock)
@patch("atdata_app.xrpc.procedures._proxy_create_record", new_callable=AsyncMock)
async def test_publish_index(mock_proxy, mock_pds, mock_auth):
    app, pool = _make_app()

    mock_auth.return_value = MagicMock(iss="did:plc:publisher1")
    mock_pds.return_value = "https://pds.example.com"
    mock_proxy.return_value = {
        "uri": "at://did:plc:publisher1/science.alt.dataset.index/3abc",
        "cid": "bafynew",
    }

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/xrpc/science.alt.dataset.publishIndex",
            json={
                "record": {
                    "name": "Genomics Index",
                    "endpointUrl": "https://example.com/skeleton",
                    "createdAt": "2025-01-01T00:00:00Z",
                },
            },
            headers={
                "Authorization": "Bearer test-token",
                "X-PDS-Auth": "pds-token",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["uri"] == "at://did:plc:publisher1/science.alt.dataset.index/3abc"
        assert data["cid"] == "bafynew"


@pytest.mark.asyncio
@patch("atdata_app.xrpc.procedures.verify_service_auth", new_callable=AsyncMock)
async def test_publish_index_missing_field(mock_auth):
    app, pool = _make_app()
    mock_auth.return_value = MagicMock(iss="did:plc:publisher1")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/xrpc/science.alt.dataset.publishIndex",
            json={
                "record": {
                    "name": "Test",
                    "createdAt": "2025-01-01T00:00:00Z",
                    # missing endpointUrl
                },
            },
            headers={
                "Authorization": "Bearer test-token",
                "X-PDS-Auth": "pds-token",
            },
        )
        assert resp.status_code == 400
        assert "endpointUrl" in resp.json()["detail"]


@pytest.mark.asyncio
@patch("atdata_app.xrpc.procedures.verify_service_auth", new_callable=AsyncMock)
async def test_publish_index_http_url_rejected(mock_auth):
    app, pool = _make_app()
    mock_auth.return_value = MagicMock(iss="did:plc:publisher1")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/xrpc/science.alt.dataset.publishIndex",
            json={
                "record": {
                    "name": "Bad Index",
                    "endpointUrl": "http://insecure.example.com/skeleton",
                    "createdAt": "2025-01-01T00:00:00Z",
                },
            },
            headers={
                "Authorization": "Bearer test-token",
                "X-PDS-Auth": "pds-token",
            },
        )
        assert resp.status_code == 400
        assert "HTTPS" in resp.json()["detail"]


@pytest.mark.asyncio
@patch("atdata_app.xrpc.procedures.verify_service_auth", new_callable=AsyncMock)
async def test_publish_index_invalid_type(mock_auth):
    app, pool = _make_app()
    mock_auth.return_value = MagicMock(iss="did:plc:publisher1")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/xrpc/science.alt.dataset.publishIndex",
            json={
                "record": {
                    "$type": "science.alt.dataset.entry",
                    "name": "Wrong Type",
                    "endpointUrl": "https://example.com/skeleton",
                    "createdAt": "2025-01-01T00:00:00Z",
                },
            },
            headers={
                "Authorization": "Bearer test-token",
                "X-PDS-Auth": "pds-token",
            },
        )
        assert resp.status_code == 400
        assert "Invalid $type" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Ingestion: index provider records
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch(f"{_DB}.upsert_index_provider", new_callable=AsyncMock)
async def test_process_commit_index_provider(mock_upsert):
    pool = AsyncMock()
    event = {
        "did": "did:plc:provider1",
        "time_us": 1725911162329308,
        "kind": "commit",
        "commit": {
            "rev": "rev1",
            "operation": "create",
            "collection": "science.alt.dataset.index",
            "rkey": "3abc",
            "record": {
                "$type": "science.alt.dataset.index",
                "name": "Genomics Index",
                "endpointUrl": "https://example.com/skeleton",
                "createdAt": "2025-01-01T00:00:00Z",
            },
            "cid": "bafyindex",
        },
    }
    await process_commit(pool, event)
    mock_upsert.assert_called_once_with(
        pool, "did:plc:provider1", "3abc", "bafyindex", event["commit"]["record"]
    )


@pytest.mark.asyncio
@patch(f"{_DB}.delete_record", new_callable=AsyncMock)
async def test_process_commit_delete_index_provider(mock_delete):
    pool = AsyncMock()
    event = {
        "did": "did:plc:provider1",
        "time_us": 1725911162329308,
        "kind": "commit",
        "commit": {
            "rev": "rev1",
            "operation": "delete",
            "collection": "science.alt.dataset.index",
            "rkey": "3abc",
        },
    }
    await process_commit(pool, event)
    mock_delete.assert_called_once_with(pool, "index_providers", "did:plc:provider1", "3abc")
