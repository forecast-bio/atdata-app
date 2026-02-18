"""Tests for analytics and usage tracking."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from atdata_app.config import AppConfig
from atdata_app.database import (
    fire_analytics_event,
    record_analytics_event,
)
from atdata_app.main import create_app
from atdata_app.models import (
    DescribeServiceResponse,
    GetAnalyticsResponse,
    GetEntryStatsResponse,
)

_DB = "atdata_app.xrpc.queries"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def config() -> AppConfig:
    return AppConfig(dev_mode=True, hostname="localhost", port=8000)


@pytest.fixture
def pool() -> AsyncMock:
    return AsyncMock()


def _mock_app(config: AppConfig, pool: AsyncMock):
    """Create a FastAPI app with mocked lifespan (no real DB)."""
    app = create_app(config)
    app.state.db_pool = pool
    return app


# ---------------------------------------------------------------------------
# record_analytics_event
# ---------------------------------------------------------------------------


def _pool_with_conn():
    """Create a mock pool whose acquire() returns an async context manager with a mock conn."""
    mock_conn = AsyncMock()
    mock_pool = MagicMock()
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    mock_pool.acquire.return_value = ctx
    return mock_pool, mock_conn


@pytest.mark.asyncio
async def test_record_analytics_event_inserts_event():
    mock_pool, mock_conn = _pool_with_conn()

    await record_analytics_event(mock_pool, "view_entry", target_did="did:plc:abc", target_rkey="3xyz")

    # Should have called execute twice: once for the event INSERT, once for the counter upsert
    assert mock_conn.execute.call_count == 2
    first_call = mock_conn.execute.call_args_list[0]
    assert "INSERT INTO analytics_events" in first_call[0][0]
    assert first_call[0][1] == "view_entry"
    assert first_call[0][2] == "did:plc:abc"
    assert first_call[0][3] == "3xyz"


@pytest.mark.asyncio
async def test_record_analytics_event_no_counter_without_target():
    mock_pool, mock_conn = _pool_with_conn()

    await record_analytics_event(mock_pool, "describe")

    # Only the event INSERT, no counter upsert
    assert mock_conn.execute.call_count == 1


@pytest.mark.asyncio
async def test_record_analytics_event_handles_db_error(pool):
    """Analytics recording should not raise even if DB fails."""
    pool.acquire.side_effect = Exception("DB down")

    # Should not raise
    await record_analytics_event(pool, "view_entry", target_did="did:plc:abc", target_rkey="3xyz")


# ---------------------------------------------------------------------------
# fire_analytics_event (fire-and-forget)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fire_analytics_event_creates_background_task(pool):
    with patch("atdata_app.database.record_analytics_event", new_callable=AsyncMock) as mock_record:
        fire_analytics_event(pool, "view_entry", target_did="did:plc:abc", target_rkey="3xyz")

        # Allow the background task to run
        await asyncio.sleep(0.01)

        mock_record.assert_called_once_with(
            pool, "view_entry", "did:plc:abc", "3xyz", None
        )


# ---------------------------------------------------------------------------
# getAnalytics endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch(f"{_DB}.query_analytics_summary", new_callable=AsyncMock)
@patch(f"{_DB}.fire_analytics_event")
async def test_get_analytics_endpoint(mock_fire, mock_summary, config, pool):
    mock_summary.return_value = {
        "totalViews": 100,
        "totalSearches": 25,
        "topDatasets": [
            {
                "uri": "at://did:plc:abc/ac.foundation.dataset.record/3xyz",
                "did": "did:plc:abc",
                "rkey": "3xyz",
                "name": "test-ds",
                "views": 50,
            }
        ],
        "topSearchTerms": [{"term": "genomics", "count": 10}],
        "recordCounts": {
            "ac.foundation.dataset.schema": 5,
            "ac.foundation.dataset.record": 20,
            "ac.foundation.dataset.label": 10,
            "ac.foundation.dataset.lens": 3,
        },
    }

    app = _mock_app(config, pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/xrpc/ac.foundation.dataset.getAnalytics", params={"period": "week"})

    assert resp.status_code == 200
    data = resp.json()
    assert data["totalViews"] == 100
    assert data["totalSearches"] == 25
    assert len(data["topDatasets"]) == 1
    assert data["topDatasets"][0]["name"] == "test-ds"
    assert len(data["topSearchTerms"]) == 1
    assert data["recordCounts"]["ac.foundation.dataset.record"] == 20


@pytest.mark.asyncio
@patch(f"{_DB}.query_analytics_summary", new_callable=AsyncMock)
@patch(f"{_DB}.fire_analytics_event")
async def test_get_analytics_default_period(mock_fire, mock_summary, config, pool):
    mock_summary.return_value = {
        "totalViews": 0,
        "totalSearches": 0,
        "topDatasets": [],
        "topSearchTerms": [],
        "recordCounts": {},
    }

    app = _mock_app(config, pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/xrpc/ac.foundation.dataset.getAnalytics")

    assert resp.status_code == 200
    mock_summary.assert_called_once_with(pool, "week")


@pytest.mark.asyncio
@patch(f"{_DB}.fire_analytics_event")
async def test_get_analytics_invalid_period(mock_fire, config, pool):
    app = _mock_app(config, pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/xrpc/ac.foundation.dataset.getAnalytics", params={"period": "year"})

    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# getEntryStats endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch(f"{_DB}.query_entry_stats", new_callable=AsyncMock)
@patch(f"{_DB}.fire_analytics_event")
async def test_get_entry_stats_endpoint(mock_fire, mock_stats, config, pool):
    mock_stats.return_value = {
        "views": 42,
        "searchAppearances": 7,
        "period": "week",
    }

    app = _mock_app(config, pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/xrpc/ac.foundation.dataset.getEntryStats",
            params={"uri": "at://did:plc:abc/ac.foundation.dataset.record/3xyz"},
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["views"] == 42
    assert data["searchAppearances"] == 7
    assert data["period"] == "week"


@pytest.mark.asyncio
@patch(f"{_DB}.fire_analytics_event")
async def test_get_entry_stats_invalid_uri(mock_fire, config, pool):
    app = _mock_app(config, pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/xrpc/ac.foundation.dataset.getEntryStats",
            params={"uri": "https://bad-uri"},
        )

    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# describeService includes analytics
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch(f"{_DB}.query_active_publishers", new_callable=AsyncMock)
@patch(f"{_DB}.query_analytics_summary", new_callable=AsyncMock)
@patch(f"{_DB}.query_record_counts", new_callable=AsyncMock)
@patch(f"{_DB}.fire_analytics_event")
async def test_describe_service_includes_analytics(
    mock_fire, mock_counts, mock_summary, mock_publishers, config, pool
):
    mock_counts.return_value = {
        "ac.foundation.dataset.schema": 5,
        "ac.foundation.dataset.record": 20,
        "ac.foundation.dataset.label": 10,
        "ac.foundation.dataset.lens": 3,
    }
    mock_summary.return_value = {
        "totalViews": 200,
        "totalSearches": 50,
        "topDatasets": [],
        "topSearchTerms": [],
        "recordCounts": {},
    }
    mock_publishers.return_value = 8

    app = _mock_app(config, pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/xrpc/ac.foundation.dataset.describeService")

    assert resp.status_code == 200
    data = resp.json()
    assert "analytics" in data
    assert data["analytics"]["totalViews"] == 200
    assert data["analytics"]["totalSearches"] == 50
    assert data["analytics"]["activePublishers"] == 8


# ---------------------------------------------------------------------------
# Analytics recording in query endpoints
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch(f"{_DB}.fire_analytics_event")
@patch(f"{_DB}.query_get_entry", new_callable=AsyncMock)
async def test_get_entry_fires_analytics(mock_query, mock_fire, config, pool):
    mock_query.return_value = {
        "did": "did:plc:abc",
        "rkey": "3xyz",
        "cid": "bafytest",
        "name": "test-ds",
        "schema_ref": "at://did:plc:abc/ac.foundation.dataset.schema/s@1.0.0",
        "storage": {"$type": "ac.foundation.dataset.storageHttp"},
        "description": None,
        "tags": None,
        "license": None,
        "size_samples": None,
        "size_bytes": None,
        "size_shards": None,
        "created_at": "2025-01-01T00:00:00Z",
    }

    app = _mock_app(config, pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/xrpc/ac.foundation.dataset.getEntry",
            params={"uri": "at://did:plc:abc/ac.foundation.dataset.record/3xyz"},
        )

    assert resp.status_code == 200
    mock_fire.assert_called_once_with(
        pool, "view_entry", target_did="did:plc:abc", target_rkey="3xyz"
    )


@pytest.mark.asyncio
@patch(f"{_DB}.fire_analytics_event")
@patch(f"{_DB}.query_search_datasets", new_callable=AsyncMock)
async def test_search_datasets_fires_analytics(mock_query, mock_fire, config, pool):
    mock_query.return_value = []

    app = _mock_app(config, pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/xrpc/ac.foundation.dataset.searchDatasets",
            params={"q": "genomics"},
        )

    assert resp.status_code == 200
    mock_fire.assert_called_once_with(
        pool, "search", query_params={"q": "genomics", "tags": None}
    )


# ---------------------------------------------------------------------------
# Response model validation
# ---------------------------------------------------------------------------


def test_get_analytics_response_model():
    resp = GetAnalyticsResponse(
        totalViews=100,
        totalSearches=25,
        topDatasets=[{"uri": "at://test", "views": 10}],
        topSearchTerms=[{"term": "ml", "count": 5}],
        recordCounts={"ac.foundation.dataset.record": 20},
    )
    assert resp.totalViews == 100
    assert resp.totalSearches == 25


def test_get_entry_stats_response_model():
    resp = GetEntryStatsResponse(views=42, searchAppearances=7, period="week")
    assert resp.views == 42
    assert resp.period == "week"


def test_describe_service_response_with_analytics():
    resp = DescribeServiceResponse(
        did="did:web:localhost%3A8000",
        availableCollections=["ac.foundation.dataset.record"],
        recordCount={"ac.foundation.dataset.record": 10},
        analytics={"totalViews": 50, "totalSearches": 10, "activePublishers": 3},
    )
    assert resp.analytics["totalViews"] == 50


def test_describe_service_response_without_analytics():
    resp = DescribeServiceResponse(
        did="did:web:localhost%3A8000",
        availableCollections=["ac.foundation.dataset.record"],
        recordCount={"ac.foundation.dataset.record": 10},
    )
    assert resp.analytics is None
