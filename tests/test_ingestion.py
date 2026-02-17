"""Tests for ingestion processor."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from atdata_app.ingestion.processor import process_commit

# All patches target the `db` module reference used inside processor.py
_DB = "atdata_app.database"


def _make_event(
    did: str = "did:plc:test123",
    collection: str = "ac.foundation.dataset.record",
    operation: str = "create",
    rkey: str = "3xyz",
    record: dict | None = None,
    cid: str = "bafytest",
) -> dict:
    commit: dict = {
        "rev": "rev1",
        "operation": operation,
        "collection": collection,
        "rkey": rkey,
    }
    if operation != "delete":
        commit["record"] = record or {
            "$type": collection,
            "name": "test-dataset",
            "schemaRef": "at://did:plc:test/ac.foundation.dataset.schema/test@1.0.0",
            "storage": {"$type": "ac.foundation.dataset.storageHttp", "shards": []},
            "createdAt": "2025-01-01T00:00:00Z",
        }
        commit["cid"] = cid

    return {
        "did": did,
        "time_us": 1725911162329308,
        "kind": "commit",
        "commit": commit,
    }


@pytest.mark.asyncio
@patch(f"{_DB}.upsert_entry", new_callable=AsyncMock)
async def test_process_commit_create(mock_upsert):
    pool = AsyncMock()
    event = _make_event(operation="create")
    await process_commit(pool, event)
    mock_upsert.assert_called_once_with(
        pool, "did:plc:test123", "3xyz", "bafytest", event["commit"]["record"]
    )


@pytest.mark.asyncio
async def test_process_commit_ignores_unknown_collection():
    pool = AsyncMock()
    event = _make_event(collection="app.bsky.feed.post")
    await process_commit(pool, event)


@pytest.mark.asyncio
@patch(f"{_DB}.delete_record", new_callable=AsyncMock)
async def test_process_commit_delete(mock_delete):
    pool = AsyncMock()
    event = _make_event(operation="delete")
    await process_commit(pool, event)
    mock_delete.assert_called_once_with(pool, "entries", "did:plc:test123", "3xyz")


@pytest.mark.asyncio
@patch(f"{_DB}.upsert_schema", new_callable=AsyncMock)
async def test_process_commit_schema(mock_upsert):
    pool = AsyncMock()
    event = _make_event(
        collection="ac.foundation.dataset.schema",
        record={
            "$type": "ac.foundation.dataset.schema",
            "name": "TestSchema",
            "version": "1.0.0",
            "schemaType": "jsonSchema",
            "schema": {"$type": "ac.foundation.dataset.schema#jsonSchemaFormat"},
            "createdAt": "2025-01-01T00:00:00Z",
        },
    )
    await process_commit(pool, event)
    mock_upsert.assert_called_once_with(
        pool, "did:plc:test123", "3xyz", "bafytest", event["commit"]["record"]
    )


@pytest.mark.asyncio
@patch(f"{_DB}.upsert_label", new_callable=AsyncMock)
async def test_process_commit_label(mock_upsert):
    pool = AsyncMock()
    event = _make_event(
        collection="ac.foundation.dataset.label",
        record={
            "$type": "ac.foundation.dataset.label",
            "name": "mnist",
            "datasetUri": "at://did:plc:test/ac.foundation.dataset.record/3xyz",
            "createdAt": "2025-01-01T00:00:00Z",
        },
    )
    await process_commit(pool, event)
    mock_upsert.assert_called_once_with(
        pool, "did:plc:test123", "3xyz", "bafytest", event["commit"]["record"]
    )


@pytest.mark.asyncio
@patch(f"{_DB}.upsert_lens", new_callable=AsyncMock)
async def test_process_commit_lens(mock_upsert):
    pool = AsyncMock()
    event = _make_event(
        collection="ac.foundation.dataset.lens",
        record={
            "$type": "ac.foundation.dataset.lens",
            "name": "test-lens",
            "sourceSchema": "at://did:plc:test/ac.foundation.dataset.schema/a@1.0.0",
            "targetSchema": "at://did:plc:test/ac.foundation.dataset.schema/b@1.0.0",
            "getterCode": {"repository": "https://github.com/test/repo", "commit": "abc", "path": "get.py"},
            "putterCode": {"repository": "https://github.com/test/repo", "commit": "abc", "path": "put.py"},
            "createdAt": "2025-01-01T00:00:00Z",
        },
    )
    await process_commit(pool, event)
    mock_upsert.assert_called_once_with(
        pool, "did:plc:test123", "3xyz", "bafytest", event["commit"]["record"]
    )


@pytest.mark.asyncio
@patch(f"{_DB}.upsert_entry", new_callable=AsyncMock)
async def test_process_commit_update(mock_upsert):
    """Update operations should route to the same upsert function as create."""
    pool = AsyncMock()
    event = _make_event(operation="update")
    await process_commit(pool, event)
    mock_upsert.assert_called_once_with(
        pool, "did:plc:test123", "3xyz", "bafytest", event["commit"]["record"]
    )


@pytest.mark.asyncio
@patch(f"{_DB}.upsert_entry", new_callable=AsyncMock)
async def test_process_commit_upsert_error_is_caught(mock_upsert):
    """Upsert failures should be logged, not raised."""
    mock_upsert.side_effect = Exception("db error")
    pool = AsyncMock()
    event = _make_event(operation="create")
    # Should not raise
    await process_commit(pool, event)
