"""Tests for ingestion processor."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from atdata_app.ingestion.processor import process_commit

_DB = "atdata_app.database"


def _make_event(
    did: str = "did:plc:test123",
    collection: str = "science.alt.dataset.entry",
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
            "schemaRef": "at://did:plc:test/science.alt.dataset.schema/test@1.0.0",
            "storage": {"$type": "science.alt.dataset.storageHttp", "shards": []},
            "createdAt": "2025-01-01T00:00:00Z",
        }
        commit["cid"] = cid

    return {
        "did": did,
        "time_us": 1725911162329308,
        "kind": "commit",
        "commit": commit,
    }


def _patch_upsert(table: str):
    """Patch a single entry in UPSERT_FNS by table name."""
    mock = AsyncMock()
    return patch.dict(f"{_DB}.UPSERT_FNS", {table: mock}), mock


@pytest.mark.asyncio
async def test_process_commit_create():
    patcher, mock_upsert = _patch_upsert("entries")
    with patcher:
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
async def test_process_commit_schema():
    patcher, mock_upsert = _patch_upsert("schemas")
    with patcher:
        pool = AsyncMock()
        event = _make_event(
            collection="science.alt.dataset.schema",
            record={
                "$type": "science.alt.dataset.schema",
                "name": "TestSchema",
                "version": "1.0.0",
                "schemaType": "jsonSchema",
                "schema": {"$type": "science.alt.dataset.schema#jsonSchemaFormat"},
                "createdAt": "2025-01-01T00:00:00Z",
            },
        )
        await process_commit(pool, event)
        mock_upsert.assert_called_once_with(
            pool, "did:plc:test123", "3xyz", "bafytest", event["commit"]["record"]
        )


@pytest.mark.asyncio
async def test_process_commit_label():
    patcher, mock_upsert = _patch_upsert("labels")
    with patcher:
        pool = AsyncMock()
        event = _make_event(
            collection="science.alt.dataset.label",
            record={
                "$type": "science.alt.dataset.label",
                "name": "mnist",
                "datasetUri": "at://did:plc:test/science.alt.dataset.entry/3xyz",
                "createdAt": "2025-01-01T00:00:00Z",
            },
        )
        await process_commit(pool, event)
        mock_upsert.assert_called_once_with(
            pool, "did:plc:test123", "3xyz", "bafytest", event["commit"]["record"]
        )


@pytest.mark.asyncio
async def test_process_commit_lens():
    patcher, mock_upsert = _patch_upsert("lenses")
    with patcher:
        pool = AsyncMock()
        event = _make_event(
            collection="science.alt.dataset.lens",
            record={
                "$type": "science.alt.dataset.lens",
                "name": "test-lens",
                "sourceSchema": "at://did:plc:test/science.alt.dataset.schema/a@1.0.0",
                "targetSchema": "at://did:plc:test/science.alt.dataset.schema/b@1.0.0",
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
async def test_process_commit_update():
    """Update operations should route to the same upsert function as create."""
    patcher, mock_upsert = _patch_upsert("entries")
    with patcher:
        pool = AsyncMock()
        event = _make_event(operation="update")
        await process_commit(pool, event)
        mock_upsert.assert_called_once_with(
            pool, "did:plc:test123", "3xyz", "bafytest", event["commit"]["record"]
        )


@pytest.mark.asyncio
async def test_process_commit_upsert_error_is_caught():
    """Upsert failures should be logged, not raised."""
    mock = AsyncMock(side_effect=Exception("db error"))
    with patch.dict(f"{_DB}.UPSERT_FNS", {"entries": mock}):
        pool = AsyncMock()
        event = _make_event(operation="create")
        # Should not raise
        await process_commit(pool, event)
