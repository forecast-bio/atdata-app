"""Tests for the change stream event bus and WebSocket subscription endpoint."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from atdata_app.changestream import ChangeEvent, ChangeStream, make_change_event
from atdata_app.ingestion.processor import process_commit
from atdata_app.xrpc.subscriptions import router as subscriptions_router


# ---------------------------------------------------------------------------
# ChangeStream unit tests
# ---------------------------------------------------------------------------


class TestChangeStream:
    def test_publish_assigns_monotonic_seq(self):
        cs = ChangeStream()
        ev1 = make_change_event(
            event_type="create",
            collection="science.alt.dataset.entry",
            did="did:plc:test",
            rkey="abc",
        )
        ev2 = make_change_event(
            event_type="update",
            collection="science.alt.dataset.entry",
            did="did:plc:test",
            rkey="def",
        )
        cs.publish(ev1)
        cs.publish(ev2)
        assert ev1.seq == 1
        assert ev2.seq == 2
        assert cs.current_seq == 2

    def test_publish_delivers_to_subscribers(self):
        cs = ChangeStream()
        sub_id, queue = cs.subscribe()

        ev = make_change_event(
            event_type="create",
            collection="science.alt.dataset.entry",
            did="did:plc:test",
            rkey="abc",
        )
        cs.publish(ev)

        assert not queue.empty()
        received = queue.get_nowait()
        assert received.seq == 1
        assert received.did == "did:plc:test"

    def test_multiple_subscribers_receive_events(self):
        cs = ChangeStream()
        _, q1 = cs.subscribe()
        _, q2 = cs.subscribe()

        ev = make_change_event(
            event_type="create",
            collection="science.alt.dataset.entry",
            did="did:plc:test",
            rkey="abc",
        )
        cs.publish(ev)

        assert not q1.empty()
        assert not q2.empty()
        assert q1.get_nowait().seq == 1
        assert q2.get_nowait().seq == 1

    def test_unsubscribe_removes_subscriber(self):
        cs = ChangeStream()
        sub_id, queue = cs.subscribe()
        assert cs.subscriber_count == 1

        cs.unsubscribe(sub_id)
        assert cs.subscriber_count == 0

        # Publishing after unsubscribe should not deliver
        ev = make_change_event(
            event_type="create",
            collection="science.alt.dataset.entry",
            did="did:plc:test",
            rkey="abc",
        )
        cs.publish(ev)
        assert queue.empty()

    def test_full_queue_drops_event(self):
        cs = ChangeStream(subscriber_queue_size=1)
        _, queue = cs.subscribe()

        ev1 = make_change_event(
            event_type="create",
            collection="science.alt.dataset.entry",
            did="did:plc:test",
            rkey="a",
        )
        ev2 = make_change_event(
            event_type="create",
            collection="science.alt.dataset.entry",
            did="did:plc:test",
            rkey="b",
        )
        cs.publish(ev1)
        cs.publish(ev2)  # Should be dropped (queue full)

        assert queue.qsize() == 1
        assert queue.get_nowait().rkey == "a"

    def test_replay_from_cursor(self):
        cs = ChangeStream(buffer_size=10)
        for i in range(5):
            ev = make_change_event(
                event_type="create",
                collection="science.alt.dataset.entry",
                did="did:plc:test",
                rkey=str(i),
            )
            cs.publish(ev)

        # Replay from seq 3 — should get events 4 and 5
        replayed = cs.replay_from(3)
        assert len(replayed) == 2
        assert replayed[0].seq == 4
        assert replayed[1].seq == 5

    def test_replay_from_zero_returns_all(self):
        cs = ChangeStream(buffer_size=10)
        for i in range(3):
            ev = make_change_event(
                event_type="create",
                collection="science.alt.dataset.entry",
                did="did:plc:test",
                rkey=str(i),
            )
            cs.publish(ev)

        replayed = cs.replay_from(0)
        assert len(replayed) == 3

    def test_replay_cursor_too_old(self):
        cs = ChangeStream(buffer_size=3)
        for i in range(5):
            ev = make_change_event(
                event_type="create",
                collection="science.alt.dataset.entry",
                did="did:plc:test",
                rkey=str(i),
            )
            cs.publish(ev)

        # Buffer only holds seq 3, 4, 5 — cursor 1 is too old
        replayed = cs.replay_from(1)
        assert len(replayed) == 0

    def test_replay_empty_buffer(self):
        cs = ChangeStream()
        assert cs.replay_from(0) == []

    def test_bounded_buffer(self):
        cs = ChangeStream(buffer_size=3)
        for i in range(10):
            ev = make_change_event(
                event_type="create",
                collection="science.alt.dataset.entry",
                did="did:plc:test",
                rkey=str(i),
            )
            cs.publish(ev)

        assert len(cs._buffer) == 3
        assert cs._buffer[0].seq == 8
        assert cs._buffer[-1].seq == 10


class TestChangeEvent:
    def test_to_dict_create(self):
        ev = ChangeEvent(
            seq=1,
            type="create",
            collection="science.alt.dataset.entry",
            did="did:plc:test",
            rkey="abc",
            timestamp="2026-01-01T00:00:00Z",
            record={"name": "test"},
            cid="bafytest",
        )
        d = ev.to_dict()
        assert d["seq"] == 1
        assert d["type"] == "create"
        assert d["record"] == {"name": "test"}
        assert d["cid"] == "bafytest"

    def test_to_dict_delete_omits_record_and_cid(self):
        ev = ChangeEvent(
            seq=2,
            type="delete",
            collection="science.alt.dataset.entry",
            did="did:plc:test",
            rkey="abc",
            timestamp="2026-01-01T00:00:00Z",
        )
        d = ev.to_dict()
        assert "record" not in d
        assert "cid" not in d


class TestMakeChangeEvent:
    def test_creates_event_with_timestamp(self):
        ev = make_change_event(
            event_type="create",
            collection="science.alt.dataset.entry",
            did="did:plc:test",
            rkey="abc",
            record={"name": "test"},
            cid="bafytest",
        )
        assert ev.seq == 0  # Not yet assigned
        assert ev.type == "create"
        assert ev.timestamp  # Should have a timestamp


# ---------------------------------------------------------------------------
# Processor integration tests
# ---------------------------------------------------------------------------

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


@pytest.mark.asyncio
async def test_processor_publishes_create_event():
    mock_upsert = AsyncMock()
    pool = AsyncMock()
    cs = ChangeStream()
    _, queue = cs.subscribe()

    event = _make_event(operation="create")
    with patch.dict(f"{_DB}.UPSERT_FNS", {"entries": mock_upsert}):
        await process_commit(pool, event, change_stream=cs)

    assert not queue.empty()
    change_event = queue.get_nowait()
    assert change_event.type == "create"
    assert change_event.collection == "science.alt.dataset.entry"
    assert change_event.did == "did:plc:test123"
    assert change_event.rkey == "3xyz"
    assert change_event.record is not None
    assert change_event.cid == "bafytest"


@pytest.mark.asyncio
@patch(f"{_DB}.delete_record", new_callable=AsyncMock)
async def test_processor_publishes_delete_event(mock_delete):
    pool = AsyncMock()
    cs = ChangeStream()
    _, queue = cs.subscribe()

    event = _make_event(operation="delete")
    await process_commit(pool, event, change_stream=cs)

    assert not queue.empty()
    change_event = queue.get_nowait()
    assert change_event.type == "delete"
    assert change_event.collection == "science.alt.dataset.entry"
    assert change_event.record is None
    assert change_event.cid is None


@pytest.mark.asyncio
async def test_processor_no_event_on_upsert_failure():
    mock_upsert = AsyncMock(side_effect=Exception("db error"))
    pool = AsyncMock()
    cs = ChangeStream()
    _, queue = cs.subscribe()

    event = _make_event(operation="create")
    with patch.dict(f"{_DB}.UPSERT_FNS", {"entries": mock_upsert}):
        await process_commit(pool, event, change_stream=cs)

    assert queue.empty()


@pytest.mark.asyncio
async def test_processor_works_without_change_stream():
    """Backward compat: process_commit works when change_stream is None."""
    mock_upsert = AsyncMock()
    pool = AsyncMock()
    event = _make_event(operation="create")
    with patch.dict(f"{_DB}.UPSERT_FNS", {"entries": mock_upsert}):
        await process_commit(pool, event)
    mock_upsert.assert_called_once()


# ---------------------------------------------------------------------------
# WebSocket endpoint tests
# ---------------------------------------------------------------------------


@pytest.fixture
def app_with_changestream():
    """Minimal app with just the subscriptions router — no DB lifespan."""
    app = FastAPI()
    app.state.change_stream = ChangeStream()
    app.include_router(subscriptions_router, prefix="/xrpc")
    return app


def test_websocket_subscribe_and_receive(app_with_changestream):
    app = app_with_changestream
    cs: ChangeStream = app.state.change_stream

    with TestClient(app) as client:
        with client.websocket_connect(
            "/xrpc/science.alt.dataset.subscribeChanges"
        ) as ws:
            # Publish an event from another "thread"
            cs.publish(
                make_change_event(
                    event_type="create",
                    collection="science.alt.dataset.entry",
                    did="did:plc:test",
                    rkey="abc",
                    record={"name": "test"},
                    cid="bafytest",
                )
            )

            data = ws.receive_json()
            assert data["seq"] == 1
            assert data["type"] == "create"
            assert data["collection"] == "science.alt.dataset.entry"
            assert data["did"] == "did:plc:test"
            assert data["record"] == {"name": "test"}


def test_websocket_cursor_replay(app_with_changestream):
    app = app_with_changestream
    cs: ChangeStream = app.state.change_stream

    # Pre-populate buffer
    for i in range(5):
        cs.publish(
            make_change_event(
                event_type="create",
                collection="science.alt.dataset.entry",
                did="did:plc:test",
                rkey=str(i),
            )
        )

    with TestClient(app) as client:
        with client.websocket_connect(
            "/xrpc/science.alt.dataset.subscribeChanges?cursor=3"
        ) as ws:
            # Should replay events 4 and 5
            msg1 = ws.receive_json()
            assert msg1["seq"] == 4

            msg2 = ws.receive_json()
            assert msg2["seq"] == 5


def test_websocket_disconnect_cleanup(app_with_changestream):
    app = app_with_changestream
    cs: ChangeStream = app.state.change_stream

    with TestClient(app) as client:
        with client.websocket_connect(
            "/xrpc/science.alt.dataset.subscribeChanges"
        ):
            assert cs.subscriber_count == 1

    # After disconnect, subscriber should be cleaned up
    assert cs.subscriber_count == 0


def test_websocket_multiple_subscribers(app_with_changestream):
    app = app_with_changestream
    cs: ChangeStream = app.state.change_stream

    with TestClient(app) as client:
        with client.websocket_connect(
            "/xrpc/science.alt.dataset.subscribeChanges"
        ) as ws1:
            with client.websocket_connect(
                "/xrpc/science.alt.dataset.subscribeChanges"
            ) as ws2:
                assert cs.subscriber_count == 2

                cs.publish(
                    make_change_event(
                        event_type="create",
                        collection="science.alt.dataset.entry",
                        did="did:plc:test",
                        rkey="abc",
                    )
                )

                d1 = ws1.receive_json()
                d2 = ws2.receive_json()
                assert d1["seq"] == d2["seq"] == 1

    assert cs.subscriber_count == 0
