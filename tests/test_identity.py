"""Tests for the DID identity endpoint."""

import pytest
from httpx import ASGITransport, AsyncClient

from atdata_app.config import AppConfig
from atdata_app.identity import did_json_handler


@pytest.fixture
def _mock_app():
    """Minimal FastAPI app with just the identity endpoint (no DB)."""
    from fastapi import FastAPI

    config = AppConfig(dev_mode=True, hostname="localhost", port=8000)
    app = FastAPI()
    app.state.config = config
    app.add_api_route("/.well-known/did.json", did_json_handler, methods=["GET"])
    return app


@pytest.mark.asyncio
async def test_did_json(_mock_app):
    transport = ASGITransport(app=_mock_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/.well-known/did.json")
        assert resp.status_code == 200
        data = resp.json()

        assert data["id"] == "did:web:localhost%3A8000"
        assert len(data["service"]) == 1

        svc = data["service"][0]
        assert svc["id"] == "#atproto_appview"
        assert svc["type"] == "AtprotoAppView"
        assert svc["serviceEndpoint"] == "http://localhost:8000"


@pytest.mark.asyncio
async def test_did_json_production():
    from fastapi import FastAPI

    config = AppConfig(dev_mode=False, hostname="datasets.atdata.blue")
    app = FastAPI()
    app.state.config = config
    app.add_api_route("/.well-known/did.json", did_json_handler, methods=["GET"])

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/.well-known/did.json")
        data = resp.json()

        assert data["id"] == "did:web:datasets.atdata.blue"
        assert data["service"][0]["serviceEndpoint"] == "https://datasets.atdata.blue"
