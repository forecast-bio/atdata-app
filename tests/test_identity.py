"""Tests for the DID identity endpoint and hostname gating."""

import pytest
from httpx import ASGITransport, AsyncClient

from atdata_app.config import AppConfig
from atdata_app.identity import did_json_handler


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_app(config: AppConfig):
    """Minimal FastAPI app with just the identity endpoint (no DB)."""
    from fastapi import FastAPI

    app = FastAPI()
    app.state.config = config
    app.add_api_route("/.well-known/did.json", did_json_handler, methods=["GET"])
    return app


def _make_full_app(config: AppConfig):
    """Full app via create_app (includes middleware and all routes)."""
    from unittest.mock import AsyncMock

    from atdata_app.main import create_app

    app = create_app(config)
    app.state.db_pool = AsyncMock()
    return app


# ---------------------------------------------------------------------------
# Single-hostname mode (dev mode, no frontend_hostname)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_did_json_dev_mode():
    config = AppConfig(dev_mode=True, hostname="localhost", port=8000)
    app = _make_app(config)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/.well-known/did.json")
        assert resp.status_code == 200
        data = resp.json()

        assert data["id"] == "did:web:localhost%3A8000"
        assert len(data["service"]) == 1

        svc = data["service"][0]
        assert svc["id"] == "#atdata_appview"
        assert svc["type"] == "AtdataAppView"
        assert svc["serviceEndpoint"] == "http://localhost:8000"

        # No verificationMethod without signing key
        assert "verificationMethod" not in data
        assert len(data["@context"]) == 1


@pytest.mark.asyncio
async def test_did_json_production():
    config = AppConfig(dev_mode=False, hostname="api.atdata.app")
    app = _make_app(config)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/.well-known/did.json")
        data = resp.json()

        assert data["id"] == "did:web:api.atdata.app"
        assert data["service"][0]["serviceEndpoint"] == "https://api.atdata.app"


# ---------------------------------------------------------------------------
# Dual-hostname mode: API hostname
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_api_hostname_returns_appview_did():
    config = AppConfig(
        dev_mode=False,
        hostname="api.atdata.app",
        frontend_hostname="atdata.app",
        pds_endpoint="https://pds.foundation.ac",
    )
    app = _make_app(config)

    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://api.atdata.app"
    ) as client:
        resp = await client.get(
            "/.well-known/did.json", headers={"host": "api.atdata.app"}
        )
        data = resp.json()

        assert data["id"] == "did:web:api.atdata.app"
        svc = data["service"][0]
        assert svc["id"] == "#atdata_appview"
        assert svc["type"] == "AtdataAppView"
        assert svc["serviceEndpoint"] == "https://api.atdata.app"


# ---------------------------------------------------------------------------
# Dual-hostname mode: Frontend hostname
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_frontend_hostname_returns_pds_did():
    config = AppConfig(
        dev_mode=False,
        hostname="api.atdata.app",
        frontend_hostname="atdata.app",
        pds_endpoint="https://pds.foundation.ac",
    )
    app = _make_app(config)

    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://atdata.app"
    ) as client:
        resp = await client.get(
            "/.well-known/did.json", headers={"host": "atdata.app"}
        )
        data = resp.json()

        assert data["id"] == "did:web:atdata.app"
        svc = data["service"][0]
        assert svc["id"] == "#atproto_pds"
        assert svc["type"] == "AtprotoPersonalDataServer"
        assert svc["serviceEndpoint"] == "https://pds.foundation.ac"


# ---------------------------------------------------------------------------
# verificationMethod (signing keys)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_appview_did_with_signing_key():
    config = AppConfig(
        dev_mode=False,
        hostname="api.atdata.app",
        signing_key="zDnaeWgbTFSBnCnPUryHDPSWJPfgt4mM4F1u21Ztc3gTS1Fxk",
    )
    app = _make_app(config)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/.well-known/did.json")
        data = resp.json()

        assert "https://w3id.org/security/multikey/v1" in data["@context"]
        assert len(data["verificationMethod"]) == 1

        vm = data["verificationMethod"][0]
        assert vm["id"] == "did:web:api.atdata.app#atproto"
        assert vm["type"] == "Multikey"
        assert vm["controller"] == "did:web:api.atdata.app"
        assert vm["publicKeyMultibase"] == "zDnaeWgbTFSBnCnPUryHDPSWJPfgt4mM4F1u21Ztc3gTS1Fxk"


@pytest.mark.asyncio
async def test_frontend_did_with_signing_key():
    config = AppConfig(
        dev_mode=False,
        hostname="api.atdata.app",
        frontend_hostname="atdata.app",
        pds_endpoint="https://pds.foundation.ac",
        frontend_signing_key="zDnaeABC123frontendkey",
    )
    app = _make_app(config)

    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://atdata.app"
    ) as client:
        resp = await client.get(
            "/.well-known/did.json", headers={"host": "atdata.app"}
        )
        data = resp.json()

        assert "https://w3id.org/security/multikey/v1" in data["@context"]
        vm = data["verificationMethod"][0]
        assert vm["id"] == "did:web:atdata.app#atproto"
        assert vm["controller"] == "did:web:atdata.app"
        assert vm["publicKeyMultibase"] == "zDnaeABC123frontendkey"


@pytest.mark.asyncio
async def test_frontend_did_without_signing_key():
    config = AppConfig(
        dev_mode=False,
        hostname="api.atdata.app",
        frontend_hostname="atdata.app",
        pds_endpoint="https://pds.foundation.ac",
    )
    app = _make_app(config)

    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://atdata.app"
    ) as client:
        resp = await client.get(
            "/.well-known/did.json", headers={"host": "atdata.app"}
        )
        data = resp.json()

        assert "verificationMethod" not in data
        assert len(data["@context"]) == 1


# ---------------------------------------------------------------------------
# Hostname gating: API hostname must NOT serve frontend routes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_api_hostname_blocks_frontend_routes():
    config = AppConfig(
        dev_mode=False,
        hostname="api.atdata.app",
        frontend_hostname="atdata.app",
    )
    app = _make_full_app(config)

    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://api.atdata.app"
    ) as client:
        for path in ["/", "/about", "/schemas", "/dataset/did:plc:abc/123"]:
            resp = await client.get(path, headers={"host": "api.atdata.app"})
            assert resp.status_code == 404, f"Expected 404 for {path} on API host"


@pytest.mark.asyncio
async def test_api_hostname_allows_shared_routes():
    config = AppConfig(
        dev_mode=False,
        hostname="api.atdata.app",
        frontend_hostname="atdata.app",
    )
    app = _make_full_app(config)

    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://api.atdata.app"
    ) as client:
        # Health check
        resp = await client.get("/health", headers={"host": "api.atdata.app"})
        assert resp.status_code == 200

        # DID document
        resp = await client.get(
            "/.well-known/did.json", headers={"host": "api.atdata.app"}
        )
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_frontend_hostname_serves_all_routes():
    """Frontend hostname should serve health, DID, and frontend routes."""
    config = AppConfig(
        dev_mode=False,
        hostname="api.atdata.app",
        frontend_hostname="atdata.app",
        pds_endpoint="https://pds.foundation.ac",
    )
    app = _make_full_app(config)

    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://atdata.app"
    ) as client:
        # Health check available on frontend host too
        resp = await client.get("/health", headers={"host": "atdata.app"})
        assert resp.status_code == 200

        # DID document
        resp = await client.get(
            "/.well-known/did.json", headers={"host": "atdata.app"}
        )
        assert resp.status_code == 200
        assert resp.json()["id"] == "did:web:atdata.app"


@pytest.mark.asyncio
async def test_no_frontend_hostname_serves_everything():
    """When frontend_hostname is not set, all routes are available (dev mode)."""
    config = AppConfig(dev_mode=True, hostname="localhost", port=8000)
    app = _make_full_app(config)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")
        assert resp.status_code == 200

        resp = await client.get("/.well-known/did.json")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Config cached properties
# ---------------------------------------------------------------------------


def test_frontend_did_property():
    config = AppConfig(
        dev_mode=False,
        hostname="api.atdata.app",
        frontend_hostname="atdata.app",
    )
    assert config.frontend_did == "did:web:atdata.app"
    assert config.frontend_endpoint == "https://atdata.app"


def test_frontend_did_property_none_when_unset():
    config = AppConfig(dev_mode=True, hostname="localhost", port=8000)
    assert config.frontend_did is None
    assert config.frontend_endpoint is None
