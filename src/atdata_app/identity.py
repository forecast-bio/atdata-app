"""DID document endpoint for did:web identity."""

from __future__ import annotations

from fastapi import Request
from fastapi.responses import JSONResponse


def _build_did_document(
    did: str,
    service_id: str,
    service_type: str,
    service_endpoint: str,
    signing_key: str | None = None,
) -> dict:
    """Build a DID document with optional verificationMethod."""
    context: list[str] = ["https://www.w3.org/ns/did/v1"]
    doc: dict = {
        "@context": context,
        "id": did,
        "service": [
            {
                "id": service_id,
                "type": service_type,
                "serviceEndpoint": service_endpoint,
            }
        ],
    }
    if signing_key:
        context.append("https://w3id.org/security/multikey/v1")
        doc["verificationMethod"] = [
            {
                "id": f"{did}#atproto",
                "type": "Multikey",
                "controller": did,
                "publicKeyMultibase": signing_key,
            }
        ]
    return doc


def _request_hostname(request: Request) -> str:
    """Extract hostname from the Host header, stripping port if present."""
    host = request.headers.get("host", "")
    return host.split(":")[0]


async def did_json_handler(request: Request) -> JSONResponse:
    config = request.app.state.config
    hostname = _request_hostname(request)

    if config.frontend_hostname and hostname == config.frontend_hostname:
        doc = _build_did_document(
            did=config.frontend_did,
            service_id="#atproto_pds",
            service_type="AtprotoPersonalDataServer",
            service_endpoint=config.pds_endpoint or "",
            signing_key=config.frontend_signing_key,
        )
    else:
        doc = _build_did_document(
            did=config.service_did,
            service_id="#atdata_appview",
            service_type="AtdataAppView",
            service_endpoint=config.service_endpoint,
            signing_key=config.signing_key,
        )

    return JSONResponse(content=doc, media_type="application/json")
