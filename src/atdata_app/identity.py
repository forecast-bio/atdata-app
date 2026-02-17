"""DID document endpoint for did:web identity."""

from __future__ import annotations

from fastapi import Request
from fastapi.responses import JSONResponse


async def did_json_handler(request: Request) -> JSONResponse:
    config = request.app.state.config
    return JSONResponse(
        content={
            "@context": ["https://www.w3.org/ns/did/v1"],
            "id": config.service_did,
            "service": [
                {
                    "id": "#atproto_appview",
                    "type": "AtprotoAppView",
                    "serviceEndpoint": config.service_endpoint,
                }
            ],
        },
        media_type="application/json",
    )
