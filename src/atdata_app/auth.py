"""Service auth JWT verification for XRPC procedures."""

from __future__ import annotations

import logging

from atproto_server.auth.jwt import verify_jwt_async
from fastapi import HTTPException, Request

from atdata_app import get_resolver

logger = logging.getLogger(__name__)


class ServiceAuthPayload:
    """Verified service auth payload."""

    def __init__(self, iss: str, aud: str):
        self.iss = iss  # Caller's DID
        self.aud = aud  # Our DID


async def verify_service_auth(
    request: Request,
    expected_nsid: str | None = None,
) -> ServiceAuthPayload:
    """Verify the service auth JWT from the Authorization header.

    Uses ``atproto_server.auth.jwt.verify_jwt_async`` to validate the token
    against the caller's ATProto signing key.

    Returns the verified payload with ``iss`` (caller DID) and ``aud`` (our DID).
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    token = auth_header[7:]
    config = request.app.state.config

    try:
        resolver = get_resolver()

        async def get_signing_key(did: str, force_refresh: bool = False) -> str:
            return await resolver.did.resolve_atproto_key(did, force_refresh)

        payload = await verify_jwt_async(
            jwt=token,
            get_signing_key_callback=get_signing_key,
            own_did=config.service_did,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid service auth: {e}") from e

    if expected_nsid:
        lxm = getattr(payload, "lxm", None)
        if lxm != expected_nsid:
            raise HTTPException(
                status_code=401,
                detail=f"JWT lxm claim '{lxm}' does not match expected '{expected_nsid}'",
            )

    return ServiceAuthPayload(iss=payload.iss, aud=payload.aud)
