"""Service auth JWT verification for XRPC procedures."""

from __future__ import annotations

import logging

from atproto_identity.resolver import AsyncIdResolver
from fastapi import HTTPException, Request

logger = logging.getLogger(__name__)

_id_resolver: AsyncIdResolver | None = None


def _get_resolver() -> AsyncIdResolver:
    global _id_resolver  # noqa: PLW0603
    if _id_resolver is None:
        _id_resolver = AsyncIdResolver()
    return _id_resolver


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
        from atproto_server.auth.jwt import verify_jwt_async

        resolver = _get_resolver()

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

    return ServiceAuthPayload(iss=payload.iss, aud=payload.aud)
