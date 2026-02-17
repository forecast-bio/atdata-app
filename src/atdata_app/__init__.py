"""ATProto AppView for ac.foundation.dataset namespace."""

from __future__ import annotations

from atproto_identity.resolver import AsyncIdResolver

_id_resolver: AsyncIdResolver | None = None


def get_resolver() -> AsyncIdResolver:
    """Return a shared AsyncIdResolver singleton (lazy-initialized)."""
    global _id_resolver  # noqa: PLW0603
    if _id_resolver is None:
        _id_resolver = AsyncIdResolver()
    return _id_resolver
