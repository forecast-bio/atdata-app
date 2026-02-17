"""Application configuration via environment variables."""

from __future__ import annotations

from functools import cached_property
from urllib.parse import quote

from pydantic_settings import BaseSettings, SettingsConfigDict


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ATDATA_")

    # Identity
    hostname: str = "localhost"
    port: int = 8000
    dev_mode: bool = True

    # Database
    database_url: str = "postgresql://localhost:5432/atdata_app"

    # Jetstream
    jetstream_url: str = "wss://jetstream2.us-east.bsky.network/subscribe"
    jetstream_collections: str = "ac.foundation.dataset.*"

    # Relay (for backfill)
    relay_host: str = "https://bsky.network"

    @cached_property
    def service_did(self) -> str:
        if self.dev_mode:
            return f"did:web:{self.hostname}{quote(f':{self.port}', safe='')}"
        return f"did:web:{self.hostname}"

    @cached_property
    def service_endpoint(self) -> str:
        if self.dev_mode:
            return f"http://{self.hostname}:{self.port}"
        return f"https://{self.hostname}"
