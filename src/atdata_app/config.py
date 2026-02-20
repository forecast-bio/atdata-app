"""Application configuration via environment variables."""

from __future__ import annotations

from functools import cached_property
from typing import Self
from urllib.parse import quote

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ATDATA_")

    # Identity
    hostname: str = "localhost"
    port: int = 8000
    dev_mode: bool = True
    signing_key: str | None = None

    # Frontend identity (optional — enables dual-hostname mode)
    frontend_hostname: str | None = None
    frontend_signing_key: str | None = None
    pds_endpoint: str | None = None

    @model_validator(mode="after")
    def _check_frontend_config(self) -> Self:
        if self.frontend_hostname and not self.pds_endpoint:
            raise ValueError(
                "ATDATA_PDS_ENDPOINT is required when ATDATA_FRONTEND_HOSTNAME is set"
            )
        return self

    # Database
    database_url: str = "postgresql://localhost:5432/atdata_app"

    # Jetstream
    jetstream_url: str = "wss://jetstream2.us-east.bsky.network/subscribe"
    jetstream_collections: str = "science.alt.dataset.*"

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

    @cached_property
    def frontend_did(self) -> str | None:
        if not self.frontend_hostname:
            return None
        return f"did:web:{self.frontend_hostname}"

    @cached_property
    def frontend_endpoint(self) -> str | None:
        if not self.frontend_hostname:
            return None
        return f"https://{self.frontend_hostname}"
