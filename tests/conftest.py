"""Shared test fixtures."""

from __future__ import annotations

import pytest

from atdata_app.config import AppConfig


@pytest.fixture
def config() -> AppConfig:
    return AppConfig(dev_mode=True, hostname="localhost", port=8000)
