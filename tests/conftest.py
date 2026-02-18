"""Shared test fixtures."""

from __future__ import annotations

import atexit
import os
import shutil
import socket
import subprocess
import time

import pytest

from atdata_app.config import AppConfig

# ---------------------------------------------------------------------------
# Auto-start PostgreSQL Docker container for integration tests
# ---------------------------------------------------------------------------
# This runs at import time (before pytest collection) so that
# TEST_DATABASE_URL is set before pytestmark skipif conditions are evaluated.

_pg_container_name: str | None = None


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _wait_for_pg(host: str, port: int, timeout: float = 30.0) -> None:
    """Poll until PostgreSQL accepts connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError:
            time.sleep(0.5)
    raise TimeoutError(f"PostgreSQL not ready on {host}:{port} after {timeout}s")


def _cleanup_container() -> None:
    if _pg_container_name:
        subprocess.run(
            ["docker", "rm", "-f", _pg_container_name],
            capture_output=True,
        )


def _maybe_start_pg() -> None:
    """Start a PostgreSQL Docker container if TEST_DATABASE_URL is not set."""
    global _pg_container_name

    if os.environ.get("TEST_DATABASE_URL"):
        return  # CI provides the database

    if not shutil.which("docker"):
        return  # No Docker — integration tests will be skipped

    port = _find_free_port()
    container_name = f"atdata-test-pg-{port}"
    pg_version = os.environ.get("TEST_PG_VERSION", "17")

    result = subprocess.run(
        [
            "docker", "run", "-d",
            "--name", container_name,
            "-e", "POSTGRES_USER=test",
            "-e", "POSTGRES_PASSWORD=test",
            "-e", "POSTGRES_DB=atdata_test",
            "-p", f"{port}:5432",
            f"postgres:{pg_version}",
        ],
        capture_output=True,
    )
    if result.returncode != 0:
        return  # Docker not running or image pull failed — skip gracefully

    _pg_container_name = container_name
    atexit.register(_cleanup_container)

    try:
        _wait_for_pg("localhost", port)
        time.sleep(1)  # Let PG finish initialization
        os.environ["TEST_DATABASE_URL"] = (
            f"postgresql://test:test@localhost:{port}/atdata_test"
        )
    except TimeoutError:
        _cleanup_container()
        _pg_container_name = None


_maybe_start_pg()

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def config() -> AppConfig:
    return AppConfig(dev_mode=True, hostname="localhost", port=8000)
