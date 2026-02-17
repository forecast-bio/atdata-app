"""Tests for AppConfig."""

from atdata_app.config import AppConfig


def test_service_did_dev_mode():
    config = AppConfig(dev_mode=True, hostname="localhost", port=8000)
    assert config.service_did == "did:web:localhost%3A8000"


def test_service_did_production():
    config = AppConfig(dev_mode=False, hostname="datasets.atdata.blue", port=443)
    assert config.service_did == "did:web:datasets.atdata.blue"


def test_service_endpoint_dev():
    config = AppConfig(dev_mode=True, hostname="localhost", port=3000)
    assert config.service_endpoint == "http://localhost:3000"


def test_service_endpoint_prod():
    config = AppConfig(dev_mode=False, hostname="datasets.atdata.blue")
    assert config.service_endpoint == "https://datasets.atdata.blue"
