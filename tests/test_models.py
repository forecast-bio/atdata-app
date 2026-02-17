"""Tests for model utilities."""

import pytest

from atdata_app.models import (
    decode_cursor,
    encode_cursor,
    make_at_uri,
    parse_at_uri,
)


def test_parse_at_uri():
    did, collection, rkey = parse_at_uri("at://did:plc:abc123/ac.foundation.dataset.record/3xyz")
    assert did == "did:plc:abc123"
    assert collection == "ac.foundation.dataset.record"
    assert rkey == "3xyz"


def test_parse_at_uri_invalid():
    with pytest.raises(ValueError):
        parse_at_uri("https://example.com")


def test_make_at_uri():
    uri = make_at_uri("did:plc:abc", "ac.foundation.dataset.record", "rkey1")
    assert uri == "at://did:plc:abc/ac.foundation.dataset.record/rkey1"


def test_cursor_roundtrip():
    encoded = encode_cursor("2025-01-01T00:00:00+00:00", "did:plc:abc", "rkey1")
    indexed_at, did, rkey = decode_cursor(encoded)
    assert indexed_at == "2025-01-01T00:00:00+00:00"
    assert did == "did:plc:abc"
    assert rkey == "rkey1"


def test_decode_cursor_invalid():
    with pytest.raises(ValueError):
        decode_cursor("invalid-base64-cursor==")
