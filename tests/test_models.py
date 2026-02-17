"""Tests for model utilities."""

import base64

import pytest

from atdata_app.models import (
    decode_cursor,
    encode_cursor,
    make_at_uri,
    parse_at_uri,
    row_to_entry,
    row_to_label,
    row_to_lens,
    row_to_schema,
)


# ---------------------------------------------------------------------------
# AT-URI parsing
# ---------------------------------------------------------------------------


def test_parse_at_uri():
    did, collection, rkey = parse_at_uri("at://did:plc:abc123/ac.foundation.dataset.record/3xyz")
    assert did == "did:plc:abc123"
    assert collection == "ac.foundation.dataset.record"
    assert rkey == "3xyz"


def test_parse_at_uri_invalid_scheme():
    with pytest.raises(ValueError):
        parse_at_uri("https://example.com")


def test_parse_at_uri_too_few_parts():
    with pytest.raises(ValueError):
        parse_at_uri("at://did:plc:abc/collection")


def test_make_at_uri():
    uri = make_at_uri("did:plc:abc", "ac.foundation.dataset.record", "rkey1")
    assert uri == "at://did:plc:abc/ac.foundation.dataset.record/rkey1"


def test_parse_make_roundtrip():
    uri = "at://did:plc:abc/ac.foundation.dataset.record/rkey1"
    assert make_at_uri(*parse_at_uri(uri)) == uri


# ---------------------------------------------------------------------------
# Cursor encoding
# ---------------------------------------------------------------------------


def test_cursor_roundtrip():
    encoded = encode_cursor("2025-01-01T00:00:00+00:00", "did:plc:abc", "rkey1")
    indexed_at, did, rkey = decode_cursor(encoded)
    assert indexed_at == "2025-01-01T00:00:00+00:00"
    assert did == "did:plc:abc"
    assert rkey == "rkey1"


def test_decode_cursor_invalid_base64():
    with pytest.raises(Exception):
        decode_cursor("invalid-base64-cursor==")


def test_decode_cursor_wrong_format():
    """Valid base64 but missing :: separators."""
    bad = base64.urlsafe_b64encode(b"no-separators-here").decode()
    with pytest.raises(ValueError):
        decode_cursor(bad)


# ---------------------------------------------------------------------------
# row_to_entry
# ---------------------------------------------------------------------------

_ENTRY_ROW_FULL = {
    "did": "did:plc:abc",
    "rkey": "3xyz",
    "cid": "bafyfull",
    "name": "test-ds",
    "schema_ref": "at://did:plc:abc/ac.foundation.dataset.schema/s@1.0.0",
    "storage": {"$type": "ac.foundation.dataset.storageHttp", "url": "https://example.com"},
    "description": "A dataset",
    "tags": ["ml", "nlp"],
    "license": "MIT",
    "size_samples": 1000,
    "size_bytes": 5000000,
    "size_shards": 4,
    "created_at": "2025-01-01T00:00:00Z",
}

_ENTRY_ROW_MINIMAL = {
    "did": "did:plc:abc",
    "rkey": "3xyz",
    "cid": "bafymin",
    "name": "bare-ds",
    "schema_ref": "at://did:plc:abc/ac.foundation.dataset.schema/s@1.0.0",
    "storage": {"$type": "ac.foundation.dataset.storageHttp"},
    "description": None,
    "tags": None,
    "license": None,
    "size_samples": None,
    "size_bytes": None,
    "size_shards": None,
    "created_at": "2025-06-01T00:00:00Z",
}


def test_row_to_entry_full():
    d = row_to_entry(_ENTRY_ROW_FULL)
    assert d["uri"] == "at://did:plc:abc/ac.foundation.dataset.record/3xyz"
    assert d["schemaRef"] == _ENTRY_ROW_FULL["schema_ref"]
    assert d["description"] == "A dataset"
    assert d["tags"] == ["ml", "nlp"]
    assert d["license"] == "MIT"
    assert d["size"] == {"samples": 1000, "bytes": 5000000, "shards": 4}


def test_row_to_entry_minimal_omits_optional_fields():
    d = row_to_entry(_ENTRY_ROW_MINIMAL)
    assert d["uri"] == "at://did:plc:abc/ac.foundation.dataset.record/3xyz"
    assert "description" not in d
    assert "tags" not in d
    assert "license" not in d
    assert "size" not in d


def test_row_to_entry_json_string_storage():
    """asyncpg may return JSONB as a string; row_to_entry should parse it."""
    row = {**_ENTRY_ROW_MINIMAL, "storage": '{"$type": "ac.foundation.dataset.storageHttp"}'}
    d = row_to_entry(row)
    assert isinstance(d["storage"], dict)
    assert d["storage"]["$type"] == "ac.foundation.dataset.storageHttp"


# ---------------------------------------------------------------------------
# row_to_schema
# ---------------------------------------------------------------------------

_SCHEMA_ROW = {
    "did": "did:plc:abc",
    "rkey": "my.schema@1.0.0",
    "cid": "bafyschema",
    "name": "my.schema",
    "version": "1.0.0",
    "schema_type": "jsonSchema",
    "schema_body": {"type": "object", "properties": {}},
    "description": "A schema",
    "created_at": "2025-01-01T00:00:00Z",
}


def test_row_to_schema():
    d = row_to_schema(_SCHEMA_ROW)
    assert d["uri"] == "at://did:plc:abc/ac.foundation.dataset.schema/my.schema@1.0.0"
    assert d["schemaType"] == "jsonSchema"
    assert d["schema"] == {"type": "object", "properties": {}}
    assert d["description"] == "A schema"


def test_row_to_schema_omits_null_description():
    row = {**_SCHEMA_ROW, "description": None}
    d = row_to_schema(row)
    assert "description" not in d


def test_row_to_schema_json_string_body():
    row = {**_SCHEMA_ROW, "schema_body": '{"type": "object"}'}
    d = row_to_schema(row)
    assert d["schema"] == {"type": "object"}


# ---------------------------------------------------------------------------
# row_to_label
# ---------------------------------------------------------------------------

_LABEL_ROW = {
    "did": "did:plc:abc",
    "rkey": "3lbl",
    "cid": "bafylabel",
    "name": "v1",
    "dataset_uri": "at://did:plc:abc/ac.foundation.dataset.record/3xyz",
    "version": "1.0.0",
    "description": "First version",
    "created_at": "2025-01-01T00:00:00Z",
}


def test_row_to_label():
    d = row_to_label(_LABEL_ROW)
    assert d["uri"] == "at://did:plc:abc/ac.foundation.dataset.label/3lbl"
    assert d["datasetUri"] == _LABEL_ROW["dataset_uri"]
    assert d["version"] == "1.0.0"
    assert d["description"] == "First version"


def test_row_to_label_omits_optional_fields():
    row = {**_LABEL_ROW, "version": None, "description": None}
    d = row_to_label(row)
    assert "version" not in d
    assert "description" not in d


# ---------------------------------------------------------------------------
# row_to_lens
# ---------------------------------------------------------------------------

_LENS_ROW = {
    "did": "did:plc:abc",
    "rkey": "3lens",
    "cid": "bafylens",
    "name": "a-to-b",
    "source_schema": "at://did:plc:abc/ac.foundation.dataset.schema/a@1.0.0",
    "target_schema": "at://did:plc:abc/ac.foundation.dataset.schema/b@1.0.0",
    "getter_code": {"repo": "https://github.com/test/repo", "path": "get.py"},
    "putter_code": {"repo": "https://github.com/test/repo", "path": "put.py"},
    "description": "Transforms A to B",
    "language": "python",
    "created_at": "2025-01-01T00:00:00Z",
}


def test_row_to_lens():
    d = row_to_lens(_LENS_ROW)
    assert d["uri"] == "at://did:plc:abc/ac.foundation.dataset.lens/3lens"
    assert d["sourceSchema"] == _LENS_ROW["source_schema"]
    assert d["getterCode"] == _LENS_ROW["getter_code"]
    assert d["description"] == "Transforms A to B"
    assert d["language"] == "python"


def test_row_to_lens_omits_optional_fields():
    row = {**_LENS_ROW, "description": None, "language": None}
    d = row_to_lens(row)
    assert "description" not in d
    assert "language" not in d


def test_row_to_lens_json_string_code():
    row = {
        **_LENS_ROW,
        "getter_code": '{"repo": "x"}',
        "putter_code": '{"repo": "y"}',
    }
    d = row_to_lens(row)
    assert d["getterCode"] == {"repo": "x"}
    assert d["putterCode"] == {"repo": "y"}
