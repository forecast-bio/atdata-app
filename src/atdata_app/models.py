"""Pydantic response models for XRPC endpoints."""

from __future__ import annotations

import base64
import json
from typing import Any

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# AT-URI parsing
# ---------------------------------------------------------------------------


def parse_at_uri(uri: str) -> tuple[str, str, str]:
    """Parse ``at://did/collection/rkey`` into (did, collection, rkey)."""
    if not uri.startswith("at://"):
        raise ValueError(f"Invalid AT-URI: {uri}")
    parts = uri[5:].split("/", 2)
    if len(parts) != 3:
        raise ValueError(f"Invalid AT-URI: {uri}")
    return parts[0], parts[1], parts[2]


def make_at_uri(did: str, collection: str, rkey: str) -> str:
    return f"at://{did}/{collection}/{rkey}"


# ---------------------------------------------------------------------------
# Cursor encoding
# ---------------------------------------------------------------------------


def encode_cursor(indexed_at: str, did: str, rkey: str) -> str:
    raw = f"{indexed_at}::{did}::{rkey}"
    return base64.urlsafe_b64encode(raw.encode()).decode()


def decode_cursor(cursor: str) -> tuple[str, str, str]:
    raw = base64.urlsafe_b64decode(cursor.encode()).decode()
    parts = raw.split("::", 2)
    if len(parts) != 3:
        raise ValueError(f"Invalid cursor: {cursor}")
    return parts[0], parts[1], parts[2]


def parse_cursor(cursor: str | None) -> tuple[str | None, str | None, str | None]:
    """Decode a cursor string, returning (None, None, None) when absent."""
    if not cursor:
        return None, None, None
    return decode_cursor(cursor)


def maybe_cursor(rows: list, limit: int) -> str | None:
    """Build a cursor from the last row if the result set is full (more pages)."""
    if len(rows) < limit:
        return None
    last = rows[-1]
    return encode_cursor(str(last["indexed_at"]), last["did"], last["rkey"])


# ---------------------------------------------------------------------------
# Record serialisation helpers
# ---------------------------------------------------------------------------


def row_to_entry(row, collection: str = "science.alt.dataset.entry") -> dict[str, Any]:
    uri = make_at_uri(row["did"], collection, row["rkey"])
    storage = row["storage"]
    if isinstance(storage, str):
        storage = json.loads(storage)

    d: dict[str, Any] = {
        "uri": uri,
        "cid": row["cid"],
        "did": row["did"],
        "name": row["name"],
        "schemaRef": row["schema_ref"],
        "storage": storage,
        "createdAt": row["created_at"],
    }
    if row["description"]:
        d["description"] = row["description"]
    if row["tags"]:
        d["tags"] = list(row["tags"])
    if row["license"]:
        d["license"] = row["license"]

    size = {}
    if row["size_samples"] is not None:
        size["samples"] = row["size_samples"]
    if row["size_bytes"] is not None:
        size["bytes"] = row["size_bytes"]
    if row["size_shards"] is not None:
        size["shards"] = row["size_shards"]
    if size:
        d["size"] = size

    return d


def row_to_schema(row) -> dict[str, Any]:
    uri = make_at_uri(row["did"], "science.alt.dataset.schema", row["rkey"])
    schema_body = row["schema_body"]
    if isinstance(schema_body, str):
        schema_body = json.loads(schema_body)

    d: dict[str, Any] = {
        "uri": uri,
        "cid": row["cid"],
        "did": row["did"],
        "name": row["name"],
        "version": row["version"],
        "schemaType": row["schema_type"],
        "schema": schema_body,
        "createdAt": row["created_at"],
    }
    if row["description"]:
        d["description"] = row["description"]
    return d


def row_to_label(row) -> dict[str, Any]:
    uri = make_at_uri(row["did"], "science.alt.dataset.label", row["rkey"])
    d: dict[str, Any] = {
        "uri": uri,
        "cid": row["cid"],
        "did": row["did"],
        "name": row["name"],
        "datasetUri": row["dataset_uri"],
        "createdAt": row["created_at"],
    }
    if row["version"]:
        d["version"] = row["version"]
    if row["description"]:
        d["description"] = row["description"]
    return d


def row_to_index_provider(row) -> dict[str, Any]:
    uri = make_at_uri(row["did"], "science.alt.dataset.index", row["rkey"])
    d: dict[str, Any] = {
        "uri": uri,
        "cid": row["cid"],
        "did": row["did"],
        "name": row["name"],
        "endpointUrl": row["endpoint_url"],
        "createdAt": row["created_at"],
    }
    if row["description"]:
        d["description"] = row["description"]
    return d


def row_to_lens(row) -> dict[str, Any]:
    uri = make_at_uri(row["did"], "science.alt.dataset.lens", row["rkey"])
    getter_code = row["getter_code"]
    putter_code = row["putter_code"]
    if isinstance(getter_code, str):
        getter_code = json.loads(getter_code)
    if isinstance(putter_code, str):
        putter_code = json.loads(putter_code)

    d: dict[str, Any] = {
        "uri": uri,
        "cid": row["cid"],
        "did": row["did"],
        "name": row["name"],
        "sourceSchema": row["source_schema"],
        "targetSchema": row["target_schema"],
        "getterCode": getter_code,
        "putterCode": putter_code,
        "createdAt": row["created_at"],
    }
    if row["description"]:
        d["description"] = row["description"]
    if row["language"]:
        d["language"] = row["language"]
    return d


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class ResolveLabelResponse(BaseModel):
    uri: str
    cid: str
    label: dict[str, Any]


class ResolveSchemaResponse(BaseModel):
    uri: str
    cid: str
    record: dict[str, Any]


class ResolveBlobsResponse(BaseModel):
    blobs: list[dict[str, Any]]


class GetEntryResponse(BaseModel):
    entry: dict[str, Any]


class GetEntriesResponse(BaseModel):
    entries: list[dict[str, Any]]


class ListEntriesResponse(BaseModel):
    entries: list[dict[str, Any]]
    cursor: str | None = None


class ListSchemasResponse(BaseModel):
    schemas: list[dict[str, Any]]
    cursor: str | None = None


class ListLensesResponse(BaseModel):
    lenses: list[dict[str, Any]]
    cursor: str | None = None


class SearchDatasetsResponse(BaseModel):
    entries: list[dict[str, Any]]
    cursor: str | None = None


class SearchLensesResponse(BaseModel):
    lenses: list[dict[str, Any]]
    cursor: str | None = None


class DescribeServiceResponse(BaseModel):
    did: str
    availableCollections: list[str]
    recordCount: dict[str, int]
    analytics: dict[str, Any] | None = None


class GetAnalyticsResponse(BaseModel):
    totalViews: int
    totalSearches: int
    topDatasets: list[dict[str, Any]]
    topSearchTerms: list[dict[str, Any]]
    recordCounts: dict[str, int]


class GetEntryStatsResponse(BaseModel):
    views: int
    searchAppearances: int
    period: str


class ListIndexesResponse(BaseModel):
    indexes: list[dict[str, Any]]
    cursor: str | None = None


class IndexSkeletonResponse(BaseModel):
    items: list[dict[str, Any]]
    cursor: str | None = None


class IndexResponse(BaseModel):
    items: list[dict[str, Any]]
    cursor: str | None = None
