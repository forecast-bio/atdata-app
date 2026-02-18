"""Frontend route handlers — server-rendered HTML via Jinja2."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from atdata_app.database import (
    COLLECTION_TABLE_MAP,
    query_get_entry,
    query_get_schema,
    query_labels_for_dataset,
    query_list_entries,
    query_list_schemas,
    query_record_counts,
    query_search_datasets,
)
from atdata_app.models import (
    decode_cursor,
    encode_cursor,
    row_to_entry,
    row_to_label,
    row_to_schema,
)

_FRONTEND_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(_FRONTEND_DIR / "templates"))

router = APIRouter()


def _entry_with_rkey(row) -> dict[str, Any]:
    """Serialize an entry row and attach ``rkey`` for URL construction."""
    d = row_to_entry(row)
    d["rkey"] = row["rkey"]
    return d


def _schema_with_rkey(row) -> dict[str, Any]:
    """Serialize a schema row and attach ``rkey`` for URL construction."""
    d = row_to_schema(row)
    d["rkey"] = row["rkey"]
    return d


def _parse_cursor(cursor: str | None) -> tuple[str | None, str | None, str | None]:
    if not cursor:
        return None, None, None
    return decode_cursor(cursor)


def _maybe_cursor(rows: list, limit: int) -> str | None:
    if len(rows) < limit:
        return None
    last = rows[-1]
    return encode_cursor(str(last["indexed_at"]), last["did"], last["rkey"])


# ---------------------------------------------------------------------------
# Home / Search
# ---------------------------------------------------------------------------


@router.get("/", response_class=HTMLResponse)
async def home(request: Request, q: str = "", cursor: str | None = None, tag: list[str] | None = None):
    pool = request.app.state.db_pool
    active_tags = tag or []
    entries: list[dict[str, Any]] = []
    next_cursor: str | None = None

    if q:
        limit = 25
        c_at, c_did, c_rkey = _parse_cursor(cursor)
        rows = await query_search_datasets(
            pool, q, active_tags or None, None, None, limit, c_did, c_rkey, c_at
        )
        entries = [_entry_with_rkey(r) for r in rows]
        next_cursor = _maybe_cursor(rows, limit)

    return templates.TemplateResponse(
        request,
        "home.html",
        {
            "q": q,
            "entries": entries,
            "cursor": next_cursor,
            "active_tags": active_tags,
            "all_tags": active_tags,
        },
    )


# ---------------------------------------------------------------------------
# Dataset detail
# ---------------------------------------------------------------------------


@router.get("/dataset/{did:path}/{rkey}", response_class=HTMLResponse)
async def dataset_detail(request: Request, did: str, rkey: str):
    pool = request.app.state.db_pool
    row = await query_get_entry(pool, did, rkey)
    if not row:
        raise HTTPException(status_code=404, detail="Dataset not found")

    entry = row_to_entry(row)

    # Parse schema ref for linking
    schema_did = ""
    schema_rkey = ""
    schema_ref = entry.get("schemaRef", "")
    if schema_ref.startswith("at://"):
        parts = schema_ref[5:].split("/", 2)
        if len(parts) == 3:
            schema_did, _, schema_rkey = parts

    # Fetch labels pointing to this dataset
    dataset_uri = entry["uri"]
    label_rows = await query_labels_for_dataset(pool, dataset_uri)
    labels = [row_to_label(r) for r in label_rows]

    return templates.TemplateResponse(
        request,
        "dataset.html",
        {
            "entry": entry,
            "schema_did": schema_did,
            "schema_rkey": schema_rkey,
            "labels": labels,
        },
    )


# ---------------------------------------------------------------------------
# Schema detail
# ---------------------------------------------------------------------------


@router.get("/schema/{did:path}/{rkey}", response_class=HTMLResponse)
async def schema_detail(request: Request, did: str, rkey: str):
    pool = request.app.state.db_pool
    row = await query_get_schema(pool, did, rkey)
    if not row:
        raise HTTPException(status_code=404, detail="Schema not found")

    schema = row_to_schema(row)
    schema_body_json = json.dumps(schema.get("schema", {}), indent=2)

    return templates.TemplateResponse(
        request,
        "schema.html",
        {
            "schema": schema,
            "schema_body_json": schema_body_json,
        },
    )


# ---------------------------------------------------------------------------
# Schemas list
# ---------------------------------------------------------------------------


@router.get("/schemas", response_class=HTMLResponse)
async def schemas_list(request: Request, cursor: str | None = None):
    pool = request.app.state.db_pool
    limit = 50
    c_at, c_did, c_rkey = _parse_cursor(cursor)
    rows = await query_list_schemas(pool, None, limit, c_did, c_rkey, c_at)
    schemas = [_schema_with_rkey(r) for r in rows]
    next_cursor = _maybe_cursor(rows, limit)

    return templates.TemplateResponse(
        request,
        "schemas.html",
        {
            "schemas": schemas,
            "cursor": next_cursor,
        },
    )


# ---------------------------------------------------------------------------
# Publisher profile
# ---------------------------------------------------------------------------


@router.get("/profile/{handle_or_did:path}", response_class=HTMLResponse)
async def profile(request: Request, handle_or_did: str):
    pool = request.app.state.db_pool

    # Resolve handle to DID if needed
    did = handle_or_did
    if not did.startswith("did:"):
        from atdata_app import get_resolver

        resolver = get_resolver()
        resolved = await resolver.handle.resolve(did)
        if resolved is None:
            raise HTTPException(status_code=404, detail=f"Could not resolve handle: {did}")
        did = resolved

    entry_rows = await query_list_entries(pool, did, 50)
    schema_rows = await query_list_schemas(pool, did, 50)

    entries = [_entry_with_rkey(r) for r in entry_rows]
    schemas = [_schema_with_rkey(r) for r in schema_rows]

    return templates.TemplateResponse(
        request,
        "profile.html",
        {
            "did": did,
            "entries": entries,
            "schemas": schemas,
        },
    )


# ---------------------------------------------------------------------------
# About / Service info
# ---------------------------------------------------------------------------


@router.get("/about", response_class=HTMLResponse)
async def about(request: Request):
    config = request.app.state.config
    pool = request.app.state.db_pool
    counts = await query_record_counts(pool)

    service = {
        "did": config.service_did,
        "availableCollections": list(COLLECTION_TABLE_MAP.keys()),
        "recordCount": counts,
    }

    return templates.TemplateResponse(
        request,
        "about.html",
        {"service": service},
    )
