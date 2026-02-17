"""XRPC query (GET) endpoint handlers."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from atdata_app import get_resolver
from atdata_app.database import (
    COLLECTION_TABLE_MAP,
    query_get_entries,
    query_get_entry,
    query_get_schema,
    query_list_entries,
    query_list_lenses,
    query_list_schemas,
    query_record_counts,
    query_resolve_label,
    query_resolve_schema,
    query_search_datasets,
    query_search_lenses,
)
from atdata_app.models import (
    DescribeServiceResponse,
    GetEntriesResponse,
    GetEntryResponse,
    ListEntriesResponse,
    ListLensesResponse,
    ListSchemasResponse,
    ResolveBlobsResponse,
    ResolveLabelResponse,
    ResolveSchemaResponse,
    SearchDatasetsResponse,
    SearchLensesResponse,
    decode_cursor,
    encode_cursor,
    parse_at_uri,
    row_to_entry,
    row_to_label,
    row_to_lens,
    row_to_schema,
)

router = APIRouter()


async def _resolve_handle(handle: str) -> str:
    """Resolve a handle or DID to a DID. Handles pass through if already a DID."""
    if handle.startswith("did:"):
        return handle
    resolver = get_resolver()
    did = await resolver.handle.resolve(handle)
    if did is None:
        raise HTTPException(status_code=400, detail=f"Could not resolve handle: {handle}")
    return did


# ---------------------------------------------------------------------------
# resolveLabel
# ---------------------------------------------------------------------------


@router.get("/ac.foundation.dataset.resolveLabel")
async def resolve_label(
    request: Request,
    handle: str = Query(...),
    name: str = Query(...),
    version: str | None = Query(None),
) -> ResolveLabelResponse:
    pool = request.app.state.db_pool
    did = await _resolve_handle(handle)
    row = await query_resolve_label(pool, did, name, version)
    if not row:
        raise HTTPException(status_code=404, detail="Label not found")

    label_dict = row_to_label(row)
    return ResolveLabelResponse(
        uri=label_dict["datasetUri"],
        cid=row["cid"] or "",
        label=label_dict,
    )


# ---------------------------------------------------------------------------
# resolveSchema
# ---------------------------------------------------------------------------


@router.get("/ac.foundation.dataset.resolveSchema")
async def resolve_schema(
    request: Request,
    handle: str = Query(...),
    schemaId: str = Query(...),
    version: str | None = Query(None),
) -> ResolveSchemaResponse:
    pool = request.app.state.db_pool
    did = await _resolve_handle(handle)
    row = await query_resolve_schema(pool, did, schemaId, version)
    if not row:
        raise HTTPException(status_code=404, detail="Schema not found")

    schema_dict = row_to_schema(row)
    return ResolveSchemaResponse(
        uri=schema_dict["uri"],
        cid=row["cid"] or "",
        record=schema_dict,
    )


# ---------------------------------------------------------------------------
# resolveBlobs
# ---------------------------------------------------------------------------


@router.get("/ac.foundation.dataset.resolveBlobs")
async def resolve_blobs(
    request: Request,
    uris: list[str] = Query(..., max_length=25),
) -> ResolveBlobsResponse:
    pool = request.app.state.db_pool
    resolver = get_resolver()
    results: list[dict[str, Any]] = []

    for uri in uris:
        try:
            did, collection, rkey = parse_at_uri(uri)
        except ValueError:
            results.append({"uri": uri, "error": "Invalid AT-URI"})
            continue

        row = await query_get_entry(pool, did, rkey)
        if not row:
            results.append({"uri": uri, "error": "Entry not found"})
            continue

        storage = row["storage"]
        if isinstance(storage, str):
            storage = json.loads(storage)

        storage_type = storage.get("$type", "")
        if "storageBlobs" not in storage_type:
            results.append({"uri": uri, "error": "Not blob storage"})
            continue

        # Resolve PDS endpoint for blob URLs
        try:
            atproto_data = await resolver.did.resolve_atproto_data(did)
            pds = atproto_data.pds if atproto_data else None
        except Exception:
            pds = None

        if not pds:
            results.append({"uri": uri, "error": "Could not resolve PDS"})
            continue

        for blob_entry in storage.get("blobs", []):
            blob_ref = blob_entry.get("blob", {})
            cid = blob_ref.get("ref", {}).get("$link", "")
            if cid:
                blob_url = f"{pds}/xrpc/com.atproto.sync.getBlob?did={did}&cid={cid}"
                results.append({"uri": uri, "cid": cid, "url": blob_url})

    return ResolveBlobsResponse(blobs=results)


# ---------------------------------------------------------------------------
# getEntry / getEntries
# ---------------------------------------------------------------------------


@router.get("/ac.foundation.dataset.getEntry")
async def get_entry(
    request: Request,
    uri: str = Query(...),
) -> GetEntryResponse:
    pool = request.app.state.db_pool
    did, collection, rkey = parse_at_uri(uri)
    row = await query_get_entry(pool, did, rkey)
    if not row:
        raise HTTPException(status_code=404, detail="Entry not found")
    return GetEntryResponse(entry=row_to_entry(row))


@router.get("/ac.foundation.dataset.getEntries")
async def get_entries(
    request: Request,
    uris: list[str] = Query(..., max_length=25),
) -> GetEntriesResponse:
    pool = request.app.state.db_pool
    keys = []
    for uri in uris:
        did, _, rkey = parse_at_uri(uri)
        keys.append((did, rkey))
    rows = await query_get_entries(pool, keys)
    return GetEntriesResponse(entries=[row_to_entry(r) for r in rows])


# ---------------------------------------------------------------------------
# getSchema
# ---------------------------------------------------------------------------


@router.get("/ac.foundation.dataset.getSchema")
async def get_schema(
    request: Request,
    uri: str = Query(...),
) -> dict[str, Any]:
    pool = request.app.state.db_pool
    did, collection, rkey = parse_at_uri(uri)
    row = await query_get_schema(pool, did, rkey)
    if not row:
        raise HTTPException(status_code=404, detail="Schema not found")
    return row_to_schema(row)


# ---------------------------------------------------------------------------
# listEntries / listSchemas / listLenses
# ---------------------------------------------------------------------------


def _parse_cursor(cursor: str | None) -> tuple[str | None, str | None, str | None]:
    if not cursor:
        return None, None, None
    return decode_cursor(cursor)


def _maybe_cursor(rows: list, limit: int) -> str | None:
    if len(rows) < limit:
        return None
    last = rows[-1]
    return encode_cursor(str(last["indexed_at"]), last["did"], last["rkey"])


@router.get("/ac.foundation.dataset.listEntries")
async def list_entries(
    request: Request,
    repo: str | None = Query(None),
    limit: int = Query(50, ge=1, le=100),
    cursor: str | None = Query(None),
) -> ListEntriesResponse:
    pool = request.app.state.db_pool
    c_at, c_did, c_rkey = _parse_cursor(cursor)
    rows = await query_list_entries(pool, repo, limit, c_did, c_rkey, c_at)
    return ListEntriesResponse(
        entries=[row_to_entry(r) for r in rows],
        cursor=_maybe_cursor(rows, limit),
    )


@router.get("/ac.foundation.dataset.listSchemas")
async def list_schemas(
    request: Request,
    repo: str | None = Query(None),
    limit: int = Query(50, ge=1, le=100),
    cursor: str | None = Query(None),
) -> ListSchemasResponse:
    pool = request.app.state.db_pool
    c_at, c_did, c_rkey = _parse_cursor(cursor)
    rows = await query_list_schemas(pool, repo, limit, c_did, c_rkey, c_at)
    return ListSchemasResponse(
        schemas=[row_to_schema(r) for r in rows],
        cursor=_maybe_cursor(rows, limit),
    )


@router.get("/ac.foundation.dataset.listLenses")
async def list_lenses(
    request: Request,
    repo: str | None = Query(None),
    sourceSchema: str | None = Query(None),
    targetSchema: str | None = Query(None),
    limit: int = Query(50, ge=1, le=100),
    cursor: str | None = Query(None),
) -> ListLensesResponse:
    pool = request.app.state.db_pool
    c_at, c_did, c_rkey = _parse_cursor(cursor)
    rows = await query_list_lenses(
        pool, repo, sourceSchema, targetSchema, limit, c_did, c_rkey, c_at
    )
    return ListLensesResponse(
        lenses=[row_to_lens(r) for r in rows],
        cursor=_maybe_cursor(rows, limit),
    )


# ---------------------------------------------------------------------------
# searchDatasets / searchLenses
# ---------------------------------------------------------------------------


@router.get("/ac.foundation.dataset.searchDatasets")
async def search_datasets(
    request: Request,
    q: str = Query(...),
    tags: list[str] | None = Query(None),
    schemaRef: str | None = Query(None),
    repo: str | None = Query(None),
    limit: int = Query(25, ge=1, le=100),
    cursor: str | None = Query(None),
) -> SearchDatasetsResponse:
    pool = request.app.state.db_pool
    c_at, c_did, c_rkey = _parse_cursor(cursor)
    rows = await query_search_datasets(
        pool, q, tags, schemaRef, repo, limit, c_did, c_rkey, c_at
    )
    return SearchDatasetsResponse(
        entries=[row_to_entry(r) for r in rows],
        cursor=_maybe_cursor(rows, limit),
    )


@router.get("/ac.foundation.dataset.searchLenses")
async def search_lenses(
    request: Request,
    sourceSchema: str | None = Query(None),
    targetSchema: str | None = Query(None),
    limit: int = Query(25, ge=1, le=100),
    cursor: str | None = Query(None),
) -> SearchLensesResponse:
    pool = request.app.state.db_pool
    c_at, c_did, c_rkey = _parse_cursor(cursor)
    rows = await query_search_lenses(
        pool, sourceSchema, targetSchema, limit, c_did, c_rkey, c_at
    )
    return SearchLensesResponse(
        lenses=[row_to_lens(r) for r in rows],
        cursor=_maybe_cursor(rows, limit),
    )


# ---------------------------------------------------------------------------
# describeService
# ---------------------------------------------------------------------------


@router.get("/ac.foundation.dataset.describeService")
async def describe_service(request: Request) -> DescribeServiceResponse:
    config = request.app.state.config
    pool = request.app.state.db_pool
    counts = await query_record_counts(pool)
    return DescribeServiceResponse(
        did=config.service_did,
        availableCollections=list(COLLECTION_TABLE_MAP.keys()),
        recordCount=counts,
    )
