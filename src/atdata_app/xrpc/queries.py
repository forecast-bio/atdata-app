"""XRPC query (GET) endpoint handlers."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

import httpx

from atdata_app import get_resolver
from atdata_app.database import (
    COLLECTION_TABLE_MAP,
    fire_analytics_event,
    query_active_publishers,
    query_analytics_summary,
    query_entry_stats,
    query_get_entries,
    query_get_entry,
    query_get_index_provider,
    query_get_schema,
    query_list_entries,
    query_list_index_providers,
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
    GetAnalyticsResponse,
    GetEntriesResponse,
    GetEntryResponse,
    GetEntryStatsResponse,
    IndexResponse,
    IndexSkeletonResponse,
    ListEntriesResponse,
    ListIndexesResponse,
    ListLensesResponse,
    ListSchemasResponse,
    ResolveBlobsResponse,
    ResolveLabelResponse,
    ResolveSchemaResponse,
    SearchDatasetsResponse,
    SearchLensesResponse,
    maybe_cursor,
    parse_at_uri,
    parse_cursor,
    row_to_entry,
    row_to_index_provider,
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


@router.get("/science.alt.dataset.resolveLabel")
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


@router.get("/science.alt.dataset.resolveSchema")
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


@router.get("/science.alt.dataset.resolveBlobs")
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


@router.get("/science.alt.dataset.getEntry")
async def get_entry(
    request: Request,
    uri: str = Query(...),
) -> GetEntryResponse:
    pool = request.app.state.db_pool
    try:
        did, _, rkey = parse_at_uri(uri)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid AT-URI")
    row = await query_get_entry(pool, did, rkey)
    if not row:
        raise HTTPException(status_code=404, detail="Entry not found")
    fire_analytics_event(pool, "view_entry", target_did=did, target_rkey=rkey)
    return GetEntryResponse(entry=row_to_entry(row))


@router.get("/science.alt.dataset.getEntries")
async def get_entries(
    request: Request,
    uris: list[str] = Query(..., max_length=25),
) -> GetEntriesResponse:
    pool = request.app.state.db_pool
    keys = []
    for uri in uris:
        try:
            did, _, rkey = parse_at_uri(uri)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid AT-URI: {uri}")
        keys.append((did, rkey))
    rows = await query_get_entries(pool, keys)
    for did, rkey in keys:
        fire_analytics_event(pool, "view_entry", target_did=did, target_rkey=rkey)
    return GetEntriesResponse(entries=[row_to_entry(r) for r in rows])


# ---------------------------------------------------------------------------
# getSchema
# ---------------------------------------------------------------------------


@router.get("/science.alt.dataset.getSchema")
async def get_schema(
    request: Request,
    uri: str = Query(...),
) -> dict[str, Any]:
    pool = request.app.state.db_pool
    try:
        did, _, rkey = parse_at_uri(uri)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid AT-URI")
    row = await query_get_schema(pool, did, rkey)
    if not row:
        raise HTTPException(status_code=404, detail="Schema not found")
    fire_analytics_event(pool, "view_schema", target_did=did, target_rkey=rkey)
    return row_to_schema(row)


# ---------------------------------------------------------------------------
# listEntries / listSchemas / listLenses
# ---------------------------------------------------------------------------


@router.get("/science.alt.dataset.listEntries")
async def list_entries(
    request: Request,
    repo: str | None = Query(None),
    limit: int = Query(50, ge=1, le=100),
    cursor: str | None = Query(None),
) -> ListEntriesResponse:
    pool = request.app.state.db_pool
    c_at, c_did, c_rkey = parse_cursor(cursor)
    rows = await query_list_entries(pool, repo, limit, c_did, c_rkey, c_at)
    fire_analytics_event(pool, "list_entries", query_params={"repo": repo} if repo else None)
    return ListEntriesResponse(
        entries=[row_to_entry(r) for r in rows],
        cursor=maybe_cursor(rows, limit),
    )


@router.get("/science.alt.dataset.listSchemas")
async def list_schemas(
    request: Request,
    repo: str | None = Query(None),
    limit: int = Query(50, ge=1, le=100),
    cursor: str | None = Query(None),
) -> ListSchemasResponse:
    pool = request.app.state.db_pool
    c_at, c_did, c_rkey = parse_cursor(cursor)
    rows = await query_list_schemas(pool, repo, limit, c_did, c_rkey, c_at)
    fire_analytics_event(pool, "list_schemas", query_params={"repo": repo} if repo else None)
    return ListSchemasResponse(
        schemas=[row_to_schema(r) for r in rows],
        cursor=maybe_cursor(rows, limit),
    )


@router.get("/science.alt.dataset.listLenses")
async def list_lenses(
    request: Request,
    repo: str | None = Query(None),
    sourceSchema: str | None = Query(None),
    targetSchema: str | None = Query(None),
    limit: int = Query(50, ge=1, le=100),
    cursor: str | None = Query(None),
) -> ListLensesResponse:
    pool = request.app.state.db_pool
    c_at, c_did, c_rkey = parse_cursor(cursor)
    rows = await query_list_lenses(
        pool, repo, sourceSchema, targetSchema, limit, c_did, c_rkey, c_at
    )
    fire_analytics_event(pool, "list_lenses", query_params={"repo": repo} if repo else None)
    return ListLensesResponse(
        lenses=[row_to_lens(r) for r in rows],
        cursor=maybe_cursor(rows, limit),
    )


# ---------------------------------------------------------------------------
# searchDatasets / searchLenses
# ---------------------------------------------------------------------------


@router.get("/science.alt.dataset.searchDatasets")
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
    c_at, c_did, c_rkey = parse_cursor(cursor)
    rows = await query_search_datasets(
        pool, q, tags, schemaRef, repo, limit, c_did, c_rkey, c_at
    )
    fire_analytics_event(pool, "search", query_params={"q": q, "tags": tags})
    return SearchDatasetsResponse(
        entries=[row_to_entry(r) for r in rows],
        cursor=maybe_cursor(rows, limit),
    )


@router.get("/science.alt.dataset.searchLenses")
async def search_lenses(
    request: Request,
    sourceSchema: str | None = Query(None),
    targetSchema: str | None = Query(None),
    limit: int = Query(25, ge=1, le=100),
    cursor: str | None = Query(None),
) -> SearchLensesResponse:
    pool = request.app.state.db_pool
    c_at, c_did, c_rkey = parse_cursor(cursor)
    rows = await query_search_lenses(
        pool, sourceSchema, targetSchema, limit, c_did, c_rkey, c_at
    )
    return SearchLensesResponse(
        lenses=[row_to_lens(r) for r in rows],
        cursor=maybe_cursor(rows, limit),
    )


# ---------------------------------------------------------------------------
# listIndexes
# ---------------------------------------------------------------------------


@router.get("/science.alt.dataset.listIndexes")
async def list_indexes(
    request: Request,
    repo: str | None = Query(None),
    limit: int = Query(50, ge=1, le=100),
    cursor: str | None = Query(None),
) -> ListIndexesResponse:
    pool = request.app.state.db_pool
    c_at, c_did, c_rkey = parse_cursor(cursor)
    rows = await query_list_index_providers(pool, repo, limit, c_did, c_rkey, c_at)
    return ListIndexesResponse(
        indexes=[row_to_index_provider(r) for r in rows],
        cursor=maybe_cursor(rows, limit),
    )


# ---------------------------------------------------------------------------
# getIndexSkeleton
# ---------------------------------------------------------------------------


async def _fetch_skeleton(
    endpoint_url: str,
    cursor: str | None,
    limit: int,
) -> dict[str, Any]:
    """Fetch skeleton from an upstream index provider."""
    params: dict[str, Any] = {"limit": limit}
    if cursor:
        params["cursor"] = cursor
    async with httpx.AsyncClient(timeout=10.0) as http:
        try:
            resp = await http.get(endpoint_url, params=params)
        except httpx.HTTPError as e:
            raise HTTPException(
                status_code=502, detail=f"Index provider unreachable: {e}"
            ) from e
    if resp.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"Index provider returned {resp.status_code}",
        )
    try:
        data = resp.json()
    except (ValueError, KeyError) as e:
        raise HTTPException(
            status_code=502, detail=f"Invalid response from index provider: {e}"
        ) from e
    if not isinstance(data.get("items"), list):
        raise HTTPException(
            status_code=502, detail="Index provider response missing 'items' array"
        )
    return data


@router.get("/science.alt.dataset.getIndexSkeleton")
async def get_index_skeleton(
    request: Request,
    index: str = Query(...),
    cursor: str | None = Query(None),
    limit: int = Query(50, ge=1, le=100),
) -> IndexSkeletonResponse:
    pool = request.app.state.db_pool
    try:
        did, _, rkey = parse_at_uri(index)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid AT-URI for index")
    provider = await query_get_index_provider(pool, did, rkey)
    if not provider:
        raise HTTPException(status_code=404, detail="Index provider not found")

    data = await _fetch_skeleton(provider["endpoint_url"], cursor, limit)
    return IndexSkeletonResponse(
        items=data["items"],
        cursor=data.get("cursor"),
    )


# ---------------------------------------------------------------------------
# getIndex (hydrated)
# ---------------------------------------------------------------------------


@router.get("/science.alt.dataset.getIndex")
async def get_index(
    request: Request,
    index: str = Query(...),
    cursor: str | None = Query(None),
    limit: int = Query(50, ge=1, le=100),
) -> IndexResponse:
    pool = request.app.state.db_pool
    try:
        did, _, rkey = parse_at_uri(index)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid AT-URI for index")
    provider = await query_get_index_provider(pool, did, rkey)
    if not provider:
        raise HTTPException(status_code=404, detail="Index provider not found")

    data = await _fetch_skeleton(provider["endpoint_url"], cursor, limit)

    # Parse URIs from skeleton items and hydrate
    keys: list[tuple[str, str]] = []
    for item in data["items"]:
        uri = item.get("uri", "")
        try:
            entry_did, _, entry_rkey = parse_at_uri(uri)
            keys.append((entry_did, entry_rkey))
        except ValueError:
            continue  # skip malformed URIs

    rows = await query_get_entries(pool, keys)

    # Build a lookup map to preserve skeleton ordering
    row_map = {(r["did"], r["rkey"]): r for r in rows}
    hydrated = []
    for entry_did, entry_rkey in keys:
        row = row_map.get((entry_did, entry_rkey))
        if row:
            hydrated.append(row_to_entry(row))

    return IndexResponse(
        items=hydrated,
        cursor=data.get("cursor"),
    )


# ---------------------------------------------------------------------------
# describeService
# ---------------------------------------------------------------------------


@router.get("/science.alt.dataset.describeService")
async def describe_service(request: Request) -> DescribeServiceResponse:
    config = request.app.state.config
    pool = request.app.state.db_pool
    counts = await query_record_counts(pool)
    fire_analytics_event(pool, "describe")

    # Analytics summary for describeService
    summary = await query_analytics_summary(pool, "month")
    active_publishers = await query_active_publishers(pool, 30)

    return DescribeServiceResponse(
        did=config.service_did,
        availableCollections=list(COLLECTION_TABLE_MAP.keys()),
        recordCount=counts,
        analytics={
            "totalViews": summary["totalViews"],
            "totalSearches": summary["totalSearches"],
            "activePublishers": active_publishers,
        },
    )


# ---------------------------------------------------------------------------
# getAnalytics
# ---------------------------------------------------------------------------


@router.get("/science.alt.dataset.getAnalytics")
async def get_analytics(
    request: Request,
    period: str = Query("week", pattern="^(day|week|month)$"),
) -> GetAnalyticsResponse:
    pool = request.app.state.db_pool
    summary = await query_analytics_summary(pool, period)
    return GetAnalyticsResponse(**summary)


# ---------------------------------------------------------------------------
# getEntryStats
# ---------------------------------------------------------------------------


@router.get("/science.alt.dataset.getEntryStats")
async def get_entry_stats(
    request: Request,
    uri: str = Query(...),
    period: str = Query("week", pattern="^(day|week|month)$"),
) -> GetEntryStatsResponse:
    pool = request.app.state.db_pool
    try:
        did, collection, rkey = parse_at_uri(uri)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid AT-URI")
    stats = await query_entry_stats(pool, did, rkey, period)
    return GetEntryStatsResponse(**stats)
