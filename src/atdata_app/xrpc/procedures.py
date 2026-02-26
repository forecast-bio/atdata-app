"""XRPC procedure (POST) endpoint handlers.

Each procedure validates the record against the AppView's indexed state,
then proxies ``com.atproto.repo.createRecord`` to the caller's PDS.
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, HTTPException, Request

from atdata_app import get_resolver
from atdata_app.auth import verify_service_auth
from atdata_app.database import (
    fire_analytics_event,
    query_get_entry,
    query_get_schema,
    query_record_exists,
)
from atdata_app.models import parse_at_uri

logger = logging.getLogger(__name__)

router = APIRouter()


async def _resolve_pds(did: str) -> str:
    resolver = get_resolver()
    data = await resolver.did.resolve_atproto_data(did)
    if not data or not data.pds:
        raise HTTPException(status_code=502, detail=f"Could not resolve PDS for {did}")
    return data.pds


async def _proxy_create_record(
    pds: str,
    pds_token: str,
    did: str,
    collection: str,
    record: dict[str, Any],
    rkey: str | None = None,
) -> dict[str, Any]:
    """Call com.atproto.repo.createRecord on the user's PDS."""
    body: dict[str, Any] = {
        "repo": did,
        "collection": collection,
        "record": record,
        "validate": False,
    }
    if rkey:
        body["rkey"] = rkey

    async with httpx.AsyncClient(timeout=30.0) as http:
        resp = await http.post(
            f"{pds}/xrpc/com.atproto.repo.createRecord",
            json=body,
            headers={"Authorization": f"Bearer {pds_token}"},
        )
        if resp.status_code != 200:
            raise HTTPException(
                status_code=502,
                detail=f"PDS createRecord failed: {resp.status_code} {resp.text}",
            )
        return resp.json()


def _require_pds_token(request: Request) -> str:
    token = request.headers.get("X-PDS-Auth", "")
    if not token:
        raise HTTPException(
            status_code=400,
            detail="Missing X-PDS-Auth header (PDS access token required for procedures)",
        )
    return token


# ---------------------------------------------------------------------------
# publishSchema
# ---------------------------------------------------------------------------


@router.post("/science.alt.dataset.publishSchema")
async def publish_schema(request: Request) -> dict[str, Any]:
    auth = await verify_service_auth(request, "science.alt.dataset.publishSchema")
    pds_token = _require_pds_token(request)
    pool = request.app.state.db_pool

    body = await request.json()
    record = body.get("record", {})
    rkey = body.get("rkey")

    # Validate $type
    record_type = record.get("$type", "")
    if record_type and record_type != "science.alt.dataset.schema":
        raise HTTPException(status_code=400, detail="Invalid $type for schema")

    # Validate required fields
    for field in ("name", "version", "schemaType", "schema", "createdAt"):
        if field not in record:
            raise HTTPException(status_code=400, detail=f"Missing required field: {field}")

    # Check for version conflict if rkey is specified
    if rkey:
        exists = await query_record_exists(pool, "schemas", auth.iss, rkey)
        if exists:
            raise HTTPException(
                status_code=409,
                detail=f"Schema record already exists at rkey {rkey}",
            )

    # Ensure $type is set
    record["$type"] = "science.alt.dataset.schema"

    # Proxy to PDS
    pds = await _resolve_pds(auth.iss)
    result = await _proxy_create_record(
        pds, pds_token, auth.iss, "science.alt.dataset.schema", record, rkey
    )
    return {"uri": result.get("uri"), "cid": result.get("cid")}


# ---------------------------------------------------------------------------
# publishDataset
# ---------------------------------------------------------------------------


@router.post("/science.alt.dataset.publishDataset")
async def publish_dataset(request: Request) -> dict[str, Any]:
    auth = await verify_service_auth(request, "science.alt.dataset.publishDataset")
    pds_token = _require_pds_token(request)
    pool = request.app.state.db_pool

    body = await request.json()
    record = body.get("record", {})
    rkey = body.get("rkey")

    record_type = record.get("$type", "")
    if record_type and record_type != "science.alt.dataset.entry":
        raise HTTPException(status_code=400, detail="Invalid $type for dataset")

    for field in ("name", "schemaRef", "storage", "createdAt"):
        if field not in record:
            raise HTTPException(status_code=400, detail=f"Missing required field: {field}")

    # Validate schemaRef exists
    schema_uri = record["schemaRef"]
    try:
        s_did, s_col, s_rkey = parse_at_uri(schema_uri)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid schemaRef URI: {schema_uri}")
    schema_row = await query_get_schema(pool, s_did, s_rkey)
    if not schema_row:
        raise HTTPException(
            status_code=400,
            detail=f"Referenced schema not found: {schema_uri}",
        )

    # Validate storage $type
    storage = record.get("storage", {})
    valid_storage_types = {
        "science.alt.dataset.storageHttp",
        "science.alt.dataset.storageS3",
        "science.alt.dataset.storageBlobs",
    }
    if storage.get("$type") not in valid_storage_types:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid storage $type: {storage.get('$type')}",
        )

    record["$type"] = "science.alt.dataset.entry"

    pds = await _resolve_pds(auth.iss)
    result = await _proxy_create_record(
        pds, pds_token, auth.iss, "science.alt.dataset.entry", record, rkey
    )
    return {"uri": result.get("uri"), "cid": result.get("cid")}


# ---------------------------------------------------------------------------
# publishLabel
# ---------------------------------------------------------------------------


@router.post("/science.alt.dataset.publishLabel")
async def publish_label(request: Request) -> dict[str, Any]:
    auth = await verify_service_auth(request, "science.alt.dataset.publishLabel")
    pds_token = _require_pds_token(request)
    pool = request.app.state.db_pool

    body = await request.json()
    record = body.get("record", {})
    rkey = body.get("rkey")

    record_type = record.get("$type", "")
    if record_type and record_type != "science.alt.dataset.label":
        raise HTTPException(status_code=400, detail="Invalid $type for label")

    for field in ("name", "datasetUri", "createdAt"):
        if field not in record:
            raise HTTPException(status_code=400, detail=f"Missing required field: {field}")

    # Validate datasetUri exists
    ds_uri = record["datasetUri"]
    try:
        d_did, d_col, d_rkey = parse_at_uri(ds_uri)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid datasetUri: {ds_uri}")
    ds_row = await query_get_entry(pool, d_did, d_rkey)
    if not ds_row:
        raise HTTPException(
            status_code=400,
            detail=f"Referenced dataset not found: {ds_uri}",
        )

    record["$type"] = "science.alt.dataset.label"

    pds = await _resolve_pds(auth.iss)
    result = await _proxy_create_record(
        pds, pds_token, auth.iss, "science.alt.dataset.label", record, rkey
    )
    return {"uri": result.get("uri"), "cid": result.get("cid")}


# ---------------------------------------------------------------------------
# publishLens
# ---------------------------------------------------------------------------


@router.post("/science.alt.dataset.publishLens")
async def publish_lens(request: Request) -> dict[str, Any]:
    auth = await verify_service_auth(request, "science.alt.dataset.publishLens")
    pds_token = _require_pds_token(request)
    pool = request.app.state.db_pool

    body = await request.json()
    record = body.get("record", {})
    rkey = body.get("rkey")

    record_type = record.get("$type", "")
    if record_type and record_type != "science.alt.dataset.lens":
        raise HTTPException(status_code=400, detail="Invalid $type for lens")

    for field in ("name", "sourceSchema", "targetSchema", "getterCode", "putterCode", "createdAt"):
        if field not in record:
            raise HTTPException(status_code=400, detail=f"Missing required field: {field}")

    # Validate both schema URIs exist
    for field_name in ("sourceSchema", "targetSchema"):
        uri = record[field_name]
        try:
            s_did, s_col, s_rkey = parse_at_uri(uri)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid {field_name} URI: {uri}")
        exists = await query_record_exists(pool, "schemas", s_did, s_rkey)
        if not exists:
            raise HTTPException(
                status_code=400,
                detail=f"Referenced schema not found: {uri}",
            )

    record["$type"] = "science.alt.dataset.lens"

    pds = await _resolve_pds(auth.iss)
    result = await _proxy_create_record(
        pds, pds_token, auth.iss, "science.alt.dataset.lens", record, rkey
    )
    return {"uri": result.get("uri"), "cid": result.get("cid")}


# ---------------------------------------------------------------------------
# publishIndex
# ---------------------------------------------------------------------------


@router.post("/science.alt.dataset.publishIndex")
async def publish_index(request: Request) -> dict[str, Any]:
    auth = await verify_service_auth(request, "science.alt.dataset.publishIndex")
    pds_token = _require_pds_token(request)

    body = await request.json()
    record = body.get("record", {})
    rkey = body.get("rkey")

    record_type = record.get("$type", "")
    if record_type and record_type != "science.alt.dataset.index":
        raise HTTPException(status_code=400, detail="Invalid $type for index")

    for field in ("name", "endpointUrl", "createdAt"):
        if field not in record:
            raise HTTPException(status_code=400, detail=f"Missing required field: {field}")

    # Validate endpoint URL is HTTPS
    parsed = urlparse(record["endpointUrl"])
    if parsed.scheme != "https" or not parsed.netloc:
        raise HTTPException(
            status_code=400, detail="endpointUrl must be a valid HTTPS URL"
        )

    record["$type"] = "science.alt.dataset.index"

    pds = await _resolve_pds(auth.iss)
    result = await _proxy_create_record(
        pds, pds_token, auth.iss, "science.alt.dataset.index", record, rkey
    )
    return {"uri": result.get("uri"), "cid": result.get("cid")}


# ---------------------------------------------------------------------------
# sendInteractions
# ---------------------------------------------------------------------------

_VALID_INTERACTION_TYPES = frozenset({"download", "citation", "derivative"})
_MAX_INTERACTIONS_BATCH = 100


@router.post("/science.alt.dataset.sendInteractions")
async def send_interactions(request: Request) -> dict[str, Any]:
    """Record dataset interaction events (anonymous, fire-and-forget)."""
    await verify_service_auth(request, "science.alt.dataset.sendInteractions")

    pool = request.app.state.db_pool

    body = await request.json()
    interactions = body.get("interactions")
    if not isinstance(interactions, list):
        raise HTTPException(status_code=400, detail="interactions must be an array")

    if len(interactions) > _MAX_INTERACTIONS_BATCH:
        raise HTTPException(
            status_code=400,
            detail=f"Batch size exceeds maximum of {_MAX_INTERACTIONS_BATCH}",
        )

    for i, item in enumerate(interactions):
        if not isinstance(item, dict):
            raise HTTPException(status_code=400, detail=f"interactions[{i}]: must be an object")

        itype = item.get("type")
        if itype not in _VALID_INTERACTION_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"interactions[{i}]: invalid type '{itype}', "
                f"must be one of: {', '.join(sorted(_VALID_INTERACTION_TYPES))}",
            )

        dataset_uri = item.get("datasetUri")
        if not isinstance(dataset_uri, str):
            raise HTTPException(
                status_code=400, detail=f"interactions[{i}]: datasetUri is required"
            )
        try:
            parse_at_uri(dataset_uri)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"interactions[{i}]: invalid AT-URI: {dataset_uri}",
            )

    # All valid — fire analytics events
    for item in interactions:
        did, _collection, rkey = parse_at_uri(item["datasetUri"])
        fire_analytics_event(pool, item["type"], target_did=did, target_rkey=rkey)

    return {}
