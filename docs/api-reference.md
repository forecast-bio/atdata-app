# API Reference

All XRPC endpoints are mounted at `/xrpc/`. Queries are `GET`, procedures are `POST`.

## Queries

| Endpoint | Parameters | Description |
|---|---|---|
| `resolveLabel` | `handle`, `name`, `version?` | Resolve a named label to its dataset URI |
| `resolveSchema` | `handle`, `schemaId`, `version?` | Resolve a schema by ID, optionally pinned to a version |
| `resolveBlobs` | `uris` (max 25) | Resolve blob storage URIs to downloadable PDS blob URLs |
| `getEntry` | `uri` | Get a single dataset entry by AT-URI |
| `getEntries` | `uris` (max 25) | Batch-get multiple dataset entries |
| `getSchema` | `uri` | Get a single schema by AT-URI |
| `listEntries` | `repo?`, `limit?`, `cursor?` | Paginated list of dataset entries |
| `listSchemas` | `repo?`, `limit?`, `cursor?` | Paginated list of schemas |
| `listLenses` | `repo?`, `sourceSchema?`, `targetSchema?`, `limit?`, `cursor?` | Paginated list of lenses, filterable by schema |
| `searchDatasets` | `q`, `tags?`, `schemaRef?`, `repo?`, `limit?`, `cursor?` | Full-text search over dataset entries |
| `searchLenses` | `sourceSchema?`, `targetSchema?`, `limit?`, `cursor?` | Search lenses by source/target schema |
| `describeService` | — | Service DID, available collections, and record counts |

All `handle` parameters accept either a handle (e.g., `alice.bsky.social`) or a DID (e.g., `did:plc:abc123`). Paginated endpoints support keyset cursor pagination.

## Procedures

Procedures require two auth headers:
- `Authorization: Bearer <service-auth-jwt>` — ATProto service auth JWT, verified against the caller's signing key
- `X-PDS-Auth: <pds-access-token>` — PDS access token for proxying `createRecord` to the caller's PDS

| Endpoint | Body | Description |
|---|---|---|
| `publishSchema` | `{record, rkey?}` | Validate and publish a schema record |
| `publishDataset` | `{record, rkey?}` | Validate schema reference and storage type, then publish a dataset entry |
| `publishLabel` | `{record, rkey?}` | Validate dataset reference, then publish a label |
| `publishLens` | `{record, rkey?}` | Validate both schema references, then publish a lens |

Each procedure validates referential integrity (e.g., a dataset's `schemaRef` must point to an existing schema), sets the `$type` field, then proxies `com.atproto.repo.createRecord` to the caller's PDS. The record is then picked up by the firehose and indexed.

## Other Routes

| Route | Description |
|---|---|
| `GET /.well-known/did.json` | DID document for `did:web` identity |
| `GET /health` | Health check (`{"status": "ok"}`) |
