# atdata-app

An [ATProto AppView](https://atproto.com/guides/applications#appview) for the `ac.foundation.dataset` lexicon namespace. It indexes dataset metadata published across the AT Protocol network and serves it through XRPC endpoints — enabling discovery, search, and resolution of datasets, schemas, labels, and lenses.

## Overview

In the AT Protocol architecture, an AppView is a service that subscribes to the network firehose, indexes records it cares about, and exposes query endpoints for clients. atdata-app does this for scientific and ML dataset metadata:

- **Schemas** define the structure of datasets (JSON Schema, Arrow schema, etc.)
- **Dataset entries** describe a dataset — its name, storage location, schema, tags, license, and size
- **Labels** are human-readable version tags pointing to a specific dataset entry (like git tags)
- **Lenses** are bidirectional schema transforms with getter/putter code for migrating data between schema versions

```
ATProto Network
    │
    ├── Jetstream (WebSocket firehose) ──► Real-time ingestion
    │                                         │
    └── BGS Relay (HTTP backfill) ──────► Historical backfill
                                              │
                                              ▼
                                         PostgreSQL
                                              │
                                              ▼
                                     XRPC Query Endpoints ──► Clients
```

## Requirements

- Python 3.12+
- PostgreSQL 14+
- [uv](https://docs.astral.sh/uv/) package manager

## Quickstart

```bash
# Install dependencies
uv sync --dev

# Set up PostgreSQL (schema auto-applies on startup)
createdb atdata_app

# Start the server
uv run uvicorn atdata_app.main:app --reload
```

The server starts with dev-mode defaults: `http://localhost:8000`, DID `did:web:localhost%3A8000`. On startup it connects to Jetstream and begins indexing `ac.foundation.dataset.*` records, and runs a one-shot backfill of historical records from the BGS relay.

## Configuration

All settings are environment variables prefixed with `ATDATA_`, managed by [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/).

| Variable | Default | Description |
|---|---|---|
| `ATDATA_HOSTNAME` | `localhost` | Public hostname, used to derive `did:web` identity |
| `ATDATA_PORT` | `8000` | Server port (included in DID in dev mode) |
| `ATDATA_DEV_MODE` | `true` | Dev mode uses `http://` and includes port in DID; production uses `https://` |
| `ATDATA_DATABASE_URL` | `postgresql://localhost:5432/atdata_app` | PostgreSQL connection string |
| `ATDATA_JETSTREAM_URL` | `wss://jetstream2.us-east.bsky.network/subscribe` | Jetstream WebSocket endpoint |
| `ATDATA_JETSTREAM_COLLECTIONS` | `ac.foundation.dataset.*` | Collections to subscribe to |
| `ATDATA_RELAY_HOST` | `https://bsky.network` | BGS relay for backfill DID discovery |

### Identity

The service derives its [`did:web`](https://w3c-ccg.github.io/did-method-web/) identity from the hostname and port:

- **Dev mode**: `did:web:localhost%3A8000` with endpoint `http://localhost:8000`
- **Production**: `did:web:datasets.example.com` with endpoint `https://datasets.example.com`

The DID document is served at `GET /.well-known/did.json` and advertises the service as an `AtprotoAppView`.

## API Reference

All XRPC endpoints are mounted at `/xrpc/`. Queries are `GET`, procedures are `POST`.

### Queries

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

### Procedures

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

### Other Routes

| Route | Description |
|---|---|
| `GET /.well-known/did.json` | DID document for `did:web` identity |
| `GET /health` | Health check (`{"status": "ok"}`) |

## Data Model

Four tables indexed from the `ac.foundation.dataset.*` namespace, plus cursor state for firehose crash recovery. All tables use `(did, rkey)` as composite primary key. Schema auto-applies on startup.

### schemas

Stores dataset schema definitions. Record key format: `{schema-id}@{semver}` (e.g., `my.schema@1.0.0`).

Fields: `name`, `version`, `schema_type` (default `jsonSchema`), `schema_body` (JSONB), `description`, `metadata` (JSONB).

### entries

Stores dataset metadata records. Includes weighted full-text search (`search_tsv`) across name (A), description (B), and tags (C).

Fields: `name`, `schema_ref` (AT-URI to schema), `storage` (JSONB — `storageHttp`, `storageS3`, or `storageBlobs`), `description`, `tags` (text array), `license`, `size_samples`, `size_bytes`, `size_shards`, `content_metadata` (JSONB).

### labels

Named version pointers to dataset entries (analogous to git tags).

Fields: `name`, `dataset_uri` (AT-URI to entry), `version`, `description`.

### lenses

Bidirectional schema transforms with executable code for migrating data between schema versions.

Fields: `name`, `source_schema` (AT-URI), `target_schema` (AT-URI), `getter_code` (JSONB), `putter_code` (JSONB), `description`, `language`.

## Development

```bash
# Run tests (no database required)
uv run pytest

# Run a single test
uv run pytest tests/test_models.py::test_parse_at_uri -v

# Run with coverage
uv run pytest --cov=atdata_app

# Lint
uv run ruff check src/ tests/
```

Tests mock all external dependencies (database, HTTP, identity resolution) using `unittest.mock.AsyncMock`. HTTP endpoint tests use httpx `ASGITransport` for in-process testing without a running server.

## License

MIT
