# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

ATProto AppView for the `ac.foundation.dataset` lexicon namespace. An AppView is a read-heavy service in the AT Protocol architecture that indexes records from the network firehose and serves them via XRPC endpoints. This one indexes dataset metadata — schemas, dataset entries, labels, and lenses (bidirectional schema transforms).

## Commands

```bash
# Install dependencies
uv sync --dev

# Run tests
uv run pytest

# Run a single test
uv run pytest tests/test_models.py::test_parse_at_uri

# Run tests with coverage
uv run pytest --cov=atdata_app

# Lint
uv run ruff check src/ tests/

# Start the server (requires PostgreSQL)
uv run uvicorn atdata_app.main:app --reload
```

## Architecture

### Data Flow

Jetstream firehose (WebSocket) → `ingestion/jetstream.py` → `ingestion/processor.py` → `database.py` upsert fns → PostgreSQL

Backfill (HTTP, one-shot) → `ingestion/backfill.py` → same upsert fns → PostgreSQL

XRPC queries (GET) ← `xrpc/queries.py` ← `database.py` query fns ← PostgreSQL
XRPC procedures (POST) → `xrpc/procedures.py` → validates + proxies `createRecord` to user's PDS

### Four Record Types

Every record type maps to a database table via `database.COLLECTION_TABLE_MAP`:

| Collection | Table | Record key format |
|---|---|---|
| `ac.foundation.dataset.schema` | `schemas` | `{NSID}@{semver}` |
| `ac.foundation.dataset.record` | `entries` | TID |
| `ac.foundation.dataset.label` | `labels` | TID |
| `ac.foundation.dataset.lens` | `lenses` | TID |

Each has a corresponding `upsert_*` function in `database.py`, a `row_to_*` serializer in `models.py`, and a `publish*` procedure in `xrpc/procedures.py`.

### Key Patterns

- **App state**: Config and DB pool live on `app.state` (FastAPI). Access via `request.app.state.config` / `request.app.state.db_pool`.
- **Identity resolution**: Shared `get_resolver()` singleton in `__init__.py` (lazy `AsyncIdResolver`), imported by `auth.py`, `xrpc/queries.py`, `xrpc/procedures.py`, and `ingestion/backfill.py`.
- **Cursor pagination**: Keyset pagination using `(indexed_at, did, rkey)` tuples, base64-encoded. See `encode_cursor`/`decode_cursor` in `models.py`.
- **Service auth**: Procedures require ATProto service auth JWT (verified in `auth.py`) plus a `X-PDS-Auth` header for PDS proxying.
- **SQL**: All queries use asyncpg positional parameters (`$1`, `$2`). Schema lives in `sql/schema.sql` and runs on startup via `run_migrations()`.

### XRPC Endpoints

All mounted under `/xrpc/` via `xrpc/router.py`. Queries are GET, procedures are POST. Endpoint names follow the lexicon NSID pattern (e.g., `/xrpc/ac.foundation.dataset.listEntries`).

## Environment Variables

All prefixed with `ATDATA_` (via pydantic-settings). Key ones:

- `ATDATA_DATABASE_URL` — PostgreSQL DSN
- `ATDATA_HOSTNAME` / `ATDATA_PORT` — used to derive `did:web` identity
- `ATDATA_DEV_MODE` — toggles `http://` vs `https://`, port in DID
- `ATDATA_JETSTREAM_URL` — Jetstream WebSocket endpoint
- `ATDATA_RELAY_HOST` — BGS relay for backfill discovery

## Testing

Tests run without a database. External dependencies (DB pool, upsert functions) are mocked with `unittest.mock.AsyncMock`. The `conftest.py` fixture provides a dev-mode `AppConfig`. Identity/DID tests use httpx `ASGITransport` to test endpoints in-process.
