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

See [docs/api-reference.md](docs/api-reference.md) for the full XRPC endpoint reference (queries, procedures, and other routes).

## Data Model

See [docs/data-model.md](docs/data-model.md) for the database schema (schemas, entries, labels, lenses).

## Docker Deployment

The app ships with a multi-stage Dockerfile using [uv](https://docs.astral.sh/uv/) for fast dependency installation.

### Build and run locally

```bash
docker build -t atdata-app .

docker run -p 8000:8000 \
  -e ATDATA_DATABASE_URL=postgresql://user:pass@host:5432/atdata_app \
  -e ATDATA_HOSTNAME=localhost \
  -e ATDATA_DEV_MODE=true \
  atdata-app
```

### Deploy on Railway

The repo includes a `railway.toml` that configures the Dockerfile builder, health checks at `/health`, and a restart-on-failure policy.

1. Connect the repo to a [Railway](https://railway.com) project
2. Add a PostgreSQL service and link it
3. Set the required environment variables:

| Variable | Value |
|---|---|
| `ATDATA_DATABASE_URL` | Provided by Railway's PostgreSQL plugin (`${{Postgres.DATABASE_URL}}`) |
| `ATDATA_HOSTNAME` | Your Railway public domain (e.g. `atdata-app-production.up.railway.app`) |
| `ATDATA_DEV_MODE` | `false` |
| `ATDATA_PORT` | Omit — Railway sets `PORT` automatically and the container respects it |

Optional variables for ingestion tuning:

| Variable | Default | Description |
|---|---|---|
| `ATDATA_JETSTREAM_URL` | `wss://jetstream2.us-east.bsky.network/subscribe` | Jetstream endpoint |
| `ATDATA_RELAY_HOST` | `https://bsky.network` | BGS relay for backfill |

Railway will auto-deploy on push, build the Docker image, and start the container.

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
