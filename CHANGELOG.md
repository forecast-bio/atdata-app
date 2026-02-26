# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.4.0b1] - 2026-02-26

### Added

- `sendInteractions` XRPC procedure for anonymous usage telemetry — fire-and-forget reporting of download, citation, and derivative events on datasets ([#21](https://github.com/forecast-bio/atdata-app/issues/21))
- Skeleton/hydration pattern for third-party dataset indexes — `getIndexSkeleton`, `getIndex`, `listIndexes`, and `publishIndex` endpoints following Bluesky's feed generator model ([#20](https://github.com/forecast-bio/atdata-app/issues/20))
- `subscribeChanges` WebSocket endpoint for real-time change streaming — in-memory event bus broadcasts create/update/delete events to subscribers with cursor-based replay ([#22](https://github.com/forecast-bio/atdata-app/issues/22))
- Array format type recognition (`sparseBytes`, `structuredBytes`, `arrowTensor`, `safetensors`) and ndarray v1.1.0 annotation display (`dtype`, `shape`, `dimensionNames`) in frontend templates ([#30](https://github.com/forecast-bio/atdata-app/issues/30))
- `atdata-lexicon` git submodule at `lexicons/` pinned to v0.2.1b1 for reference and CI validation ([#27](https://github.com/forecast-bio/atdata-app/issues/27))
- CI checkout steps now initialize submodules

### Changed

- Ingestion processor refactored to use `UPSERT_FNS` dispatch dict instead of if/elif chain
- Index provider records (`science.alt.dataset.index`) added to `COLLECTION_TABLE_MAP` for firehose ingestion

### Security

- **SSRF protection**: Skeleton fetch now validates endpoint URLs with DNS resolution and blocks private/reserved IP ranges at both fetch time and firehose ingestion time
- **Auth**: `sendInteractions` endpoint now requires ATProto service auth (was previously unauthenticated)
- **XSS**: Storage URLs in dataset detail pages are only rendered as clickable links when using `http(s)://` schemes, preventing `javascript:` URI injection
- **Input validation**: `publishIndex` rejects endpoint URLs containing embedded credentials or fragments; `sendInteractions` validates that URIs reference the `science.alt.dataset.entry` collection

### Fixed

- **ChangeStream backpressure**: Subscribers that fall behind are now tracked and explicitly disconnected with WebSocket close code 4000, instead of silently dropping events
- **ChangeStream subscriber limit**: Capped at 1000 concurrent subscribers; new connections receive close code 1013 when full
- **WebSocket keepalive**: Restructured the `subscribeChanges` event loop so the 30-second idle keepalive correctly re-enters the processing loop (was previously broken)
- **Replay deduplication**: Track last replayed sequence number to prevent duplicate events when replay buffer overlaps with the live queue
- **Task GC**: Fire-and-forget analytics tasks now retain references to prevent garbage collection before completion
- **Skeleton response cap**: Enforce the requested `limit` on items returned by external index providers, and cap response body size to 1 MiB
- **Skeleton item sanitization**: Whitelist upstream skeleton items to only the `uri` field; validate cursor strings for length and null bytes
- **Query guard**: `query_get_entries` now rejects requests with more than 100 keys to prevent unbounded OR-clause queries
- **Template robustness**: `shape` and `dimensionNames` join filters now guard against non-iterable data from malformed firehose records
- Removed dead `_validate_iso8601` timestamp validation code from `sendInteractions`

## [0.3.0b1] - 2026-02-22

### Changed

- **Breaking**: Rename collection NSID from `science.alt.dataset.record` to `science.alt.dataset.entry` to align with upstream lexicon v0.2.1b1 — avoids ambiguity with ATProto's "record" concept

## [0.2.3b1] - 2026-02-20

### Changed

- **Breaking**: Rename lexicon namespace from `ac.foundation.dataset.*` to `science.alt.dataset.*` across all XRPC endpoints, firehose filters, SQL schema, and configuration ([#17](https://github.com/forecast-bio/atdata-app/issues/17))
- DID document service entry updated from `#atproto_appview` / `AtprotoAppView` to `#atdata_appview` / `AtdataAppView`

### Added

- Dual-hostname DID document support — serve different `did:web` documents for `api.atdata.app` (appview identity) and `atdata.app` (atproto account identity) based on the `Host` header ([#19](https://github.com/forecast-bio/atdata-app/issues/19))
- Host-based route gating middleware — frontend HTML routes are only served on the frontend hostname; the API subdomain serves only XRPC, health, and DID endpoints
- Optional `verificationMethod` (Multikey) in DID documents when signing keys are configured
- New config vars: `ATDATA_FRONTEND_HOSTNAME`, `ATDATA_PDS_ENDPOINT`, `ATDATA_SIGNING_KEY`, `ATDATA_FRONTEND_SIGNING_KEY`
- Startup validation requiring `ATDATA_PDS_ENDPOINT` when `ATDATA_FRONTEND_HOSTNAME` is set

## [0.2.2b1] - 2026-02-18

### Added

- PostgreSQL integration test suite (58 tests) covering schema validation, upserts, queries, search, analytics, pagination, and edge cases
- Docker auto-start for local integration testing — `conftest.py` spins up a PostgreSQL container when `TEST_DATABASE_URL` is not set
- `integration-test` CI job running against PostgreSQL 15, 16, and 17

### Fixed

- Schema: `array_to_string()` is `STABLE`, not `IMMUTABLE` — added `immutable_array_to_string()` wrapper so the `search_tsv` generated column works on all PostgreSQL versions
- Database: cursor pagination passed `indexed_at` as string instead of `datetime`, causing asyncpg `DataError` with extended query protocol
- Database: analytics interval queries passed string literals instead of `timedelta` objects, causing asyncpg encoding failures
- CI: `schema-check` job silently ignored SQL errors — added `ON_ERROR_STOP=1` to `psql` invocations

## [0.2.1b1] - 2026-02-17

### Security

- Validate `lxm` claim in service auth JWT to prevent cross-endpoint token reuse

### Added

- PostgreSQL version matrix in CI (`schema-check` job testing against PG 15, 16, 17)

### Fixed

- Schema: use explicit `'english'::regconfig` cast in `search_tsv` generated column for PostgreSQL 17 compatibility
- Fix `last_time_us` UnboundLocalError in Jetstream consumer on early cancellation
- Return 400 instead of 500 for invalid AT-URIs in `getEntry`, `getEntries`, `getSchema`
- Add missing database index on `labels.dataset_uri` for `query_labels_for_dataset`
- Deduplicate cursor pagination helpers into `models.py`

## [0.2.0b1] - 2026-02-17

### Added

- Server-rendered dataset browser frontend with Jinja2 templates, HTMX, and PicoCSS — home/search, dataset detail, schema detail, schemas list, publisher profile, and about pages
- MCP (Model Context Protocol) server for agent-based dataset queries — exposes search, list, get, and describe tools for LLM agents (`mcp_server.py`)
- `atdata-mcp` CLI entry point for running the MCP server
- Lightweight server-side analytics: `analytics_events` table, `analytics_counters` summary table, fire-and-forget event recording via `asyncio.create_task()`
- XRPC analytics endpoints: `getAnalytics` (service-wide stats by period) and `getEntryStats` (per-dataset view/search counts)
- Analytics summary in `describeService` response (total views, searches, active publishers)
- `query_labels_for_dataset` database helper for retrieving labels by dataset URI
- PyPI publish workflow via GitHub Actions with OIDC trusted publishing

### Fixed

- Dockerfile: added `--no-editable` to `uv sync` so the package installs into `site-packages` instead of using a dangling `.pth` reference in the runtime stage

## [0.1.0b1] - 2026-02-16

First beta release of the ATProto AppView for `science.alt.dataset`.

### Added

- ATProto AppView serving XRPC endpoints for schemas, dataset entries, labels, and lenses
- Jetstream firehose ingestion via WebSocket with backfill support
- XRPC query endpoints: `listSchemas`, `listEntries`, `listLabels`, `listLenses`, `getSchema`, `getEntry`, `getLabel`, `getLens`
- XRPC procedure endpoints: `publishSchema`, `publishEntry`, `publishLabel`, `publishLens` with ATProto service auth and PDS proxying
- Keyset cursor pagination using `(indexed_at, did, rkey)` tuples
- `did:web` identity resolution for the AppView service
- Dockerfile with multi-stage uv build, non-root user, and Railway `PORT` env var support
- `.dockerignore` and `railway.toml` for Railway deployment
- PostgreSQL schema with migrations (`sql/schema.sql`)
- Comprehensive test suite (33 tests) with full mock coverage
