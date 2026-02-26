# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.3.0b1] - 2026-02-22

### Changed

- **Breaking**: Rename collection NSID from `science.alt.dataset.record` to `science.alt.dataset.entry` to align with upstream lexicon v0.2.1b1 — avoids ambiguity with ATProto's "record" concept

## [0.2.3b1] - 2026-02-20

### Changed

- **Breaking**: Rename lexicon namespace from `ac.foundation.dataset.*` to `science.alt.dataset.*` across all XRPC endpoints, firehose filters, SQL schema, and configuration ([#17](https://github.com/forecast-bio/atdata-app/issues/17))
- DID document service entry updated from `#atproto_appview` / `AtprotoAppView` to `#atdata_appview` / `AtdataAppView`

### Added
- Adversarial review: sendInteractions feature and surrounding code (round 3) (#39)
- Add sendInteractions XRPC procedure for usage telemetry (#35)

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
