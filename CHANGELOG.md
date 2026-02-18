# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

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

First beta release of the ATProto AppView for `ac.foundation.dataset`.

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
