# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

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
