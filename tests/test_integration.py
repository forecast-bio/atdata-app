"""Integration tests that run against a real PostgreSQL database.

WHY THESE TESTS EXIST
---------------------
All other test files in this suite mock the database entirely — database.py
functions are patched with AsyncMock, and no test ever executes real SQL against
a real PostgreSQL instance. This means:

  1. Schema syntax errors are never caught (e.g. the PG17 tsvector immutability bug)
  2. SQL query bugs (wrong column names, bad joins, parameter counts) are invisible
  3. PostgreSQL version-specific incompatibilities slip through
  4. Generated columns, indexes, and constraints are never validated
  5. Upsert ON CONFLICT behaviour is never verified

These integration tests fill that gap by running against a real PostgreSQL
instance. They are SKIPPED when no database is available (the default for local
``uv run pytest``), and run in CI when ``TEST_DATABASE_URL`` is set.

COVERAGE GAP ANALYSIS
---------------------
The following database.py functions had ZERO real-SQL coverage before this file:

  Upserts:        upsert_schema, upsert_entry, upsert_label, upsert_lens
  Deletes:        delete_record
  Cursor:         get_cursor, set_cursor
  Queries:        query_get_entry, query_get_entries, query_get_schema,
                  query_list_entries, query_list_schemas, query_list_lenses,
                  query_search_datasets, query_search_lenses,
                  query_resolve_label, query_resolve_schema,
                  query_labels_for_dataset, query_record_counts,
                  query_record_exists, query_active_publishers
  Analytics:      record_analytics_event, query_analytics_summary,
                  query_entry_stats

  Schema:         run_migrations (schema.sql application)

All of these were only exercised via mocked calls in the existing tests.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

SKIP_REASON = "TEST_DATABASE_URL not set (requires PostgreSQL)"
pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason=SKIP_REASON,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def db_pool():
    """Create a real asyncpg pool, apply schema, yield, then tear down."""
    import asyncpg

    url = os.environ["TEST_DATABASE_URL"]
    pool = await asyncpg.create_pool(url, min_size=2, max_size=5)

    schema_sql = (
        Path(__file__).parent.parent / "src" / "atdata_app" / "sql" / "schema.sql"
    ).read_text()

    async with pool.acquire() as conn:
        await conn.execute(schema_sql)

    yield pool

    # Tear down: drop everything so the next test starts clean
    async with pool.acquire() as conn:
        await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    await pool.close()


# ---------------------------------------------------------------------------
# Realistic test data (mirrors ATProto conventions)
# ---------------------------------------------------------------------------

_DID_ALICE = "did:plc:alice000000000000000000"
_DID_BOB = "did:plc:bob00000000000000000000"

_SCHEMA_RECORD = {
    "name": "com.example.genomics",
    "version": "1.0.0",
    "schemaType": "jsonSchema",
    "schema": {"type": "object", "properties": {"samples": {"type": "integer"}}},
    "description": "Genomics dataset schema",
    "metadata": {"author": "alice"},
    "createdAt": "2025-01-15T10:00:00Z",
}

_ENTRY_RECORD = {
    "name": "Human Genome Variants",
    "schemaRef": f"at://{_DID_ALICE}/science.alt.dataset.schema/com.example.genomics@1.0.0",
    "storage": {
        "$type": "science.alt.dataset.entry#httpStorage",
        "url": "https://example.com/data.parquet",
    },
    "description": "Comprehensive human genome variant dataset for ML research",
    "tags": ["genomics", "variants", "machine-learning"],
    "license": "CC-BY-4.0",
    "size": {"samples": 50000, "bytes": 1073741824, "shards": 8},
    "contentMetadata": {"format": "parquet", "columns": 42},
    "createdAt": "2025-02-01T12:00:00Z",
}

_LABEL_RECORD = {
    "name": "v1-stable",
    "datasetUri": f"at://{_DID_ALICE}/science.alt.dataset.entry/3jqfcqzm3fp2k",
    "version": "1.0",
    "description": "First stable release",
    "createdAt": "2025-02-10T08:00:00Z",
}

_LENS_RECORD = {
    "name": "genomics-to-clinical",
    "sourceSchema": f"at://{_DID_ALICE}/science.alt.dataset.schema/com.example.genomics@1.0.0",
    "targetSchema": f"at://{_DID_BOB}/science.alt.dataset.schema/com.example.clinical@2.0.0",
    "getterCode": {
        "repository": "https://github.com/example/lenses",
        "commit": "abc123",
        "path": "genomics_to_clinical.py",
    },
    "putterCode": {
        "repository": "https://github.com/example/lenses",
        "commit": "abc123",
        "path": "clinical_to_genomics.py",
    },
    "description": "Bidirectional transform between genomics and clinical schemas",
    "language": "python",
    "metadata": {"version": "0.1.0"},
    "createdAt": "2025-03-01T09:00:00Z",
}


# ===================================================================
# A. SCHEMA VALIDATION TESTS
# ===================================================================


class TestSchemaValidation:
    """Verify schema.sql applies cleanly and creates expected structures."""

    async def test_schema_applies_without_errors(self, db_pool):
        """Schema application should not raise — already done in fixture."""
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT 1 AS ok")
            assert row["ok"] == 1

    async def test_all_expected_tables_exist(self, db_pool):
        """All tables defined in schema.sql must exist."""
        expected = {
            "schemas",
            "entries",
            "labels",
            "lenses",
            "index_providers",
            "cursor_state",
            "analytics_events",
            "analytics_counters",
        }
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public'
                """
            )
            tables = {r["tablename"] for r in rows}

        assert expected.issubset(tables), f"Missing tables: {expected - tables}"

    async def test_expected_indexes_exist(self, db_pool):
        """Key indexes must be present after migration."""
        expected_indexes = {
            "idx_schemas_name",
            "idx_schemas_did",
            "idx_entries_name",
            "idx_entries_did",
            "idx_entries_schema_ref",
            "idx_entries_tags",
            "idx_entries_indexed_at",
            "idx_entries_search",
            "idx_labels_name",
            "idx_labels_did",
            "idx_labels_dataset_uri",
            "idx_lenses_source_schema",
            "idx_lenses_target_schema",
            "idx_lenses_did",
            "idx_analytics_events_type_created",
            "idx_analytics_events_target",
            "idx_index_providers_did",
            "idx_index_providers_indexed_at",
        }
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT indexname FROM pg_indexes
                WHERE schemaname = 'public'
                """
            )
            indexes = {r["indexname"] for r in rows}

        assert expected_indexes.issubset(indexes), (
            f"Missing indexes: {expected_indexes - indexes}"
        )

    async def test_search_tsv_generated_column_exists(self, db_pool):
        """The search_tsv GENERATED column must exist on entries."""
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT column_name, is_generated
                FROM information_schema.columns
                WHERE table_name = 'entries' AND column_name = 'search_tsv'
                """
            )
        assert row is not None, "search_tsv column missing from entries table"

    async def test_search_tsv_populated_on_insert(self, db_pool):
        """INSERT into entries should auto-populate search_tsv via generated column."""
        from atdata_app.database import upsert_entry

        await upsert_entry(db_pool, _DID_ALICE, "3jqfcqzm3fp2k", "bafytest1", _ENTRY_RECORD)

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT search_tsv FROM entries WHERE did = $1 AND rkey = $2",
                _DID_ALICE,
                "3jqfcqzm3fp2k",
            )

        assert row is not None
        assert row["search_tsv"] is not None
        tsv_str = str(row["search_tsv"])
        # The tsvector should contain stems from name/description/tags
        assert "genom" in tsv_str or "variant" in tsv_str

    async def test_fulltext_search_end_to_end(self, db_pool):
        """INSERT a row, then verify full-text search with @@ operator works."""
        from atdata_app.database import upsert_entry

        await upsert_entry(db_pool, _DID_ALICE, "3jqfcqzm3fp2k", "bafytest1", _ENTRY_RECORD)

        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT name FROM entries
                WHERE search_tsv @@ plainto_tsquery('english'::regconfig, $1)
                """,
                "genome variants",
            )

        assert len(rows) == 1
        assert rows[0]["name"] == "Human Genome Variants"

    async def test_schema_is_idempotent(self, db_pool):
        """Running schema.sql a second time should not error."""
        schema_sql = (
            Path(__file__).parent.parent / "src" / "atdata_app" / "sql" / "schema.sql"
        ).read_text()
        async with db_pool.acquire() as conn:
            await conn.execute(schema_sql)

        # Tables should still exist
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT COUNT(*) AS cnt FROM pg_tables WHERE schemaname = 'public'"
            )
            assert row["cnt"] >= 7


# ===================================================================
# B. DATABASE FUNCTION INTEGRATION TESTS
# ===================================================================


class TestUpserts:
    """Test all upsert_* functions with real SQL."""

    async def test_upsert_schema(self, db_pool):
        from atdata_app.database import upsert_schema

        await upsert_schema(
            db_pool, _DID_ALICE, "com.example.genomics@1.0.0", "bafyschema1", _SCHEMA_RECORD
        )

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM schemas WHERE did = $1 AND rkey = $2",
                _DID_ALICE,
                "com.example.genomics@1.0.0",
            )

        assert row is not None
        assert row["name"] == "com.example.genomics"
        assert row["version"] == "1.0.0"
        assert row["schema_type"] == "jsonSchema"
        assert row["description"] == "Genomics dataset schema"

    async def test_upsert_entry(self, db_pool):
        from atdata_app.database import upsert_entry

        await upsert_entry(db_pool, _DID_ALICE, "3jqfcqzm3fp2k", "bafyentry1", _ENTRY_RECORD)

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM entries WHERE did = $1 AND rkey = $2",
                _DID_ALICE,
                "3jqfcqzm3fp2k",
            )

        assert row is not None
        assert row["name"] == "Human Genome Variants"
        assert row["description"] == "Comprehensive human genome variant dataset for ML research"
        assert list(row["tags"]) == ["genomics", "variants", "machine-learning"]
        assert row["license"] == "CC-BY-4.0"
        assert row["size_samples"] == 50000
        assert row["size_bytes"] == 1073741824
        assert row["size_shards"] == 8

    async def test_upsert_label(self, db_pool):
        from atdata_app.database import upsert_label

        await upsert_label(db_pool, _DID_ALICE, "3jqlabel0001", "bafylabel1", _LABEL_RECORD)

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM labels WHERE did = $1 AND rkey = $2",
                _DID_ALICE,
                "3jqlabel0001",
            )

        assert row is not None
        assert row["name"] == "v1-stable"
        assert row["dataset_uri"] == _LABEL_RECORD["datasetUri"]
        assert row["version"] == "1.0"

    async def test_upsert_lens(self, db_pool):
        from atdata_app.database import upsert_lens

        await upsert_lens(db_pool, _DID_ALICE, "3jqlens00001", "bafylens1", _LENS_RECORD)

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM lenses WHERE did = $1 AND rkey = $2",
                _DID_ALICE,
                "3jqlens00001",
            )

        assert row is not None
        assert row["name"] == "genomics-to-clinical"
        assert row["source_schema"] == _LENS_RECORD["sourceSchema"]
        assert row["target_schema"] == _LENS_RECORD["targetSchema"]
        assert row["language"] == "python"


class TestUpsertIdempotency:
    """Upserting the same record twice should update, not duplicate."""

    async def test_upsert_entry_twice_updates(self, db_pool):
        from atdata_app.database import upsert_entry

        await upsert_entry(db_pool, _DID_ALICE, "3jqfcqzm3fp2k", "bafycid1", _ENTRY_RECORD)

        updated = {**_ENTRY_RECORD, "name": "Updated Genome Variants", "description": "Updated"}
        await upsert_entry(db_pool, _DID_ALICE, "3jqfcqzm3fp2k", "bafycid2", updated)

        async with db_pool.acquire() as conn:
            count = await conn.fetchrow(
                "SELECT COUNT(*) AS cnt FROM entries WHERE did = $1 AND rkey = $2",
                _DID_ALICE,
                "3jqfcqzm3fp2k",
            )
            row = await conn.fetchrow(
                "SELECT * FROM entries WHERE did = $1 AND rkey = $2",
                _DID_ALICE,
                "3jqfcqzm3fp2k",
            )

        assert count["cnt"] == 1
        assert row["name"] == "Updated Genome Variants"
        assert row["cid"] == "bafycid2"

    async def test_upsert_schema_twice_updates(self, db_pool):
        from atdata_app.database import upsert_schema

        await upsert_schema(
            db_pool, _DID_ALICE, "com.example.genomics@1.0.0", "bafycid1", _SCHEMA_RECORD
        )

        updated = {**_SCHEMA_RECORD, "description": "Updated description"}
        await upsert_schema(
            db_pool, _DID_ALICE, "com.example.genomics@1.0.0", "bafycid2", updated
        )

        async with db_pool.acquire() as conn:
            count = await conn.fetchrow(
                "SELECT COUNT(*) AS cnt FROM schemas WHERE did = $1 AND rkey = $2",
                _DID_ALICE,
                "com.example.genomics@1.0.0",
            )
            row = await conn.fetchrow(
                "SELECT * FROM schemas WHERE did = $1 AND rkey = $2",
                _DID_ALICE,
                "com.example.genomics@1.0.0",
            )

        assert count["cnt"] == 1
        assert row["description"] == "Updated description"
        assert row["cid"] == "bafycid2"


class TestDeleteRecord:
    """Test delete_record with real SQL."""

    async def test_delete_existing_record(self, db_pool):
        from atdata_app.database import delete_record, upsert_entry

        await upsert_entry(db_pool, _DID_ALICE, "3jqfcqzm3fp2k", "bafycid1", _ENTRY_RECORD)
        await delete_record(db_pool, "entries", _DID_ALICE, "3jqfcqzm3fp2k")

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM entries WHERE did = $1 AND rkey = $2",
                _DID_ALICE,
                "3jqfcqzm3fp2k",
            )
        assert row is None

    async def test_delete_nonexistent_record_is_noop(self, db_pool):
        from atdata_app.database import delete_record

        # Should not raise
        await delete_record(db_pool, "entries", _DID_ALICE, "nonexistent")

    async def test_delete_invalid_table_is_noop(self, db_pool):
        from atdata_app.database import delete_record

        # Should silently return (table not in COLLECTION_TABLE_MAP.values())
        await delete_record(db_pool, "evil_table", _DID_ALICE, "3xyz")


class TestCursorState:
    """Test jetstream cursor persistence."""

    async def test_get_cursor_empty(self, db_pool):
        from atdata_app.database import get_cursor

        result = await get_cursor(db_pool)
        assert result is None

    async def test_set_and_get_cursor(self, db_pool):
        from atdata_app.database import get_cursor, set_cursor

        await set_cursor(db_pool, 1725911162329308)
        result = await get_cursor(db_pool)
        assert result == 1725911162329308

    async def test_set_cursor_updates_existing(self, db_pool):
        from atdata_app.database import get_cursor, set_cursor

        await set_cursor(db_pool, 100)
        await set_cursor(db_pool, 200)
        result = await get_cursor(db_pool)
        assert result == 200


class TestQueryFunctions:
    """Test query_* functions against real data."""

    async def _seed_entries(self, db_pool):
        """Insert several entries for query testing."""
        from atdata_app.database import upsert_entry

        entries = [
            (_DID_ALICE, "3jqentry00001", "bafycid1", {
                **_ENTRY_RECORD,
                "name": "Genomics Dataset Alpha",
                "description": "Alpha variant dataset",
                "tags": ["genomics", "alpha"],
            }),
            (_DID_ALICE, "3jqentry00002", "bafycid2", {
                **_ENTRY_RECORD,
                "name": "Proteomics Dataset Beta",
                "description": "Beta proteomics dataset",
                "tags": ["proteomics", "beta"],
            }),
            (_DID_BOB, "3jqentry00003", "bafycid3", {
                **_ENTRY_RECORD,
                "name": "Clinical Trial Gamma",
                "description": "Gamma clinical trial data",
                "tags": ["clinical", "gamma"],
            }),
        ]
        for did, rkey, cid, record in entries:
            await upsert_entry(db_pool, did, rkey, cid, record)

    async def _seed_schemas(self, db_pool):
        """Insert several schemas for query testing."""
        from atdata_app.database import upsert_schema

        schemas = [
            (_DID_ALICE, "com.example.genomics@1.0.0", "bafysc1", _SCHEMA_RECORD),
            (_DID_ALICE, "com.example.genomics@2.0.0", "bafysc2", {
                **_SCHEMA_RECORD,
                "version": "2.0.0",
                "description": "Genomics schema v2",
            }),
            (_DID_BOB, "com.example.clinical@1.0.0", "bafysc3", {
                **_SCHEMA_RECORD,
                "name": "com.example.clinical",
                "version": "1.0.0",
                "description": "Clinical schema",
            }),
        ]
        for did, rkey, cid, record in schemas:
            await upsert_schema(db_pool, did, rkey, cid, record)

    async def test_query_get_entry(self, db_pool):
        from atdata_app.database import query_get_entry

        await self._seed_entries(db_pool)
        row = await query_get_entry(db_pool, _DID_ALICE, "3jqentry00001")
        assert row is not None
        assert row["name"] == "Genomics Dataset Alpha"

    async def test_query_get_entry_not_found(self, db_pool):
        from atdata_app.database import query_get_entry

        row = await query_get_entry(db_pool, _DID_ALICE, "nonexistent")
        assert row is None

    async def test_query_get_entries_batch(self, db_pool):
        from atdata_app.database import query_get_entries

        await self._seed_entries(db_pool)
        rows = await query_get_entries(
            db_pool,
            [(_DID_ALICE, "3jqentry00001"), (_DID_BOB, "3jqentry00003")],
        )
        assert len(rows) == 2
        names = {r["name"] for r in rows}
        assert "Genomics Dataset Alpha" in names
        assert "Clinical Trial Gamma" in names

    async def test_query_get_entries_empty_keys(self, db_pool):
        from atdata_app.database import query_get_entries

        rows = await query_get_entries(db_pool, [])
        assert rows == []

    async def test_query_get_schema(self, db_pool):
        from atdata_app.database import query_get_schema

        await self._seed_schemas(db_pool)
        row = await query_get_schema(db_pool, _DID_ALICE, "com.example.genomics@1.0.0")
        assert row is not None
        assert row["name"] == "com.example.genomics"

    async def test_query_get_schema_not_found(self, db_pool):
        from atdata_app.database import query_get_schema

        row = await query_get_schema(db_pool, _DID_ALICE, "nonexistent@0.0.0")
        assert row is None

    async def test_query_list_entries_all(self, db_pool):
        from atdata_app.database import query_list_entries

        await self._seed_entries(db_pool)
        rows = await query_list_entries(db_pool, limit=50)
        assert len(rows) == 3

    async def test_query_list_entries_by_repo(self, db_pool):
        from atdata_app.database import query_list_entries

        await self._seed_entries(db_pool)
        rows = await query_list_entries(db_pool, repo=_DID_ALICE, limit=50)
        assert len(rows) == 2
        for row in rows:
            assert row["did"] == _DID_ALICE

    async def test_query_list_entries_pagination(self, db_pool):
        from atdata_app.database import query_list_entries

        await self._seed_entries(db_pool)

        # Get first page (limit=2)
        page1 = await query_list_entries(db_pool, limit=2)
        assert len(page1) == 2

        # Use last row as cursor for next page
        last = page1[-1]
        page2 = await query_list_entries(
            db_pool,
            limit=2,
            cursor_indexed_at=str(last["indexed_at"]),
            cursor_did=last["did"],
            cursor_rkey=last["rkey"],
        )
        assert len(page2) == 1

        # No overlap between pages
        page1_keys = {(r["did"], r["rkey"]) for r in page1}
        page2_keys = {(r["did"], r["rkey"]) for r in page2}
        assert page1_keys.isdisjoint(page2_keys)

    async def test_query_list_schemas_all(self, db_pool):
        from atdata_app.database import query_list_schemas

        await self._seed_schemas(db_pool)
        rows = await query_list_schemas(db_pool, limit=50)
        assert len(rows) == 3

    async def test_query_list_schemas_by_repo(self, db_pool):
        from atdata_app.database import query_list_schemas

        await self._seed_schemas(db_pool)
        rows = await query_list_schemas(db_pool, repo=_DID_ALICE, limit=50)
        assert len(rows) == 2

    async def test_query_list_lenses(self, db_pool):
        from atdata_app.database import query_list_lenses, upsert_lens

        await upsert_lens(db_pool, _DID_ALICE, "3jqlens00001", "bafylens1", _LENS_RECORD)
        await upsert_lens(db_pool, _DID_BOB, "3jqlens00002", "bafylens2", {
            **_LENS_RECORD,
            "name": "another-lens",
            "sourceSchema": f"at://{_DID_BOB}/science.alt.dataset.schema/x@1.0.0",
            "targetSchema": f"at://{_DID_BOB}/science.alt.dataset.schema/y@1.0.0",
        })

        all_rows = await query_list_lenses(db_pool, limit=50)
        assert len(all_rows) == 2

        filtered = await query_list_lenses(
            db_pool,
            source_schema=_LENS_RECORD["sourceSchema"],
            limit=50,
        )
        assert len(filtered) == 1
        assert filtered[0]["name"] == "genomics-to-clinical"

    async def test_query_resolve_label(self, db_pool):
        from atdata_app.database import query_resolve_label, upsert_label

        await upsert_label(db_pool, _DID_ALICE, "3jqlabel0001", "bafylbl1", _LABEL_RECORD)

        row = await query_resolve_label(db_pool, _DID_ALICE, "v1-stable")
        assert row is not None
        assert row["name"] == "v1-stable"

    async def test_query_resolve_label_with_version(self, db_pool):
        from atdata_app.database import query_resolve_label, upsert_label

        await upsert_label(db_pool, _DID_ALICE, "3jqlabel0001", "bafylbl1", _LABEL_RECORD)

        row = await query_resolve_label(db_pool, _DID_ALICE, "v1-stable", version="1.0")
        assert row is not None

        row_miss = await query_resolve_label(
            db_pool, _DID_ALICE, "v1-stable", version="9.9"
        )
        assert row_miss is None

    async def test_query_resolve_schema(self, db_pool):
        from atdata_app.database import query_resolve_schema

        await self._seed_schemas(db_pool)

        # Without version — should return latest (2.0.0 sorts after 1.0.0)
        row = await query_resolve_schema(db_pool, _DID_ALICE, "com.example.genomics")
        assert row is not None
        assert row["rkey"] == "com.example.genomics@2.0.0"

        # With version — exact match
        row_v1 = await query_resolve_schema(
            db_pool, _DID_ALICE, "com.example.genomics", version="1.0.0"
        )
        assert row_v1 is not None
        assert row_v1["rkey"] == "com.example.genomics@1.0.0"

    async def test_query_labels_for_dataset(self, db_pool):
        from atdata_app.database import query_labels_for_dataset, upsert_label

        ds_uri = f"at://{_DID_ALICE}/science.alt.dataset.entry/3jqfcqzm3fp2k"
        await upsert_label(db_pool, _DID_ALICE, "3jqlbl001", "bafylbl1", {
            **_LABEL_RECORD,
            "datasetUri": ds_uri,
            "name": "v1",
        })
        await upsert_label(db_pool, _DID_ALICE, "3jqlbl002", "bafylbl2", {
            **_LABEL_RECORD,
            "datasetUri": ds_uri,
            "name": "v2",
        })

        rows = await query_labels_for_dataset(db_pool, ds_uri)
        assert len(rows) == 2

    async def test_query_record_counts(self, db_pool):
        from atdata_app.database import query_record_counts, upsert_entry, upsert_schema

        await upsert_schema(
            db_pool, _DID_ALICE, "com.example.test@1.0.0", "bafysc1", _SCHEMA_RECORD
        )
        await upsert_entry(db_pool, _DID_ALICE, "3jqentry00001", "bafye1", _ENTRY_RECORD)
        await upsert_entry(db_pool, _DID_ALICE, "3jqentry00002", "bafye2", _ENTRY_RECORD)

        counts = await query_record_counts(db_pool)
        assert counts["science.alt.dataset.schema"] == 1
        assert counts["science.alt.dataset.entry"] == 2
        assert counts["science.alt.dataset.label"] == 0
        assert counts["science.alt.dataset.lens"] == 0

    async def test_query_record_exists(self, db_pool):
        from atdata_app.database import query_record_exists, upsert_entry

        await upsert_entry(db_pool, _DID_ALICE, "3jqentry00001", "bafye1", _ENTRY_RECORD)

        assert await query_record_exists(db_pool, "entries", _DID_ALICE, "3jqentry00001") is True
        assert await query_record_exists(db_pool, "entries", _DID_ALICE, "missing") is False
        assert await query_record_exists(db_pool, "bad_table", _DID_ALICE, "x") is False


class TestSearch:
    """Test full-text search via query_search_datasets."""

    async def _seed_searchable_entries(self, db_pool):
        from atdata_app.database import upsert_entry

        datasets = [
            ("3jqsrch00001", {
                **_ENTRY_RECORD,
                "name": "Genomics ML Dataset",
                "description": "A machine learning dataset for genomics research",
                "tags": ["genomics", "machine-learning"],
            }),
            ("3jqsrch00002", {
                **_ENTRY_RECORD,
                "name": "Proteomics Analysis",
                "description": "High-throughput proteomics data for protein folding",
                "tags": ["proteomics", "protein"],
            }),
            ("3jqsrch00003", {
                **_ENTRY_RECORD,
                "name": "Climate Change Dataset",
                "description": "Global temperature measurements since 1900",
                "tags": ["climate", "environment"],
            }),
        ]
        for rkey, record in datasets:
            await upsert_entry(db_pool, _DID_ALICE, rkey, f"bafy{rkey}", record)

    async def test_search_finds_matching_entries(self, db_pool):
        from atdata_app.database import query_search_datasets

        await self._seed_searchable_entries(db_pool)

        rows = await query_search_datasets(db_pool, q="genomics")
        assert len(rows) >= 1
        names = [r["name"] for r in rows]
        assert "Genomics ML Dataset" in names

    async def test_search_no_results(self, db_pool):
        from atdata_app.database import query_search_datasets

        await self._seed_searchable_entries(db_pool)
        rows = await query_search_datasets(db_pool, q="quantum computing blockchain")
        assert len(rows) == 0

    async def test_search_by_description(self, db_pool):
        from atdata_app.database import query_search_datasets

        await self._seed_searchable_entries(db_pool)
        rows = await query_search_datasets(db_pool, q="protein folding")
        assert len(rows) >= 1
        assert any(r["name"] == "Proteomics Analysis" for r in rows)

    async def test_search_with_tag_filter(self, db_pool):
        from atdata_app.database import query_search_datasets

        await self._seed_searchable_entries(db_pool)

        # Search for "data" but filter to climate tag
        rows = await query_search_datasets(
            db_pool, q="dataset", tags=["climate"]
        )
        for r in rows:
            assert "climate" in list(r["tags"])

    async def test_search_with_repo_filter(self, db_pool):
        from atdata_app.database import query_search_datasets, upsert_entry

        await self._seed_searchable_entries(db_pool)

        # Add one entry from Bob
        await upsert_entry(db_pool, _DID_BOB, "3jqsrch00099", "bafybob", {
            **_ENTRY_RECORD,
            "name": "Bob Genomics Dataset",
            "description": "Bob's genomics data",
            "tags": ["genomics"],
        })

        rows = await query_search_datasets(db_pool, q="genomics", repo=_DID_BOB)
        assert len(rows) == 1
        assert rows[0]["did"] == _DID_BOB

    async def test_search_special_characters(self, db_pool):
        """Special characters in search terms must not cause SQL injection or errors."""
        from atdata_app.database import query_search_datasets

        await self._seed_searchable_entries(db_pool)

        # These should all return empty or valid results without raising
        for q in [
            "'; DROP TABLE entries; --",
            "test' OR '1'='1",
            "<script>alert(1)</script>",
            "test & | ! ( ) *",
            "",
        ]:
            if q:  # plainto_tsquery requires non-empty
                rows = await query_search_datasets(db_pool, q=q)
                assert isinstance(rows, list)


class TestSearchLenses:
    """Test lens search queries."""

    async def _seed_lenses(self, db_pool):
        from atdata_app.database import upsert_lens

        src_a = f"at://{_DID_ALICE}/science.alt.dataset.schema/a@1.0.0"
        src_b = f"at://{_DID_ALICE}/science.alt.dataset.schema/b@1.0.0"
        tgt_c = f"at://{_DID_BOB}/science.alt.dataset.schema/c@1.0.0"

        await upsert_lens(db_pool, _DID_ALICE, "3jqlens001", "bafyl1", {
            **_LENS_RECORD,
            "sourceSchema": src_a,
            "targetSchema": tgt_c,
        })
        await upsert_lens(db_pool, _DID_BOB, "3jqlens002", "bafyl2", {
            **_LENS_RECORD,
            "sourceSchema": src_b,
            "targetSchema": tgt_c,
        })

    async def test_search_lenses_all(self, db_pool):
        from atdata_app.database import query_search_lenses

        await self._seed_lenses(db_pool)
        rows = await query_search_lenses(db_pool, limit=50)
        assert len(rows) == 2

    async def test_search_lenses_by_source(self, db_pool):
        from atdata_app.database import query_search_lenses

        await self._seed_lenses(db_pool)
        src = f"at://{_DID_ALICE}/science.alt.dataset.schema/a@1.0.0"
        rows = await query_search_lenses(db_pool, source_schema=src, limit=50)
        assert len(rows) == 1

    async def test_search_lenses_by_both(self, db_pool):
        from atdata_app.database import query_search_lenses

        await self._seed_lenses(db_pool)
        src = f"at://{_DID_ALICE}/science.alt.dataset.schema/a@1.0.0"
        tgt = f"at://{_DID_BOB}/science.alt.dataset.schema/c@1.0.0"
        rows = await query_search_lenses(
            db_pool, source_schema=src, target_schema=tgt, limit=50
        )
        assert len(rows) >= 1


# ===================================================================
# C. ANALYTICS INTEGRATION TESTS
# ===================================================================


class TestAnalytics:
    """Test analytics recording and querying against real PostgreSQL."""

    async def test_record_analytics_event_basic(self, db_pool):
        from atdata_app.database import record_analytics_event

        await record_analytics_event(
            db_pool, "view_entry", target_did=_DID_ALICE, target_rkey="3jqentry00001"
        )

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM analytics_events WHERE event_type = $1", "view_entry"
            )
        assert row is not None
        assert row["target_did"] == _DID_ALICE

    async def test_record_analytics_event_increments_counter(self, db_pool):
        from atdata_app.database import record_analytics_event

        await record_analytics_event(
            db_pool, "view_entry", target_did=_DID_ALICE, target_rkey="3jqentry00001"
        )
        await record_analytics_event(
            db_pool, "view_entry", target_did=_DID_ALICE, target_rkey="3jqentry00001"
        )

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT count FROM analytics_counters
                WHERE target_did = $1 AND target_rkey = $2 AND event_type = $3
                """,
                _DID_ALICE,
                "3jqentry00001",
                "view_entry",
            )
        assert row is not None
        assert row["count"] == 2

    async def test_record_analytics_event_without_target(self, db_pool):
        from atdata_app.database import record_analytics_event

        await record_analytics_event(db_pool, "describe")

        async with db_pool.acquire() as conn:
            event_row = await conn.fetchrow(
                "SELECT * FROM analytics_events WHERE event_type = $1", "describe"
            )
            counter_count = await conn.fetchrow(
                "SELECT COUNT(*) AS cnt FROM analytics_counters"
            )

        assert event_row is not None
        assert event_row["target_did"] is None
        assert counter_count["cnt"] == 0

    async def test_record_analytics_event_with_query_params(self, db_pool):
        from atdata_app.database import record_analytics_event

        await record_analytics_event(
            db_pool, "search", query_params={"q": "genomics", "tags": ["ml"]}
        )

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT query_params FROM analytics_events WHERE event_type = $1",
                "search",
            )
        assert row is not None
        params = row["query_params"]
        if isinstance(params, str):
            params = json.loads(params)
        assert params["q"] == "genomics"

    async def test_query_analytics_summary(self, db_pool):
        from atdata_app.database import (
            query_analytics_summary,
            record_analytics_event,
            upsert_entry,
        )

        # Seed an entry so top_datasets join works
        await upsert_entry(db_pool, _DID_ALICE, "3jqentry00001", "bafye1", _ENTRY_RECORD)

        # Record some events
        await record_analytics_event(
            db_pool, "view_entry", target_did=_DID_ALICE, target_rkey="3jqentry00001"
        )
        await record_analytics_event(
            db_pool, "view_entry", target_did=_DID_ALICE, target_rkey="3jqentry00001"
        )
        await record_analytics_event(
            db_pool, "search", query_params={"q": "genomics"}
        )

        summary = await query_analytics_summary(db_pool, period="week")

        assert summary["totalViews"] == 2
        assert summary["totalSearches"] == 1
        assert len(summary["topDatasets"]) >= 1
        assert summary["topDatasets"][0]["did"] == _DID_ALICE
        assert len(summary["topSearchTerms"]) >= 1
        assert summary["topSearchTerms"][0]["term"] == "genomics"
        assert "science.alt.dataset.entry" in summary["recordCounts"]

    async def test_query_entry_stats(self, db_pool):
        from atdata_app.database import query_entry_stats, record_analytics_event

        await record_analytics_event(
            db_pool, "view_entry", target_did=_DID_ALICE, target_rkey="3jqentry00001"
        )
        await record_analytics_event(
            db_pool, "search", target_did=_DID_ALICE, target_rkey="3jqentry00001"
        )

        stats = await query_entry_stats(db_pool, _DID_ALICE, "3jqentry00001", period="week")
        assert stats["views"] == 1
        assert stats["searchAppearances"] == 1
        assert stats["period"] == "week"

    async def test_query_active_publishers(self, db_pool):
        from atdata_app.database import query_active_publishers, upsert_entry, upsert_schema

        await upsert_entry(db_pool, _DID_ALICE, "3jqentry00001", "bafye1", _ENTRY_RECORD)
        await upsert_schema(
            db_pool, _DID_BOB, "com.example.test@1.0.0", "bafysc1", _SCHEMA_RECORD
        )

        count = await query_active_publishers(db_pool, days=30)
        assert count == 2


# ===================================================================
# D. EDGE CASE TESTS
# ===================================================================


class TestEdgeCases:
    """Boundary conditions and robustness checks."""

    async def test_pagination_no_more_results(self, db_pool):
        """When there are fewer results than the limit, no cursor should be needed."""
        from atdata_app.database import query_list_entries, upsert_entry
        from atdata_app.models import maybe_cursor

        await upsert_entry(db_pool, _DID_ALICE, "3jqentry00001", "bafye1", _ENTRY_RECORD)

        rows = await query_list_entries(db_pool, limit=50)
        assert len(rows) == 1
        cursor = maybe_cursor(rows, 50)
        assert cursor is None

    async def test_very_long_text_fields(self, db_pool):
        """Long descriptions and many tags should be handled without error."""
        from atdata_app.database import query_get_entry, upsert_entry

        long_desc = "A" * 10000
        many_tags = [f"tag-{i}" for i in range(100)]

        await upsert_entry(db_pool, _DID_ALICE, "3jqlong00001", "bafylong", {
            **_ENTRY_RECORD,
            "description": long_desc,
            "tags": many_tags,
        })

        row = await query_get_entry(db_pool, _DID_ALICE, "3jqlong00001")
        assert row is not None
        assert row["description"] == long_desc
        assert len(row["tags"]) == 100

    async def test_null_optional_fields(self, db_pool):
        """Entries with all optional fields as None should insert fine."""
        from atdata_app.database import query_get_entry, upsert_entry

        minimal_record = {
            "name": "Bare Minimum Dataset",
            "schemaRef": f"at://{_DID_ALICE}/science.alt.dataset.schema/s@1.0.0",
            "storage": {"$type": "science.alt.dataset.entry#httpStorage"},
            "createdAt": "2025-01-01T00:00:00Z",
        }
        await upsert_entry(db_pool, _DID_ALICE, "3jqbare00001", "bafybare", minimal_record)

        row = await query_get_entry(db_pool, _DID_ALICE, "3jqbare00001")
        assert row is not None
        assert row["name"] == "Bare Minimum Dataset"
        assert row["description"] is None
        assert row["tags"] is None
        assert row["license"] is None

    async def test_schema_with_minimal_record(self, db_pool):
        """Schema with no optional metadata should insert fine."""
        from atdata_app.database import query_get_schema, upsert_schema

        minimal = {
            "name": "minimal-schema",
            "version": "0.1.0",
            "schema": {"type": "string"},
            "createdAt": "2025-01-01T00:00:00Z",
        }
        await upsert_schema(
            db_pool, _DID_ALICE, "minimal-schema@0.1.0", "bafyminsc", minimal
        )

        row = await query_get_schema(db_pool, _DID_ALICE, "minimal-schema@0.1.0")
        assert row is not None
        assert row["schema_type"] == "jsonSchema"  # default
        assert row["description"] is None


class TestRunMigrations:
    """Test the run_migrations function from database.py."""

    async def test_run_migrations_applies_schema(self, db_pool):
        """run_migrations should be callable on an already-migrated pool."""
        from atdata_app.database import run_migrations

        # The fixture already applied schema. run_migrations should be idempotent.
        await run_migrations(db_pool)

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT COUNT(*) AS cnt FROM pg_tables WHERE schemaname = 'public'"
            )
            assert row["cnt"] >= 7
