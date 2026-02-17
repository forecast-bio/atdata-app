# Data Model

Four tables indexed from the `ac.foundation.dataset.*` namespace, plus cursor state for firehose crash recovery. All tables use `(did, rkey)` as composite primary key. Schema auto-applies on startup.

## schemas

Stores dataset schema definitions. Record key format: `{schema-id}@{semver}` (e.g., `my.schema@1.0.0`).

Fields: `name`, `version`, `schema_type` (default `jsonSchema`), `schema_body` (JSONB), `description`, `metadata` (JSONB).

## entries

Stores dataset metadata records. Includes weighted full-text search (`search_tsv`) across name (A), description (B), and tags (C).

Fields: `name`, `schema_ref` (AT-URI to schema), `storage` (JSONB — `storageHttp`, `storageS3`, or `storageBlobs`), `description`, `tags` (text array), `license`, `size_samples`, `size_bytes`, `size_shards`, `content_metadata` (JSONB).

## labels

Named version pointers to dataset entries (analogous to git tags).

Fields: `name`, `dataset_uri` (AT-URI to entry), `version`, `description`.

## lenses

Bidirectional schema transforms with executable code for migrating data between schema versions.

Fields: `name`, `source_schema` (AT-URI), `target_schema` (AT-URI), `getter_code` (JSONB), `putter_code` (JSONB), `description`, `language`.
