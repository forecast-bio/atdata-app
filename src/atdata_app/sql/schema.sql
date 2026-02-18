-- atdata-app database schema
-- All ac.foundation.dataset.* record types + cursor state

-- Schemas (ac.foundation.dataset.schema)
-- rkey format: {NSID}@{semver}
CREATE TABLE IF NOT EXISTS schemas (
    did         TEXT NOT NULL,
    rkey        TEXT NOT NULL,
    cid         TEXT,
    name        TEXT NOT NULL,
    version     TEXT NOT NULL,
    schema_type TEXT NOT NULL DEFAULT 'jsonSchema',
    schema_body JSONB NOT NULL,
    description TEXT,
    metadata    JSONB,
    created_at  TEXT NOT NULL,
    indexed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (did, rkey)
);

CREATE INDEX IF NOT EXISTS idx_schemas_name ON schemas (name);
CREATE INDEX IF NOT EXISTS idx_schemas_did ON schemas (did);

-- Dataset entries (ac.foundation.dataset.record)
-- rkey format: TID
CREATE TABLE IF NOT EXISTS entries (
    did                 TEXT NOT NULL,
    rkey                TEXT NOT NULL,
    cid                 TEXT,
    name                TEXT NOT NULL,
    schema_ref          TEXT NOT NULL,
    storage             JSONB NOT NULL,
    description         TEXT,
    tags                TEXT[],
    license             TEXT,
    size_samples        BIGINT,
    size_bytes          BIGINT,
    size_shards         INTEGER,
    metadata_schema_ref TEXT,
    content_metadata    JSONB,
    created_at          TEXT NOT NULL,
    indexed_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (did, rkey)
);

CREATE INDEX IF NOT EXISTS idx_entries_name ON entries (name);
CREATE INDEX IF NOT EXISTS idx_entries_did ON entries (did);
CREATE INDEX IF NOT EXISTS idx_entries_schema_ref ON entries (schema_ref);
CREATE INDEX IF NOT EXISTS idx_entries_tags ON entries USING GIN (tags);
CREATE INDEX IF NOT EXISTS idx_entries_indexed_at ON entries (indexed_at DESC);

-- Full-text search
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'entries' AND column_name = 'search_tsv'
    ) THEN
        ALTER TABLE entries ADD COLUMN search_tsv TSVECTOR
            GENERATED ALWAYS AS (
                setweight(to_tsvector('english'::regconfig, coalesce(name, '')), 'A') ||
                setweight(to_tsvector('english'::regconfig, coalesce(description, '')), 'B') ||
                setweight(to_tsvector('english'::regconfig, coalesce(array_to_string(tags, ' '), '')), 'C')
            ) STORED;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_entries_search ON entries USING GIN (search_tsv);

-- Labels (ac.foundation.dataset.label)
CREATE TABLE IF NOT EXISTS labels (
    did         TEXT NOT NULL,
    rkey        TEXT NOT NULL,
    cid         TEXT,
    name        TEXT NOT NULL,
    dataset_uri TEXT NOT NULL,
    version     TEXT,
    description TEXT,
    created_at  TEXT NOT NULL,
    indexed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (did, rkey)
);

CREATE INDEX IF NOT EXISTS idx_labels_name ON labels (did, name);
CREATE INDEX IF NOT EXISTS idx_labels_did ON labels (did);
CREATE INDEX IF NOT EXISTS idx_labels_dataset_uri ON labels (dataset_uri);

-- Lenses (ac.foundation.dataset.lens)
CREATE TABLE IF NOT EXISTS lenses (
    did              TEXT NOT NULL,
    rkey             TEXT NOT NULL,
    cid              TEXT,
    name             TEXT NOT NULL,
    source_schema    TEXT NOT NULL,
    target_schema    TEXT NOT NULL,
    getter_code      JSONB NOT NULL,
    putter_code      JSONB NOT NULL,
    description      TEXT,
    language         TEXT,
    metadata         JSONB,
    created_at       TEXT NOT NULL,
    indexed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (did, rkey)
);

CREATE INDEX IF NOT EXISTS idx_lenses_source_schema ON lenses (source_schema);
CREATE INDEX IF NOT EXISTS idx_lenses_target_schema ON lenses (target_schema);
CREATE INDEX IF NOT EXISTS idx_lenses_did ON lenses (did);

-- Cursor state for firehose crash recovery
CREATE TABLE IF NOT EXISTS cursor_state (
    service     TEXT PRIMARY KEY DEFAULT 'jetstream',
    cursor      BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Analytics events (lightweight server-side request counting)
CREATE TABLE IF NOT EXISTS analytics_events (
    id           BIGSERIAL PRIMARY KEY,
    event_type   TEXT NOT NULL,
    target_did   TEXT,
    target_rkey  TEXT,
    query_params JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analytics_events_type_created
    ON analytics_events (event_type, created_at);
CREATE INDEX IF NOT EXISTS idx_analytics_events_target
    ON analytics_events (target_did, target_rkey, event_type);

-- Pre-aggregated analytics counters (avoids expensive COUNT on events table)
CREATE TABLE IF NOT EXISTS analytics_counters (
    target_did   TEXT NOT NULL,
    target_rkey  TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    count        BIGINT NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (target_did, target_rkey, event_type)
);
