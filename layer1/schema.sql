-- In(Sights) Tracker Postgres schema
-- Run once against the production Postgres database.
-- For Hetzner self-hosted Postgres, install pgvector before running this file.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS raw_articles (
    id                BIGSERIAL PRIMARY KEY,
    url_hash          TEXT UNIQUE NOT NULL,
    url               TEXT NOT NULL,
    source_name       TEXT NOT NULL,
    source_tier       TEXT NOT NULL DEFAULT 'pan-africa',
    hard_country_tags JSONB,
    language          TEXT NOT NULL DEFAULT 'en',
    paywall_status    TEXT NOT NULL DEFAULT 'open',
    headline          TEXT,
    lede              TEXT,
    published_at      TIMESTAMPTZ,
    scraped_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_raw_unprocessed
    ON raw_articles (processed_at)
    WHERE processed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_raw_scraped_at
    ON raw_articles (scraped_at DESC);

CREATE TABLE IF NOT EXISTS enriched_articles (
    id                BIGSERIAL PRIMARY KEY,
    raw_id            BIGINT UNIQUE NOT NULL REFERENCES raw_articles(id) ON DELETE CASCADE,
    taxonomy_version  TEXT NOT NULL,

    primary_country   TEXT NOT NULL,
    countries         JSONB NOT NULL DEFAULT '[]'::jsonb,
    regions           JSONB NOT NULL DEFAULT '[]'::jsonb,

    primary_sector    TEXT NOT NULL,
    sector_tags       JSONB NOT NULL DEFAULT '[]'::jsonb,
    themes            JSONB NOT NULL DEFAULT '[]'::jsonb,
    event_types       JSONB NOT NULL DEFAULT '[]'::jsonb,
    entities          JSONB NOT NULL DEFAULT '[]'::jsonb,

    sentiment         TEXT,
    time_horizon      TEXT,
    relevance_score   INTEGER CHECK (relevance_score BETWEEN 1 AND 5),
    relevance_reason  TEXT,
    summary           TEXT,
    embedding         VECTOR(384),

    tag_status        TEXT NOT NULL DEFAULT 'ai_generated',
    enriched_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    reviewed_at       TIMESTAMPTZ,
    reviewer          TEXT,
    review_notes      TEXT,

    -- Backward-compatible aliases for older ad hoc queries.
    country           TEXT,
    sector            TEXT
);

CREATE INDEX IF NOT EXISTS idx_enriched_primary_country
    ON enriched_articles (primary_country);

CREATE INDEX IF NOT EXISTS idx_enriched_primary_sector
    ON enriched_articles (primary_sector);

CREATE INDEX IF NOT EXISTS idx_enriched_relevance
    ON enriched_articles (relevance_score DESC);

CREATE INDEX IF NOT EXISTS idx_enriched_countries_gin
    ON enriched_articles USING GIN (countries);

CREATE INDEX IF NOT EXISTS idx_enriched_regions_gin
    ON enriched_articles USING GIN (regions);

CREATE INDEX IF NOT EXISTS idx_enriched_sector_tags_gin
    ON enriched_articles USING GIN (sector_tags);

CREATE INDEX IF NOT EXISTS idx_enriched_themes_gin
    ON enriched_articles USING GIN (themes);

CREATE INDEX IF NOT EXISTS idx_enriched_event_types_gin
    ON enriched_articles USING GIN (event_types);

CREATE INDEX IF NOT EXISTS idx_enriched_embedding
    ON enriched_articles USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

CREATE TABLE IF NOT EXISTS report_runs (
    id               BIGSERIAL PRIMARY KEY,
    report_month     TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'drafted',
    filters          JSONB NOT NULL DEFAULT '{}'::jsonb,
    report_path      TEXT,
    report_json_path TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS source_health (
    id                BIGSERIAL PRIMARY KEY,
    source_name       TEXT NOT NULL,
    source_tier       TEXT,
    source_url        TEXT,
    run_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    articles_fetched  INTEGER DEFAULT 0,
    articles_inserted INTEGER DEFAULT 0,
    articles_duped    INTEGER DEFAULT 0,
    had_error         BOOLEAN DEFAULT FALSE,
    error_msg         TEXT
);

CREATE INDEX IF NOT EXISTS idx_health_source
    ON source_health (source_name, run_at DESC);

-- App-layer additions: clustering, briefs, audit, saved views, users.

ALTER TABLE enriched_articles ADD COLUMN IF NOT EXISTS cluster_id BIGINT;
ALTER TABLE enriched_articles ADD COLUMN IF NOT EXISTS cluster_rank INTEGER;

CREATE INDEX IF NOT EXISTS idx_enriched_cluster ON enriched_articles (cluster_id);

CREATE TABLE IF NOT EXISTS briefs (
    id              BIGSERIAL PRIMARY KEY,
    title           TEXT NOT NULL,
    period_days     INTEGER NOT NULL,
    min_relevance   INTEGER NOT NULL,
    filters         JSONB NOT NULL DEFAULT '{}'::jsonb,
    report_markdown TEXT NOT NULL,
    report_json     JSONB NOT NULL DEFAULT '{}'::jsonb,
    rows_analyzed   INTEGER NOT NULL DEFAULT 0,
    model           TEXT,
    author          TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_briefs_created ON briefs (created_at DESC);

CREATE TABLE IF NOT EXISTS brief_jobs (
    id          BIGSERIAL PRIMARY KEY,
    status      TEXT NOT NULL DEFAULT 'queued',
    params      JSONB NOT NULL DEFAULT '{}'::jsonb,
    brief_id    BIGINT REFERENCES briefs(id) ON DELETE SET NULL,
    log         TEXT,
    author      TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at  TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_brief_jobs_status ON brief_jobs (status, created_at DESC);

CREATE TABLE IF NOT EXISTS tag_audit (
    id         BIGSERIAL PRIMARY KEY,
    raw_id     BIGINT NOT NULL REFERENCES raw_articles(id) ON DELETE CASCADE,
    reviewer   TEXT,
    before_val JSONB NOT NULL DEFAULT '{}'::jsonb,
    after_val  JSONB NOT NULL DEFAULT '{}'::jsonb,
    notes      TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tag_audit_raw ON tag_audit (raw_id, created_at DESC);

CREATE TABLE IF NOT EXISTS saved_views (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    owner      TEXT,
    filters    JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (name, owner)
);

CREATE TABLE IF NOT EXISTS app_users (
    id            BIGSERIAL PRIMARY KEY,
    username      TEXT UNIQUE NOT NULL,
    display_name  TEXT,
    password_hash TEXT NOT NULL,
    role          TEXT NOT NULL DEFAULT 'analyst',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_login_at TIMESTAMPTZ
);
