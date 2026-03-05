-- IOA Intelligence Briefing — Supabase Schema
-- Run once in Supabase SQL editor to set up Layer 1 storage
-- Enable pgvector extension first: Extensions → pgvector → Enable

-- ── Layer 1: Raw Articles ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_articles (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    url_hash          TEXT    UNIQUE NOT NULL,   -- SHA256[:16] of URL for dedup
    url               TEXT    NOT NULL,
    source_name       TEXT    NOT NULL,
    source_tier       TEXT    NOT NULL DEFAULT 'pan-africa',
    hard_country_tags TEXT,                      -- JSON list e.g. '["NG"]', null for Tier 1
    language          TEXT    NOT NULL DEFAULT 'en',  -- ISO 639-1: en|fr|pt|ar — Layer 2 translation routing
    paywall_status    TEXT    NOT NULL DEFAULT 'open', -- open|restricted|paywalled — logged on block
    headline          TEXT,
    lede              TEXT,                      -- first ~1000 chars, HTML stripped
    published_at      TIMESTAMPTZ,
    scraped_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at      TIMESTAMPTZ DEFAULT NULL   -- set by Layer 2 enrichment
);

-- Index for Layer 2 to efficiently fetch unprocessed articles
CREATE INDEX IF NOT EXISTS idx_raw_unprocessed
    ON raw_articles (processed_at)
    WHERE processed_at IS NULL;

-- Index for date-range queries (monthly report generation)
CREATE INDEX IF NOT EXISTS idx_raw_scraped_at
    ON raw_articles (scraped_at DESC);


-- ── Layer 2: Enriched Articles ────────────────────────────────────────────
-- Created here for reference — populated by Layer 2 enrichment service
CREATE TABLE IF NOT EXISTS enriched_articles (
    id               BIGSERIAL PRIMARY KEY,
    raw_id           BIGINT REFERENCES raw_articles(id) ON DELETE CASCADE,
    country          TEXT,                  -- primary African country or 'Pan-Africa'
    sector           TEXT,                  -- Energy|Mining|Tech|Finance|Policy|Agriculture|Infrastructure|Other
    relevance_score  INTEGER CHECK (relevance_score BETWEEN 1 AND 5),
    relevance_reason TEXT,
    summary          TEXT,                  -- 3-sentence summary from Haiku
    embedding        VECTOR(384),           -- all-MiniLM-L6-v2 embeddings for RAG
    enriched_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index for monthly report queries by country + sector
CREATE INDEX IF NOT EXISTS idx_enriched_country_sector
    ON enriched_articles (country, sector);

-- Index for relevance filtering
CREATE INDEX IF NOT EXISTS idx_enriched_relevance
    ON enriched_articles (relevance_score DESC);

-- pgvector index for RAG similarity search
CREATE INDEX IF NOT EXISTS idx_enriched_embedding
    ON enriched_articles USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);


-- ── Layer 3: Report Runs ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS report_runs (
    id           BIGSERIAL PRIMARY KEY,
    report_month TEXT        NOT NULL,      -- e.g. '2025-03'
    status       TEXT        NOT NULL DEFAULT 'pending',  -- pending|drafted|approved|sent
    draft_url    TEXT,                      -- Google Drive link to draft
    approved_at  TIMESTAMPTZ,
    sent_at      TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- ── Source Health Log ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS source_health (
    id           BIGSERIAL PRIMARY KEY,
    source_name  TEXT        NOT NULL,
    run_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    articles_fetched  INTEGER DEFAULT 0,
    articles_inserted INTEGER DEFAULT 0,
    articles_duped    INTEGER DEFAULT 0,
    had_error    BOOLEAN DEFAULT FALSE,
    error_msg    TEXT
);

CREATE INDEX IF NOT EXISTS idx_health_source
    ON source_health (source_name, run_at DESC);
