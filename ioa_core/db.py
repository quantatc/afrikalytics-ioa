from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any

from .env import load_repo_env
from .paths import SQLITE_DB_PATH


JSON_COLUMNS = {
    "hard_country_tags",
    "countries",
    "regions",
    "sector_tags",
    "themes",
    "event_types",
    "entities",
    "filters",
}


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True)


def json_loads(value: Any, default: Any = None) -> Any:
    if value is None:
        return [] if default is None else default
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if not isinstance(value, str) or not value.strip():
        return [] if default is None else default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return [] if default is None else default


def connect_db(mode: str = "dev"):
    """Return (connection, db_type), where db_type is sqlite or postgres."""
    load_repo_env()

    if mode == "prod":
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise RuntimeError("DATABASE_URL is required when --mode prod is used")
        import psycopg
        from psycopg.rows import dict_row

        conn = psycopg.connect(database_url, row_factory=dict_row)
        return conn, "postgres"

    SQLITE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(SQLITE_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    ensure_sqlite_schema(conn)
    return conn, "sqlite"


def ensure_sqlite_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_articles (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            url_hash          TEXT    UNIQUE NOT NULL,
            url               TEXT    NOT NULL,
            source_name       TEXT    NOT NULL,
            source_tier       TEXT    NOT NULL DEFAULT 'pan-africa',
            hard_country_tags TEXT,
            language          TEXT    NOT NULL DEFAULT 'en',
            paywall_status    TEXT    NOT NULL DEFAULT 'open',
            headline          TEXT,
            lede              TEXT,
            published_at      TEXT,
            scraped_at        TEXT    NOT NULL,
            processed_at      TEXT    DEFAULT NULL
        )
        """
    )
    _ensure_columns(
        conn,
        "raw_articles",
        {
            "language": "TEXT NOT NULL DEFAULT 'en'",
            "paywall_status": "TEXT NOT NULL DEFAULT 'open'",
        },
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS enriched_articles (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_id            INTEGER UNIQUE NOT NULL,
            taxonomy_version  TEXT,
            primary_country   TEXT,
            countries         TEXT NOT NULL DEFAULT '[]',
            regions           TEXT NOT NULL DEFAULT '[]',
            primary_sector    TEXT,
            sector_tags       TEXT NOT NULL DEFAULT '[]',
            themes            TEXT NOT NULL DEFAULT '[]',
            event_types       TEXT NOT NULL DEFAULT '[]',
            entities          TEXT NOT NULL DEFAULT '[]',
            sentiment         TEXT,
            time_horizon      TEXT,
            relevance_score   INTEGER,
            relevance_reason  TEXT,
            summary           TEXT,
            embedding         TEXT,
            tag_status        TEXT NOT NULL DEFAULT 'ai_generated',
            enriched_at       TEXT NOT NULL,
            reviewed_at       TEXT,
            reviewer          TEXT,
            review_notes      TEXT,
            country           TEXT,
            sector            TEXT,
            FOREIGN KEY(raw_id) REFERENCES raw_articles(id) ON DELETE CASCADE
        )
        """
    )
    _ensure_columns(
        conn,
        "enriched_articles",
        {
            "taxonomy_version": "TEXT",
            "primary_country": "TEXT",
            "countries": "TEXT NOT NULL DEFAULT '[]'",
            "regions": "TEXT NOT NULL DEFAULT '[]'",
            "primary_sector": "TEXT",
            "sector_tags": "TEXT NOT NULL DEFAULT '[]'",
            "themes": "TEXT NOT NULL DEFAULT '[]'",
            "event_types": "TEXT NOT NULL DEFAULT '[]'",
            "entities": "TEXT NOT NULL DEFAULT '[]'",
            "sentiment": "TEXT",
            "time_horizon": "TEXT",
            "tag_status": "TEXT NOT NULL DEFAULT 'ai_generated'",
            "reviewed_at": "TEXT",
            "reviewer": "TEXT",
            "review_notes": "TEXT",
            "country": "TEXT",
            "sector": "TEXT",
            "cluster_id": "INTEGER",
            "cluster_rank": "INTEGER",
        },
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_health (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name       TEXT NOT NULL,
            source_tier       TEXT,
            source_url        TEXT,
            run_at            TEXT NOT NULL,
            articles_fetched  INTEGER DEFAULT 0,
            articles_inserted INTEGER DEFAULT 0,
            articles_duped    INTEGER DEFAULT 0,
            had_error         INTEGER DEFAULT 0,
            error_msg         TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS report_runs (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            report_month     TEXT NOT NULL,
            status           TEXT NOT NULL DEFAULT 'drafted',
            filters          TEXT NOT NULL DEFAULT '{}',
            report_path      TEXT,
            report_json_path TEXT,
            created_at       TEXT NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS briefs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            title           TEXT NOT NULL,
            period_days     INTEGER NOT NULL,
            min_relevance   INTEGER NOT NULL,
            filters         TEXT NOT NULL DEFAULT '{}',
            report_markdown TEXT NOT NULL,
            report_json     TEXT NOT NULL DEFAULT '{}',
            rows_analyzed   INTEGER NOT NULL DEFAULT 0,
            model           TEXT,
            author          TEXT,
            created_at      TEXT NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS brief_jobs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            status      TEXT NOT NULL DEFAULT 'queued',
            params      TEXT NOT NULL DEFAULT '{}',
            brief_id    INTEGER,
            log         TEXT,
            author      TEXT,
            created_at  TEXT NOT NULL,
            started_at  TEXT,
            finished_at TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tag_audit (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_id     INTEGER NOT NULL,
            reviewer   TEXT,
            before_val TEXT NOT NULL DEFAULT '{}',
            after_val  TEXT NOT NULL DEFAULT '{}',
            notes      TEXT,
            created_at TEXT NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS saved_views (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            name       TEXT NOT NULL,
            owner      TEXT,
            filters    TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            UNIQUE (name, owner)
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            username      TEXT UNIQUE NOT NULL,
            display_name  TEXT,
            password_hash TEXT NOT NULL,
            role          TEXT NOT NULL DEFAULT 'analyst',
            created_at    TEXT NOT NULL,
            last_login_at TEXT
        )
        """
    )

    conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_unprocessed ON raw_articles (processed_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_scraped_at ON raw_articles (scraped_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_enriched_relevance ON enriched_articles (relevance_score DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_enriched_primary_country ON enriched_articles (primary_country)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_enriched_primary_sector ON enriched_articles (primary_sector)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_enriched_cluster ON enriched_articles (cluster_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_health_source ON source_health (source_name, run_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_briefs_created ON briefs (created_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_brief_jobs_status ON brief_jobs (status, created_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tag_audit_raw ON tag_audit (raw_id, created_at DESC)")
    conn.commit()


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, definition in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")


def vector_for_db(vector: list[float], db_type: str) -> Any:
    if db_type == "postgres":
        return "[" + ",".join(f"{float(v):.8f}" for v in vector) + "]"
    return json_dumps(vector)


def json_for_db(value: Any, db_type: str) -> Any:
    if db_type == "postgres":
        from psycopg.types.json import Jsonb

        return Jsonb(value)
    return json_dumps(value)
