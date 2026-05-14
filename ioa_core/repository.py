"""Shared read/write helpers for the Streamlit app.

Centralizes SQL so pages don't each reinvent it.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pandas as pd

from .countries import country_display_name
from .db import connect_db, json_dumps, json_for_db, json_loads, now_utc_iso


JSON_FIELDS = ("countries", "regions", "sector_tags", "themes", "event_types", "entities")


def _cutoff(days: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=int(days))


def _rows(conn, db_type: str, sqlite_sql: str, pg_sql: str, sqlite_params=(), pg_params=()) -> list[dict]:
    if db_type == "sqlite":
        return [dict(r) for r in conn.execute(sqlite_sql, sqlite_params).fetchall()]
    return [dict(r) for r in conn.execute(pg_sql, pg_params).fetchall()]


def fetch_articles(mode: str, days: int, query: str | None = None) -> pd.DataFrame:
    """Enriched articles in a window, with optional server-side text search."""
    conn, db_type = connect_db(mode)
    cutoff_dt = _cutoff(days)
    cutoff = cutoff_dt.isoformat()
    query = (query or "").strip()
    like = f"%{query.lower()}%" if query else None

    base_cols = """
        e.raw_id, e.taxonomy_version, e.primary_country, e.countries, e.regions,
        e.primary_sector, e.sector_tags, e.themes, e.event_types, e.entities,
        e.sentiment, e.time_horizon, e.relevance_score, e.relevance_reason,
        e.summary, e.tag_status, e.enriched_at, e.reviewed_at, e.reviewer,
        e.cluster_id, e.cluster_rank,
        r.headline, r.url, r.source_name, r.source_tier, r.published_at, r.scraped_at
    """

    if db_type == "sqlite":
        if like:
            sql = f"""
                SELECT {base_cols}
                FROM enriched_articles e
                JOIN raw_articles r ON r.id = e.raw_id
                WHERE e.enriched_at >= ?
                  AND (
                      LOWER(COALESCE(r.headline, '')) LIKE ?
                      OR LOWER(COALESCE(e.summary, '')) LIKE ?
                      OR LOWER(COALESCE(r.source_name, '')) LIKE ?
                  )
                ORDER BY e.enriched_at DESC
            """
            params = (cutoff, like, like, like)
        else:
            sql = f"""
                SELECT {base_cols}
                FROM enriched_articles e
                JOIN raw_articles r ON r.id = e.raw_id
                WHERE e.enriched_at >= ?
                ORDER BY e.enriched_at DESC
            """
            params = (cutoff,)
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    else:
        if like:
            sql = f"""
                SELECT {base_cols}
                FROM enriched_articles e
                JOIN raw_articles r ON r.id = e.raw_id
                WHERE e.enriched_at >= %s
                  AND (
                      COALESCE(r.headline, '') ILIKE %s
                      OR COALESCE(e.summary, '') ILIKE %s
                      OR COALESCE(r.source_name, '') ILIKE %s
                  )
                ORDER BY e.enriched_at DESC
            """
            params = (cutoff_dt, like, like, like)
        else:
            sql = f"""
                SELECT {base_cols}
                FROM enriched_articles e
                JOIN raw_articles r ON r.id = e.raw_id
                WHERE e.enriched_at >= %s
                ORDER BY e.enriched_at DESC
            """
            params = (cutoff_dt,)
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for field in JSON_FIELDS:
        if field in df.columns:
            df[field] = df[field].apply(json_loads)
    df["country_label"] = df["primary_country"].apply(country_display_name)
    df["headline"] = df["headline"].fillna("")
    df["source_name"] = df["source_name"].fillna("Unknown")
    return df


def fetch_raw_count(mode: str) -> int:
    conn, db_type = connect_db(mode)
    row = conn.execute("SELECT COUNT(*) AS c FROM raw_articles").fetchone()
    return int(row["c"]) if row else 0


def fetch_raw_articles(mode: str, days: int) -> pd.DataFrame:
    conn, db_type = connect_db(mode)
    cutoff_dt = _cutoff(days)
    rows = _rows(
        conn,
        db_type,
        """
        SELECT id, headline, url, source_name, source_tier, hard_country_tags,
               language, paywall_status, published_at, scraped_at, processed_at
        FROM raw_articles
        WHERE scraped_at >= ?
        ORDER BY scraped_at DESC
        LIMIT 1000
        """,
        """
        SELECT id, headline, url, source_name, source_tier, hard_country_tags,
               language, paywall_status, published_at, scraped_at, processed_at
        FROM raw_articles
        WHERE scraped_at >= %s
        ORDER BY scraped_at DESC
        LIMIT 1000
        """,
        (cutoff_dt.isoformat(),),
        (cutoff_dt,),
    )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["headline"] = df["headline"].fillna("")
        df["source_name"] = df["source_name"].fillna("Unknown")
    return df


def fetch_health(mode: str) -> pd.DataFrame:
    conn, db_type = connect_db(mode)
    rows = _rows(
        conn,
        db_type,
        """
        SELECT source_name, source_tier, source_url, run_at, articles_fetched,
               articles_inserted, articles_duped, had_error, error_msg
        FROM source_health
        ORDER BY run_at DESC
        LIMIT 2000
        """,
        """
        SELECT source_name, source_tier, source_url, run_at, articles_fetched,
               articles_inserted, articles_duped, had_error, error_msg
        FROM source_health
        ORDER BY run_at DESC
        LIMIT 2000
        """,
    )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["run_at"] = pd.to_datetime(df["run_at"], errors="coerce", utc=True)
        df["had_error"] = df["had_error"].fillna(0).astype(int)
    return df


def list_enriched_sources(mode: str, days: int = 60) -> list[str]:
    """Distinct source_name values present in enriched_articles within the window."""
    conn, db_type = connect_db(mode)
    cutoff = _cutoff(days).isoformat()
    rows = _rows(
        conn,
        db_type,
        (
            "SELECT DISTINCT r.source_name FROM enriched_articles e "
            "JOIN raw_articles r ON r.id = e.raw_id "
            "WHERE e.enriched_at >= ? AND r.source_name IS NOT NULL AND TRIM(r.source_name) != '' "
            "ORDER BY r.source_name"
        ),
        (
            "SELECT DISTINCT r.source_name FROM enriched_articles e "
            "JOIN raw_articles r ON r.id = e.raw_id "
            "WHERE e.enriched_at >= %s AND r.source_name IS NOT NULL AND TRIM(r.source_name) != '' "
            "ORDER BY r.source_name"
        ),
        sqlite_params=(cutoff,),
        pg_params=(cutoff,),
    )
    return [r["source_name"] for r in rows if r.get("source_name")]


def save_tag_audit(
    mode: str,
    raw_id: int,
    reviewer: str,
    before_val: dict,
    after_val: dict,
    notes: str,
) -> None:
    conn, db_type = connect_db(mode)
    ts = now_utc_iso()
    if db_type == "sqlite":
        conn.execute(
            """
            INSERT INTO tag_audit (raw_id, reviewer, before_val, after_val, notes, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (raw_id, reviewer, json_dumps(before_val), json_dumps(after_val), notes, ts),
        )
    else:
        conn.execute(
            """
            INSERT INTO tag_audit (raw_id, reviewer, before_val, after_val, notes, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (raw_id, reviewer, json_for_db(before_val, db_type), json_for_db(after_val, db_type), notes, ts),
        )
    conn.commit()


def fetch_tag_audit(mode: str, raw_id: int) -> list[dict]:
    conn, db_type = connect_db(mode)
    rows = _rows(
        conn,
        db_type,
        """
        SELECT id, reviewer, before_val, after_val, notes, created_at
        FROM tag_audit WHERE raw_id = ? ORDER BY created_at DESC LIMIT 50
        """,
        """
        SELECT id, reviewer, before_val, after_val, notes, created_at
        FROM tag_audit WHERE raw_id = %s ORDER BY created_at DESC LIMIT 50
        """,
        (raw_id,),
        (raw_id,),
    )
    for r in rows:
        r["before_val"] = json_loads(r.get("before_val"), default={})
        r["after_val"] = json_loads(r.get("after_val"), default={})
    return rows


def apply_tag_update(mode: str, raw_id: int, values: dict[str, Any]) -> None:
    """Persist reviewer-approved tags; assumes caller already computed regions."""
    conn, db_type = connect_db(mode)
    if db_type == "sqlite":
        conn.execute(
            """
            UPDATE enriched_articles
            SET primary_country = ?, countries = ?, regions = ?, primary_sector = ?,
                sector_tags = ?, themes = ?, event_types = ?, entities = ?,
                sentiment = ?, time_horizon = ?, tag_status = ?,
                reviewed_at = ?, reviewer = ?, review_notes = ?, country = ?, sector = ?
            WHERE raw_id = ?
            """,
            (
                values["primary_country"],
                json_dumps(values["countries"]),
                json_dumps(values["regions"]),
                values["primary_sector"],
                json_dumps(values["sector_tags"]),
                json_dumps(values["themes"]),
                json_dumps(values["event_types"]),
                json_dumps(values["entities"]),
                values["sentiment"],
                values["time_horizon"],
                values.get("tag_status", "approved"),
                values["reviewed_at"],
                values["reviewer"],
                values.get("review_notes", ""),
                values["primary_country"],
                values["primary_sector"],
                raw_id,
            ),
        )
    else:
        conn.execute(
            """
            UPDATE enriched_articles
            SET primary_country = %s, countries = %s, regions = %s, primary_sector = %s,
                sector_tags = %s, themes = %s, event_types = %s, entities = %s,
                sentiment = %s, time_horizon = %s, tag_status = %s,
                reviewed_at = %s, reviewer = %s, review_notes = %s, country = %s, sector = %s
            WHERE raw_id = %s
            """,
            (
                values["primary_country"],
                json_for_db(values["countries"], db_type),
                json_for_db(values["regions"], db_type),
                values["primary_sector"],
                json_for_db(values["sector_tags"], db_type),
                json_for_db(values["themes"], db_type),
                json_for_db(values["event_types"], db_type),
                json_for_db(values["entities"], db_type),
                values["sentiment"],
                values["time_horizon"],
                values.get("tag_status", "approved"),
                values["reviewed_at"],
                values["reviewer"],
                values.get("review_notes", ""),
                values["primary_country"],
                values["primary_sector"],
                raw_id,
            ),
        )
    conn.commit()


def bulk_approve(mode: str, raw_ids: list[int], reviewer: str) -> int:
    if not raw_ids:
        return 0
    conn, db_type = connect_db(mode)
    ts = now_utc_iso()
    if db_type == "sqlite":
        placeholders = ",".join("?" for _ in raw_ids)
        cur = conn.execute(
            f"""
            UPDATE enriched_articles
            SET tag_status = 'approved', reviewed_at = ?, reviewer = ?
            WHERE raw_id IN ({placeholders}) AND tag_status != 'approved'
            """,
            (ts, reviewer, *raw_ids),
        )
        changed = cur.rowcount or 0
    else:
        cur = conn.execute(
            """
            UPDATE enriched_articles
            SET tag_status = 'approved', reviewed_at = %s, reviewer = %s
            WHERE raw_id = ANY(%s) AND tag_status != 'approved'
            """,
            (ts, reviewer, list(raw_ids)),
        )
        changed = cur.rowcount or 0
    conn.commit()
    return int(changed)


def save_view(mode: str, name: str, owner: str, filters: dict) -> None:
    conn, db_type = connect_db(mode)
    ts = now_utc_iso()
    if db_type == "sqlite":
        conn.execute(
            "INSERT OR REPLACE INTO saved_views (name, owner, filters, created_at) VALUES (?, ?, ?, ?)",
            (name, owner, json_dumps(filters), ts),
        )
    else:
        conn.execute(
            """
            INSERT INTO saved_views (name, owner, filters, created_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (name, owner) DO UPDATE SET filters = EXCLUDED.filters, created_at = EXCLUDED.created_at
            """,
            (name, owner, json_for_db(filters, db_type), ts),
        )
    conn.commit()


def list_views(mode: str, owner: Optional[str] = None) -> list[dict]:
    conn, db_type = connect_db(mode)
    if db_type == "sqlite":
        if owner:
            rows = conn.execute(
                "SELECT id, name, owner, filters, created_at FROM saved_views WHERE owner = ? OR owner IS NULL ORDER BY name",
                (owner,),
            ).fetchall()
        else:
            rows = conn.execute("SELECT id, name, owner, filters, created_at FROM saved_views ORDER BY name").fetchall()
    else:
        if owner:
            rows = conn.execute(
                "SELECT id, name, owner, filters, created_at FROM saved_views WHERE owner = %s OR owner IS NULL ORDER BY name",
                (owner,),
            ).fetchall()
        else:
            rows = conn.execute("SELECT id, name, owner, filters, created_at FROM saved_views ORDER BY name").fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["filters"] = json_loads(d.get("filters"), default={})
        out.append(d)
    return out


def delete_view(mode: str, view_id: int, owner: str) -> None:
    conn, db_type = connect_db(mode)
    if db_type == "sqlite":
        conn.execute("DELETE FROM saved_views WHERE id = ? AND (owner = ? OR owner IS NULL)", (view_id, owner))
    else:
        conn.execute("DELETE FROM saved_views WHERE id = %s AND (owner = %s OR owner IS NULL)", (view_id, owner))
    conn.commit()


def list_briefs(mode: str, limit: int = 100) -> pd.DataFrame:
    conn, db_type = connect_db(mode)
    if db_type == "sqlite":
        rows = conn.execute(
            "SELECT id, title, period_days, min_relevance, rows_analyzed, model, author, created_at, filters FROM briefs ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, title, period_days, min_relevance, rows_analyzed, model, author, created_at, filters FROM briefs ORDER BY created_at DESC LIMIT %s",
            (limit,),
        ).fetchall()
    df = pd.DataFrame([dict(r) for r in rows])
    if not df.empty:
        df["filters"] = df["filters"].apply(lambda v: json_loads(v, default={}))
    return df


def fetch_brief(mode: str, brief_id: int) -> Optional[dict]:
    conn, db_type = connect_db(mode)
    if db_type == "sqlite":
        row = conn.execute("SELECT * FROM briefs WHERE id = ?", (brief_id,)).fetchone()
    else:
        row = conn.execute("SELECT * FROM briefs WHERE id = %s", (brief_id,)).fetchone()
    if not row:
        return None
    d = dict(row)
    d["filters"] = json_loads(d.get("filters"), default={})
    d["report_json"] = json_loads(d.get("report_json"), default={})
    return d


def delete_brief(mode: str, brief_id: int) -> None:
    conn, db_type = connect_db(mode)
    if db_type == "sqlite":
        conn.execute("DELETE FROM briefs WHERE id = ?", (brief_id,))
    else:
        conn.execute("DELETE FROM briefs WHERE id = %s", (brief_id,))
    conn.commit()


def insert_brief(
    mode: str,
    title: str,
    period_days: int,
    min_relevance: int,
    filters: dict,
    report_markdown: str,
    report_json: dict,
    rows_analyzed: int,
    model: str,
    author: str,
) -> int:
    conn, db_type = connect_db(mode)
    ts = now_utc_iso()
    if db_type == "sqlite":
        cur = conn.execute(
            """
            INSERT INTO briefs (title, period_days, min_relevance, filters, report_markdown, report_json, rows_analyzed, model, author, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                title,
                period_days,
                min_relevance,
                json_dumps(filters),
                report_markdown,
                json_dumps(report_json),
                rows_analyzed,
                model,
                author,
                ts,
            ),
        )
        brief_id = cur.lastrowid
    else:
        row = conn.execute(
            """
            INSERT INTO briefs (title, period_days, min_relevance, filters, report_markdown, report_json, rows_analyzed, model, author, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                title,
                period_days,
                min_relevance,
                json_for_db(filters, db_type),
                report_markdown,
                json_for_db(report_json, db_type),
                rows_analyzed,
                model,
                author,
                ts,
            ),
        ).fetchone()
        brief_id = row["id"] if row else None
    conn.commit()
    return int(brief_id) if brief_id else 0
