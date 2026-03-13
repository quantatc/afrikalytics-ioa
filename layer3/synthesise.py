"""
IOA Intelligence Briefing — Layer 3: Synthesis
===============================================
Builds cross-article intelligence briefings from enriched_articles and emits:
  - Markdown report
  - JSON structured report payload
  - Optional report_runs row in DB

Usage (from repo root):
    uv run python layer3/synthesise.py --mode prod --period-days 7
    uv run python layer3/synthesise.py --mode prod --period-days 30 --max-articles 160
"""

# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "openai>=1.99.0",
#   "supabase>=2.10.0",
# ]
# ///

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from openai import BadRequestError, OpenAI


def load_repo_env() -> None:
    """Load repo-root .env if present (without overriding existing env vars)."""
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if value and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        os.environ.setdefault(key, value)


load_repo_env()


# Logging
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            LOG_DIR / f"synthesise_{datetime.now().strftime('%Y-%m-%d')}.log",
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger("ioa.synthesise")


DEFAULT_MODEL = os.getenv("LAYER3_MODEL", "gpt-4o-mini")
DEFAULT_PERIOD_DAYS = int(os.getenv("LAYER3_PERIOD_DAYS", "7"))
DEFAULT_MAX_ARTICLES = int(os.getenv("LAYER3_MAX_ARTICLES", "120"))
DEFAULT_MIN_RELEVANCE = int(os.getenv("LAYER3_MIN_RELEVANCE", "3"))
REPORTS_DIR = Path(__file__).parent / "reports"
REPORTS_DIR.mkdir(exist_ok=True)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_utc_iso() -> str:
    return now_utc().isoformat()


def get_db(mode: str = "dev"):
    """Return SQLite connection (dev) or Supabase client (prod)."""
    if mode == "prod":
        from supabase import create_client

        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_KEY"]
        return create_client(url, key), "supabase"

    db_path = Path(__file__).resolve().parents[1] / "layer1" / "ioa_dev.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS report_runs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            report_month TEXT        NOT NULL,
            status       TEXT        NOT NULL DEFAULT 'pending',
            draft_url    TEXT,
            approved_at  TEXT,
            sent_at      TEXT,
            created_at   TEXT        NOT NULL
        )
        """
    )
    conn.commit()
    return conn, "sqlite"


def _chunk(values: list[int], size: int) -> list[list[int]]:
    return [values[i : i + size] for i in range(0, len(values), size)]


def fetch_enriched_with_raw(
    db,
    db_type: str,
    start_dt: datetime,
    end_dt: datetime,
    min_relevance: int,
    max_articles: int,
) -> list[dict]:
    """
    Fetch enriched rows and attach raw article metadata.
    Period filter is based on enriched_at to avoid complex remote joins.
    """
    if db_type == "sqlite":
        rows = db.execute(
            """
            SELECT
                e.raw_id, e.country, e.sector, e.relevance_score, e.relevance_reason,
                e.summary, e.enriched_at,
                r.url, r.source_name, r.headline, r.published_at, r.scraped_at
            FROM enriched_articles e
            JOIN raw_articles r ON r.id = e.raw_id
            WHERE e.enriched_at >= ? AND e.enriched_at <= ? AND e.relevance_score >= ?
            ORDER BY e.relevance_score DESC, e.enriched_at DESC
            LIMIT ?
            """,
            (start_dt.isoformat(), end_dt.isoformat(), min_relevance, max_articles),
        ).fetchall()
        return [dict(r) for r in rows]

    enriched = (
        db.table("enriched_articles")
        .select("raw_id,country,sector,relevance_score,relevance_reason,summary,enriched_at")
        .gte("enriched_at", start_dt.isoformat())
        .lte("enriched_at", end_dt.isoformat())
        .gte("relevance_score", min_relevance)
        .order("relevance_score", desc=True)
        .order("enriched_at", desc=True)
        .limit(max_articles)
        .execute()
    ).data or []

    if not enriched:
        return []

    raw_ids = [int(x["raw_id"]) for x in enriched if x.get("raw_id") is not None]
    raw_by_id: dict[int, dict] = {}
    for chunk_ids in _chunk(raw_ids, 200):
        raw_rows = (
            db.table("raw_articles")
            .select("id,url,source_name,headline,published_at,scraped_at")
            .in_("id", chunk_ids)
            .execute()
        ).data or []
        for row in raw_rows:
            raw_by_id[int(row["id"])] = row

    merged = []
    for e in enriched:
        rid = int(e["raw_id"])
        r = raw_by_id.get(rid, {})
        merged.append(
            {
                "raw_id": rid,
                "country": e.get("country"),
                "sector": e.get("sector"),
                "relevance_score": e.get("relevance_score"),
                "relevance_reason": e.get("relevance_reason"),
                "summary": e.get("summary"),
                "enriched_at": e.get("enriched_at"),
                "url": r.get("url"),
                "source_name": r.get("source_name"),
                "headline": r.get("headline"),
                "published_at": r.get("published_at"),
                "scraped_at": r.get("scraped_at"),
            }
        )
    return merged


def build_synthesis_payload(rows: list[dict], start_dt: datetime, end_dt: datetime) -> dict:
    sector_counts = Counter((r.get("sector") or "Other") for r in rows)
    country_counts = Counter((r.get("country") or "PAN") for r in rows)
    source_counts = Counter((r.get("source_name") or "Unknown") for r in rows)

    # Keep prompt compact for cost control.
    top_rows = rows[: min(len(rows), 90)]
    items = []
    for r in top_rows:
        items.append(
            {
                "raw_id": r.get("raw_id"),
                "country": r.get("country"),
                "sector": r.get("sector"),
                "relevance_score": r.get("relevance_score"),
                "headline": (r.get("headline") or "")[:220],
                "summary": (r.get("summary") or "")[:340],
                "source_name": r.get("source_name"),
                "url": r.get("url"),
            }
        )

    return {
        "time_window": {
            "start_utc": start_dt.isoformat(),
            "end_utc": end_dt.isoformat(),
            "articles_used": len(rows),
            "articles_prompted": len(items),
        },
        "metrics": {
            "sector_counts": sector_counts.most_common(),
            "country_counts": country_counts.most_common(),
            "source_counts": source_counts.most_common(15),
            "avg_relevance": round(sum(int(r.get("relevance_score", 0)) for r in rows) / max(len(rows), 1), 2),
        },
        "articles": items,
    }


def call_openai_synthesis(client: OpenAI, model: str, payload: dict) -> dict:
    system = (
        "You are a senior Africa-focused geopolitical and macro intelligence editor.\n"
        "Generate an executive-ready weekly briefing from structured article evidence.\n"
        "Return ONLY JSON with keys: title, executive_summary, key_themes, country_hotspots, "
        "risk_watchlist, opportunities, slack_digest, report_markdown.\n"
        "Constraints:\n"
        "- executive_summary: 120-180 words.\n"
        "- key_themes: list of 3-6 objects with fields theme, why_it_matters, evidence_raw_ids.\n"
        "- country_hotspots: list of 3-8 objects with fields country, note.\n"
        "- risk_watchlist: list of short bullet strings.\n"
        "- opportunities: list of short bullet strings.\n"
        "- slack_digest: <= 900 characters, concise bullets.\n"
        "- report_markdown: complete markdown report with clear sections and references to raw_ids."
    )

    request_payload = {
        "model": model,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=True)},
        ],
    }

    # Keep non-gpt5 models deterministic-ish.
    if not model.lower().startswith("gpt-5"):
        request_payload["temperature"] = 0.2

    try:
        resp = client.chat.completions.create(**request_payload)
    except BadRequestError as e:
        # Some models reject explicit temperature.
        if "temperature" in str(e).lower() and "default (1)" in str(e).lower():
            request_payload.pop("temperature", None)
            resp = client.chat.completions.create(**request_payload)
        else:
            raise

    raw = (resp.choices[0].message.content or "").strip()
    data = json.loads(raw)

    report_markdown = str(data.get("report_markdown", "")).strip()
    if not report_markdown:
        report_markdown = "# IOA Intelligence Brief\n\nNo report content generated."

    slack_digest = str(data.get("slack_digest", "")).strip()[:900]

    return {
        "title": str(data.get("title", "IOA Intelligence Brief")).strip(),
        "executive_summary": str(data.get("executive_summary", "")).strip(),
        "key_themes": data.get("key_themes") or [],
        "country_hotspots": data.get("country_hotspots") or [],
        "risk_watchlist": data.get("risk_watchlist") or [],
        "opportunities": data.get("opportunities") or [],
        "slack_digest": slack_digest,
        "report_markdown": report_markdown,
    }


def persist_report_run(db, db_type: str, report_month: str, draft_url: str) -> None:
    if db_type == "sqlite":
        db.execute(
            """
            INSERT INTO report_runs (report_month, status, draft_url, created_at)
            VALUES (?, 'drafted', ?, ?)
            """,
            (report_month, draft_url, now_utc_iso()),
        )
        db.commit()
        return

    db.table("report_runs").insert(
        {"report_month": report_month, "status": "drafted", "draft_url": draft_url}
    ).execute()


def run(
    mode: str = "dev",
    model: str = DEFAULT_MODEL,
    period_days: int = DEFAULT_PERIOD_DAYS,
    max_articles: int = DEFAULT_MAX_ARTICLES,
    min_relevance: int = DEFAULT_MIN_RELEVANCE,
    no_db_write: bool = False,
) -> dict:
    if "OPENAI_API_KEY" not in os.environ:
        raise RuntimeError("OPENAI_API_KEY is required")

    if period_days <= 0:
        raise ValueError("period_days must be > 0")

    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    db, db_type = get_db(mode)

    end_dt = now_utc()
    start_dt = end_dt - timedelta(days=period_days)

    rows = fetch_enriched_with_raw(
        db=db,
        db_type=db_type,
        start_dt=start_dt,
        end_dt=end_dt,
        min_relevance=min_relevance,
        max_articles=max_articles,
    )
    if not rows:
        log.warning("No enriched rows found for synthesis window.")
        return {"ok": False, "reason": "no_rows"}

    log.info(
        "Starting Layer 3 synthesis: rows=%s mode=%s model=%s period_days=%s min_relevance=%s max_articles=%s",
        len(rows),
        mode,
        model,
        period_days,
        min_relevance,
        max_articles,
    )

    prompt_payload = build_synthesis_payload(rows, start_dt, end_dt)
    synthesis = call_openai_synthesis(client=client, model=model, payload=prompt_payload)

    stamp = end_dt.strftime("%Y%m%d_%H%M%S")
    md_path = REPORTS_DIR / f"ioa_brief_{stamp}.md"
    json_path = REPORTS_DIR / f"ioa_brief_{stamp}.json"

    md_path.write_text(synthesis["report_markdown"], encoding="utf-8")
    report_payload = {
        "generated_at_utc": now_utc_iso(),
        "window_start_utc": start_dt.isoformat(),
        "window_end_utc": end_dt.isoformat(),
        "rows_analyzed": len(rows),
        "model": model,
        "synthesis": synthesis,
        "input_metrics": prompt_payload["metrics"],
    }
    json_path.write_text(json.dumps(report_payload, ensure_ascii=True, indent=2), encoding="utf-8")

    report_month = end_dt.strftime("%Y-%m")
    if not no_db_write:
        persist_report_run(
            db=db,
            db_type=db_type,
            report_month=report_month,
            draft_url=str(md_path.resolve()),
        )

    log.info("Layer 3 complete: markdown=%s json=%s", md_path, json_path)
    return {
        "ok": True,
        "rows_analyzed": len(rows),
        "report_markdown": str(md_path.resolve()),
        "report_json": str(json_path.resolve()),
        "slack_digest": synthesis.get("slack_digest", ""),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IOA Layer 3 Synthesis")
    parser.add_argument("--mode", choices=["dev", "prod"], default="dev")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--period-days", type=int, default=DEFAULT_PERIOD_DAYS)
    parser.add_argument("--max-articles", type=int, default=DEFAULT_MAX_ARTICLES)
    parser.add_argument("--min-relevance", type=int, default=DEFAULT_MIN_RELEVANCE)
    parser.add_argument("--no-db-write", action="store_true", help="Generate files only; do not insert report_runs row")
    args = parser.parse_args()

    run(
        mode=args.mode,
        model=args.model,
        period_days=args.period_days,
        max_articles=args.max_articles,
        min_relevance=args.min_relevance,
        no_db_write=args.no_db_write,
    )
