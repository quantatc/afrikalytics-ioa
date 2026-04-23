"""
In(Sights) Tracker Layer 3: briefing synthesis.

Builds an executive briefing from enriched article evidence. This is now a
database/UI backend function.

Usage:
    uv run python layer3/synthesise.py --mode dev --period-days 7
    uv run python layer3/synthesise.py --mode prod --period-days 30 --sector Energy
"""

# ruff: noqa: E402

# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "openai>=1.99.0",
#   "psycopg[binary]>=3.2.0",
#   "pyyaml>=6.0.2",
# ]
# ///

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from openai import BadRequestError, OpenAI

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ioa_core.db import connect_db, json_for_db, json_loads, now_utc_iso
from ioa_core.env import load_repo_env


load_repo_env()

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
LOG_DIR = Path(__file__).parent / "logs"
REPORTS_DIR = Path(__file__).parent / "reports"
LOG_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)

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


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _decode_row(row: dict[str, Any]) -> dict[str, Any]:
    for field in ("countries", "regions", "sector_tags", "themes", "event_types", "entities"):
        row[field] = json_loads(row.get(field))
    return row


def fetch_enriched_with_raw(
    db,
    db_type: str,
    start_dt: datetime,
    end_dt: datetime,
    min_relevance: int,
    max_articles: int,
) -> list[dict]:
    if db_type == "sqlite":
        rows = db.execute(
            """
            SELECT
                e.raw_id, e.taxonomy_version, e.primary_country, e.countries, e.regions,
                e.primary_sector, e.sector_tags, e.themes, e.event_types, e.entities,
                e.sentiment, e.time_horizon, e.relevance_score, e.relevance_reason,
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
        return [_decode_row(dict(r)) for r in rows]

    rows = db.execute(
        """
        SELECT
            e.raw_id, e.taxonomy_version, e.primary_country, e.countries, e.regions,
            e.primary_sector, e.sector_tags, e.themes, e.event_types, e.entities,
            e.sentiment, e.time_horizon, e.relevance_score, e.relevance_reason,
            e.summary, e.enriched_at,
            r.url, r.source_name, r.headline, r.published_at, r.scraped_at
        FROM enriched_articles e
        JOIN raw_articles r ON r.id = e.raw_id
        WHERE e.enriched_at >= %s AND e.enriched_at <= %s AND e.relevance_score >= %s
        ORDER BY e.relevance_score DESC, e.enriched_at DESC
        LIMIT %s
        """,
        (start_dt, end_dt, min_relevance, max_articles),
    ).fetchall()
    return [_decode_row(dict(r)) for r in rows]


def apply_filters(
    rows: list[dict],
    country: str | None = None,
    region: str | None = None,
    sector: str | None = None,
    theme: str | None = None,
    event_type: str | None = None,
) -> list[dict]:
    country = (country or "").strip().upper()
    region = (region or "").strip().lower()
    sector_l = (sector or "").strip().lower()
    theme_l = (theme or "").strip().lower()
    event_l = (event_type or "").strip().lower()

    out = []
    for row in rows:
        if country and country not in {str(c).upper() for c in row.get("countries", [])}:
            continue
        if region and region not in {str(r).lower() for r in row.get("regions", [])}:
            continue
        if sector_l and sector_l not in {str(s).lower() for s in row.get("sector_tags", [])}:
            continue
        if theme_l and theme_l not in {str(t).lower() for t in row.get("themes", [])}:
            continue
        if event_l and event_l not in {str(e).lower() for e in row.get("event_types", [])}:
            continue
        out.append(row)
    return out


def build_synthesis_payload(rows: list[dict], start_dt: datetime, end_dt: datetime, filters: dict) -> dict:
    sector_counts = Counter(tag for r in rows for tag in (r.get("sector_tags") or []))
    theme_counts = Counter(tag for r in rows for tag in (r.get("themes") or []))
    event_counts = Counter(tag for r in rows for tag in (r.get("event_types") or []))
    country_counts = Counter(tag for r in rows for tag in (r.get("countries") or []))
    source_counts = Counter((r.get("source_name") or "Unknown") for r in rows)

    items = []
    for r in rows[: min(len(rows), 90)]:
        items.append(
            {
                "raw_id": r.get("raw_id"),
                "countries": r.get("countries"),
                "regions": r.get("regions"),
                "primary_sector": r.get("primary_sector"),
                "sector_tags": r.get("sector_tags"),
                "themes": r.get("themes"),
                "event_types": r.get("event_types"),
                "sentiment": r.get("sentiment"),
                "time_horizon": r.get("time_horizon"),
                "relevance_score": r.get("relevance_score"),
                "headline": (r.get("headline") or "")[:220],
                "summary": (r.get("summary") or "")[:440],
                "source_name": r.get("source_name"),
                "url": r.get("url"),
            }
        )

    return {
        "request": {
            "filters": filters,
            "time_window": {
                "start_utc": start_dt.isoformat(),
                "end_utc": end_dt.isoformat(),
                "articles_used": len(rows),
                "articles_prompted": len(items),
            },
        },
        "metrics": {
            "sector_counts": sector_counts.most_common(12),
            "theme_counts": theme_counts.most_common(12),
            "event_counts": event_counts.most_common(12),
            "country_counts": country_counts.most_common(20),
            "source_counts": source_counts.most_common(15),
            "avg_relevance": round(
                sum(int(r.get("relevance_score", 0)) for r in rows) / max(len(rows), 1),
                2,
            ),
        },
        "articles": items,
    }


def call_openai_synthesis(client: OpenAI, model: str, payload: dict) -> dict:
    system = (
        "You are a senior Africa-focused geopolitical and macro intelligence editor.\n"
        "Generate an executive-ready briefing from structured article evidence.\n"
        "Use only the provided evidence. Never invent URLs, claims, countries, sectors, or events.\n"
        "Return ONLY JSON with keys: title, executive_summary, key_findings, "
        "country_or_region_hotspots, sector_view, event_watch, references, report_markdown.\n"
        "key_findings: list of objects with finding, why_it_matters, evidence_raw_ids.\n"
        "country_or_region_hotspots: list of objects with location, note, evidence_raw_ids.\n"
        "sector_view: list of objects with sector, note, evidence_raw_ids.\n"
        "event_watch: list of objects with event_type, note, evidence_raw_ids.\n"
        "references: list of objects with headline, url, source_name.\n"
        "report_markdown: complete markdown report with raw_id references."
    )

    request_payload = {
        "model": model,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=True)},
        ],
    }
    if not model.lower().startswith("gpt-5"):
        request_payload["temperature"] = 0.2

    try:
        resp = client.chat.completions.create(**request_payload)
    except BadRequestError as e:
        if "temperature" in str(e).lower() and "default (1)" in str(e).lower():
            request_payload.pop("temperature", None)
            resp = client.chat.completions.create(**request_payload)
        else:
            raise

    raw = (resp.choices[0].message.content or "").strip()
    data = json.loads(raw)
    report_markdown = str(data.get("report_markdown") or "").strip()
    if not report_markdown:
        report_markdown = "# IOA Intelligence Brief\n\nNo report content generated."

    data["report_markdown"] = report_markdown
    return data


def persist_report_run(db, db_type: str, report_month: str, filters: dict, md_path: Path, json_path: Path) -> None:
    if db_type == "sqlite":
        db.execute(
            """
            INSERT INTO report_runs
                (report_month, status, filters, report_path, report_json_path, created_at)
            VALUES
                (?, 'drafted', ?, ?, ?, ?)
            """,
            (
                report_month,
                json.dumps(filters, ensure_ascii=True),
                str(md_path.resolve()),
                str(json_path.resolve()),
                now_utc_iso(),
            ),
        )
        db.commit()
        return

    db.execute(
        """
        INSERT INTO report_runs
            (report_month, status, filters, report_path, report_json_path, created_at)
        VALUES
            (%s, 'drafted', %s, %s, %s, %s)
        """,
        (
            report_month,
            json_for_db(filters, db_type),
            str(md_path.resolve()),
            str(json_path.resolve()),
            now_utc_iso(),
        ),
    )
    db.commit()


def run(
    mode: str = "dev",
    model: str = DEFAULT_MODEL,
    period_days: int = DEFAULT_PERIOD_DAYS,
    max_articles: int = DEFAULT_MAX_ARTICLES,
    min_relevance: int = DEFAULT_MIN_RELEVANCE,
    country: str | None = None,
    region: str | None = None,
    sector: str | None = None,
    theme: str | None = None,
    event_type: str | None = None,
    no_db_write: bool = False,
) -> dict:
    if "OPENAI_API_KEY" not in os.environ:
        raise RuntimeError("OPENAI_API_KEY is required")
    if period_days <= 0:
        raise ValueError("period_days must be > 0")

    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    db, db_type = connect_db(mode)

    end_dt = now_utc()
    start_dt = end_dt - timedelta(days=period_days)
    filters = {
        "period_days": period_days,
        "min_relevance": min_relevance,
        "country": country or "",
        "region": region or "",
        "sector": sector or "",
        "theme": theme or "",
        "event_type": event_type or "",
    }

    rows = fetch_enriched_with_raw(
        db=db,
        db_type=db_type,
        start_dt=start_dt,
        end_dt=end_dt,
        min_relevance=min_relevance,
        max_articles=max_articles,
    )
    rows = apply_filters(rows, country=country, region=region, sector=sector, theme=theme, event_type=event_type)
    rows = rows[:max_articles]

    if not rows:
        log.warning("No enriched rows found for synthesis window and filters.")
        return {"ok": False, "reason": "no_rows"}

    log.info(
        "Starting synthesis rows=%s mode=%s model=%s period_days=%s min_relevance=%s filters=%s",
        len(rows),
        mode,
        model,
        period_days,
        min_relevance,
        filters,
    )

    prompt_payload = build_synthesis_payload(rows, start_dt, end_dt, filters)
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
        "filters": filters,
        "synthesis": synthesis,
        "input_metrics": prompt_payload["metrics"],
    }
    json_path.write_text(json.dumps(report_payload, ensure_ascii=True, indent=2), encoding="utf-8")

    if not no_db_write:
        persist_report_run(db, db_type, end_dt.strftime("%Y-%m"), filters, md_path, json_path)

    log.info("Layer 3 complete markdown=%s json=%s", md_path, json_path)
    return {
        "ok": True,
        "rows_analyzed": len(rows),
        "report_markdown": str(md_path.resolve()),
        "report_json": str(json_path.resolve()),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="In(Sights) Tracker Layer 3 synthesis")
    parser.add_argument("--mode", choices=["dev", "prod"], default="dev")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--period-days", type=int, default=DEFAULT_PERIOD_DAYS)
    parser.add_argument("--max-articles", type=int, default=DEFAULT_MAX_ARTICLES)
    parser.add_argument("--min-relevance", type=int, default=DEFAULT_MIN_RELEVANCE)
    parser.add_argument("--country")
    parser.add_argument("--region")
    parser.add_argument("--sector")
    parser.add_argument("--theme")
    parser.add_argument("--event-type")
    parser.add_argument("--no-db-write", action="store_true")
    args = parser.parse_args()

    run(
        mode=args.mode,
        model=args.model,
        period_days=args.period_days,
        max_articles=args.max_articles,
        min_relevance=args.min_relevance,
        country=args.country,
        region=args.region,
        sector=args.sector,
        theme=args.theme,
        event_type=args.event_type,
        no_db_write=args.no_db_write,
    )
