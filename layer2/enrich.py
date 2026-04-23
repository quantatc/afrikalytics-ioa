"""
In(Sights) Tracker Layer 2: enrichment and taxonomy tagging.

Fetches unprocessed raw articles, asks OpenAI for structured tags against
config/taxonomy.yaml, stores the result in enriched_articles, and marks each
raw article as processed.

Usage:
    uv run python layer2/enrich.py --mode dev --batch-size 50
    uv run python layer2/enrich.py --mode prod --batch-size 100 --drain
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
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from openai import BadRequestError, OpenAI

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ioa_core.db import connect_db, json_for_db, json_loads, now_utc_iso, vector_for_db
from ioa_core.env import load_repo_env
from ioa_core.taxonomy import load_taxonomy, normalize_enrichment_output, taxonomy_prompt


load_repo_env()

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            LOG_DIR / f"enrich_{datetime.now().strftime('%Y-%m-%d')}.log",
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger("ioa.enrich")


DEFAULT_MODEL = os.getenv("LAYER2_MODEL", "gpt-4o-mini")
DEFAULT_EMBEDDING_MODEL = os.getenv("LAYER2_EMBEDDING_MODEL", "text-embedding-3-small")
DEFAULT_EMBEDDING_DIMS = int(os.getenv("LAYER2_EMBEDDING_DIMS", "384"))
DEFAULT_BATCH_SIZE = int(os.getenv("LAYER2_BATCH_SIZE", "50"))


def parse_hard_country_tags(value: Any) -> list[str]:
    parsed = json_loads(value)
    if isinstance(parsed, list):
        return [str(v).strip().upper() for v in parsed if str(v).strip()]
    if isinstance(value, str) and value.strip():
        return [p.strip().upper() for p in value.split(",") if p.strip()]
    return []


def fetch_unprocessed(db, db_type: str, batch_size: int) -> list[dict]:
    if db_type == "sqlite":
        rows = db.execute(
            """
            SELECT
                id, url_hash, url, source_name, source_tier, hard_country_tags,
                language, headline, lede, published_at, scraped_at
            FROM raw_articles
            WHERE processed_at IS NULL
            ORDER BY scraped_at ASC
            LIMIT ?
            """,
            (batch_size,),
        ).fetchall()
        return [dict(r) for r in rows]

    rows = db.execute(
        """
        SELECT
            id, url_hash, url, source_name, source_tier, hard_country_tags,
            language, headline, lede, published_at, scraped_at
        FROM raw_articles
        WHERE processed_at IS NULL
        ORDER BY scraped_at ASC
        LIMIT %s
        """,
        (batch_size,),
    ).fetchall()
    return [dict(r) for r in rows]


def _chat_json(client: OpenAI, request_payload: dict) -> dict:
    try:
        resp = client.chat.completions.create(**request_payload)
    except BadRequestError as e:
        if "temperature" in str(e).lower() and "default (1)" in str(e).lower():
            request_payload.pop("temperature", None)
            resp = client.chat.completions.create(**request_payload)
        else:
            raise

    raw = (resp.choices[0].message.content or "").strip()
    return json.loads(raw)


def call_openai_enrichment(
    client: OpenAI,
    model: str,
    article: dict,
    country_hint: str | None,
    taxonomy: dict,
) -> dict:
    system = (
        "You are an Africa-focused intelligence analyst tagging articles for a research database.\n"
        "Return ONLY valid JSON. Use only the provided taxonomy values.\n"
        "Required keys: primary_country, countries, primary_sector, sector_tags, themes, "
        "event_types, entities, sentiment, time_horizon, relevance_score, relevance_reason, summary.\n"
        "primary_country: African ISO-2 code or PAN. countries: list of African ISO-2 codes or PAN.\n"
        "entities: list of objects with keys name and type.\n"
        "summary: exactly 3 concise business-English sentences.\n"
        "relevance_score: integer 1-5 for IOA executive decision relevance.\n"
        "Do not output section 7 advanced layers: risk_categories, strategic_signals, or project_stage."
    )

    payload = {
        "taxonomy": json.loads(taxonomy_prompt(taxonomy)),
        "article": {
            "source_name": article.get("source_name"),
            "source_tier": article.get("source_tier"),
            "country_hint": country_hint,
            "language": article.get("language"),
            "headline": (article.get("headline") or "")[:300],
            "lede": (article.get("lede") or "")[:1800],
            "url": article.get("url"),
            "published_at": str(article.get("published_at") or ""),
        },
    }

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

    raw_data = _chat_json(client, request_payload)
    return normalize_enrichment_output(raw_data, country_hint=country_hint, taxonomy=taxonomy)


def make_embedding(
    client: OpenAI,
    embedding_model: str,
    embedding_dims: int,
    text: str,
) -> list[float]:
    compact = text[:3000]
    try:
        emb = client.embeddings.create(
            model=embedding_model,
            input=compact,
            dimensions=embedding_dims,
        )
    except TypeError:
        emb = client.embeddings.create(model=embedding_model, input=compact)

    vector = emb.data[0].embedding
    if len(vector) > embedding_dims:
        return vector[:embedding_dims]
    if len(vector) < embedding_dims:
        return vector + [0.0] * (embedding_dims - len(vector))
    return vector


def write_enriched_and_mark_processed(db, db_type: str, raw_id: int, record: dict) -> str:
    processed_at = now_utc_iso()

    if db_type == "sqlite":
        existing = db.execute(
            "SELECT id FROM enriched_articles WHERE raw_id = ?",
            (raw_id,),
        ).fetchone()
        if existing:
            db.execute("UPDATE raw_articles SET processed_at = ? WHERE id = ?", (processed_at, raw_id))
            db.commit()
            return "exists"

        db.execute(
            """
            INSERT INTO enriched_articles
                (raw_id, taxonomy_version, primary_country, countries, regions,
                 primary_sector, sector_tags, themes, event_types, entities,
                 sentiment, time_horizon, relevance_score, relevance_reason, summary,
                 embedding, tag_status, enriched_at, country, sector)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ai_generated', ?, ?, ?)
            """,
            (
                raw_id,
                record["taxonomy_version"],
                record["primary_country"],
                json.dumps(record["countries"], ensure_ascii=True),
                json.dumps(record["regions"], ensure_ascii=True),
                record["primary_sector"],
                json.dumps(record["sector_tags"], ensure_ascii=True),
                json.dumps(record["themes"], ensure_ascii=True),
                json.dumps(record["event_types"], ensure_ascii=True),
                json.dumps(record["entities"], ensure_ascii=True),
                record["sentiment"],
                record["time_horizon"],
                record["relevance_score"],
                record["relevance_reason"],
                record["summary"],
                vector_for_db(record["embedding"], db_type),
                processed_at,
                record["primary_country"],
                record["primary_sector"],
            ),
        )
        db.execute("UPDATE raw_articles SET processed_at = ? WHERE id = ?", (processed_at, raw_id))
        db.commit()
        return "inserted"

    existing = db.execute("SELECT id FROM enriched_articles WHERE raw_id = %s LIMIT 1", (raw_id,)).fetchone()
    if existing:
        db.execute("UPDATE raw_articles SET processed_at = %s WHERE id = %s", (processed_at, raw_id))
        db.commit()
        return "exists"

    db.execute(
        """
        INSERT INTO enriched_articles
            (raw_id, taxonomy_version, primary_country, countries, regions,
             primary_sector, sector_tags, themes, event_types, entities,
             sentiment, time_horizon, relevance_score, relevance_reason, summary,
             embedding, tag_status, enriched_at, country, sector)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             'ai_generated', %s, %s, %s)
        """,
        (
            raw_id,
            record["taxonomy_version"],
            record["primary_country"],
            json_for_db(record["countries"], db_type),
            json_for_db(record["regions"], db_type),
            record["primary_sector"],
            json_for_db(record["sector_tags"], db_type),
            json_for_db(record["themes"], db_type),
            json_for_db(record["event_types"], db_type),
            json_for_db(record["entities"], db_type),
            record["sentiment"],
            record["time_horizon"],
            record["relevance_score"],
            record["relevance_reason"],
            record["summary"],
            vector_for_db(record["embedding"], db_type),
            processed_at,
            record["primary_country"],
            record["primary_sector"],
        ),
    )
    db.execute("UPDATE raw_articles SET processed_at = %s WHERE id = %s", (processed_at, raw_id))
    db.commit()
    return "inserted"


def run(
    batch_size: int = DEFAULT_BATCH_SIZE,
    mode: str = "dev",
    model: str = DEFAULT_MODEL,
    embedding_model: str = DEFAULT_EMBEDDING_MODEL,
    embedding_dims: int = DEFAULT_EMBEDDING_DIMS,
    drain: bool = False,
):
    if "OPENAI_API_KEY" not in os.environ:
        raise RuntimeError("OPENAI_API_KEY is required")

    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    taxonomy = load_taxonomy()
    db, db_type = connect_db(mode)

    total_processed = 0
    total_inserted = 0
    total_exists = 0
    total_errors = 0
    batches = 0

    while True:
        articles = fetch_unprocessed(db, db_type, batch_size)
        if not articles:
            if batches == 0:
                log.info("No unprocessed raw_articles found.")
            break

        batches += 1
        log.info(
            "Starting enrichment batch %s: articles=%s mode=%s db=%s model=%s taxonomy=%s drain=%s",
            batches,
            len(articles),
            mode,
            db_type,
            model,
            taxonomy.get("version"),
            drain,
        )

        inserted = 0
        exists = 0
        errors = 0

        for idx, article in enumerate(articles, start=1):
            raw_id = int(article["id"])
            try:
                hard_tags = parse_hard_country_tags(article.get("hard_country_tags"))
                country_hint = hard_tags[0] if hard_tags else None

                enriched = call_openai_enrichment(
                    client=client,
                    model=model,
                    article=article,
                    country_hint=country_hint,
                    taxonomy=taxonomy,
                )

                vector_text = "\n\n".join(
                    [
                        article.get("headline") or "",
                        article.get("lede") or "",
                        enriched["summary"],
                        " ".join(enriched["themes"]),
                        " ".join(enriched["sector_tags"]),
                    ]
                )
                enriched["embedding"] = make_embedding(
                    client=client,
                    embedding_model=embedding_model,
                    embedding_dims=embedding_dims,
                    text=vector_text,
                )

                status = write_enriched_and_mark_processed(db, db_type, raw_id, enriched)
                if status == "inserted":
                    inserted += 1
                else:
                    exists += 1

                if idx % 10 == 0:
                    log.info(
                        "Batch %s progress %s/%s inserted=%s exists=%s errors=%s",
                        batches,
                        idx,
                        len(articles),
                        inserted,
                        exists,
                        errors,
                    )

                time.sleep(0.2)

            except Exception as e:
                errors += 1
                log.exception("Failed raw_id=%s: %s", raw_id, e)
                try:
                    db.rollback()
                except Exception:
                    pass

        total_processed += len(articles)
        total_inserted += inserted
        total_exists += exists
        total_errors += errors

        log.info(
            "Batch %s complete: processed=%s inserted=%s exists=%s errors=%s",
            batches,
            len(articles),
            inserted,
            exists,
            errors,
        )

        if not drain:
            break

    log.info(
        "Layer 2 complete: batches=%s processed=%s inserted=%s exists=%s errors=%s",
        batches,
        total_processed,
        total_inserted,
        total_exists,
        total_errors,
    )
    return {
        "batches": batches,
        "processed": total_processed,
        "inserted": total_inserted,
        "exists": total_exists,
        "errors": total_errors,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="In(Sights) Tracker Layer 2 enrichment")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--mode", choices=["dev", "prod"], default="dev")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--embedding-model", default=DEFAULT_EMBEDDING_MODEL)
    parser.add_argument("--embedding-dims", type=int, default=DEFAULT_EMBEDDING_DIMS)
    parser.add_argument("--drain", action="store_true", help="Process batches until no unprocessed rows remain")
    args = parser.parse_args()

    run(
        batch_size=args.batch_size,
        mode=args.mode,
        model=args.model,
        embedding_model=args.embedding_model,
        embedding_dims=args.embedding_dims,
        drain=args.drain,
    )
