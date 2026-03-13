"""
IOA Intelligence Briefing — Layer 2: Enrichment
===============================================
Fetches unprocessed raw articles, enriches them with OpenAI, stores structured
outputs in enriched_articles, and marks raw_articles.processed_at.

Usage (from repo root):
    uv run python layer2/enrich.py
    uv run python layer2/enrich.py --batch-size 100
    uv run python layer2/enrich.py --mode prod
"""

# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "openai>=1.99.0",
#   "supabase>=2.10.0",
# ]
# ///

import argparse
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from openai import BadRequestError, OpenAI
from countries import country_display_name, normalize_country_code


def load_repo_env() -> None:
    """
    Lightweight .env loader (no external dependency).
    Loads variables from repo-root .env if present, without overriding existing
    process environment variables.
    """
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
            LOG_DIR / f"enrich_{datetime.now().strftime('%Y-%m-%d')}.log",
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger("ioa.enrich")


# Defaults (cheap + high quality for this workload)
DEFAULT_MODEL = os.getenv("LAYER2_MODEL", "gpt-4o-mini")
DEFAULT_EMBEDDING_MODEL = os.getenv("LAYER2_EMBEDDING_MODEL", "text-embedding-3-small")
DEFAULT_EMBEDDING_DIMS = int(os.getenv("LAYER2_EMBEDDING_DIMS", "384"))
DEFAULT_BATCH_SIZE = int(os.getenv("LAYER2_BATCH_SIZE", "50"))

SECTORS = {
    "Energy",
    "Mining",
    "Tech",
    "Finance",
    "Policy",
    "Agriculture",
    "Infrastructure",
    "Other",
}


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_db(mode: str = "dev"):
    """Return SQLite connection (dev) or Supabase client (prod)."""
    if mode == "prod":
        from supabase import create_client

        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_KEY"]
        return create_client(url, key), "supabase"

    # Always point to layer1 local DB to avoid CWD surprises.
    db_path = Path(__file__).resolve().parents[1] / "layer1" / "ioa_dev.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS enriched_articles (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_id           INTEGER UNIQUE,
            country          TEXT,
            sector           TEXT,
            relevance_score  INTEGER,
            relevance_reason TEXT,
            summary          TEXT,
            embedding        TEXT,
            enriched_at      TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn, "sqlite"


def parse_hard_country_tags(value):
    """Normalize hard country tags from JSON/text/list to list[str]."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(v).strip() for v in parsed if str(v).strip()]
        except json.JSONDecodeError:
            pass
        return [p.strip() for p in raw.split(",") if p.strip()]
    return []


def fetch_unprocessed(db, db_type: str, batch_size: int) -> list[dict]:
    if db_type == "sqlite":
        rows = db.execute(
            """
            SELECT
                id, url_hash, url, source_name, source_tier, hard_country_tags,
                headline, lede, published_at, scraped_at
            FROM raw_articles
            WHERE processed_at IS NULL
            ORDER BY scraped_at ASC
            LIMIT ?
            """,
            (batch_size,),
        ).fetchall()
        return [dict(r) for r in rows]

    result = (
        db.table("raw_articles")
        .select(
            "id,url_hash,url,source_name,source_tier,hard_country_tags,"
            "headline,lede,published_at,scraped_at"
        )
        .is_("processed_at", "null")
        .order("scraped_at", desc=False)
        .limit(batch_size)
        .execute()
    )
    return result.data or []


def call_openai_enrichment(client: OpenAI, model: str, article: dict, country_hint: str | None):
    system = (
        "You are an Africa-focused intelligence analyst. "
        "Classify each article for executive briefing use.\n"
        "Return ONLY valid JSON with keys: country, sector, relevance_score, relevance_reason, summary.\n"
        "country: only African ISO-2 country code (e.g. NG, ZA, AO) or PAN for pan-African coverage. "
        "Never output non-African country codes.\n"
        "sector: one of Energy, Mining, Tech, Finance, Policy, Agriculture, Infrastructure, Other.\n"
        "relevance_score: integer 1-5 where 5 is highly decision-relevant for investors/policy executives.\n"
        "relevance_reason: one concise sentence.\n"
        "summary: exactly 3 sentences in plain business English."
    )

    payload = {
        "source_name": article.get("source_name"),
        "source_tier": article.get("source_tier"),
        "country_hint": country_hint,
        "headline": (article.get("headline") or "")[:300],
        "lede": (article.get("lede") or "")[:1600],
        "url": article.get("url"),
    }

    request_payload = {
        "model": model,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=True)},
        ],
    }

    # Some OpenAI models only allow default temperature=1 and reject explicit values.
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

    raw_country = str(data.get("country", "")).strip()
    country, country_reason = normalize_country_code(raw_country, country_hint=country_hint)
    if raw_country and raw_country.strip().upper() != country:
        log.warning(
            "Country normalized: raw='%s' -> '%s' (%s) for source=%s url=%s",
            raw_country,
            country,
            country_reason,
            article.get("source_name"),
            article.get("url"),
        )

    sector = str(data.get("sector", "Other")).strip().title()
    if sector not in SECTORS:
        sector = "Other"

    relevance_score = data.get("relevance_score", 3)
    try:
        relevance_score = int(relevance_score)
    except (TypeError, ValueError):
        relevance_score = 3
    relevance_score = max(1, min(5, relevance_score))

    relevance_reason = str(data.get("relevance_reason", "")).strip()[:400]
    summary = str(data.get("summary", "")).strip()[:1600]

    if not summary:
        summary = "No summary provided."
    if not relevance_reason:
        relevance_reason = "Model returned no reason."

    return {
        "country": country,
        "country_name": country_display_name(country),
        "sector": sector,
        "relevance_score": relevance_score,
        "relevance_reason": relevance_reason,
        "summary": summary,
    }


def make_embedding(
    client: OpenAI,
    embedding_model: str,
    embedding_dims: int,
    text: str,
) -> list[float]:
    # Keep payload small to reduce embedding cost.
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
    """
    Returns status: inserted | exists.
    """
    processed_at = now_utc_iso()

    if db_type == "sqlite":
        existing = db.execute(
            "SELECT id FROM enriched_articles WHERE raw_id = ?",
            (raw_id,),
        ).fetchone()
        if existing:
            db.execute(
                "UPDATE raw_articles SET processed_at = ? WHERE id = ?",
                (processed_at, raw_id),
            )
            db.commit()
            return "exists"

        db.execute(
            """
            INSERT INTO enriched_articles
                (raw_id, country, sector, relevance_score, relevance_reason, summary, embedding, enriched_at)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                raw_id,
                record["country"],
                record["sector"],
                record["relevance_score"],
                record["relevance_reason"],
                record["summary"],
                json.dumps(record["embedding"]),
                processed_at,
            ),
        )
        db.execute(
            "UPDATE raw_articles SET processed_at = ? WHERE id = ?",
            (processed_at, raw_id),
        )
        db.commit()
        return "inserted"

    existing = db.table("enriched_articles").select("id").eq("raw_id", raw_id).limit(1).execute()
    if existing.data:
        (
            db.table("raw_articles")
            .update({"processed_at": processed_at})
            .eq("id", raw_id)
            .execute()
        )
        return "exists"

    db.table("enriched_articles").insert(
        {
            "raw_id": raw_id,
            "country": record["country"],
            "sector": record["sector"],
            "relevance_score": record["relevance_score"],
            "relevance_reason": record["relevance_reason"],
            "summary": record["summary"],
            "embedding": record["embedding"],
            "enriched_at": processed_at,
        }
    ).execute()

    db.table("raw_articles").update({"processed_at": processed_at}).eq("id", raw_id).execute()
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
    db, db_type = get_db(mode)

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
            "Starting Layer 2 enrichment batch %s: %s articles (mode=%s, db=%s, model=%s, embed=%s/%s, drain=%s)",
            batches,
            len(articles),
            mode,
            db_type,
            model,
            embedding_model,
            embedding_dims,
            drain,
        )

        inserted = 0
        exists = 0
        errors = 0

        for idx, article in enumerate(articles, start=1):
            raw_id = int(article["id"])
            try:
                hard_tags = parse_hard_country_tags(article.get("hard_country_tags"))
                country_hint = hard_tags[0].upper() if hard_tags else None

                enriched = call_openai_enrichment(
                    client=client,
                    model=model,
                    article=article,
                    country_hint=country_hint,
                )

                vector_text = (
                    f"{article.get('headline', '')}\n\n"
                    f"{article.get('lede', '')}\n\n"
                    f"{enriched['summary']}"
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
                        "Batch %s progress %s/%s (inserted=%s, exists=%s, errors=%s)",
                        batches,
                        idx,
                        len(articles),
                        inserted,
                        exists,
                        errors,
                    )

                # Small pause to avoid burst limits on smaller API tiers.
                time.sleep(0.2)

            except Exception as e:
                errors += 1
                log.exception("Failed raw_id=%s: %s", raw_id, e)

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
    parser = argparse.ArgumentParser(description="IOA Layer 2 Enrichment")
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
