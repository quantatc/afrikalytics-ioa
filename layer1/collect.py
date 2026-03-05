"""
IOA Intelligence Briefing — Layer 1: Collection
================================================
Handles RSS ingestion and HTML scraping for both source tiers:
  - Tier 1: Pan-Africa sources  (sources key in sources.yaml)
  - Tier 2: Country-specific    (country_specific_sources key in sources.yaml)

Country-specific sources are hard-tagged at ingestion time with their ISO
country code, saving Haiku tokens in Layer 2 (country field pre-filled,
AI only classifies sector + relevance).

Writes raw articles to local SQLite (dev) or Supabase (prod).
Run via cron or triggered by n8n HTTP POST.

Usage (with uv):
    uv run python collect.py                        # all active sources, dev mode
    uv run python collect.py --source "Reuters"     # single source by name fragment
    uv run python collect.py --tier country         # only country-specific sources
    uv run python collect.py --tier pan             # only pan-africa sources
    uv run python collect.py --mode prod            # write to Supabase
"""

# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "feedparser>=6.0.11",
#   "requests>=2.32.3",
#   "beautifulsoup4>=4.12.3",
#   "lxml>=5.3.0",
#   "pyyaml>=6.0.2",
#   "supabase>=2.10.0",
# ]
# ///

import argparse
import hashlib
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import feedparser
import requests
import yaml
from bs4 import BeautifulSoup

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
LOG_DIR    = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),                                         # console
        logging.FileHandler(                                             # rolling daily log
            LOG_DIR / f"collect_{datetime.now().strftime('%Y-%m-%d')}.log",
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger("ioa.collect")

# ── Config ────────────────────────────────────────────────────────────────────
SOURCES_FILE    = Path(__file__).parent / "sources.yaml"
HEADERS         = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}
ALT_HEADERS     = {
    **HEADERS,
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36 "
        "(compatible; IOA-IntelBot/1.0; +https://www.inonafrica.com)"
    ),
}
REQUEST_TIMEOUT = 15   # seconds
SCRAPE_DELAY    = 2    # seconds between scrape requests — be polite


# ── Database ──────────────────────────────────────────────────────────────────

def get_db(mode: str = "dev"):
    """Return SQLite connection (dev) or Supabase client (prod)."""
    if mode == "prod":
        from supabase import create_client
        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_KEY"]
        return create_client(url, key), "supabase"

    conn = sqlite3.connect("ioa_dev.db")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_articles (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            url_hash          TEXT    UNIQUE NOT NULL,
            url               TEXT    NOT NULL,
            source_name       TEXT    NOT NULL,
            source_tier       TEXT    NOT NULL DEFAULT 'pan-africa',
            hard_country_tags TEXT,        -- JSON list e.g. '["NG"]', null for Tier 1
            headline          TEXT,
            lede              TEXT,
            published_at      TEXT,
            scraped_at        TEXT    NOT NULL,
            processed_at      TEXT    DEFAULT NULL
        )
    """)
    conn.commit()
    return conn, "sqlite"


def url_hash(url: str) -> str:
    return hashlib.sha256(url.strip().encode()).hexdigest()[:16]


def article_exists(db, db_type: str, uhash: str) -> bool:
    if db_type == "sqlite":
        row = db.execute(
            "SELECT 1 FROM raw_articles WHERE url_hash = ?", (uhash,)
        ).fetchone()
        return row is not None
    else:
        result = db.table("raw_articles").select("id").eq("url_hash", uhash).execute()
        return len(result.data) > 0


def insert_article(db, db_type: str, record: dict) -> bool:
    """Returns True if inserted, False if duplicate."""
    import json
    if db_type == "sqlite":
        # Serialise list to JSON string for SQLite
        if isinstance(record.get("hard_country_tags"), list):
            record["hard_country_tags"] = json.dumps(record["hard_country_tags"])
        try:
            db.execute(
                """
                INSERT INTO raw_articles
                    (url_hash, url, source_name, source_tier, hard_country_tags,
                     headline, lede, published_at, scraped_at)
                VALUES
                    (:url_hash, :url, :source_name, :source_tier, :hard_country_tags,
                     :headline, :lede, :published_at, :scraped_at)
                """,
                record,
            )
            db.commit()
            return True
        except sqlite3.IntegrityError:
            return False
    else:
        try:
            db.table("raw_articles").insert(record).execute()
            return True
        except Exception:
            return False


# ── Shared helpers ────────────────────────────────────────────────────────────

def build_record(source: dict, url: str, headline: str, lede: str,
                 published_at=None) -> dict:
    """Build a raw_article record, applying hard country tags for Tier 2 sources."""
    countries = source.get("countries", [])
    hard_tags = countries if source.get("source_tier") == "country-specific" and countries else None

    return {
        "url_hash":          url_hash(url),
        "url":               url,
        "source_name":       source["name"],
        "source_tier":       source.get("source_tier", "pan-africa"),
        "hard_country_tags": hard_tags,   # None for Tier 1 — Layer 2 will detect
        "language":          source.get("language", "en"),          # Layer 2 translation routing
        "paywall_status":    source.get("paywall_status", "open"),  # open | restricted | paywalled
        "headline":          headline,
        "lede":              lede[:1000] if lede else "",
        "published_at":      published_at,
        "scraped_at":        datetime.now(timezone.utc).isoformat(),
    }


def strip_html(raw: str) -> str:
    return BeautifulSoup(raw, "html.parser").get_text(separator=" ").strip()


def fetch_with_header_fallback(url: str) -> requests.Response:
    """
    Try browser-like headers first, then a bot-compatible UA fallback when blocked.
    Some sites (e.g. FR24/RFI) allow one profile and block the other.
    """
    last_error = None
    for idx, headers in enumerate((HEADERS, ALT_HEADERS)):
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            last_error = e
            status = getattr(getattr(e, "response", None), "status_code", None)
            retryable = status in (401, 403, 415)
            if not retryable or idx == 1:
                continue
    raise last_error


def article_link_score(href: str, text: str = "") -> int:
    """Score whether a link looks like an article URL."""
    href_l = href.lower()
    blocked = (
        "mailto:", "javascript:", "#", "/tag/", "/tags/", "/topic/", "/topics/",
        "/category/", "/categories/", "/author/", "/authors/", "/about", "/contact",
        "/privacy", "/terms", "/login", "/signup", "/subscribe", "/advert", "/careers",
        "facebook.com", "twitter.com", "x.com", "linkedin.com", "instagram.com", "youtube.com"
    )
    if any(p in href_l for p in blocked):
        return -10

    score = 0
    positive_tokens = (
        "news", "article", "stories", "insights", "updates", "press",
        "media-room", "politic", "business", "econom", "finance",
        "sport", "tech", "world", "society", "health", "eng_"
    )
    if any(t in href_l for t in positive_tokens):
        score += 2
    if re.search(r"/20\d{2}/", href_l):
        score += 2
    if re.search(r"/\d{4,}", href_l) or re.search(r"-\d{4,}", href_l):
        score += 1
    if href_l.count("-") >= 2:
        score += 1
    text_len = len((text or "").strip())
    if text_len >= 25:
        score += 1

    return score


# ── RSS Ingestion ─────────────────────────────────────────────────────────────

def ingest_rss(source: dict, db, db_type: str) -> dict:
    stats = {"source": source["name"], "tier": source.get("source_tier", "pan-africa"),
             "url": source.get("rss_url") or source.get("url", ""),
             "fetched": 0, "inserted": 0, "dupes": 0, "errors": 0, "error_reason": ""}

    rss_url = source.get("rss_url")
    if not rss_url:
        log.warning(f"[{source['name']}] No RSS URL — skipping")
        return stats

    paywall = source.get("paywall_status", "open")
    if paywall != "open":
        log.info(f"[{source['name']}] Attempting collection on {paywall} source")

    log.info(f"[{source['name']}] RSS ← {rss_url}")
    feed = feedparser.parse(rss_url, request_headers=HEADERS)

    if feed.bozo and not feed.entries:
        if paywall != "open":
            reason = f"RSS blocked — likely paywall ({paywall})"
            log.warning(f"[{source['name']}] {reason}: {feed.bozo_exception}")
        else:
            reason = f"RSS parse error: {feed.bozo_exception}"
            log.error(f"[{source['name']}] {reason}")
        stats["errors"] += 1
        stats["error_reason"] = reason
        return stats

    for entry in feed.entries:
        stats["fetched"] += 1
        url = entry.get("link", "").strip()
        if not url:
            continue

        if article_exists(db, db_type, url_hash(url)):
            stats["dupes"] += 1
            continue

        lede = entry.get("summary", "")
        if not lede and entry.get("content"):
            lede = entry["content"][0].get("value", "")
        lede = strip_html(lede)

        published_at = None
        if entry.get("published_parsed"):
            published_at = datetime(
                *entry.published_parsed[:6], tzinfo=timezone.utc
            ).isoformat()

        record = build_record(
            source, url,
            headline=entry.get("title", "").strip(),
            lede=lede,
            published_at=published_at,
        )

        if insert_article(db, db_type, record):
            stats["inserted"] += 1
            log.debug(f"  ✓ {record['headline'][:80]}")
        else:
            stats["dupes"] += 1

    log.info(
        f"[{source['name']}] done — "
        f"fetched={stats['fetched']} new={stats['inserted']} dupes={stats['dupes']}"
    )
    return stats


# ── HTML Scraper ──────────────────────────────────────────────────────────────

def ingest_scraper(source: dict, db, db_type: str) -> dict:
    stats = {"source": source["name"], "tier": source.get("source_tier", "pan-africa"),
             "url": source.get("url", ""),
             "fetched": 0, "inserted": 0, "dupes": 0, "errors": 0, "error_reason": ""}

    url      = source["url"]
    selector = source.get("css_selector", "h2 a, h3 a")
    paywall  = source.get("paywall_status", "open")

    if paywall != "open":
        log.info(f"[{source['name']}] Attempting collection on {paywall} source")

    log.info(f"[{source['name']}] Scraping ← {url}")

    try:
        resp = fetch_with_header_fallback(url)
    except requests.RequestException as e:
        if paywall != "open":
            reason = f"Scrape blocked — likely paywall ({paywall})"
            log.warning(f"[{source['name']}] {reason}: {e}")
        else:
            reason = f"Request failed: {e}"
            log.error(f"[{source['name']}] {reason}")
        stats["errors"] += 1
        stats["error_reason"] = reason
        return stats

    soup       = BeautifulSoup(resp.text, "html.parser")
    links      = soup.select(selector)
    final_host = urlparse(resp.url).netloc

    title = (soup.title.get_text(" ", strip=True).lower() if soup.title else "")
    if "one moment, please" in title and "window.location.reload" in resp.text.lower():
        reason = "Blocked by anti-bot challenge page"
        log.warning(f"[{source['name']}] {reason}")
        stats["errors"] += 1
        stats["error_reason"] = reason
        return stats

    if not links:
        fallback_selectors = [
            "article a[href]",
            ".entry-title a[href], .post-title a[href], .article-title a[href], .jeg_post_title a[href]",
            "h1 a[href], h2 a[href], h3 a[href], h4 a[href]",
            "a[href*='/news/'], a[href*='/article/'], a[href*='/media-room/'], a[href*='/20']",
            "a[href]",
        ]

        for fallback in fallback_selectors:
            candidates = []
            seen = set()
            for tag in soup.select(fallback):
                href = urljoin(resp.url, tag.get("href", ""))
                if not href.startswith("http"):
                    continue
                host = urlparse(href).netloc
                score = article_link_score(href, tag.get_text(separator=" ").strip())

                # Prefer same-host links, but allow strong external article links for aggregators.
                if host and host != final_host and score < 4:
                    continue
                if href in seen:
                    continue
                if score < 2:
                    continue
                seen.add(href)
                candidates.append(tag)

            if len(candidates) >= 3:
                links = candidates
                log.info(
                    f"[{source['name']}] Selector fallback matched "
                    f"{len(links)} links using '{fallback}'"
                )
                break

    if not links:
        reason = f"No links matched selector '{selector}' — structure may have changed"
        log.warning(f"[{source['name']}] {reason}")
        stats["errors"] += 1
        stats["error_reason"] = reason
        return stats

    seen_hrefs = set()
    for tag in links:
        headline = tag.get_text(separator=" ").strip()
        href     = urljoin(resp.url, tag.get("href", ""))
        if not href.startswith("http"):
            continue
        if href in seen_hrefs:
            continue
        seen_hrefs.add(href)

        stats["fetched"] += 1

        if article_exists(db, db_type, url_hash(href)):
            stats["dupes"] += 1
            continue

        parent = tag.find_parent(["article", "div", "li", "section"])
        lede   = ""
        if parent:
            p_tags = parent.find_all("p")
            if p_tags:
                lede = p_tags[0].get_text(separator=" ").strip()

        record = build_record(source, href, headline=headline, lede=lede)

        if insert_article(db, db_type, record):
            stats["inserted"] += 1
        else:
            stats["dupes"] += 1

    time.sleep(SCRAPE_DELAY)
    log.info(
        f"[{source['name']}] done — "
        f"fetched={stats['fetched']} new={stats['inserted']} dupes={stats['dupes']}"
    )
    return stats


# ── Source loader ─────────────────────────────────────────────────────────────

def load_sources(tier_filter: str = None, source_filter: str = None) -> list:
    """Load and merge both tiers from sources.yaml. Apply optional filters."""
    with open(SOURCES_FILE, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Merge both tiers into one list
    all_sources = config.get("sources", []) + config.get("country_specific_sources", [])

    # Only active sources
    all_sources = [s for s in all_sources if s.get("active", True)]

    # Tier filter
    if tier_filter == "pan":
        all_sources = [s for s in all_sources if s.get("source_tier") == "pan-africa"]
    elif tier_filter == "country":
        all_sources = [s for s in all_sources if s.get("source_tier") == "country-specific"]

    # Name filter
    if source_filter:
        all_sources = [s for s in all_sources if source_filter.lower() in s["name"].lower()]

    return all_sources


# ── Health check ──────────────────────────────────────────────────────────────

def check_health(stats_list: list) -> list:
    return [
        {
            "source":  s["source"],
            "tier":    s["tier"],
            "url":     s.get("url", ""),
            "reason":  s.get("error_reason") or "zero articles fetched",
        }
        for s in stats_list
        if s["fetched"] == 0 or s["errors"] > 0
    ]


# ── Main ──────────────────────────────────────────────────────────────────────

def run(source_filter: str = None, tier_filter: str = None, mode: str = "dev"):
    sources = load_sources(tier_filter=tier_filter, source_filter=source_filter)

    if not sources:
        log.error("No sources matched filters — nothing to process")
        return

    db, db_type = get_db(mode)
    log.info(f"Starting collection — {len(sources)} sources, mode={mode}, db={db_type}")

    all_stats = []
    for source in sources:
        try:
            source_type = source.get("type", "rss")
            if source_type == "rss":
                stats = ingest_rss(source, db, db_type)
            elif source_type == "scraper":
                stats = ingest_scraper(source, db, db_type)
            elif source_type == "rss+scraper":
                stats = ingest_rss(source, db, db_type)
                if stats["fetched"] == 0:
                    log.info(f"[{source['name']}] RSS empty — falling back to scraper")
                    stats = ingest_scraper(source, db, db_type)
            else:
                log.warning(f"Unknown type '{source_type}' for {source['name']}")
                continue

            all_stats.append(stats)

        except Exception as e:
            log.exception(f"Unhandled error on {source['name']}: {e}")

    # Summary
    total_new   = sum(s["inserted"] for s in all_stats)
    total_dupes = sum(s["dupes"]    for s in all_stats)
    tier1_new   = sum(s["inserted"] for s in all_stats if s["tier"] == "pan-africa")
    tier2_new   = sum(s["inserted"] for s in all_stats if s["tier"] == "country-specific")

    log.info(f"\n{'='*60}")
    log.info(f"Collection complete")
    log.info(f"  New articles : {total_new} (pan-africa={tier1_new}, country-specific={tier2_new})")
    log.info(f"  Duplicates   : {total_dupes}")

    alerts   = check_health(all_stats)
    run_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    if alerts:
        log.warning(f"\n⚠️  {len(alerts)} sources need attention:")
        for a in alerts:
            log.warning(f"  [{a['tier']}] {a['source']} ({a['url']}): {a['reason']}")

    # ── Write structured health report ────────────────────────────────────────
    report_path = LOG_DIR / f"health_{datetime.now().strftime('%Y-%m-%d')}.md"
    working     = [s for s in all_stats if s["fetched"] > 0 and s["errors"] == 0]

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"# IOA Collector Health Report\n")
        f.write(f"**Run:** {run_time}  \n")
        f.write(f"**Sources processed:** {len(all_stats)}  \n")
        f.write(f"**New articles:** {total_new} (pan-africa: {tier1_new} | country-specific: {tier2_new})  \n")
        f.write(f"**Duplicates skipped:** {total_dupes}  \n\n")

        f.write(f"---\n\n")

        # Working sources
        f.write(f"## ✅ Working Sources ({len(working)})\n\n")
        f.write(f"| Source | Tier | New | Dupes |\n")
        f.write(f"|--------|------|-----|-------|\n")
        for s in sorted(working, key=lambda x: x["inserted"], reverse=True):
            f.write(f"| {s['source']} | {s['tier']} | {s['inserted']} | {s['dupes']} |\n")

        f.write(f"\n---\n\n")

        # Failed sources grouped by likely cause
        f.write(f"## ❌ Failed Sources ({len(alerts)}) — Needs Inspection\n\n")
        f.write(f"| Source | Tier | URL | Likely Cause |\n")
        f.write(f"|--------|------|-----|---------------|\n")
        for a in sorted(alerts, key=lambda x: x["tier"]):
            f.write(f"| {a['source']} | {a['tier']} | {a['url']} | {a['reason']} |\n")

    log.info(f"\n📋 Health report written → {report_path}")

    return {"inserted": total_new, "dupes": total_dupes, "alerts": alerts}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IOA Layer 1 Collector")
    parser.add_argument("--source", help="Filter by source name (partial match)")
    parser.add_argument("--tier",   choices=["pan", "country"], help="Filter by source tier")
    parser.add_argument("--mode",   default="dev", choices=["dev", "prod"])
    args = parser.parse_args()
    run(source_filter=args.source, tier_filter=args.tier, mode=args.mode)
