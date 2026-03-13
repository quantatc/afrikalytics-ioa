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
import csv
import hashlib
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import feedparser
import requests
import yaml
from bs4 import BeautifulSoup


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
SECTOR_CSV_FILE = Path(__file__).parent / "insights-tracker-sources.csv"
GDELT_DOC_API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
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

# 54 UN-recognized African countries + practical aliases used in headlines.
AFRICAN_COUNTRY_TERMS = [
    "Algeria", "Angola", "Benin", "Botswana", "Burkina Faso", "Burundi",
    "Cabo Verde", "Cape Verde", "Cameroon", "Central African Republic",
    "Chad", "Comoros", "Congo", "Republic of the Congo", "Congo-Brazzaville",
    "Democratic Republic of the Congo", "DR Congo", "Congo-Kinshasa",
    "Cote d'Ivoire", "Ivory Coast", "Djibouti", "Egypt",
    "Equatorial Guinea", "Eritrea", "Eswatini", "Ethiopia", "Gabon", "Gambia",
    "The Gambia", "Ghana", "Guinea", "Guinea-Bissau", "Kenya", "Lesotho",
    "Liberia", "Libya", "Madagascar", "Malawi", "Mali", "Mauritania",
    "Mauritius", "Morocco", "Mozambique", "Namibia", "Niger", "Nigeria",
    "Rwanda", "Sao Tome and Principe", "Senegal",
    "Seychelles", "Sierra Leone", "Somalia", "South Africa", "South Sudan",
    "Sudan", "Tanzania", "Togo", "Tunisia", "Uganda", "Zambia", "Zimbabwe",
]

DEFAULT_SECTOR_LABELS = [
    "Business and Economics",
    "Politics and Security",
    "Agri, Energy and Natural Resources",
    "Infrastructure",
    "Science and Technology",
    "Multiple Sectors",
]

SECTOR_QUERY_HINTS = {
    "business and economics": [
        "business", "economy", "finance", "markets", "trade", "investment", "inflation", "banking",
    ],
    "politics and security": [
        "politics", "election", "government", "policy", "security", "defense", "conflict", "diplomacy",
    ],
    "agri, energy and natural resources": [
        "agriculture", "agri", "food", "farming", "energy", "oil", "gas", "power", "mining", "resources",
    ],
    "infrastructure": [
        "infrastructure", "transport", "rail", "port", "road", "construction", "logistics",
    ],
    "science and technology": [
        "technology", "tech", "telecom", "digital", "AI", "innovation", "startup", "cybersecurity",
    ],
    "multiple sectors": [
        "business", "politics", "security", "infrastructure", "technology", "energy",
    ],
}


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


def load_sector_labels_from_csv(csv_path: Path = SECTOR_CSV_FILE) -> list:
    """Return unique sector labels from insights-tracker-sources.csv."""
    labels = []
    seen = set()
    try:
        with open(csv_path, encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                label = (row.get("sectors") or "").strip()
                if label and label not in seen:
                    seen.add(label)
                    labels.append(label)
    except FileNotFoundError:
        log.warning(f"Sector CSV not found ({csv_path}); using defaults for GDELT query")
    except Exception as e:
        log.warning(f"Failed reading sector CSV ({csv_path}); using defaults for GDELT query: {e}")

    return labels or DEFAULT_SECTOR_LABELS


def build_gdelt_country_terms(source: dict) -> list:
    countries = AFRICAN_COUNTRY_TERMS if source.get("gdelt_all_african_countries", True) else []
    return list(dict.fromkeys(countries))


def build_gdelt_theme_terms(source: dict) -> list:
    sector_labels = (
        load_sector_labels_from_csv()
        if source.get("gdelt_use_csv_sectors", True)
        else DEFAULT_SECTOR_LABELS
    )
    theme_terms = []
    seen_terms = set()
    for label in sector_labels:
        key = label.lower().strip()
        expanded = SECTOR_QUERY_HINTS.get(key, [label])
        for term in expanded:
            normalized = term.strip()
            if normalized and normalized.lower() not in seen_terms:
                seen_terms.add(normalized.lower())
                theme_terms.append(normalized)
    return theme_terms


def build_gdelt_query_from_terms(country_terms: list, theme_terms: list) -> str:
    country_clause = " OR ".join(f'"{c}"' for c in country_terms) if country_terms else '"Africa"'
    theme_clause = " OR ".join(f'"{t}"' for t in theme_terms) if theme_terms else '"news"'
    return f"({country_clause}) AND ({theme_clause})"


def split_gdelt_queries(country_terms: list, theme_terms: list, max_query_len: int) -> list:
    """
    Split a large boolean query into multiple equivalent queries whose union
    matches the original intent.
    """
    def recurse(c_terms: list, t_terms: list) -> list:
        candidate = build_gdelt_query_from_terms(c_terms, t_terms)
        if len(candidate) <= max_query_len:
            return [candidate]

        if len(c_terms) <= 1 and len(t_terms) <= 1:
            log.warning(
                "GDELT query term still exceeds max length (%s chars) even at minimal split (%s chars)",
                max_query_len,
                len(candidate),
            )
            return [candidate]

        if len(c_terms) >= len(t_terms) and len(c_terms) > 1:
            mid = len(c_terms) // 2
            return recurse(c_terms[:mid], t_terms) + recurse(c_terms[mid:], t_terms)

        if len(t_terms) > 1:
            mid = len(t_terms) // 2
            return recurse(c_terms, t_terms[:mid]) + recurse(c_terms, t_terms[mid:])

        mid = max(1, len(c_terms) // 2)
        return recurse(c_terms[:mid], t_terms) + recurse(c_terms[mid:], t_terms)

    raw_queries = recurse(country_terms, theme_terms)
    unique_queries = []
    seen = set()
    for query in raw_queries:
        if query in seen:
            continue
        seen.add(query)
        unique_queries.append(query)
    return unique_queries


def build_gdelt_queries(source: dict, max_query_len: int = 3500) -> list:
    """
    Build one or more GDELT queries.
    If query is too long, split into equivalent subqueries.
    """
    explicit_query = (source.get("gdelt_query") or "").strip()
    if explicit_query:
        if len(explicit_query) > max_query_len:
            log.warning(
                "[%s] Explicit gdelt_query length (%s) exceeds gdelt_max_query_len (%s)",
                source.get("name", "GDELT"),
                len(explicit_query),
                max_query_len,
            )
        return [explicit_query]

    countries = build_gdelt_country_terms(source)
    themes = build_gdelt_theme_terms(source)
    if not countries:
        countries = ["Africa"]
    if not themes:
        themes = ["news"]

    return split_gdelt_queries(countries, themes, max_query_len)


def parse_gdelt_datetime(raw_value: str):
    if not raw_value:
        return None
    candidates = (
        "%Y%m%dT%H%M%SZ",
        "%Y%m%d%H%M%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
    )
    for fmt in candidates:
        try:
            return datetime.strptime(raw_value, fmt).replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    return None


def request_gdelt_payload_with_retry(
    endpoint: str,
    params: dict,
    source_name: str,
    subquery_label: str,
    min_interval_seconds: int,
    max_retries: int,
    last_request_mono: float,
) -> tuple[dict, float]:
    """
    Request GDELT payload with rate-limit aware pacing/retries.
    GDELT can return plain-text throttling responses instead of JSON.
    """
    attempts = max_retries + 1
    for attempt in range(1, attempts + 1):
        if last_request_mono:
            elapsed = time.monotonic() - last_request_mono
            if elapsed < min_interval_seconds:
                time.sleep(min_interval_seconds - elapsed)

        try:
            resp = requests.get(endpoint, params=params, headers=ALT_HEADERS, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            if attempt < attempts:
                wait_seconds = min_interval_seconds * attempt
                log.warning(
                    "[%s] %s request error (attempt %s/%s): %s. Retrying in %ss",
                    source_name,
                    subquery_label,
                    attempt,
                    attempts,
                    e,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue
            raise RuntimeError(str(e)) from e

        last_request_mono = time.monotonic()
        status_code = resp.status_code
        body_text = (resp.text or "").strip()

        if status_code == 429:
            retry_after_raw = (resp.headers.get("Retry-After") or "").strip()
            retry_after_seconds = int(retry_after_raw) if retry_after_raw.isdigit() else min_interval_seconds
            wait_seconds = max(min_interval_seconds, retry_after_seconds)
            if attempt < attempts:
                log.warning(
                    "[%s] %s rate-limited (429) attempt %s/%s. Retrying in %ss",
                    source_name,
                    subquery_label,
                    attempt,
                    attempts,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue
            snippet = (body_text[:180] + "...") if len(body_text) > 180 else body_text
            raise RuntimeError(f"GDELT rate-limited (HTTP 429): {snippet or 'empty body'}")

        if status_code >= 400:
            snippet = (body_text[:180] + "...") if len(body_text) > 180 else body_text
            raise RuntimeError(f"GDELT HTTP {status_code}: {snippet or 'empty body'}")

        try:
            payload = resp.json()
            return payload, last_request_mono
        except ValueError:
            body_l = body_text.lower()
            looks_rate_limited = (
                "please limit requests" in body_l
                or "too many requests" in body_l
                or "rate limit" in body_l
            )
            if looks_rate_limited and attempt < attempts:
                wait_seconds = min_interval_seconds * attempt
                log.warning(
                    "[%s] %s non-JSON rate-limit response (attempt %s/%s). Retrying in %ss",
                    source_name,
                    subquery_label,
                    attempt,
                    attempts,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue
            snippet = (body_text[:180] + "...") if len(body_text) > 180 else body_text
            raise RuntimeError(
                f"GDELT returned non-JSON response (HTTP {status_code}): {snippet or 'empty body'}"
            )

    raise RuntimeError("GDELT retries exhausted")


def is_gdelt_query_size_error(message: str) -> bool:
    msg_l = (message or "").lower()
    return (
        "too short or too long" in msg_l
        or "query was too short" in msg_l
        or "query was too long" in msg_l
    )


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


def ingest_gdelt_doc(source: dict, db, db_type: str) -> dict:
    stats = {"source": source["name"], "tier": source.get("source_tier", "pan-africa"),
             "url": source.get("url", GDELT_DOC_API_URL),
             "fetched": 0, "inserted": 0, "dupes": 0, "errors": 0, "error_reason": ""}

    endpoint = source.get("gdelt_endpoint", GDELT_DOC_API_URL)
    lookback_days = int(source.get("gdelt_lookback_days", 14))
    max_records = int(source.get("gdelt_max_records", 250))
    max_query_len = int(source.get("gdelt_max_query_len", 900))
    min_interval_seconds = int(source.get("gdelt_min_interval_seconds", 5))
    max_retries = int(source.get("gdelt_max_retries", 3))
    split_attempts = int(source.get("gdelt_split_attempts", 4))
    min_query_len_floor = int(source.get("gdelt_min_query_len_floor", 250))

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=lookback_days)
    adaptive_max_query_len = max_query_len
    last_request_mono = 0.0

    for split_try in range(1, split_attempts + 1):
        queries = build_gdelt_queries(source, max_query_len=adaptive_max_query_len)
        log.info(
            f"[{source['name']}] GDELT DOC API <= {endpoint} "
            f"(lookback={lookback_days}d, max={max_records}, subqueries={len(queries)}, "
            f"query_len<={adaptive_max_query_len}, interval={min_interval_seconds}s, retries={max_retries})"
        )

        seen_urls = set()
        too_long_error_seen = False
        pass_errors = 0
        pass_error_reason = ""

        for idx, query in enumerate(queries, start=1):
            subquery_label = f"subquery {idx}/{len(queries)}"
            params = {
                "query": query,
                "mode": "artlist",
                "format": "json",
                "sort": "DateDesc",
                "maxrecords": max_records,
                "startdatetime": start_dt.strftime("%Y%m%d%H%M%S"),
                "enddatetime": end_dt.strftime("%Y%m%d%H%M%S"),
            }

            try:
                payload, last_request_mono = request_gdelt_payload_with_retry(
                    endpoint=endpoint,
                    params=params,
                    source_name=source["name"],
                    subquery_label=subquery_label,
                    min_interval_seconds=min_interval_seconds,
                    max_retries=max_retries,
                    last_request_mono=last_request_mono,
                )
            except RuntimeError as e:
                message = str(e)
                reason = f"GDELT {subquery_label} failed: {message}"
                log.error(f"[{source['name']}] {reason}")
                pass_errors += 1
                pass_error_reason = reason
                if is_gdelt_query_size_error(message):
                    too_long_error_seen = True
                continue

            articles = payload.get("articles") or []
            for article in articles:
                url = (article.get("url") or "").strip()
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)
                stats["fetched"] += 1

                published_at = (
                    parse_gdelt_datetime(article.get("seendate"))
                    or parse_gdelt_datetime(article.get("published"))
                    or parse_gdelt_datetime(article.get("date"))
                )
                if published_at:
                    published_dt = datetime.fromisoformat(published_at)
                    if published_dt < start_dt:
                        continue

                if article_exists(db, db_type, url_hash(url)):
                    stats["dupes"] += 1
                    continue

                headline = (article.get("title") or "").strip()
                lede = (
                    article.get("description")
                    or article.get("snippet")
                    or article.get("sourcecommonname")
                    or ""
                )

                record = build_record(source, url, headline=headline, lede=lede, published_at=published_at)
                if insert_article(db, db_type, record):
                    stats["inserted"] += 1
                else:
                    stats["dupes"] += 1

        if pass_errors:
            stats["errors"] = pass_errors
            stats["error_reason"] = pass_error_reason

        no_progress = (stats["fetched"] == 0 and stats["inserted"] == 0)
        can_shrink = (adaptive_max_query_len > min_query_len_floor)
        has_next_try = (split_try < split_attempts)
        if too_long_error_seen and no_progress and can_shrink and has_next_try:
            next_len = max(min_query_len_floor, adaptive_max_query_len // 2)
            log.warning(
                "[%s] Query rejected as too short/long. Retrying with tighter gdelt_max_query_len=%s (attempt %s/%s)",
                source["name"],
                next_len,
                split_try + 1,
                split_attempts,
            )
            adaptive_max_query_len = next_len
            stats["errors"] = 0
            stats["error_reason"] = ""
            continue
        break

    if stats["fetched"] == 0 and not stats["error_reason"]:
        reason = "GDELT returned no articles for query window"
        log.warning(f"[{source['name']}] {reason}")
        stats["error_reason"] = reason

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
            elif source_type == "gdelt-doc":
                stats = ingest_gdelt_doc(source, db, db_type)
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
