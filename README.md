# 🌍 afrikalytics

> **Automated intelligence pipeline for Africa-focused decision-makers.**
> Collects, enriches, and synthesises news from across the continent into structured monthly briefings — powering IOA's *In(sights) on Africa* product.

---

## Project Overview

African markets are fast-moving, fragmented, and underserved by mainstream news aggregators. Analysts at IOA currently monitor 30+ sources manually to produce monthly intelligence briefings for senior executives, fund managers, and policy advisors operating across the continent.

**afrikalytics** automates that pipeline end-to-end:

```
Collect → Deduplicate → Enrich → Synthesise → Deliver
```

The system is built as a **hybrid architecture**: Python handles the technically demanding collection and enrichment work; n8n orchestrates scheduling, workflow logic, and report delivery. Both talk through a shared Supabase database, making them independently deployable and maintainable.

---

## Repository Structure

```
afrikalytics/
├── layer1/                  # Collection — scraping & RSS ingestion
│   ├── collect.py           # Main collector script
│   ├── sources.yaml         # Source registry (all 30+ sources + country-specific)
│   ├── schema.sql           # Supabase database schema
│   └── pyproject.toml       # uv dependency management
│
├── layer2/                  # Enrichment — AI classification & summarisation (planned)
│   ├── enrich.py
│   └── pyproject.toml
│
├── layer3/                  # Synthesis & report generation (planned)
│   ├── synthesise.py
│   └── pyproject.toml
│
└── README.md
```

---

## Layer 1 — Collection

Layer 1 is the data foundation. It ingests raw articles from all configured sources, deduplicates by URL hash, and stores structured records in Supabase (or local SQLite during development). No AI is involved at this stage — the goal is clean, high-volume, low-cost ingestion.

### How it works

Sources are defined in `sources.yaml` and split into two tiers:

| Tier | Description | Count |
|---|---|---|
| **Tier 1 — Pan-Africa** | The core 30 sources covering the continent broadly | 30 |
| **Tier 2 — Country-Specific** | Targeted national publications added as IOA expands coverage | Growing |

For each source, the collector supports three ingestion strategies:

- **`rss`** — Parses the source's RSS/Atom feed directly. Fast, stable, and low-maintenance. ~24 of the 30 core sources support this.
- **`scraper`** — Fetches the index page and extracts article links via CSS selectors. Used for the ~6 sources with no RSS feed.
- **`rss+scraper`** — Tries RSS first; falls back to scraping if the feed returns nothing.

**Country-specific sources are hard-tagged at ingestion.** When a Tier 2 source (e.g. a Nigerian newspaper) is ingested, its ISO country code is stamped directly onto the article record. This means Layer 2's AI enrichment can skip country detection entirely for those articles — saving API tokens and improving accuracy.

### Source registry (`sources.yaml`)

All source configuration lives in `sources.yaml`. Adding a new source — including country-specific sources shared by the analyst team — requires no code changes:

```yaml
# Adding a new country-specific source:
- name: Businessday Nigeria
  url: https://businessday.ng/
  rss_url: https://businessday.ng/feed/
  type: rss
  source_tier: country-specific
  region: west-africa
  countries: [NG]          # ISO 3166-1 alpha-2 — hard-tagged at ingestion
  sectors: [business, finance, economics]
  active: true
```

To pause a source without removing it, set `active: false`.

### Database schema

Layer 1 writes to the `raw_articles` table. Key fields:

| Field | Description |
|---|---|
| `url_hash` | SHA-256 hash of URL — primary deduplication key |
| `source_tier` | `pan-africa` or `country-specific` |
| `hard_country_tags` | Pre-filled ISO codes for Tier 2 sources; `null` for Tier 1 |
| `headline` | Article title |
| `lede` | First ~1000 characters of article text, HTML-stripped |
| `processed_at` | Set by Layer 2 when enrichment is complete; `null` = unprocessed |

---

## Setup

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) — fast Python package manager
- Supabase project (free tier is sufficient) — or SQLite for local dev

### Install

```bash
# Install uv (one-time)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and install dependencies
git clone https://github.com/your-org/afrikalytics.git
cd afrikalytics/layer1
uv sync
```

### Configure

For production (Supabase), set environment variables:

```bash
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_KEY="your-anon-key"
```

For local development, no configuration is needed — the collector writes to `ioa_dev.db` (SQLite) automatically.

### Database setup (Supabase only)

Run `schema.sql` once in the Supabase SQL editor to create all tables. Enable the `pgvector` extension first under **Database → Extensions**.

---

## Running the Collector

```bash
# All active sources — dev mode (SQLite)
uv run python collect.py

# All active sources — production (Supabase)
uv run python collect.py --mode prod

# Single source by name (partial match)
uv run python collect.py --source "Reuters"

# Pan-Africa sources only
uv run python collect.py --tier pan

# Country-specific sources only (e.g. after Zahra's team adds new ones)
uv run python collect.py --tier country
```

### Logs and Health Reports

Every run produces two files in the `logs/` directory (auto-created):

| File | Description |
|---|---|
| `logs/collect_YYYY-MM-DD.log` | Full run log — all INFO and WARNING messages, UTF-8 encoded |
| `logs/health_YYYY-MM-DD.md` | Structured health report — working sources table + failed sources table with URLs and likely causes |

The health report is designed to be shared directly with the analyst team for source inspection. It groups failures with plain-English diagnosis:

- **zero articles fetched** — RSS URL is wrong or returns HTML
- **RSS blocked / paywall** — expected for paywalled sources
- **getaddrinfo failed** — DNS or network issue
- **junk after document element** — RSS URL points to HTML, not XML
- **No links matched selector** — site structure changed, CSS selector needs updating

```bash
# Run at 6am and 6pm daily
0 6,18 * * * cd /opt/afrikalytics/layer1 && uv run python collect.py --mode prod >> /var/log/ioa_collect.log 2>&1
```

Alternatively, trigger via an **n8n Schedule node** with an HTTP POST to a FastAPI endpoint wrapping the collector — this gives full pipeline visibility from the n8n dashboard.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 1 — Collection (Python)                                  │
│  RSS feeds + HTML scrapers → Dedup → Supabase: raw_articles     │
└──────────────────────────┬──────────────────────────────────────┘
                           │ new articles (polled every 6h)
┌──────────────────────────▼──────────────────────────────────────┐
│  LAYER 2 — Enrichment (Python + Claude Haiku)                   │
│  Country tagging · Sector classification · Relevance scoring    │
│  3-sentence summaries · Embedding generation (pgvector)         │
└──────────────────────────┬──────────────────────────────────────┘
                           │ enriched articles
┌──────────────────────────▼──────────────────────────────────────┐
│  LAYER 3 — Orchestration & Delivery (n8n)                       │
│  Monthly trigger → Fetch clusters → RAG synthesis →             │
│  Report assembly → Google Drive → Analyst review → Send         │
└─────────────────────────────────────────────────────────────────┘
```

**Why hybrid?** Python handles what it does best — robust scraping, semantic deduplication, embeddings, and AI enrichment. n8n handles what it does best — scheduling, workflow orchestration, and analyst-facing tooling that non-developers can maintain and extend without touching code.

**Infrastructure cost:** ~$15–20/month (€8 Hetzner VPS + ~$5 Claude Haiku API usage + Supabase free tier).

---

## Roadmap

- [x] Layer 1 — Collection (RSS + scraping, dual-tier source registry)
- [ ] Layer 2 — Enrichment (Haiku classification, summaries, pgvector embeddings)
- [ ] Layer 3 — Synthesis (RAG-based cross-article trend analysis)
- [ ] n8n workflow — Monthly report orchestration and delivery
- [ ] Analyst review UI — Airtable/Google Sheets integration for human-in-the-loop oversight
- [ ] Source health dashboard — alerting for broken scrapers and paywalled sources

---

## Contributing / Adding Sources

### Via CSV (recommended — for analyst team)

The primary way to add sources is via the CSV file that analysts maintain (`insights-tracker-sources.csv`). When new rows are added:

```bash
uv run python migrate_sources.py --dry-run   # preview what will be added
uv run python migrate_sources.py             # merge into sources.yaml
```

The script auto-handles country→ISO mapping, sector normalisation, language inference, and deduplication. No code changes ever needed.

### Via `sources.yaml` directly (for developers)

1. Open `layer1/sources.yaml`
2. Add an entry under `country_specific_sources` using the template at the bottom of that file
3. Run `uv run python collect.py --source "Your New Source"` to test in isolation
4. Commit and push

### Fixing a broken source

Check `logs/health_YYYY-MM-DD.md` for the failure reason, then update the relevant field in `sources.yaml`:

| Failure reason | Fix |
|---|---|
| RSS parse / junk after document | Change `rss_url` or switch `type` to `rss+scraper` |
| No links matched selector | Update `css_selector` |
| getaddrinfo failed | Verify URL is reachable; may be geo-blocked |
| Paywall blocked | Expected — no fix needed, already tagged |

---

## License

Private — In On Africa (IOA). Not for public distribution.
