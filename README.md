# ðŸŒ afrikalytics

> **Automated intelligence pipeline for Africa-focused decision-makers.**
> Collects, enriches, and synthesises news from across the continent into structured monthly briefings â€” powering IOA's *In(sights) on Africa* product.

---

## Project Overview

African markets are fast-moving, fragmented, and underserved by mainstream news aggregators. Analysts at IOA currently monitor 30+ sources manually to produce monthly intelligence briefings for senior executives, fund managers, and policy advisors operating across the continent.

**afrikalytics** automates that pipeline end-to-end:

```
Collect â†’ Deduplicate â†’ Enrich â†’ Synthesise â†’ Deliver
```

The system is built as a **hybrid architecture**: Python handles the technically demanding collection and enrichment work; n8n orchestrates scheduling, workflow logic, and report delivery. Both talk through a shared Supabase database, making them independently deployable and maintainable.

---

## Repository Structure

```
afrikalytics/
â”œâ”€â”€ layer1/                  # Collection â€” scraping & RSS ingestion
â”‚   â”œâ”€â”€ collect.py           # Main collector script
â”‚   â”œâ”€â”€ sources.yaml         # Source registry (all 30+ sources + country-specific)
â”‚   â”œâ”€â”€ schema.sql           # Supabase database schema
â”‚   â””â”€â”€ pyproject.toml       # uv dependency management
â”‚
â”œâ”€â”€ layer2/                  # Enrichment â€” OpenAI classification & summarisation
â”‚   â”œâ”€â”€ enrich.py
â”‚   â””â”€â”€ pyproject.toml
â”‚
â”œâ”€â”€ layer3/                  # Synthesis & report generation
â”‚   â”œâ”€â”€ synthesise.py
â”‚   â””â”€â”€ pyproject.toml
â”‚
â”œâ”€â”€ orchestration/           # n8n bridge and setup docs
â”‚   â”œâ”€â”€ runner_api.py
â”‚   â””â”€â”€ N8N_SETUP.md
â”‚
â””â”€â”€ README.md
```

---

## Layer 1 â€” Collection

Layer 1 is the data foundation. It ingests raw articles from all configured sources, deduplicates by URL hash, and stores structured records in Supabase (or local SQLite during development). No AI is involved at this stage â€” the goal is clean, high-volume, low-cost ingestion.

### How it works

Sources are defined in `sources.yaml` and split into two tiers:

| Tier | Description | Count |
|---|---|---|
| **Tier 1 â€” Pan-Africa** | The core 30 sources covering the continent broadly | 30 |
| **Tier 2 â€” Country-Specific** | Targeted national publications added as IOA expands coverage | Growing |

For each source, the collector supports three ingestion strategies:

- **`rss`** â€” Parses the source's RSS/Atom feed directly. Fast, stable, and low-maintenance. ~24 of the 30 core sources support this.
- **`scraper`** â€” Fetches the index page and extracts article links via CSS selectors. Used for the ~6 sources with no RSS feed.
- **`rss+scraper`** â€” Tries RSS first; falls back to scraping if the feed returns nothing.

**Country-specific sources are hard-tagged at ingestion.** When a Tier 2 source (e.g. a Nigerian newspaper) is ingested, its ISO country code is stamped directly onto the article record. This means Layer 2's AI enrichment can skip country detection entirely for those articles â€” saving API tokens and improving accuracy.

### Source registry (`sources.yaml`)

All source configuration lives in `sources.yaml`. Adding a new source â€” including country-specific sources shared by the analyst team â€” requires no code changes:

```yaml
# Adding a new country-specific source:
- name: Businessday Nigeria
  url: https://businessday.ng/
  rss_url: https://businessday.ng/feed/
  type: rss
  source_tier: country-specific
  region: west-africa
  countries: [NG]          # ISO 3166-1 alpha-2 â€” hard-tagged at ingestion
  sectors: [business, finance, economics]
  active: true
```

To pause a source without removing it, set `active: false`.

### Database schema

Layer 1 writes to the `raw_articles` table. Key fields:

| Field | Description |
|---|---|
| `url_hash` | SHA-256 hash of URL â€” primary deduplication key |
| `source_tier` | `pan-africa` or `country-specific` |
| `hard_country_tags` | Pre-filled ISO codes for Tier 2 sources; `null` for Tier 1 |
| `headline` | Article title |
| `lede` | First ~1000 characters of article text, HTML-stripped |
| `processed_at` | Set by Layer 2 when enrichment is complete; `null` = unprocessed |

---

## Setup

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) â€” fast Python package manager
- Supabase project (free tier is sufficient) â€” or SQLite for local dev

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

Create a repo-root `.env` file (recommended), then paste your keys:

```bash
cp .env.example .env
```

`.env` values:

```bash
SUPABASE_URL="https://your-project.supabase.co"
SUPABASE_KEY="your-service-role-key"
OPENAI_API_KEY="sk-..."

# Optional Layer 2 tuning (cost/quality defaults)
LAYER2_MODEL="gpt-4o-mini"
LAYER2_EMBEDDING_MODEL="text-embedding-3-small"
LAYER2_EMBEDDING_DIMS="384"

# Layer 3 defaults
LAYER3_MODEL="gpt-4o-mini"
LAYER3_PERIOD_DAYS="7"
LAYER3_MAX_ARTICLES="120"
LAYER3_MIN_RELEVANCE="3"

# n8n Cloud trial webhook token
ORCH_RUN_TOKEN="replace_with_a_long_random_token"
```

For local development, no configuration is needed for SQLite reads/writes, but Layer 2 still requires `OPENAI_API_KEY`.

Both `layer1/collect.py` and `layer2/enrich.py` auto-load `.env` at startup.

If you prefer shell variables instead of `.env`, you can still export them manually.

### Database setup (Supabase only)

Run `schema.sql` once in the Supabase SQL editor to create all tables. Enable the `pgvector` extension first under **Database â†’ Extensions**.

---

## Running the Collector

```bash
# All active sources â€” dev mode (SQLite)
uv run python collect.py

# All active sources â€” production (Supabase)
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
| `logs/collect_YYYY-MM-DD.log` | Full run log â€” all INFO and WARNING messages, UTF-8 encoded |
| `logs/health_YYYY-MM-DD.md` | Structured health report â€” working sources table + failed sources table with URLs and likely causes |

The health report is designed to be shared directly with the analyst team for source inspection. It groups failures with plain-English diagnosis:

- **zero articles fetched** â€” RSS URL is wrong or returns HTML
- **RSS blocked / paywall** â€” expected for paywalled sources
- **getaddrinfo failed** â€” DNS or network issue
- **junk after document element** â€” RSS URL points to HTML, not XML
- **No links matched selector** â€” site structure changed, CSS selector needs updating

```bash
# Run at 6am and 6pm daily
0 6,18 * * * cd /opt/afrikalytics/layer1 && uv run python collect.py --mode prod >> /var/log/ioa_collect.log 2>&1
```

Alternatively, trigger via an **n8n Schedule node** with an HTTP POST to a FastAPI endpoint wrapping the collector â€” this gives full pipeline visibility from the n8n dashboard.

---

## Running Layer 2 Enrichment

```bash
# Local dev (SQLite in layer1/ioa_dev.db)
uv run python layer2/enrich.py --mode dev --batch-size 50

# Production (Supabase)
uv run python layer2/enrich.py --mode prod --batch-size 100
```

Layer 2 processes `raw_articles.processed_at IS NULL`, writes structured output to `enriched_articles`, generates 384-d vectors, and marks each raw article as processed.
Country output is normalized to African ISO-2 codes plus `PAN` (invalid/non-African outputs are auto-mapped to `PAN`), and `layer2/countries.py` provides display-name mapping for Slack/UI.

---

## Running Layer 3 Synthesis

```bash
# Weekly synthesis (default)
uv run python layer3/synthesise.py --mode prod --period-days 7

# Monthly-style synthesis over 30 days
uv run python layer3/synthesise.py --mode prod --period-days 30 --max-articles 160 --min-relevance 3
```

Outputs are written to `layer3/reports/` as markdown + JSON, and a `report_runs` row is inserted with `status='drafted'`.

---

## n8n Orchestration Setup

Use [`orchestration/N8N_SETUP.md`](orchestration/N8N_SETUP.md) for:

- n8n Cloud trial (secure webhook runner + tunnel)
- self-hosted n8n (direct command execution)

---

## Architecture

```
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚  LAYER 1 â€” Collection (Python)                                  â”‚
â”‚  RSS feeds + HTML scrapers â†’ Dedup â†’ Supabase: raw_articles     â”‚
â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                           â”‚ new articles (polled every 6h)
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â–¼â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚  LAYER 2 â€” Enrichment (Python + OpenAI gpt-4o-mini)             â”‚
â”‚  Country tagging Â· Sector classification Â· Relevance scoring    â”‚
â”‚  3-sentence summaries Â· Embedding generation (pgvector)         â”‚
â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                           â”‚ enriched articles
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â–¼â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚  LAYER 3 â€” Synthesis (Python + OpenAI)                           â”‚
â”‚  Weekly/monthly trigger â†’ Trend synthesis â†’ report artifacts     â”‚
â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                           â”‚ report outputs
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â–¼â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚  Orchestration & Delivery (n8n)                                   â”‚
â”‚  Schedule â†’ run Layer 1/2/3 â†’ Slack/Drive/Email â†’ human review   â”‚
â”‚  Report assembly â†’ Google Drive â†’ Analyst review â†’ Send         â”‚
â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
```

**Why hybrid?** Python handles what it does best â€” robust scraping, semantic deduplication, embeddings, and AI enrichment. n8n handles what it does best â€” scheduling, workflow orchestration, and analyst-facing tooling that non-developers can maintain and extend without touching code.

**Infrastructure cost:** ~low double-digits/month (infra + OpenAI usage + Supabase free tier).

---

## Roadmap

- [x] Layer 1 â€” Collection (RSS + scraping, dual-tier source registry)
- [x] Layer 2 â€” Enrichment (OpenAI classification, summaries, pgvector embeddings)
- [x] Layer 3 â€” Synthesis (cross-article trend analysis + report generation)
- [x] n8n workflow bridge â€” Cloud-trial webhook runner + self-host path docs
- [ ] Analyst review UI â€” Airtable/Google Sheets integration for human-in-the-loop oversight
- [ ] Source health dashboard â€” alerting for broken scrapers and paywalled sources

---

## Contributing / Adding Sources

### Via CSV (recommended â€” for analyst team)

The primary way to add sources is via the CSV file that analysts maintain (`insights-tracker-sources.csv`). When new rows are added:

```bash
uv run python migrate_sources.py --dry-run   # preview what will be added
uv run python migrate_sources.py             # merge into sources.yaml
```

The script auto-handles countryâ†’ISO mapping, sector normalisation, language inference, and deduplication. No code changes ever needed.

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
| Paywall blocked | Expected â€” no fix needed, already tagged |

---

## License

Private â€” In On Africa (IOA). Not for public distribution.



