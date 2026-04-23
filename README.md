# In(Sights) Tracker

Africa-focused intelligence collection, enrichment, exploration, and briefing generation for IOA.

The project ships as a Streamlit analyst workspace backed by SQLite for local development and Postgres for deployment.

```text
Collect -> Enrich -> Cluster -> Explore -> Review -> Brief
```

## Architecture

```text
layer1/            RSS, scraper, and GDELT collection
layer2/            OpenAI enrichment, taxonomy tagging, and duplicate clustering
layer3/            Evidence-based briefing synthesis
app/
  Home.py          Landing dashboard (entrypoint; also hosts auth gate)
  pages/           Explore, Detail, Review, Briefs, Entities, Health
  components/      Cards, chips, choropleth, filter bar, network, timeline
  state.py         Session bootstrap + cached data loaders
config/            Versioned tagging taxonomy
ioa_core/          Shared DB, repository, jobs, auth, taxonomy, countries
.streamlit/        Theme + server config
branding/          Logo assets
docs/              Deployment notes
```

Production storage is plain Postgres via `DATABASE_URL`. The Docker setup uses a pgvector-enabled Postgres container, which makes the Hetzner deployment easy to move later.

## Local Development

Local dev uses [layer1/ioa_dev.db](layer1/ioa_dev.db) automatically.

```bash
uv sync
uv run streamlit run app/Home.py
```

Collect locally:

```bash
uv run python layer1/collect.py --mode dev
```

Enrich locally:

```bash
uv run python layer2/enrich.py --mode dev --batch-size 50
```

Generate a local brief:

```bash
uv run python layer3/synthesise.py --mode dev --period-days 30 --min-relevance 3
```

## Docker Deployment

Copy `.env.example` to `.env` and set:

```bash
POSTGRES_PASSWORD=replace_with_a_strong_password
OPENAI_API_KEY=sk-...

# Optional: seeds the first admin account on first app launch.
IOA_ADMIN_USERNAME=admin
IOA_ADMIN_PASSWORD=change-me-now
IOA_ADMIN_DISPLAY=Lead Analyst
```

Start the app and Postgres:

```bash
docker compose up -d --build
```

Open:

```text
http://localhost:8501
```

Run pipeline jobs inside Docker:

```bash
docker compose run --rm app python layer1/collect.py --mode prod
docker compose run --rm app python layer2/enrich.py --mode prod --batch-size 100
docker compose run --rm app python layer3/synthesise.py --mode prod --period-days 30 --min-relevance 3
```

## Scheduled Jobs

Keep the Streamlit UI and database running continuously:

```bash
docker compose up -d app db
```

For weekly processing on a Hetzner host, edit cron:

```bash
crontab -e
```

Run collection, enrichment, and weekly brief generation every Monday:

```cron
# Collect Monday 06:00
0 6 * * 1 cd /opt/insights-tracker && docker compose run --rm app python layer1/collect.py --mode prod >> /var/log/insights-tracker-collect.log 2>&1

# Enrich Monday 06:30
30 6 * * 1 cd /opt/insights-tracker && docker compose run --rm app python layer2/enrich.py --mode prod --batch-size 100 --drain >> /var/log/insights-tracker-enrich.log 2>&1

# Generate weekly brief Monday 08:00
0 8 * * 1 cd /opt/insights-tracker && docker compose run --rm app python layer3/synthesise.py --mode prod --period-days 7 --min-relevance 3 --max-articles 120 >> /var/log/insights-tracker-brief.log 2>&1
```

Recommended operating pattern: collect and enrich daily, then generate the brief weekly. This avoids a large Monday backlog and keeps the UI fresh through the week:

```cron
# Collect daily 06:00
0 6 * * * cd /opt/insights-tracker && docker compose run --rm app python layer1/collect.py --mode prod >> /var/log/insights-tracker-collect.log 2>&1

# Enrich daily 06:30
30 6 * * * cd /opt/insights-tracker && docker compose run --rm app python layer2/enrich.py --mode prod --batch-size 100 >> /var/log/insights-tracker-enrich.log 2>&1

# Generate weekly brief Monday 08:00
0 8 * * 1 cd /opt/insights-tracker && docker compose run --rm app python layer3/synthesise.py --mode prod --period-days 7 --min-relevance 3 --max-articles 120 >> /var/log/insights-tracker-brief.log 2>&1
```

For Hetzner-specific setup, see [docs/DEPLOY_HETZNER.md](docs/DEPLOY_HETZNER.md).

## Tagging Model

The taxonomy lives in [config/taxonomy.yaml](config/taxonomy.yaml). Layer 2 tags each article across multiple dimensions:

- Geography: primary country, country list, region list
- Sector: primary sector plus sector tags
- Theme: policy, capital flows, macroeconomics, ESG, trade, supply chains, and related topics
- Event type: announcements, policy changes, investments, project launches, M&A, crises, litigation, and related events
- Entities: companies, government bodies, multilaterals, and key individuals
- Sentiment and time horizon

Section 7 advanced layers from the tagging proposal are intentionally excluded for now: risk categories, strategic signals, and project development stage.

## Production Database

If you are not using Docker Compose, create a Postgres database and run:

```bash
psql "$DATABASE_URL" -f layer1/schema.sql
```

Example external database URL:

```bash
DATABASE_URL=postgresql://insights_tracker:strong_password@127.0.0.1:5432/insights_tracker
```

## Streamlit UI

Multi-page analyst workspace with IOA theming, password auth, and persistent session.

- **Home** — Africa choropleth, volume timeline, top themes, quick links.
- **Explore** — choropleth + card layout, horizontal filter bar with saved views,
  CSV + Markdown export, per-dimension colored tag chips, sentiment trend.
- **Detail** — single-article deep dive with timeline context for the same
  country + sector, related cluster siblings, full edit history (diff view).
- **Review** — bulk approve by relevance threshold, single-article editor with
  prev / next navigation, approve / reject / save-edits actions, full audit log.
- **Briefs** — non-blocking brief generation (background worker + DB-backed job
  queue), brief library with Markdown + HTML download, delete.
- **Entities** — co-occurrence network graph of companies, government bodies,
  multilaterals, and key individuals; top-entities leaderboard.
- **Health** — error-rate bar chart per source, silent-source (>48h) list,
  latest-run table.

Authentication is a minimal password gate backed by bcrypt hashes in
`app_users`. Set `IOA_ADMIN_USERNAME` / `IOA_ADMIN_PASSWORD` in `.env` before
first launch; the app seeds an admin account automatically (defaults to
`admin` / `ioa-admin` — change immediately in any real deployment).

### Duplicate clustering

Layer 2b groups near-duplicate coverage (same story from multiple wires) so the
UI can flag siblings. Run alongside enrichment:

```bash
uv run python layer2/cluster.py --mode dev --window-days 14
uv run python layer2/cluster.py --mode prod --window-days 7 --threshold 0.82
```

Each enriched article gets a `cluster_id` and `cluster_rank` (1 = canonical).
The Detail page surfaces sibling articles; card badges show the rank.

## Adding Sources

Edit [layer1/sources.yaml](layer1/sources.yaml), or use the CSV migration flow:

```bash
cd layer1
uv run python migrate_sources.py --dry-run
uv run python migrate_sources.py
```

Country-specific sources should include ISO-2 country codes in `countries`; these become Layer 2 country hints.

## License

Private - In On Africa (IOA). Not for public distribution.
