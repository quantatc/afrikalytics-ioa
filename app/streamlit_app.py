from __future__ import annotations

# ruff: noqa: E402

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ioa_core.countries import AFRICAN_COUNTRY_CODE_TO_NAME, country_display_name
from ioa_core.db import connect_db, json_dumps, json_loads, now_utc_iso
from ioa_core.env import load_repo_env
from ioa_core.taxonomy import dimension_values, load_taxonomy, regions_for_countries, sector_parents, sector_values


load_repo_env()

st.set_page_config(
    page_title="In(Sights) Tracker",
    layout="wide",
    initial_sidebar_state="expanded",
)


JSON_FIELDS = ("countries", "regions", "sector_tags", "themes", "event_types", "entities")


def default_mode() -> str:
    return "prod" if os.getenv("DATABASE_URL") else "dev"


def get_connection(mode: str):
    return connect_db(mode)


def query_rows(mode: str, sqlite_sql: str, pg_sql: str, sqlite_params=(), pg_params=()) -> list[dict]:
    conn, db_type = get_connection(mode)
    if db_type == "sqlite":
        rows = conn.execute(sqlite_sql, sqlite_params).fetchall()
        return [dict(r) for r in rows]
    rows = conn.execute(pg_sql, pg_params).fetchall()
    return [dict(r) for r in rows]


@st.cache_data(ttl=60)
def fetch_articles(mode: str, days: int) -> pd.DataFrame:
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff = cutoff_dt.isoformat()
    rows = query_rows(
        mode,
        """
        SELECT
            e.raw_id, e.taxonomy_version, e.primary_country, e.countries, e.regions,
            e.primary_sector, e.sector_tags, e.themes, e.event_types, e.entities,
            e.sentiment, e.time_horizon, e.relevance_score, e.relevance_reason,
            e.summary, e.tag_status, e.enriched_at, e.reviewed_at, e.reviewer,
            r.headline, r.url, r.source_name, r.source_tier, r.published_at, r.scraped_at
        FROM enriched_articles e
        JOIN raw_articles r ON r.id = e.raw_id
        WHERE e.enriched_at >= ?
        ORDER BY e.enriched_at DESC
        """,
        """
        SELECT
            e.raw_id, e.taxonomy_version, e.primary_country, e.countries, e.regions,
            e.primary_sector, e.sector_tags, e.themes, e.event_types, e.entities,
            e.sentiment, e.time_horizon, e.relevance_score, e.relevance_reason,
            e.summary, e.tag_status, e.enriched_at, e.reviewed_at, e.reviewer,
            r.headline, r.url, r.source_name, r.source_tier, r.published_at, r.scraped_at
        FROM enriched_articles e
        JOIN raw_articles r ON r.id = e.raw_id
        WHERE e.enriched_at >= %s
        ORDER BY e.enriched_at DESC
        """,
        (cutoff,),
        (cutoff_dt,),
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for field in JSON_FIELDS:
        df[field] = df[field].apply(json_loads)
    df["country_label"] = df["primary_country"].apply(country_display_name)
    df["headline"] = df["headline"].fillna("")
    df["source_name"] = df["source_name"].fillna("Unknown")
    return df


@st.cache_data(ttl=60)
def fetch_raw_count(mode: str) -> int:
    rows = query_rows(
        mode,
        "SELECT COUNT(*) AS count FROM raw_articles",
        "SELECT COUNT(*) AS count FROM raw_articles",
    )
    return int(rows[0]["count"]) if rows else 0


@st.cache_data(ttl=60)
def fetch_raw_articles(mode: str, days: int) -> pd.DataFrame:
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff = cutoff_dt.isoformat()
    rows = query_rows(
        mode,
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
        (cutoff,),
        (cutoff_dt,),
    )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["headline"] = df["headline"].fillna("")
        df["source_name"] = df["source_name"].fillna("Unknown")
    return df


@st.cache_data(ttl=60)
def fetch_health(mode: str) -> pd.DataFrame:
    rows = query_rows(
        mode,
        """
        SELECT source_name, source_tier, source_url, run_at, articles_fetched,
               articles_inserted, articles_duped, had_error, error_msg
        FROM source_health
        ORDER BY run_at DESC
        LIMIT 500
        """,
        """
        SELECT source_name, source_tier, source_url, run_at, articles_fetched,
               articles_inserted, articles_duped, had_error, error_msg
        FROM source_health
        ORDER BY run_at DESC
        LIMIT 500
        """,
    )
    return pd.DataFrame(rows)


def render_health(mode: str) -> None:
    health = fetch_health(mode)
    if health.empty:
        st.info("No source health rows have been recorded yet.")
    else:
        h1, h2, h3 = st.columns(3)
        h1.metric("Health rows", f"{len(health):,}")
        h2.metric("Sources", f"{health['source_name'].nunique():,}")
        h3.metric("Errors", f"{int(health['had_error'].fillna(0).astype(int).sum()):,}")
        st.dataframe(health, hide_index=True, width="stretch")


def list_options(df: pd.DataFrame, field: str) -> list[str]:
    if df.empty or field not in df:
        return []
    values: set[str] = set()
    for items in df[field]:
        for item in items or []:
            values.add(str(item))
    return sorted(values)


def contains_any(items: Any, selected: list[str]) -> bool:
    if not selected:
        return True
    return bool(set(str(x) for x in (items or [])) & set(selected))


def filter_list_column(df: pd.DataFrame, column: str, selected: list[str]) -> pd.DataFrame:
    if not selected or df.empty or column not in df.columns:
        return df
    mask = df[column].apply(lambda x: contains_any(x, selected)).astype(bool)
    return df.loc[mask].copy()


def filter_value_column(df: pd.DataFrame, column: str, selected: list[str]) -> pd.DataFrame:
    if not selected or df.empty or column not in df.columns:
        return df
    return df.loc[df[column].isin(selected)].copy()


def filter_articles(
    df: pd.DataFrame,
    min_relevance: int,
    countries: list[str],
    regions: list[str],
    sectors: list[str],
    themes: list[str],
    event_types: list[str],
    sources: list[str],
    statuses: list[str],
    query: str,
) -> pd.DataFrame:
    if df.empty:
        return df
    filtered = df[df["relevance_score"].fillna(0).astype(int) >= min_relevance].copy()
    filtered = filter_list_column(filtered, "countries", countries)
    filtered = filter_list_column(filtered, "regions", regions)
    filtered = filter_list_column(filtered, "sector_tags", sectors)
    filtered = filter_list_column(filtered, "themes", themes)
    filtered = filter_list_column(filtered, "event_types", event_types)
    filtered = filter_value_column(filtered, "source_name", sources)
    filtered = filter_value_column(filtered, "tag_status", statuses)
    if query.strip() and not filtered.empty:
        q = query.strip().lower()
        mask = filtered.apply(
                lambda r: q in f"{r.get('headline', '')} {r.get('summary', '')} {r.get('source_name', '')}".lower(),
                axis=1,
        ).astype(bool)
        filtered = filtered.loc[mask].copy()
    return filtered


def join_list(values: Any) -> str:
    if isinstance(values, list):
        return ", ".join(str(v) for v in values)
    return ""


def render_count_chart(values: pd.Series, empty_label: str) -> None:
    counts = values.value_counts().head(10) if not values.empty else pd.Series(dtype=int)
    if counts.empty:
        st.caption(empty_label)
        return
    st.bar_chart(counts, width="stretch")


def flatten_counts(df: pd.DataFrame, column: str) -> pd.Series:
    if df.empty or column not in df.columns:
        return pd.Series(dtype=str)
    return pd.Series([item for values in df[column] for item in (values or [])], dtype=str)


def entities_to_text(entities: list[dict]) -> str:
    lines = []
    for entity in entities or []:
        name = str(entity.get("name") or "").strip()
        etype = str(entity.get("type") or "Company").strip()
        if name:
            lines.append(f"{etype}: {name}")
    return "\n".join(lines)


def parse_entities(text: str, allowed_types: list[str]) -> list[dict[str, str]]:
    allowed_lookup = {v.lower(): v for v in allowed_types}
    entities = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if ":" in line:
            raw_type, name = line.split(":", 1)
            etype = allowed_lookup.get(raw_type.strip().lower(), "Company")
            name = name.strip()
        else:
            etype = "Company"
            name = line
        if name:
            entities.append({"type": etype, "name": name[:160]})
    return entities[:10]


def update_tags(mode: str, raw_id: int, values: dict[str, Any]) -> None:
    conn, db_type = get_connection(mode)
    reviewed_at = now_utc_iso()
    regions = regions_for_countries(values["countries"])
    values = {
        **values,
        "regions": regions,
        "primary_country": values["countries"][0] if values["countries"] else "PAN",
        "reviewed_at": reviewed_at,
        "country": values["countries"][0] if values["countries"] else "PAN",
        "sector": values["primary_sector"],
    }

    if db_type == "sqlite":
        conn.execute(
            """
            UPDATE enriched_articles
            SET primary_country = ?, countries = ?, regions = ?, primary_sector = ?,
                sector_tags = ?, themes = ?, event_types = ?, entities = ?,
                sentiment = ?, time_horizon = ?, tag_status = 'approved',
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
                values["reviewed_at"],
                values["reviewer"],
                values["review_notes"],
                values["country"],
                values["sector"],
                raw_id,
            ),
        )
    else:
        from ioa_core.db import json_for_db

        conn.execute(
            """
            UPDATE enriched_articles
            SET primary_country = %s, countries = %s, regions = %s, primary_sector = %s,
                sector_tags = %s, themes = %s, event_types = %s, entities = %s,
                sentiment = %s, time_horizon = %s, tag_status = 'approved',
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
                values["reviewed_at"],
                values["reviewer"],
                values["review_notes"],
                values["country"],
                values["sector"],
                raw_id,
            ),
        )
    conn.commit()
    fetch_articles.clear()


def run_report(
    mode: str,
    period_days: int,
    min_relevance: int,
    max_articles: int,
    country: str,
    region: str,
    sector: str,
    theme: str,
    event_type: str,
) -> subprocess.CompletedProcess:
    cmd = [
        sys.executable,
        str(REPO_ROOT / "layer3" / "synthesise.py"),
        "--mode",
        mode,
        "--period-days",
        str(period_days),
        "--min-relevance",
        str(min_relevance),
        "--max-articles",
        str(max_articles),
    ]
    for flag, value in (
        ("--country", country),
        ("--region", region),
        ("--sector", sector),
        ("--theme", theme),
        ("--event-type", event_type),
    ):
        if value:
            cmd.extend([flag, value])
    return subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=240)


taxonomy = load_taxonomy()
mode = st.sidebar.radio("Database", ["dev", "prod"], index=0 if default_mode() == "dev" else 1)
days = st.sidebar.slider("Window", min_value=1, max_value=180, value=30, step=1)

raw_count = fetch_raw_count(mode)
articles = fetch_articles(mode, days)

st.title("In(Sights) Tracker")

if articles.empty:
    raw_articles = fetch_raw_articles(mode, days)
    st.info(f"No enriched articles in the last {days} days. Raw articles available: {raw_count}.")
    raw_tab, health_tab = st.tabs(["Raw", "Health"])
    with raw_tab:
        if raw_articles.empty:
            st.info("No raw articles found in this window.")
        else:
            r1, r2, r3 = st.columns(3)
            r1.metric("Raw in window", f"{len(raw_articles):,}")
            r2.metric("Sources", f"{raw_articles['source_name'].nunique():,}")
            r3.metric("Unprocessed", f"{raw_articles['processed_at'].isna().sum():,}")
            raw_query = st.text_input("Raw search")
            raw_filtered = raw_articles
            if raw_query.strip():
                q = raw_query.strip().lower()
                raw_filtered = raw_articles[
                    raw_articles.apply(
                        lambda r: q in f"{r.get('headline', '')} {r.get('source_name', '')}".lower(),
                        axis=1,
                    )
                ]
            st.dataframe(
                raw_filtered[
                    [
                        "id",
                        "headline",
                        "source_name",
                        "source_tier",
                        "published_at",
                        "scraped_at",
                        "processed_at",
                        "url",
                    ]
                ],
                hide_index=True,
                width="stretch",
                column_config={"url": st.column_config.LinkColumn("URL")},
            )
    with health_tab:
        render_health(mode)
    st.stop()

country_options = ["PAN"] + sorted(AFRICAN_COUNTRY_CODE_TO_NAME)
region_options = dimension_values("regions", taxonomy)
sector_options = sector_values(taxonomy)
theme_options = dimension_values("themes", taxonomy)
event_options = dimension_values("event_types", taxonomy)
source_options = sorted(articles["source_name"].dropna().unique().tolist())
status_options = sorted(articles["tag_status"].dropna().unique().tolist())

with st.sidebar:
    min_relevance = st.slider("Relevance", min_value=1, max_value=5, value=3)
    selected_countries = st.multiselect("Countries", country_options, format_func=country_display_name)
    selected_regions = st.multiselect("Regions", region_options)
    selected_sectors = st.multiselect("Sectors", sector_options)
    selected_themes = st.multiselect("Themes", theme_options)
    selected_events = st.multiselect("Events", event_options)
    selected_sources = st.multiselect("Sources", source_options)
    selected_statuses = st.multiselect("Review", status_options)
    search = st.text_input("Search")
    if st.button("Refresh"):
        fetch_articles.clear()
        fetch_health.clear()
        st.rerun()

filtered = filter_articles(
    articles,
    min_relevance=min_relevance,
    countries=selected_countries,
    regions=selected_regions,
    sectors=selected_sectors,
    themes=selected_themes,
    event_types=selected_events,
    sources=selected_sources,
    statuses=selected_statuses,
    query=search,
)

explore_tab, detail_tab, review_tab, reports_tab, health_tab = st.tabs(
    ["Explore", "Detail", "Review", "Briefs", "Health"]
)

with explore_tab:
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Filtered", f"{len(filtered):,}")
    m2.metric("Enriched", f"{len(articles):,}")
    m3.metric("Raw", f"{raw_count:,}")
    avg = filtered["relevance_score"].mean() if not filtered.empty else 0
    m4.metric("Avg relevance", f"{avg:.2f}")

    c1, c2, c3 = st.columns(3)
    with c1:
        render_count_chart(flatten_counts(filtered, "sector_tags"), "No sector matches.")
    with c2:
        render_count_chart(flatten_counts(filtered, "themes"), "No theme matches.")
    with c3:
        country_values = filtered["primary_country"] if "primary_country" in filtered.columns else pd.Series(dtype=str)
        render_count_chart(country_values, "No country matches.")

    if filtered.empty:
        st.info("No enriched articles match the current filters.")
    else:
        table = filtered.copy()
        for field in ("countries", "regions", "sector_tags", "themes", "event_types"):
            table[field] = table[field].apply(join_list)
        st.dataframe(
            table[
                [
                    "raw_id",
                    "headline",
                    "source_name",
                    "primary_country",
                    "primary_sector",
                    "themes",
                    "event_types",
                    "relevance_score",
                    "tag_status",
                    "url",
                ]
            ],
            hide_index=True,
            width="stretch",
            column_config={"url": st.column_config.LinkColumn("URL")},
        )

with detail_tab:
    choices = filtered.sort_values(["relevance_score", "enriched_at"], ascending=[False, False])
    if choices.empty:
        st.info("No article is available for the current filters.")
    else:
        headline_by_id = choices.drop_duplicates("raw_id").set_index("raw_id")["headline"].to_dict()
        raw_id = st.selectbox(
            "Article",
            choices["raw_id"].tolist(),
            format_func=lambda rid: str(headline_by_id.get(rid, "Untitled"))[:120],
        )
        row_matches = choices.loc[choices["raw_id"] == raw_id]
        if row_matches.empty:
            st.info("The selected article is no longer in the filtered set.")
        else:
            row = row_matches.iloc[0].to_dict()
            st.subheader(row.get("headline") or "Untitled")
            st.link_button("Open source", row.get("url") or "")
            d1, d2, d3, d4 = st.columns(4)
            d1.metric("Source", row.get("source_name") or "Unknown")
            d2.metric("Country", row.get("primary_country") or "")
            d3.metric("Sector", row.get("primary_sector") or "")
            d4.metric("Relevance", int(row.get("relevance_score") or 0))
            st.write(row.get("summary") or "")
            st.markdown("**Tags**")
            st.write(
                {
                    "countries": row.get("countries"),
                    "regions": row.get("regions"),
                    "sectors": row.get("sector_tags"),
                    "themes": row.get("themes"),
                    "events": row.get("event_types"),
                    "sentiment": row.get("sentiment"),
                    "time_horizon": row.get("time_horizon"),
                }
            )
            st.markdown("**Entities**")
            st.dataframe(pd.DataFrame(row.get("entities") or []), hide_index=True, width="stretch")

with review_tab:
    review_choices = filtered.sort_values(["tag_status", "enriched_at"], ascending=[True, False])
    if review_choices.empty:
        st.info("No article is available for review under the current filters.")
    else:
        review_headline_by_id = review_choices.drop_duplicates("raw_id").set_index("raw_id")["headline"].to_dict()
        review_raw_id = st.selectbox(
            "Review article",
            review_choices["raw_id"].tolist(),
            format_func=lambda rid: str(review_headline_by_id.get(rid, "Untitled"))[:120],
            key="review_raw_id",
        )
        row_matches = review_choices.loc[review_choices["raw_id"] == review_raw_id]
        if row_matches.empty:
            st.info("The selected review article is no longer in the filtered set.")
        else:
            row = row_matches.iloc[0].to_dict()
            st.subheader(row.get("headline") or "Untitled")
            st.caption(row.get("url") or "")
            with st.form("tag_review"):
                countries = st.multiselect(
                    "Countries",
                    country_options,
                    default=[c for c in row.get("countries", []) if c in country_options],
                    format_func=country_display_name,
                )
                primary_sector = st.selectbox(
                    "Primary sector",
                    sector_parents(taxonomy),
                    index=sector_parents(taxonomy).index(row.get("primary_sector"))
                    if row.get("primary_sector") in sector_parents(taxonomy)
                    else 0,
                )
                sector_tags = st.multiselect(
                    "Sector tags",
                    sector_options,
                    default=[s for s in row.get("sector_tags", []) if s in sector_options],
                )
                themes = st.multiselect(
                    "Themes",
                    theme_options,
                    default=[s for s in row.get("themes", []) if s in theme_options],
                )
                event_types = st.multiselect(
                    "Event types",
                    event_options,
                    default=[s for s in row.get("event_types", []) if s in event_options],
                )
                sentiment = st.selectbox(
                    "Sentiment",
                    dimension_values("sentiment", taxonomy),
                    index=dimension_values("sentiment", taxonomy).index(row.get("sentiment"))
                    if row.get("sentiment") in dimension_values("sentiment", taxonomy)
                    else 0,
                )
                time_horizon = st.selectbox(
                    "Time horizon",
                    dimension_values("time_horizon", taxonomy),
                    index=dimension_values("time_horizon", taxonomy).index(row.get("time_horizon"))
                    if row.get("time_horizon") in dimension_values("time_horizon", taxonomy)
                    else 0,
                )
                entities_text = st.text_area("Entities", value=entities_to_text(row.get("entities") or []), height=150)
                reviewer = st.text_input("Reviewer", value=os.getenv("USER") or os.getenv("USERNAME") or "")
                review_notes = st.text_area("Notes", value="")
                submitted = st.form_submit_button("Approve tags")
                if submitted:
                    update_tags(
                        mode,
                        int(review_raw_id),
                        {
                            "countries": countries or ["PAN"],
                            "primary_sector": primary_sector,
                            "sector_tags": sector_tags or [primary_sector],
                            "themes": themes,
                            "event_types": event_types,
                            "entities": parse_entities(entities_text, dimension_values("entity_types", taxonomy)),
                            "sentiment": sentiment,
                            "time_horizon": time_horizon,
                            "reviewer": reviewer,
                            "review_notes": review_notes,
                        },
                    )
                    st.success("Tags approved.")
                    st.rerun()

with reports_tab:
    with st.form("brief_builder"):
        r1, r2, r3 = st.columns(3)
        period_days = r1.number_input("Days", min_value=1, max_value=180, value=30)
        min_rel = r2.number_input("Min relevance", min_value=1, max_value=5, value=3)
        max_articles = r3.number_input("Max articles", min_value=20, max_value=300, value=120)
        country = st.selectbox("Country", [""] + country_options, format_func=lambda x: "All" if not x else country_display_name(x))
        region = st.selectbox("Region", [""] + region_options, format_func=lambda x: "All" if not x else x)
        sector = st.selectbox("Sector", [""] + sector_options, format_func=lambda x: "All" if not x else x)
        theme = st.selectbox("Theme", [""] + theme_options, format_func=lambda x: "All" if not x else x)
        event_type = st.selectbox("Event", [""] + event_options, format_func=lambda x: "All" if not x else x)
        generate = st.form_submit_button("Generate brief")

    if generate:
        if not os.getenv("OPENAI_API_KEY"):
            st.error("OPENAI_API_KEY is required to generate a brief.")
        else:
            with st.spinner("Generating brief"):
                result = run_report(
                    mode,
                    int(period_days),
                    int(min_rel),
                    int(max_articles),
                    country,
                    region,
                    sector,
                    theme,
                    event_type,
                )
            if result.returncode == 0:
                st.success("Brief generated.")
                st.code(result.stdout or result.stderr)
            else:
                st.error("Brief generation failed.")
                st.code(result.stderr or result.stdout)

with health_tab:
    render_health(mode)
