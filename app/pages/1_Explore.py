"""Explore — choropleth, cards, and filters."""

from __future__ import annotations

# ruff: noqa: E402

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.components.cards import render_card_list
from app.components.filters import apply_filters_to_df, current_filters, render_filter_bar
from app.components.map_view import render_africa_map
from app.components.timeline import render_sentiment_trend, render_timeline
from app.state import bootstrap, load_articles, load_raw_count


ctx = bootstrap(page_subtitle="Explore")
mode = ctx["mode"]
user = ctx["user"]
taxonomy = ctx["taxonomy"]

initial = current_filters()
articles = load_articles(mode, initial["days"], initial["search"])

source_options = sorted(articles["source_name"].dropna().unique().tolist()) if not articles.empty else []
status_options = sorted(articles["tag_status"].dropna().unique().tolist()) if not articles.empty else []

filters = render_filter_bar(taxonomy, mode, user, source_options, status_options)

if filters["days"] != initial["days"] or filters["search"] != initial["search"]:
    articles = load_articles(mode, filters["days"], filters["search"])

filtered = apply_filters_to_df(articles, filters)
raw_count = load_raw_count(mode)

st.divider()

m = st.columns(4)
with m[0]:
    st.metric("Filtered", f"{len(filtered):,}")
with m[1]:
    st.metric("In window", f"{len(articles):,}")
with m[2]:
    st.metric("Raw total", f"{raw_count:,}")
with m[3]:
    avg = float(filtered["relevance_score"].fillna(0).mean()) if not filtered.empty else 0.0
    st.metric("Avg relevance", f"{avg:.2f}")

map_col, chart_col = st.columns([0.58, 0.42])
with map_col:
    st.markdown("##### Coverage map")
    render_africa_map(filtered, metric="articles", height=430)
with chart_col:
    st.markdown("##### Volume")
    render_timeline(filtered, height=200)
    st.markdown("##### Sentiment mix")
    render_sentiment_trend(filtered, height=200)

st.divider()

header = st.columns([0.6, 0.2, 0.2])
with header[0]:
    st.markdown(f"##### Articles — {len(filtered):,} match")
with header[1]:
    if not filtered.empty:
        export = filtered.copy()
        for col in ("countries", "regions", "sector_tags", "themes", "event_types"):
            if col in export.columns:
                export[col] = export[col].apply(lambda v: ", ".join(v or []) if isinstance(v, list) else "")
        if "entities" in export.columns:
            export["entities"] = export["entities"].apply(
                lambda v: "; ".join(f"{e.get('type', '')}:{e.get('name', '')}" for e in (v or [])) if isinstance(v, list) else ""
            )
        cols_for_csv = [
            c for c in [
                "raw_id", "headline", "source_name", "primary_country", "primary_sector",
                "themes", "event_types", "sector_tags", "relevance_score", "sentiment",
                "tag_status", "published_at", "url",
            ] if c in export.columns
        ]
        csv = export[cols_for_csv].to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV",
            csv,
            file_name=f"ioa_articles_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
            width="stretch",
        )
with header[2]:
    if not filtered.empty:
        md_lines = [f"# IOA article list — {len(filtered):,} items", ""]
        for _, r in filtered.head(500).iterrows():
            md_lines.append(
                f"- **{r.get('headline', '')}** — {r.get('source_name', '')} ({r.get('primary_country', '')}) — <{r.get('url', '')}>"
            )
        st.download_button(
            "Download Markdown",
            "\n".join(md_lines).encode("utf-8"),
            file_name=f"ioa_articles_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M')}.md",
            mime="text/markdown",
            width="stretch",
        )

render_card_list(filtered, page_size=20, key="explore")
