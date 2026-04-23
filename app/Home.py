"""In(Sights) Tracker — Streamlit entrypoint (Home).

Functional pages live under app/pages/. This file renders the landing dashboard
(headline metrics, activity map, jump links) and hosts the auth gate.
"""

from __future__ import annotations

# ruff: noqa: E402

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.components.chips import chip_row
from app.components.map_view import render_africa_map
from app.components.timeline import render_timeline
from app.state import bootstrap, load_articles, load_raw_count


ctx = bootstrap(page_subtitle="Home")
mode = ctx["mode"]

articles = load_articles(mode, days=30, query="")
raw_count = load_raw_count(mode)

m1, m2, m3, m4 = st.columns(4)
with m1:
    st.metric("Enriched (30d)", f"{len(articles):,}")
with m2:
    st.metric("Raw collected", f"{raw_count:,}")
with m3:
    pending = 0
    if not articles.empty and "tag_status" in articles.columns:
        pending = int((articles["tag_status"] != "approved").sum())
    st.metric("Pending review", f"{pending:,}")
with m4:
    avg = float(articles["relevance_score"].fillna(0).mean()) if not articles.empty else 0.0
    st.metric("Avg relevance", f"{avg:.2f}")

st.divider()

left, right = st.columns([0.58, 0.42])
with left:
    st.markdown("##### Africa activity &middot; last 30 days")
    render_africa_map(articles, metric="articles", height=430)
with right:
    st.markdown("##### Volume over time")
    render_timeline(articles, date_col="published_at", height=220)

    st.markdown("##### Top themes")
    if not articles.empty and "themes" in articles.columns:
        counts = (
            pd.Series([t for ts in articles["themes"] for t in (ts or [])], dtype=str)
            .value_counts()
            .head(8)
        )
        if counts.empty:
            st.caption("No theme data yet.")
        else:
            st.markdown(
                "<div style='margin-top:8px;line-height:2;'>"
                + chip_row(list(counts.index), "theme")
                + "</div>",
                unsafe_allow_html=True,
            )
    else:
        st.caption("No articles yet.")

st.divider()

st.markdown("##### Jump in")
jump = st.columns(5)
links = [
    ("Explore", "Browse and filter enriched articles.", "pages/1_Explore.py"),
    ("Detail", "Deep-dive on a single article.", "pages/2_Detail.py"),
    ("Review", "Approve AI tags, bulk or single.", "pages/3_Review.py"),
    ("Briefs", "Generate and download briefings.", "pages/4_Briefs.py"),
    ("Entities", "Network view of co-mentioned entities.", "pages/5_Entities.py"),
]
for col, (label, desc, page) in zip(jump, links):
    with col:
        if st.button(label, key=f"jump_{label}", width="stretch", type="secondary"):
            try:
                st.switch_page(page)
            except Exception:
                st.info(f"Open {label} from the sidebar.")
        st.caption(desc)
