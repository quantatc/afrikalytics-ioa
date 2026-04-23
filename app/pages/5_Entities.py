"""Entities — co-occurrence network view."""

from __future__ import annotations

# ruff: noqa: E402

import sys
from collections import Counter
from pathlib import Path

import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.components.filters import apply_filters_to_df, current_filters, render_filter_bar
from app.components.network import ENTITY_COLORS, render_network
from app.state import bootstrap, load_articles


ctx = bootstrap(page_subtitle="Entities")
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

st.divider()

controls = st.columns([0.3, 0.3, 0.4])
with controls[0]:
    top_n = st.slider("Max nodes", min_value=10, max_value=80, value=35, step=5)
with controls[1]:
    min_mentions = st.slider("Min mentions", min_value=1, max_value=10, value=2)
with controls[2]:
    legend_items = "".join(
        (
            "<span style='display:inline-flex;align-items:center;margin-right:14px;font-size:12px;color:#C9D1D9;'>"
            f"<span style='display:inline-block;width:10px;height:10px;border-radius:50%;background:{color};margin-right:5px;'></span>{label}"
            "</span>"
        )
        for label, color in ENTITY_COLORS.items()
    )
    st.markdown(
        "<div style='padding-top:16px;'>" + legend_items + "</div>",
        unsafe_allow_html=True,
    )

render_network(filtered, top_n=top_n, min_mentions=min_mentions, height=540)

st.divider()

st.markdown("##### Top entities")
entity_rows: list[dict] = []
for entities in filtered.get("entities", []):
    for e in entities or []:
        name = str(e.get("name") or "").strip()
        if not name:
            continue
        entity_rows.append({"name": name, "type": str(e.get("type") or "Company")})

if not entity_rows:
    st.caption("No entities in the current filter set.")
else:
    df = pd.DataFrame(entity_rows)
    counts = df["name"].value_counts().reset_index()
    counts.columns = ["name", "mentions"]
    types = df.drop_duplicates("name").set_index("name")["type"]
    counts["type"] = counts["name"].map(types)
    st.dataframe(counts.head(30), hide_index=True, width="stretch")
