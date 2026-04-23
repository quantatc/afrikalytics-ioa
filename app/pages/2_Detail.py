"""Detail — full view of a single article with timeline context."""

from __future__ import annotations

# ruff: noqa: E402

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.components.chips import chip, chip_row, relevance_pill, sentiment_chip, status_chip
from app.components.filters import apply_filters_to_df, current_filters, render_filter_bar
from app.components.timeline import render_timeline
from app.state import bootstrap, load_articles
from ioa_core.countries import country_display_name
from ioa_core.repository import fetch_tag_audit


ctx = bootstrap(page_subtitle="Detail")
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

filtered = apply_filters_to_df(articles, filters).sort_values(
    ["relevance_score", "enriched_at"], ascending=[False, False]
)

st.divider()

if filtered.empty:
    st.info("No articles match the current filters.")
    st.stop()

headline_by_id = filtered.drop_duplicates("raw_id").set_index("raw_id")["headline"].to_dict()
default_id = st.session_state.get("detail_raw_id") or filtered["raw_id"].iloc[0]
raw_id = st.selectbox(
    "Article",
    filtered["raw_id"].tolist(),
    format_func=lambda rid: f"REL {int(filtered.loc[filtered['raw_id'] == rid, 'relevance_score'].iloc[0] or 0)} · {str(headline_by_id.get(rid, 'Untitled'))[:140]}",
    index=filtered["raw_id"].tolist().index(default_id) if default_id in filtered["raw_id"].tolist() else 0,
    key="_detail_selectbox",
)
st.session_state["detail_raw_id"] = raw_id

row_df = filtered.loc[filtered["raw_id"] == raw_id]
if row_df.empty:
    st.info("Selected article is no longer in the filtered set.")
    st.stop()

row = row_df.iloc[0].to_dict()

_headline = row.get("headline") or "Untitled"
_meta_bits = [
    row.get("source_name") or "Unknown",
    country_display_name(row.get("primary_country") or ""),
    row.get("primary_sector") or "",
    str(row.get("published_at") or row.get("enriched_at") or ""),
]
_meta = " &middot; ".join(b for b in _meta_bits if b)
st.markdown(
    (
        '<div style="padding:18px 0 6px 0;">'
        f'<div style="font-size:24px;font-weight:700;color:#F0F2F6;line-height:1.3;">{_headline}</div>'
        f'<div style="color:#9AA0A6;font-size:13px;margin-top:6px;">{_meta}</div>'
        '</div>'
    ),
    unsafe_allow_html=True,
)

meta_row = st.columns([0.55, 0.45])
with meta_row[0]:
    st.markdown(
        relevance_pill(row.get("relevance_score"))
        + status_chip(row.get("tag_status"))
        + sentiment_chip(row.get("sentiment"))
        + (chip(row.get("time_horizon"), "theme") if row.get("time_horizon") else ""),
        unsafe_allow_html=True,
    )
with meta_row[1]:
    if row.get("url"):
        st.link_button("Open source ↗", row["url"], width="stretch")

st.divider()

body = st.columns([0.62, 0.38])
with body[0]:
    st.markdown("##### Summary")
    st.write(row.get("summary") or "No summary available.")

    if row.get("relevance_reason"):
        st.markdown("##### Why it's relevant")
        st.caption(row["relevance_reason"])

    st.markdown("##### Tags")
    tag_rows = [
        ("Countries", row.get("countries") or [], "country"),
        ("Regions", row.get("regions") or [], "theme"),
        ("Sectors", row.get("sector_tags") or [], "sector"),
        ("Themes", row.get("themes") or [], "theme"),
        ("Events", row.get("event_types") or [], "event"),
    ]
    for label, values, dim in tag_rows:
        if values:
            st.markdown(
                f"<div class='ioa-section-title'>{label}</div>{chip_row(values, dim)}",
                unsafe_allow_html=True,
            )

    entities = row.get("entities") or []
    if entities:
        st.markdown("##### Entities")
        ent_df = pd.DataFrame(entities)
        st.dataframe(ent_df, hide_index=True, width="stretch")

    audit = fetch_tag_audit(mode, int(raw_id))
    if audit:
        with st.expander(f"Edit history ({len(audit)} revisions)", expanded=False):
            for a in audit:
                st.markdown(
                    f"**{a['created_at']}** · {a.get('reviewer') or '—'}"
                )
                before = a.get("before_val") or {}
                after = a.get("after_val") or {}
                changes = []
                for k in sorted(set(before) | set(after)):
                    bv, av = before.get(k), after.get(k)
                    if bv != av:
                        changes.append(f"- **{k}**: `{bv}` → `{av}`")
                if changes:
                    st.markdown("\n".join(changes))
                if a.get("notes"):
                    st.caption(f"Notes: {a['notes']}")
                st.divider()

with body[1]:
    st.markdown("##### Volume in this country + sector")
    country = row.get("primary_country")
    sector = row.get("primary_sector")
    ctx_df = articles.copy()
    if country and "primary_country" in ctx_df.columns:
        ctx_df = ctx_df[ctx_df["primary_country"] == country]
    if sector and "primary_sector" in ctx_df.columns:
        ctx_df = ctx_df[ctx_df["primary_sector"] == sector]
    render_timeline(ctx_df, height=200)

    if row.get("cluster_id"):
        st.markdown("##### Related (same cluster)")
        related = articles[articles["cluster_id"] == row["cluster_id"]]
        related = related[related["raw_id"] != raw_id].head(6)
        if related.empty:
            st.caption("No related articles in this window.")
        else:
            for _, r in related.iterrows():
                st.markdown(
                    f"- **[{r['headline']}]({r['url']})** — {r['source_name']}"
                )
