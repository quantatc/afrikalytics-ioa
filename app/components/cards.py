"""Article card renderer — card list replaces the raw dataframe."""

from __future__ import annotations

import html
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import streamlit as st

from ioa_core.countries import country_display_name

from .chips import chip_row, relevance_pill, sentiment_chip, status_chip


def _time_ago(value: Any) -> str:
    if not value:
        return ""
    try:
        if isinstance(value, str):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        elif isinstance(value, datetime):
            dt = value
        else:
            return ""
    except (TypeError, ValueError):
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    seconds = int(delta.total_seconds())
    if seconds < 60:
        return "just now"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h ago"
    days = hours // 24
    if days < 30:
        return f"{days}d ago"
    return dt.strftime("%Y-%m-%d")


def _status_class(tag_status: str | None) -> str:
    key = (tag_status or "").strip().lower()
    if key == "approved":
        return "approved"
    if key == "rejected":
        return "rejected"
    if key in ("pending", "needs_review"):
        return "pending"
    return "ai"


def render_card(row: dict) -> None:
    """Render a single article card via raw HTML."""
    headline = html.escape(str(row.get("headline") or "Untitled"))
    url = str(row.get("url") or "#")
    source = html.escape(str(row.get("source_name") or "Unknown"))
    country = html.escape(country_display_name(row.get("primary_country") or ""))
    sector = html.escape(str(row.get("primary_sector") or ""))
    summary = html.escape(str(row.get("summary") or "")[:320])
    if row.get("summary") and len(str(row["summary"])) > 320:
        summary += "…"

    when = _time_ago(row.get("published_at") or row.get("enriched_at"))
    rel = relevance_pill(row.get("relevance_score"))
    status = status_chip(row.get("tag_status"))
    sent = sentiment_chip(row.get("sentiment"))
    card_cls = _status_class(row.get("tag_status"))

    cluster_html = ""
    if row.get("cluster_id") and row.get("cluster_rank"):
        rank = int(row["cluster_rank"])
        if rank > 1:
            cluster_html = f'<span class="ioa-cluster">cluster · #{rank}</span>'

    themes = chip_row(row.get("themes") or [], "theme", limit=4)
    events = chip_row(row.get("event_types") or [], "event", limit=3)
    sectors = chip_row(row.get("sector_tags") or [], "sector", limit=4)

    html_block = (
        f'<div class="ioa-card {card_cls}">'
        f'<h4>{rel}<a href="{url}" target="_blank" rel="noopener">{headline}</a>{cluster_html}</h4>'
        f'<div class="ioa-meta"><strong>{source}</strong> &middot; {country} &middot; {sector} &middot; {when} &nbsp; {status} {sent}</div>'
        f'<div class="ioa-summary">{summary}</div>'
        f'<div>{themes}{events}{sectors}</div>'
        f'</div>'
    )
    st.markdown(html_block, unsafe_allow_html=True)


def render_card_list(df: pd.DataFrame, page_size: int = 25, key: str = "cards") -> None:
    if df.empty:
        st.info("No articles match the current filters.")
        return

    page_key = f"{key}_page"
    total = len(df)
    max_page = max(0, (total - 1) // page_size)
    current = int(st.session_state.get(page_key, 0))
    current = max(0, min(current, max_page))

    start = current * page_size
    end = min(start + page_size, total)
    slice_df = df.iloc[start:end]

    c1, c2, c3 = st.columns([0.2, 0.6, 0.2])
    with c1:
        if st.button("Previous", key=f"{key}_prev", disabled=current == 0, width="stretch"):
            st.session_state[page_key] = max(0, current - 1)
            st.rerun()
    with c2:
        st.markdown(
            (
                f"<div style='text-align:center;color:#9AA0A6;padding-top:6px;'>"
                f"Showing <strong>{start + 1}&ndash;{end}</strong> of <strong>{total:,}</strong></div>"
            ),
            unsafe_allow_html=True,
        )
    with c3:
        if st.button("Next", key=f"{key}_next", disabled=current >= max_page, width="stretch"):
            st.session_state[page_key] = min(max_page, current + 1)
            st.rerun()

    for _, row in slice_df.iterrows():
        render_card(row.to_dict())
