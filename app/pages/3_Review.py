"""Review — bulk approve + single-article tag editor with audit."""

from __future__ import annotations

# ruff: noqa: E402

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.components.chips import chip_row, relevance_pill, status_chip
from app.components.filters import apply_filters_to_df, current_filters, render_filter_bar
from app.state import bootstrap, load_articles
from ioa_core.countries import AFRICAN_COUNTRY_CODE_TO_NAME, country_display_name
from ioa_core.db import now_utc_iso
from ioa_core.repository import apply_tag_update, bulk_approve, save_tag_audit
from ioa_core.taxonomy import dimension_values, regions_for_countries, sector_parents, sector_values


ctx = bootstrap(page_subtitle="Review")
mode = ctx["mode"]
user = ctx["user"]
taxonomy = ctx["taxonomy"]
reviewer = user.get("display_name") or user.get("username") or os.getenv("USER") or "reviewer"

initial = current_filters()
articles = load_articles(mode, initial["days"], initial["search"])

source_options = sorted(articles["source_name"].dropna().unique().tolist()) if not articles.empty else []
status_options = sorted(articles["tag_status"].dropna().unique().tolist()) if not articles.empty else []

filters = render_filter_bar(taxonomy, mode, user, source_options, status_options)
if filters["days"] != initial["days"] or filters["search"] != initial["search"]:
    articles = load_articles(mode, filters["days"], filters["search"])

queue = apply_filters_to_df(articles, filters)
queue = queue[queue["tag_status"] != "approved"].sort_values(
    ["relevance_score", "enriched_at"], ascending=[False, False]
).reset_index(drop=True)

st.divider()

st.markdown("##### Queue")
m = st.columns(4)
with m[0]:
    st.metric("Pending", f"{len(queue):,}")
with m[1]:
    high = int((queue["relevance_score"].fillna(0) >= 4).sum()) if not queue.empty else 0
    st.metric("High rel (4+)", f"{high:,}")
with m[2]:
    approved = int((articles["tag_status"] == "approved").sum()) if not articles.empty else 0
    st.metric("Approved (window)", f"{approved:,}")
with m[3]:
    st.metric("You", reviewer)

if queue.empty:
    st.success("Nothing pending in the current filter set. All caught up.")
    st.stop()

bulk_c = st.columns([0.25, 0.2, 0.25, 0.3])
with bulk_c[0]:
    bulk_threshold = st.selectbox(
        "Bulk-approve relevance ≥",
        [5, 4, 3, 2, 1],
        index=1,
        key="_bulk_threshold",
    )
with bulk_c[1]:
    candidates = queue[queue["relevance_score"].fillna(0).astype(int) >= int(bulk_threshold)]
    st.metric("Matching", f"{len(candidates):,}")
with bulk_c[2]:
    if st.button(
        "Approve matching",
        type="primary",
        width="stretch",
        disabled=candidates.empty,
    ):
        ids = [int(v) for v in candidates["raw_id"].tolist()]
        n = bulk_approve(mode, ids, reviewer)
        for rid in ids:
            save_tag_audit(
                mode,
                int(rid),
                reviewer,
                before_val={"tag_status": "ai_generated"},
                after_val={"tag_status": "approved"},
                notes="bulk approve",
            )
        st.cache_data.clear()
        st.session_state[idx_key] = 0
        st.success(f"Approved {n} articles.")
        st.rerun()
with bulk_c[3]:
    st.caption("Bulk approve skips articles already in the approved state.")

st.divider()

idx_key = "_review_idx"
if idx_key not in st.session_state:
    st.session_state[idx_key] = 0
idx = min(max(0, int(st.session_state[idx_key])), len(queue) - 1)

nav = st.columns([0.15, 0.15, 0.55, 0.15])
with nav[0]:
    if st.button("◀ Prev", width="stretch", disabled=idx == 0):
        st.session_state[idx_key] = max(0, idx - 1)
        st.rerun()
with nav[1]:
    if st.button("Next ▶", width="stretch", disabled=idx >= len(queue) - 1):
        st.session_state[idx_key] = min(len(queue) - 1, idx + 1)
        st.rerun()
with nav[2]:
    st.markdown(
        f"<div style='text-align:center;color:#9AA0A6;padding-top:6px;'>"
        f"Reviewing <strong>{idx + 1} / {len(queue):,}</strong></div>",
        unsafe_allow_html=True,
    )
with nav[3]:
    if st.button("Skip", width="stretch"):
        st.session_state[idx_key] = min(len(queue) - 1, idx + 1)
        st.rerun()

row = queue.iloc[idx].to_dict()

_r_head = row.get("headline") or "Untitled"
_r_meta = " &middot; ".join(
    b for b in [
        row.get("source_name") or "Unknown",
        country_display_name(row.get("primary_country") or ""),
        str(row.get("published_at") or row.get("enriched_at") or ""),
    ] if b
)
_r_pills = f"{relevance_pill(row.get('relevance_score'))}{status_chip(row.get('tag_status'))}"
st.markdown(
    (
        '<div style="padding:12px 0;">'
        f'<div style="font-size:20px;font-weight:700;line-height:1.35;color:#F0F2F6;">{_r_head}</div>'
        f'<div style="color:#9AA0A6;font-size:12px;margin-top:4px;">{_r_meta}</div>'
        f'<div style="margin-top:8px;">{_r_pills}</div>'
        '</div>'
    ),
    unsafe_allow_html=True,
)

if row.get("url"):
    st.link_button("Open source ↗", row["url"])

with st.expander("Summary and AI tags", expanded=True):
    st.write(row.get("summary") or "")
    tag_rows = [
        ("Countries (AI)", row.get("countries") or [], "country"),
        ("Sectors (AI)", row.get("sector_tags") or [], "sector"),
        ("Themes (AI)", row.get("themes") or [], "theme"),
        ("Events (AI)", row.get("event_types") or [], "event"),
    ]
    for label, values, dim in tag_rows:
        if values:
            st.markdown(f"<div class='ioa-section-title'>{label}</div>{chip_row(values, dim)}", unsafe_allow_html=True)

country_options = ["PAN"] + sorted(AFRICAN_COUNTRY_CODE_TO_NAME)
sector_opts = sector_values(taxonomy)
theme_opts = dimension_values("themes", taxonomy)
event_opts = dimension_values("event_types", taxonomy)
entity_types = dimension_values("entity_types", taxonomy)
sentiment_opts = dimension_values("sentiment", taxonomy)
horizon_opts = dimension_values("time_horizon", taxonomy)


def _entities_text(entities: list[dict]) -> str:
    lines = []
    for e in entities or []:
        name = str(e.get("name") or "").strip()
        etype = str(e.get("type") or "Company").strip()
        if name:
            lines.append(f"{etype}: {name}")
    return "\n".join(lines)


def _parse_entities(text: str) -> list[dict[str, str]]:
    allowed = {v.lower(): v for v in entity_types}
    out = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if ":" in line:
            raw_type, name = line.split(":", 1)
            etype = allowed.get(raw_type.strip().lower(), "Company")
            name = name.strip()
        else:
            etype, name = "Company", line
        if name:
            out.append({"type": etype, "name": name[:160]})
    return out[:10]


with st.form(f"review_form_{row['raw_id']}"):
    countries = st.multiselect(
        "Countries",
        country_options,
        default=[c for c in (row.get("countries") or []) if c in country_options],
        format_func=country_display_name,
    )
    parent_sectors = sector_parents(taxonomy)
    primary_idx = parent_sectors.index(row.get("primary_sector")) if row.get("primary_sector") in parent_sectors else 0
    primary_sector = st.selectbox("Primary sector", parent_sectors, index=primary_idx)
    sector_tags = st.multiselect(
        "Sector tags",
        sector_opts,
        default=[s for s in (row.get("sector_tags") or []) if s in sector_opts],
    )
    themes = st.multiselect("Themes", theme_opts, default=[t for t in (row.get("themes") or []) if t in theme_opts])
    event_types = st.multiselect(
        "Event types", event_opts, default=[e for e in (row.get("event_types") or []) if e in event_opts]
    )
    sent_idx = sentiment_opts.index(row.get("sentiment")) if row.get("sentiment") in sentiment_opts else 0
    sentiment = st.selectbox("Sentiment", sentiment_opts, index=sent_idx)
    hor_idx = horizon_opts.index(row.get("time_horizon")) if row.get("time_horizon") in horizon_opts else 0
    time_horizon = st.selectbox("Time horizon", horizon_opts, index=hor_idx)
    entities_text = st.text_area(
        "Entities  (one per line: `Type: Name`)",
        value=_entities_text(row.get("entities") or []),
        height=120,
    )
    review_notes = st.text_area("Notes", value="", height=80)

    actions = st.columns([0.25, 0.25, 0.25, 0.25])
    with actions[0]:
        approve = st.form_submit_button("Approve", type="primary", width="stretch")
    with actions[1]:
        reject = st.form_submit_button("Reject", width="stretch")
    with actions[2]:
        save_only = st.form_submit_button("Save edits", width="stretch")
    with actions[3]:
        st.caption("Advance with ▶ Next above.")

    if approve or reject or save_only:
        countries_final = countries or ["PAN"]
        primary_country = countries_final[0]
        regions = regions_for_countries(countries_final)
        parsed_entities = _parse_entities(entities_text)

        before_val = {
            "primary_country": row.get("primary_country"),
            "countries": row.get("countries"),
            "regions": row.get("regions"),
            "primary_sector": row.get("primary_sector"),
            "sector_tags": row.get("sector_tags"),
            "themes": row.get("themes"),
            "event_types": row.get("event_types"),
            "entities": row.get("entities"),
            "sentiment": row.get("sentiment"),
            "time_horizon": row.get("time_horizon"),
            "tag_status": row.get("tag_status"),
        }
        after_val = {
            "primary_country": primary_country,
            "countries": countries_final,
            "regions": regions,
            "primary_sector": primary_sector,
            "sector_tags": sector_tags or [primary_sector],
            "themes": themes,
            "event_types": event_types,
            "entities": parsed_entities,
            "sentiment": sentiment,
            "time_horizon": time_horizon,
            "tag_status": "approved" if approve else ("rejected" if reject else row.get("tag_status") or "ai_generated"),
        }

        apply_tag_update(
            mode,
            int(row["raw_id"]),
            {
                **after_val,
                "reviewed_at": now_utc_iso(),
                "reviewer": reviewer,
                "review_notes": review_notes,
            },
        )
        save_tag_audit(
            mode,
            int(row["raw_id"]),
            reviewer,
            before_val=before_val,
            after_val=after_val,
            notes=review_notes,
        )
        st.cache_data.clear()
        st.success("Saved.")
        # Don't advance idx on approve/reject — the article leaves the queue,
        # so idx naturally points to the next pending article on rerun.
        if save_only:
            st.session_state[idx_key] = min(len(queue) - 1, idx + 1)
        st.rerun()
