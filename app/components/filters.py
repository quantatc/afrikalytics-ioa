"""Horizontal filter bar with saved-views support.

Replaces the sidebar full of multiselects. Filters live in session state so
all pages share the same selection.
"""

from __future__ import annotations

from typing import Any

import streamlit as st

from ioa_core.countries import AFRICAN_COUNTRY_CODE_TO_NAME, country_display_name
from ioa_core.repository import delete_view, list_views, save_view
from ioa_core.taxonomy import dimension_values, sector_values


FILTER_DEFAULTS: dict[str, Any] = {
    "days": 30,
    "min_relevance": 3,
    "countries": [],
    "regions": [],
    "sectors": [],
    "themes": [],
    "events": [],
    "sources": [],
    "statuses": [],
    "search": "",
}


def ensure_filter_state() -> None:
    # Handle any pending reset / apply-view from a prior run. These must be
    # applied BEFORE widgets are instantiated — Streamlit disallows mutating
    # st.session_state[key] after the widget with that key has rendered.
    if st.session_state.pop("_flt_pending_reset", False):
        for key, default in FILTER_DEFAULTS.items():
            st.session_state[f"flt_{key}"] = default

    pending = st.session_state.pop("_flt_pending_apply", None)
    if isinstance(pending, dict):
        for key, default in FILTER_DEFAULTS.items():
            st.session_state[f"flt_{key}"] = pending.get(key, default)

    for key, default in FILTER_DEFAULTS.items():
        state_key = f"flt_{key}"
        if state_key not in st.session_state:
            st.session_state[state_key] = default


def current_filters() -> dict[str, Any]:
    ensure_filter_state()
    return {key: st.session_state.get(f"flt_{key}", default) for key, default in FILTER_DEFAULTS.items()}


def apply_filter_dict(values: dict[str, Any]) -> None:
    for key, default in FILTER_DEFAULTS.items():
        st.session_state[f"flt_{key}"] = values.get(key, default)


def reset_filters() -> None:
    apply_filter_dict(FILTER_DEFAULTS)


def render_filter_bar(
    taxonomy: dict[str, Any],
    mode: str,
    user: dict[str, Any] | None,
    source_options: list[str],
    status_options: list[str],
) -> dict[str, Any]:
    """Render horizontal filter row. Returns the currently active filters."""
    ensure_filter_state()

    country_opts = ["PAN"] + sorted(AFRICAN_COUNTRY_CODE_TO_NAME)
    region_opts = dimension_values("regions", taxonomy)
    sector_opts = sector_values(taxonomy)
    theme_opts = dimension_values("themes", taxonomy)
    event_opts = dimension_values("event_types", taxonomy)

    top = st.columns([0.16, 0.14, 0.5, 0.1, 0.1])
    with top[0]:
        st.slider(
            "Window (days)",
            min_value=1,
            max_value=180,
            step=1,
            key="flt_days",
        )
    with top[1]:
        st.select_slider(
            "Min relevance",
            options=[1, 2, 3, 4, 5],
            key="flt_min_relevance",
        )
    with top[2]:
        st.text_input(
            "Search headlines, summary, source",
            key="flt_search",
            placeholder="Try: lithium, eurobond, Dangote…",
        )
    with top[3]:
        if st.button("Reset", width="stretch"):
            st.session_state["_flt_pending_reset"] = True
            st.rerun()
    with top[4]:
        if st.button("Refresh", width="stretch"):
            st.cache_data.clear()
            st.rerun()

    with st.expander("Filters — geography, sector, themes, events, sources, status", expanded=False):
        r1 = st.columns(3)
        with r1[0]:
            st.multiselect(
                "Countries",
                country_opts,
                format_func=country_display_name,
                key="flt_countries",
            )
        with r1[1]:
            st.multiselect("Regions", region_opts, key="flt_regions")
        with r1[2]:
            st.multiselect("Sectors", sector_opts, key="flt_sectors")

        r2 = st.columns(3)
        with r2[0]:
            st.multiselect("Themes", theme_opts, key="flt_themes")
        with r2[1]:
            st.multiselect("Events", event_opts, key="flt_events")
        with r2[2]:
            st.multiselect("Sources", source_options, key="flt_sources")

        r3 = st.columns(3)
        with r3[0]:
            st.multiselect("Review status", status_options, key="flt_statuses")

    _render_saved_views(mode, user)
    return current_filters()


def _render_saved_views(mode: str, user: dict[str, Any] | None) -> None:
    owner = (user or {}).get("username") or ""
    views = list_views(mode, owner=owner)

    with st.expander("Saved views", expanded=False):
        cols = st.columns([0.5, 0.2, 0.15, 0.15])
        with cols[0]:
            name = st.text_input("Save current filters as", key="_view_name_input", placeholder="e.g. Nigeria energy")
        with cols[1]:
            if st.button("Save view", disabled=not name.strip(), width="stretch"):
                save_view(mode, name.strip(), owner, current_filters())
                st.success(f"Saved: {name.strip()}")
                st.rerun()
        with cols[2]:
            pass
        with cols[3]:
            pass

        if not views:
            st.caption("No saved views yet.")
            return

        for view in views:
            row = st.columns([0.55, 0.15, 0.15, 0.15])
            with row[0]:
                scope = "shared" if not view.get("owner") else "private"
                st.markdown(f"**{view['name']}**  ·  _{scope}_")
            with row[1]:
                if st.button("Apply", key=f"view_apply_{view['id']}", width="stretch"):
                    st.session_state["_flt_pending_apply"] = view["filters"]
                    st.rerun()
            with row[2]:
                st.caption("")
            with row[3]:
                if st.button("Delete", key=f"view_del_{view['id']}", width="stretch"):
                    delete_view(mode, int(view["id"]), owner)
                    st.rerun()


def contains_any(items: Any, selected: list[str]) -> bool:
    if not selected:
        return True
    return bool(set(str(x) for x in (items or [])) & set(selected))


def apply_filters_to_df(df, filters: dict[str, Any]):
    if df.empty:
        return df
    out = df[df["relevance_score"].fillna(0).astype(int) >= int(filters.get("min_relevance", 1))].copy()

    def _list_filter(col: str, key: str):
        nonlocal out
        selected = filters.get(key) or []
        if not selected or col not in out.columns:
            return
        mask = out[col].apply(lambda x: contains_any(x, selected))
        out = out.loc[mask.astype(bool)].copy()

    _list_filter("countries", "countries")
    _list_filter("regions", "regions")
    _list_filter("sector_tags", "sectors")
    _list_filter("themes", "themes")
    _list_filter("event_types", "events")

    for col, key in (("source_name", "sources"), ("tag_status", "statuses")):
        selected = filters.get(key) or []
        if selected and col in out.columns:
            out = out.loc[out[col].isin(selected)].copy()

    return out
