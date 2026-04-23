"""Shared theme tokens and global CSS injected once per page."""

from __future__ import annotations

from pathlib import Path

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
LOGO_PATH = REPO_ROOT / "branding" / "IOA-White-Logo-transparent.webp"

BRAND_PRIMARY = "#E8833A"
BRAND_ACCENT = "#4DA3FF"
BRAND_OK = "#4CAF50"
BRAND_WARN = "#F5A623"
BRAND_ERR = "#E5484D"

DIMENSION_COLORS = {
    "theme": "#4DA3FF",
    "event": "#E8833A",
    "sector": "#4CAF50",
    "country": "#AB47BC",
    "sentiment_positive": "#4CAF50",
    "sentiment_negative": "#E5484D",
    "sentiment_neutral": "#9AA0A6",
    "sentiment_mixed": "#F5A623",
    "status_approved": "#4CAF50",
    "status_pending": "#F5A623",
    "status_ai": "#4DA3FF",
    "status_rejected": "#E5484D",
}

_GLOBAL_CSS = """
<style>
    .main .block-container { padding-top: 1.2rem; padding-bottom: 3rem; max-width: 1400px; }

    .ioa-chip {
        display: inline-block;
        padding: 2px 9px;
        margin: 1px 3px 1px 0;
        border-radius: 10px;
        font-size: 11px;
        font-weight: 500;
        line-height: 1.5;
        white-space: nowrap;
        border: 1px solid rgba(255,255,255,0.08);
    }

    .ioa-card {
        background: #161B22;
        border: 1px solid rgba(255,255,255,0.06);
        border-left: 4px solid #30363D;
        border-radius: 8px;
        padding: 14px 16px;
        margin-bottom: 10px;
        transition: border-color 120ms ease;
    }
    .ioa-card:hover { border-color: rgba(232,131,58,0.5); }
    .ioa-card.pending  { border-left-color: #F5A623; }
    .ioa-card.approved { border-left-color: #4CAF50; opacity: 0.82; }
    .ioa-card.rejected { border-left-color: #E5484D; opacity: 0.6; }
    .ioa-card.ai       { border-left-color: #4DA3FF; }

    .ioa-card h4 { margin: 0 0 6px 0; font-size: 15px; line-height: 1.35; color: #F0F2F6; }
    .ioa-card h4 a { color: #F0F2F6; text-decoration: none; }
    .ioa-card h4 a:hover { color: #E8833A; text-decoration: underline; }
    .ioa-card .ioa-meta { color: #9AA0A6; font-size: 12px; margin-bottom: 8px; }
    .ioa-card .ioa-summary { color: #C9D1D9; font-size: 13px; margin: 8px 0 10px 0; line-height: 1.5; }

    .ioa-rel {
        display: inline-block;
        padding: 1px 8px;
        border-radius: 6px;
        font-weight: 600;
        font-size: 11px;
        margin-right: 6px;
    }
    .ioa-rel-5 { background: #2D1810; color: #E8833A; }
    .ioa-rel-4 { background: #2D1810; color: #E8833A; }
    .ioa-rel-3 { background: #1A2630; color: #4DA3FF; }
    .ioa-rel-2 { background: #181A1E; color: #9AA0A6; }
    .ioa-rel-1 { background: #181A1E; color: #6B6F76; }

    .ioa-cluster {
        display: inline-block;
        padding: 1px 7px;
        border-radius: 10px;
        background: #1A2630;
        color: #4DA3FF;
        font-size: 10px;
        font-weight: 600;
        margin-left: 6px;
    }

    .ioa-section-title {
        font-size: 11px;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #6B6F76;
        margin: 4px 0;
    }
</style>
"""


def inject_global_css() -> None:
    if st.session_state.get("_ioa_css_done"):
        return
    st.markdown(_GLOBAL_CSS, unsafe_allow_html=True)
    st.session_state["_ioa_css_done"] = True


def render_header(subtitle: str | None = None) -> None:
    """Top-of-page header with logo + wordmark."""
    try:
        if LOGO_PATH.exists():
            st.logo(str(LOGO_PATH))
    except Exception:
        pass
    cols = st.columns([0.85, 0.15])
    with cols[0]:
        sub = subtitle or "Africa-focused intelligence workspace"
        st.markdown(
            (
                '<div style="display:flex;align-items:baseline;gap:14px;margin-bottom:4px;">'
                '<span style="font-size:26px;font-weight:700;color:#F0F2F6;letter-spacing:-0.01em;">'
                f'In(Sights) <span style="color:{BRAND_PRIMARY};">Tracker</span>'
                '</span>'
                f'<span style="color:#6B6F76;font-size:13px;">{sub}</span>'
                '</div>'
            ),
            unsafe_allow_html=True,
        )
