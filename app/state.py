"""Shared Streamlit session bootstrap — auth gate, mode, data loaders."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ioa_core.auth import authenticate, ensure_default_admin  # noqa: E402
from ioa_core.env import load_repo_env  # noqa: E402
from ioa_core.repository import fetch_articles, fetch_health, fetch_raw_articles, fetch_raw_count  # noqa: E402
from ioa_core.taxonomy import load_taxonomy  # noqa: E402

from app.components.theme import inject_global_css, render_header  # noqa: E402

load_repo_env()


def _default_mode() -> str:
    return "prod" if os.getenv("DATABASE_URL") else "dev"


def _ensure_session() -> None:
    if "mode" not in st.session_state:
        st.session_state["mode"] = _default_mode()
    if "user" not in st.session_state:
        st.session_state["user"] = None


def _login_gate(mode: str) -> dict | None:
    st.markdown(
        (
            '<div style="margin-top:60px;display:flex;flex-direction:column;align-items:center;">'
            '<div style="font-size:30px;font-weight:700;color:#F0F2F6;">'
            'In(Sights) <span style="color:#E8833A;">Tracker</span>'
            '</div>'
            '<div style="color:#6B6F76;margin-top:4px;">Africa-focused intelligence workspace</div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )

    with st.container():
        c = st.columns([0.3, 0.4, 0.3])
        with c[1]:
            st.markdown("<div style='height:30px;'></div>", unsafe_allow_html=True)
            with st.form("login_form", clear_on_submit=False):
                username = st.text_input("Username", key="_login_user")
                password = st.text_input("Password", type="password", key="_login_pw")
                submitted = st.form_submit_button("Sign in", width="stretch", type="primary")

            if submitted:
                user = authenticate(mode, username, password)
                if user:
                    st.session_state["user"] = user
                    st.rerun()
                else:
                    st.error("Invalid credentials.")

            st.caption("Contact your administrator if you need access.")
    return None


def bootstrap(page_subtitle: str | None = None) -> dict[str, Any] | None:
    """Call from every page. Returns a context dict once the user is logged in."""
    st.set_page_config(
        page_title="In(Sights) Tracker",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _ensure_session()
    inject_global_css()

    mode = st.session_state["mode"]
    try:
        ensure_default_admin(mode)
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Could not initialise user store: {exc}")

    user = st.session_state.get("user")
    if not user:
        _login_gate(mode)
        st.stop()

    render_header(page_subtitle)
    with st.sidebar:
        st.markdown(
            (
                "<div style='padding:4px 0 10px 0;line-height:1.4;'>"
                "<div style='font-size:11px;color:#6B6F76;text-transform:uppercase;letter-spacing:0.06em;'>Signed in</div>"
                f"<div style='font-weight:600;font-size:14px;'>{user['display_name']}</div>"
                f"<div style='font-size:11px;color:#9AA0A6;'>{user.get('username', '')} &middot; {user['role']}</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        mode_choice = st.radio(
            "Database",
            ["dev", "prod"],
            index=0 if mode == "dev" else 1,
            horizontal=True,
            key="_mode_radio",
        )
        if mode_choice != mode:
            st.session_state["mode"] = mode_choice
            st.cache_data.clear()
            st.rerun()
        if st.button("Sign out", width="stretch"):
            st.session_state["user"] = None
            st.rerun()

    return {
        "mode": st.session_state["mode"],
        "user": user,
        "taxonomy": load_taxonomy(),
    }


@st.cache_data(ttl=45)
def load_articles(mode: str, days: int, query: str) -> pd.DataFrame:
    return fetch_articles(mode, days, query=query)


@st.cache_data(ttl=45)
def load_raw_articles(mode: str, days: int) -> pd.DataFrame:
    return fetch_raw_articles(mode, days)


@st.cache_data(ttl=45)
def load_raw_count(mode: str) -> int:
    return fetch_raw_count(mode)


@st.cache_data(ttl=45)
def load_health(mode: str) -> pd.DataFrame:
    return fetch_health(mode)
