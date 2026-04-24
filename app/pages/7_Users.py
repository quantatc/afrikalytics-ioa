"""User management — admin only."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.state import bootstrap
from ioa_core.auth import change_password, create_user, delete_user, list_users

ctx = bootstrap(page_subtitle="Users")
mode = ctx["mode"]
user = ctx["user"]

if user.get("role") != "admin":
    st.error("Admin access required.")
    st.stop()

st.markdown("##### User accounts")

users = list_users(mode)

for u in users:
    row = st.columns([0.35, 0.25, 0.1, 0.15, 0.15])
    with row[0]:
        st.markdown(f"**{u['display_name']}** `{u['username']}`")
        st.caption(f"Last login: {u.get('last_login_at') or 'never'}")
    with row[1]:
        st.caption(f"Role: {u['role']}  ·  Created: {str(u.get('created_at') or '')[:10]}")
    with row[2]:
        pass
    with row[3]:
        with st.popover("Set password", use_container_width=True):
            new_pw = st.text_input("New password", type="password", key=f"pw_{u['id']}")
            if st.button("Save", key=f"pw_save_{u['id']}", disabled=not new_pw):
                change_password(mode, int(u["id"]), new_pw)
                st.success("Password updated.")
    with row[4]:
        is_self = int(u["id"]) == int(user["id"])
        if st.button("Delete", key=f"del_{u['id']}", disabled=is_self, use_container_width=True):
            delete_user(mode, int(u["id"]))
            st.rerun()
    st.divider()

st.markdown("##### Add user")
with st.form("add_user_form"):
    c = st.columns(2)
    with c[0]:
        new_username = st.text_input("Username")
        new_display = st.text_input("Display name")
    with c[1]:
        new_password = st.text_input("Password", type="password")
        new_role = st.selectbox("Role", ["analyst", "admin"])
    submitted = st.form_submit_button("Create user", type="primary")

if submitted:
    if not new_username.strip() or not new_password.strip():
        st.error("Username and password are required.")
    else:
        try:
            create_user(mode, new_username, new_display or new_username, new_password, new_role)
            st.success(f"User `{new_username}` created.")
            st.rerun()
        except Exception as exc:
            st.error(f"Could not create user: {exc}")
