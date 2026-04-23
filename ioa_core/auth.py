from __future__ import annotations

import os
from typing import Optional

import bcrypt

from .db import connect_db, now_utc_iso


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    if not hashed:
        return False
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except ValueError:
        return False


def _fetch_user(mode: str, username: str) -> Optional[dict]:
    conn, db_type = connect_db(mode)
    if db_type == "sqlite":
        row = conn.execute(
            "SELECT id, username, display_name, password_hash, role FROM app_users WHERE username = ?",
            (username,),
        ).fetchone()
        return dict(row) if row else None
    row = conn.execute(
        "SELECT id, username, display_name, password_hash, role FROM app_users WHERE username = %s",
        (username,),
    ).fetchone()
    return dict(row) if row else None


def _touch_login(mode: str, user_id: int) -> None:
    conn, db_type = connect_db(mode)
    ts = now_utc_iso()
    if db_type == "sqlite":
        conn.execute("UPDATE app_users SET last_login_at = ? WHERE id = ?", (ts, user_id))
    else:
        conn.execute("UPDATE app_users SET last_login_at = %s WHERE id = %s", (ts, user_id))
    conn.commit()


def ensure_default_admin(mode: str) -> None:
    """Seed an admin account from env on first run.

    Uses IOA_ADMIN_USERNAME / IOA_ADMIN_PASSWORD if set, otherwise admin / ioa-admin.
    """
    username = os.getenv("IOA_ADMIN_USERNAME", "admin")
    password = os.getenv("IOA_ADMIN_PASSWORD", "ioa-admin")
    display = os.getenv("IOA_ADMIN_DISPLAY", "Administrator")

    conn, db_type = connect_db(mode)
    if db_type == "sqlite":
        count = conn.execute("SELECT COUNT(*) AS c FROM app_users").fetchone()["c"]
    else:
        count = conn.execute("SELECT COUNT(*) AS c FROM app_users").fetchone()["c"]

    if count and int(count) > 0:
        return

    pw_hash = hash_password(password)
    ts = now_utc_iso()
    if db_type == "sqlite":
        conn.execute(
            "INSERT INTO app_users (username, display_name, password_hash, role, created_at) VALUES (?, ?, ?, 'admin', ?)",
            (username, display, pw_hash, ts),
        )
    else:
        conn.execute(
            "INSERT INTO app_users (username, display_name, password_hash, role, created_at) VALUES (%s, %s, %s, 'admin', %s)",
            (username, display, pw_hash, ts),
        )
    conn.commit()


def authenticate(mode: str, username: str, password: str) -> Optional[dict]:
    user = _fetch_user(mode, username.strip())
    if not user:
        return None
    if not verify_password(password, user.get("password_hash") or ""):
        return None
    _touch_login(mode, int(user["id"]))
    return {
        "id": int(user["id"]),
        "username": user["username"],
        "display_name": user.get("display_name") or user["username"],
        "role": user.get("role") or "analyst",
    }
