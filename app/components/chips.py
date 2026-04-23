"""HTML chip renderers — color-coded per dimension."""

from __future__ import annotations

import html
from typing import Iterable

from .theme import DIMENSION_COLORS

_HEX_CACHE: dict[str, str] = {}


def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    key = f"{hex_color}:{alpha}"
    if key in _HEX_CACHE:
        return _HEX_CACHE[key]
    c = hex_color.lstrip("#")
    r, g, b = int(c[0:2], 16), int(c[2:4], 16), int(c[4:6], 16)
    rgba = f"rgba({r},{g},{b},{alpha})"
    _HEX_CACHE[key] = rgba
    return rgba


def chip(label: str, dimension: str = "theme") -> str:
    color = DIMENSION_COLORS.get(dimension, "#9AA0A6")
    bg = _hex_to_rgba(color, 0.14)
    border = _hex_to_rgba(color, 0.38)
    safe = html.escape(str(label))
    return (
        f'<span class="ioa-chip" style="background:{bg};color:{color};'
        f'border:1px solid {border};">{safe}</span>'
    )


def chip_row(values: Iterable[str], dimension: str, limit: int | None = None) -> str:
    vals = [v for v in (values or []) if v]
    if limit:
        vals = vals[:limit]
    return "".join(chip(v, dimension) for v in vals)


def sentiment_chip(sentiment: str | None) -> str:
    if not sentiment:
        return ""
    key = f"sentiment_{str(sentiment).strip().lower()}"
    return chip(sentiment, key if key in DIMENSION_COLORS else "sentiment_neutral")


def status_chip(status: str | None) -> str:
    if not status:
        return ""
    mapping = {
        "approved": ("Approved", "status_approved"),
        "ai_generated": ("AI-tagged", "status_ai"),
        "pending": ("Pending", "status_pending"),
        "rejected": ("Rejected", "status_rejected"),
    }
    key = str(status).strip().lower()
    label, dim = mapping.get(key, (status, "status_ai"))
    return chip(label, dim)


def relevance_pill(score: int | float | None) -> str:
    try:
        n = int(score or 0)
    except (TypeError, ValueError):
        n = 0
    n = max(1, min(5, n)) if n else 0
    if n == 0:
        return ""
    return f'<span class="ioa-rel ioa-rel-{n}">REL {n}/5</span>'
