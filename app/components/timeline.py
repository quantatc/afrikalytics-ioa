"""Time-series charts for article volume over the selected window."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st


def _parse_datetime_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def render_timeline(df: pd.DataFrame, date_col: str = "published_at", height: int = 220) -> None:
    if df.empty or date_col not in df.columns:
        st.caption("No timeline data available.")
        return

    dates = _parse_datetime_series(df[date_col]).dropna()
    if dates.empty:
        st.caption("No timeline data available.")
        return

    bucket = "D" if (dates.max() - dates.min()).days <= 60 else "W"
    counts = dates.dt.to_period(bucket).value_counts().sort_index()
    frame = pd.DataFrame(
        {
            "date": counts.index.to_timestamp().to_pydatetime(),
            "articles": counts.values,
        }
    )

    fig = px.area(frame, x="date", y="articles")
    fig.update_traces(
        line_color="#E8833A",
        fillcolor="rgba(232,131,58,0.18)",
        hovertemplate="%{x|%Y-%m-%d}<br>%{y} articles<extra></extra>",
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=6, b=0),
        height=height,
        xaxis=dict(title="", gridcolor="rgba(255,255,255,0.05)"),
        yaxis=dict(title="", gridcolor="rgba(255,255,255,0.05)"),
    )
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})


def render_sentiment_trend(df: pd.DataFrame, date_col: str = "published_at", height: int = 220) -> None:
    if df.empty or "sentiment" not in df.columns:
        st.caption("No sentiment data available.")
        return

    dates = _parse_datetime_series(df[date_col])
    frame = df.copy()
    frame["date"] = dates.dt.to_period("W").dt.to_timestamp()
    frame = frame.dropna(subset=["date", "sentiment"])
    if frame.empty:
        st.caption("No sentiment data available.")
        return

    grouped = frame.groupby(["date", "sentiment"]).size().reset_index(name="count")

    colors = {
        "Positive": "#4CAF50",
        "Negative": "#E5484D",
        "Neutral": "#9AA0A6",
        "Mixed": "#F5A623",
    }
    fig = px.bar(
        grouped,
        x="date",
        y="count",
        color="sentiment",
        color_discrete_map=colors,
        barmode="stack",
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=6, b=0),
        height=height,
        legend=dict(orientation="h", y=-0.2),
        xaxis=dict(title="", gridcolor="rgba(255,255,255,0.05)"),
        yaxis=dict(title="", gridcolor="rgba(255,255,255,0.05)"),
    )
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
