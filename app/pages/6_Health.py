"""Health — actionable source health: error trends, silent sources, latest runs."""

from __future__ import annotations

# ruff: noqa: E402

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.state import bootstrap, load_health


ctx = bootstrap(page_subtitle="Health")
mode = ctx["mode"]
health = load_health(mode)

if health.empty:
    st.info("No source health rows recorded yet. Run a Layer 1 collection to populate this dashboard.")
    st.stop()

latest = health.sort_values("run_at", ascending=False).drop_duplicates("source_name")

total_sources = int(latest["source_name"].nunique())
active = int((latest["articles_inserted"].fillna(0) > 0).sum())
errors = int(latest["had_error"].fillna(0).astype(int).sum())
now = datetime.now(timezone.utc)
silent_cutoff = now - timedelta(hours=48)
silent = latest[pd.to_datetime(latest["run_at"], utc=True, errors="coerce") < silent_cutoff]

m = st.columns(4)
with m[0]:
    st.metric("Sources", f"{total_sources:,}")
with m[1]:
    st.metric("Fresh (any insert)", f"{active:,}")
with m[2]:
    st.metric("Errors (last run)", f"{errors:,}")
with m[3]:
    st.metric("Silent >48h", f"{len(silent):,}")

st.divider()

st.markdown("##### Error rate by source (last 14 days)")
run_at_utc = pd.to_datetime(health["run_at"], utc=True, errors="coerce")
cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=14)
recent = health[run_at_utc >= cutoff].copy()
if recent.empty:
    st.caption("No runs in the last 14 days.")
else:
    err_by_source = recent.groupby("source_name").agg(
        runs=("id", "count") if "id" in recent.columns else ("source_name", "count"),
        errors=("had_error", "sum"),
    )
    err_by_source["error_rate"] = err_by_source["errors"] / err_by_source["runs"].replace(0, 1)
    err_by_source = err_by_source.sort_values("error_rate", ascending=False).head(20).reset_index()
    if err_by_source.empty:
        st.caption("No data.")
    else:
        fig = px.bar(
            err_by_source,
            x="error_rate",
            y="source_name",
            orientation="h",
            color="error_rate",
            color_continuous_scale=["#4CAF50", "#F5A623", "#E5484D"],
            labels={"error_rate": "Error rate", "source_name": ""},
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=6, b=0),
            height=420,
            xaxis=dict(tickformat=",.0%", gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(autorange="reversed"),
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})

st.divider()

st.markdown("##### Silent sources — no run in the last 48 hours")
if silent.empty:
    st.success("All sources have run in the last 48 hours.")
else:
    show = silent[["source_name", "source_tier", "run_at", "had_error", "error_msg"]].copy()
    show["run_at"] = show["run_at"].astype(str)
    st.dataframe(show, hide_index=True, width="stretch")

st.divider()

st.markdown("##### Latest run — per source")
latest_table = latest[
    [c for c in ["source_name", "source_tier", "run_at", "articles_fetched", "articles_inserted", "articles_duped", "had_error", "error_msg"] if c in latest.columns]
].copy()
if "run_at" in latest_table.columns:
    latest_table["run_at"] = latest_table["run_at"].astype(str)
st.dataframe(latest_table, hide_index=True, width="stretch")
