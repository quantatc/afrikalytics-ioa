"""Briefs — async generation, library, and export."""

from __future__ import annotations

# ruff: noqa: E402

import os
import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.state import bootstrap
from ioa_core.countries import AFRICAN_COUNTRY_CODE_TO_NAME, country_display_name
from ioa_core.jobs import enqueue_brief_job, get_job, list_jobs, spawn_worker
from ioa_core.repository import delete_brief, fetch_brief, list_briefs
from ioa_core.taxonomy import dimension_values, sector_values


ctx = bootstrap(page_subtitle="Briefs")
mode = ctx["mode"]
user = ctx["user"]
taxonomy = ctx["taxonomy"]
author = user.get("display_name") or user.get("username") or "analyst"

gen_tab, queue_tab, library_tab = st.tabs(["Generate", "Jobs", "Library"])

with gen_tab:
    st.markdown("##### New brief")
    if not os.getenv("OPENAI_API_KEY"):
        st.warning("`OPENAI_API_KEY` not found in environment — generation will fail until it is set.")

    country_opts = ["PAN"] + sorted(AFRICAN_COUNTRY_CODE_TO_NAME)
    region_opts = dimension_values("regions", taxonomy)
    sector_opts = sector_values(taxonomy)
    theme_opts = dimension_values("themes", taxonomy)
    event_opts = dimension_values("event_types", taxonomy)

    with st.form("brief_form"):
        r1 = st.columns(3)
        with r1[0]:
            period_days = st.number_input("Window (days)", min_value=1, max_value=180, value=30)
        with r1[1]:
            min_rel = st.number_input("Min relevance", min_value=1, max_value=5, value=3)
        with r1[2]:
            max_articles = st.number_input("Max articles", min_value=20, max_value=300, value=120)

        r2 = st.columns(3)
        with r2[0]:
            countries = st.multiselect(
                "Countries", country_opts, format_func=country_display_name,
                help="Empty = no country filter (all Africa).",
            )
        with r2[1]:
            regions = st.multiselect("Regions", region_opts)
        with r2[2]:
            sectors = st.multiselect("Sectors", sector_opts)

        r3 = st.columns(2)
        with r3[0]:
            themes = st.multiselect("Themes", theme_opts)
        with r3[1]:
            event_types = st.multiselect("Events", event_opts)

        submit = st.form_submit_button("Queue brief", type="primary", width="stretch")

    if submit:
        params = {
            "period_days": int(period_days),
            "min_relevance": int(min_rel),
            "max_articles": int(max_articles),
            "country": countries,
            "region": regions,
            "sector": sectors,
            "theme": themes,
            "event_type": event_types,
        }
        job_id = enqueue_brief_job(mode, params, author)
        spawn_worker(mode, job_id)
        st.session_state["watch_job_id"] = job_id
        st.success(f"Brief job queued (#{job_id}). Follow progress in the Jobs tab.")

    watch_id = st.session_state.get("watch_job_id")
    if watch_id:
        job = get_job(mode, int(watch_id))
        if job:
            status = job.get("status")
            if status in ("queued", "running"):
                with st.status(f"Job #{watch_id} — {status}", expanded=False, state="running"):
                    time.sleep(2)
                    st.rerun()
            elif status == "done":
                st.success(f"Job #{watch_id} complete. See Library tab for brief #{job.get('brief_id')}.")
                st.session_state["watch_job_id"] = None
            else:
                st.error(f"Job #{watch_id} failed. Check the Jobs tab for the log.")
                st.session_state["watch_job_id"] = None


with queue_tab:
    st.markdown("##### Recent jobs")
    try:
        jobs = list_jobs(mode, limit=30)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not load jobs: {exc}")
        jobs = None

    if jobs is None or jobs.empty:
        st.info("No brief jobs yet. Submit one from the **Generate** tab to see it here.")
    else:
        for _, j in jobs.iterrows():
            status = j.get("status") or "?"
            color = {
                "done": "#4CAF50",
                "failed": "#E5484D",
                "running": "#4DA3FF",
                "queued": "#F5A623",
            }.get(status, "#9AA0A6")
            head = st.columns([0.5, 0.15, 0.15, 0.2])
            with head[0]:
                params = j.get("params") or {}
                def _fmt(v):
                    if isinstance(v, (list, tuple)):
                        return ",".join(str(x) for x in v) if v else ""
                    return str(v) if v else ""
                filt_label = " · ".join(
                    f"{k}={_fmt(v)}"
                    for k, v in params.items()
                    if _fmt(v) and k in ("country", "region", "sector", "theme", "event_type")
                ) or "no filters"
                st.markdown(
                    f"**#{j['id']}** · <span style='color:{color};font-weight:600;'>{status}</span> · "
                    f"<span style='color:#9AA0A6;font-size:12px;'>{params.get('period_days', '?')}d · min rel {params.get('min_relevance', '?')} · {filt_label}</span>",
                    unsafe_allow_html=True,
                )
            with head[1]:
                st.caption(f"by {j.get('author') or '-'}")
            with head[2]:
                st.caption(str(j.get("created_at") or ""))
            with head[3]:
                if status == "done" and j.get("brief_id"):
                    if st.button(f"Open brief #{int(j['brief_id'])}", key=f"open_brief_{j['id']}", width="stretch"):
                        st.session_state["open_brief_id"] = int(j["brief_id"])
                        st.rerun()
            if status == "failed" and j.get("log"):
                with st.expander("Error log", expanded=False):
                    st.code(j["log"])
            st.divider()

        if st.button("Refresh jobs", width="stretch"):
            st.rerun()


with library_tab:
    st.markdown("##### Brief library")
    try:
        briefs = list_briefs(mode, limit=200)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not load briefs: {exc}")
        briefs = None

    if briefs is None or briefs.empty:
        st.info(
            "No briefs in the library yet. Queue one from the **Generate** tab above — "
            "it runs in the background and lands here when ready."
        )
    else:
        open_id = st.session_state.get("open_brief_id")
        ids = briefs["id"].tolist()
        default_idx = ids.index(open_id) if open_id in ids else 0
        sel = st.selectbox(
            "Select brief",
            ids,
            index=default_idx,
            format_func=lambda i: f"#{i} · {briefs.loc[briefs['id'] == i, 'title'].iloc[0]} · {briefs.loc[briefs['id'] == i, 'created_at'].iloc[0]}",
        )

        brief = None
        try:
            brief = fetch_brief(mode, int(sel))
        except Exception as exc:  # noqa: BLE001
            st.error(f"Could not load brief #{sel}: {exc}")

        if not brief:
            st.warning("Brief not found. It may have been deleted — pick another from the list above.")
        else:
            header = st.columns([0.6, 0.15, 0.15, 0.1])
            with header[0]:
                st.markdown(f"### {brief.get('title')}")
                st.caption(
                    f"By {brief.get('author') or '-'} &middot; {brief.get('rows_analyzed', 0)} articles analyzed "
                    f"&middot; {brief.get('period_days')}d window &middot; min rel {brief.get('min_relevance')} "
                    f"&middot; {brief.get('created_at')}"
                )
            md = brief.get("report_markdown") or ""
            with header[1]:
                st.download_button(
                    "Markdown",
                    md.encode("utf-8"),
                    file_name=f"ioa_brief_{brief['id']}.md",
                    mime="text/markdown",
                    width="stretch",
                    disabled=not md,
                )
            with header[2]:
                html_doc = f"""<!doctype html>
<html><head><meta charset='utf-8'><title>{brief.get('title')}</title>
<style>
body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; max-width: 900px; margin: 40px auto; padding: 0 20px; color: #222; line-height: 1.6; }}
h1, h2, h3 {{ color: #E8833A; }}
code {{ background:#f5f5f5; padding:2px 4px; border-radius:3px; }}
blockquote {{ border-left:3px solid #E8833A; padding-left:10px; color:#555; }}
</style></head><body>
<pre style='white-space:pre-wrap;font-family:inherit;'>{md}</pre>
</body></html>"""
                st.download_button(
                    "HTML",
                    html_doc.encode("utf-8"),
                    file_name=f"ioa_brief_{brief['id']}.html",
                    mime="text/html",
                    width="stretch",
                    disabled=not md,
                )
            with header[3]:
                if st.button("Delete", width="stretch"):
                    delete_brief(mode, int(brief["id"]))
                    st.session_state["open_brief_id"] = None
                    st.rerun()

            st.divider()
            if md:
                st.markdown(md)
            else:
                st.caption("This brief has no markdown body. Re-run the job if the source content is still available.")
