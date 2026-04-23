"""Background brief-generation jobs.

Writes queue rows in brief_jobs, spawns a daemon thread, and reports status
back through the table. Streamlit polls the table — no Celery required.
"""

from __future__ import annotations

import json
import logging
import sys
import threading
import traceback
from pathlib import Path
from typing import Optional

import pandas as pd

from .db import connect_db, json_dumps, json_for_db, json_loads, now_utc_iso
from .repository import insert_brief

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

log = logging.getLogger("ioa.jobs")


def _update_job(mode: str, job_id: int, **fields) -> None:
    conn, db_type = connect_db(mode)
    sets = []
    params: list = []
    for k, v in fields.items():
        sets.append(f"{k} = ?") if db_type == "sqlite" else sets.append(f"{k} = %s")
        params.append(v)
    params.append(job_id)
    where = " WHERE id = ?" if db_type == "sqlite" else " WHERE id = %s"
    conn.execute(f"UPDATE brief_jobs SET {', '.join(sets)}" + where, tuple(params))
    conn.commit()


def enqueue_brief_job(mode: str, params: dict, author: str) -> int:
    conn, db_type = connect_db(mode)
    ts = now_utc_iso()
    if db_type == "sqlite":
        cur = conn.execute(
            "INSERT INTO brief_jobs (status, params, author, created_at) VALUES ('queued', ?, ?, ?)",
            (json_dumps(params), author, ts),
        )
        job_id = cur.lastrowid
    else:
        row = conn.execute(
            "INSERT INTO brief_jobs (status, params, author, created_at) VALUES ('queued', %s, %s, %s) RETURNING id",
            (json_for_db(params, db_type), author, ts),
        ).fetchone()
        job_id = row["id"] if row else None
    conn.commit()
    return int(job_id) if job_id else 0


def list_jobs(mode: str, limit: int = 25) -> pd.DataFrame:
    conn, db_type = connect_db(mode)
    if db_type == "sqlite":
        rows = conn.execute(
            "SELECT id, status, params, brief_id, log, author, created_at, started_at, finished_at FROM brief_jobs ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, status, params, brief_id, log, author, created_at, started_at, finished_at FROM brief_jobs ORDER BY created_at DESC LIMIT %s",
            (limit,),
        ).fetchall()
    df = pd.DataFrame([dict(r) for r in rows])
    if not df.empty:
        df["params"] = df["params"].apply(lambda v: json_loads(v, default={}))
    return df


def get_job(mode: str, job_id: int) -> Optional[dict]:
    conn, db_type = connect_db(mode)
    if db_type == "sqlite":
        row = conn.execute("SELECT * FROM brief_jobs WHERE id = ?", (job_id,)).fetchone()
    else:
        row = conn.execute("SELECT * FROM brief_jobs WHERE id = %s", (job_id,)).fetchone()
    if not row:
        return None
    d = dict(row)
    d["params"] = json_loads(d.get("params"), default={})
    return d


def _run_brief_job(mode: str, job_id: int) -> None:
    """Worker body — imports synthesise lazily to keep startup light."""
    try:
        _update_job(mode, job_id, status="running", started_at=now_utc_iso())
        job = get_job(mode, job_id)
        if not job:
            return
        params = job["params"] or {}

        from layer3 import synthesise  # type: ignore

        result = synthesise.run(
            mode=mode,
            period_days=int(params.get("period_days", 30)),
            min_relevance=int(params.get("min_relevance", 3)),
            max_articles=int(params.get("max_articles", 120)),
            country=params.get("country") or None,
            region=params.get("region") or None,
            sector=params.get("sector") or None,
            theme=params.get("theme") or None,
            event_type=params.get("event_type") or None,
            no_db_write=True,
        )

        if not result.get("ok"):
            _update_job(
                mode,
                job_id,
                status="failed",
                log=f"synthesis returned: {json.dumps(result)[:2000]}",
                finished_at=now_utc_iso(),
            )
            return

        md_path = Path(result["report_markdown"])
        json_path = Path(result["report_json"])
        report_md = md_path.read_text(encoding="utf-8") if md_path.exists() else ""
        report_json_raw = json_path.read_text(encoding="utf-8") if json_path.exists() else "{}"
        try:
            report_json = json.loads(report_json_raw)
        except json.JSONDecodeError:
            report_json = {}

        title_parts = []
        if params.get("country"):
            title_parts.append(params["country"])
        if params.get("region"):
            title_parts.append(params["region"])
        if params.get("sector"):
            title_parts.append(params["sector"])
        if params.get("theme"):
            title_parts.append(params["theme"])
        if not title_parts:
            title_parts.append("Pan-Africa")
        title = f"IOA Brief — {' / '.join(title_parts)} ({params.get('period_days', 30)}d)"

        brief_id = insert_brief(
            mode=mode,
            title=title,
            period_days=int(params.get("period_days", 30)),
            min_relevance=int(params.get("min_relevance", 3)),
            filters=params,
            report_markdown=report_md,
            report_json=report_json,
            rows_analyzed=int(result.get("rows_analyzed") or 0),
            model=str(report_json.get("model") or ""),
            author=job.get("author") or "",
        )

        _update_job(
            mode,
            job_id,
            status="done",
            brief_id=brief_id,
            log="ok",
            finished_at=now_utc_iso(),
        )
    except Exception as exc:  # noqa: BLE001
        log.exception("brief job %s failed", job_id)
        _update_job(
            mode,
            job_id,
            status="failed",
            log=f"{exc}\n{traceback.format_exc()[:4000]}",
            finished_at=now_utc_iso(),
        )


def spawn_worker(mode: str, job_id: int) -> None:
    t = threading.Thread(target=_run_brief_job, args=(mode, job_id), daemon=True)
    t.start()
