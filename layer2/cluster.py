"""In(Sights) Tracker Layer 2b: duplicate clustering.

Embeds enriched summaries with OpenAI, then groups near-duplicate stories
(Reuters + Bloomberg + local wire coverage of the same event) with a single
cluster_id so the UI can collapse them.

Simple online clustering: for each article, compute its embedding and compare
cosine similarity to existing cluster centroids. If the best match is above
threshold, join that cluster; otherwise start a new one. The first article in a
cluster gets cluster_rank=1 (canonical); later members get 2, 3, ...

Usage:
    uv run python layer2/cluster.py --mode dev --window-days 14
    uv run python layer2/cluster.py --mode prod --window-days 7 --threshold 0.82
"""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ioa_core.db import connect_db
from ioa_core.env import load_repo_env


load_repo_env()
log = logging.getLogger("ioa.cluster")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")

EMBED_MODEL = os.getenv("LAYER2B_EMBED_MODEL", "text-embedding-3-small")
EMBED_DIMS = 384  # matches schema VECTOR(384)


def _fetch_window(db, db_type: str, days: int) -> list[dict]:
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=days)
    if db_type == "sqlite":
        rows = db.execute(
            """
            SELECT e.raw_id, e.summary, e.primary_sector, e.primary_country,
                   e.enriched_at, r.headline, r.source_name
            FROM enriched_articles e
            JOIN raw_articles r ON r.id = e.raw_id
            WHERE e.enriched_at >= ?
            ORDER BY e.enriched_at ASC
            """,
            (cutoff_dt.isoformat(),),
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT e.raw_id, e.summary, e.primary_sector, e.primary_country,
                   e.enriched_at, r.headline, r.source_name
            FROM enriched_articles e
            JOIN raw_articles r ON r.id = e.raw_id
            WHERE e.enriched_at >= %s
            ORDER BY e.enriched_at ASC
            """,
            (cutoff_dt,),
        ).fetchall()
    return [dict(r) for r in rows]


def _embed(client, texts: list[str]) -> np.ndarray:
    resp = client.embeddings.create(model=EMBED_MODEL, input=texts, dimensions=EMBED_DIMS)
    return np.array([d.embedding for d in resp.data], dtype=np.float32)


def _normalize(v: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(v, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return v / norms


def _write_cluster(db, db_type: str, raw_id: int, cluster_id: int, rank: int) -> None:
    if db_type == "sqlite":
        db.execute(
            "UPDATE enriched_articles SET cluster_id = ?, cluster_rank = ? WHERE raw_id = ?",
            (cluster_id, rank, raw_id),
        )
    else:
        db.execute(
            "UPDATE enriched_articles SET cluster_id = %s, cluster_rank = %s WHERE raw_id = %s",
            (cluster_id, rank, raw_id),
        )


def run(mode: str = "dev", window_days: int = 14, threshold: float = 0.82, batch_size: int = 64) -> dict:
    if "OPENAI_API_KEY" not in os.environ:
        raise RuntimeError("OPENAI_API_KEY is required for clustering.")

    from openai import OpenAI

    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    db, db_type = connect_db(mode)
    rows = _fetch_window(db, db_type, window_days)
    if not rows:
        log.info("No articles in window.")
        return {"ok": True, "processed": 0, "clusters": 0}

    texts = [((r.get("headline") or "") + ". " + (r.get("summary") or ""))[:1500] or "-" for r in rows]

    vectors_list: list[np.ndarray] = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        vectors_list.append(_embed(client, batch))
    vectors = _normalize(np.vstack(vectors_list))

    centroids: list[np.ndarray] = []
    cluster_next_rank: list[int] = []
    next_cluster_id = int(datetime.now(timezone.utc).timestamp())

    updates = 0
    cluster_ids: list[int] = []

    for vec, row in zip(vectors, rows):
        if not centroids:
            centroids.append(vec.copy())
            cluster_next_rank.append(1)
            cluster_ids.append(next_cluster_id)
            _write_cluster(db, db_type, int(row["raw_id"]), next_cluster_id, 1)
            cluster_next_rank[-1] = 2
            next_cluster_id += 1
            updates += 1
            continue

        matrix = np.vstack(centroids)
        sims = matrix @ vec
        best = int(np.argmax(sims))
        if float(sims[best]) >= threshold and row.get("primary_sector"):
            cid = cluster_ids[best]
            rank = cluster_next_rank[best]
            cluster_next_rank[best] = rank + 1
            # Incremental centroid average.
            centroids[best] = _normalize(((centroids[best] * (rank - 1) + vec) / rank)[None, :])[0]
        else:
            cid = next_cluster_id
            next_cluster_id += 1
            centroids.append(vec.copy())
            cluster_next_rank.append(2)
            cluster_ids.append(cid)
            rank = 1

        _write_cluster(db, db_type, int(row["raw_id"]), cid, rank)
        updates += 1

    db.commit()
    log.info("Clustering complete processed=%s clusters=%s window_days=%s threshold=%.2f",
             updates, len(centroids), window_days, threshold)
    return {"ok": True, "processed": updates, "clusters": len(centroids)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Layer 2b duplicate clustering")
    parser.add_argument("--mode", choices=["dev", "prod"], default="dev")
    parser.add_argument("--window-days", type=int, default=14)
    parser.add_argument("--threshold", type=float, default=0.82)
    parser.add_argument("--batch-size", type=int, default=64)
    args = parser.parse_args()

    run(
        mode=args.mode,
        window_days=args.window_days,
        threshold=args.threshold,
        batch_size=args.batch_size,
    )
