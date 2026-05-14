"""One-off — remove articles with empty/null headlines from raw_articles + enriched_articles."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ioa_core.db import connect_db


def run(mode: str) -> None:
    conn, db_type = connect_db(mode)
    if db_type == "sqlite":
        n_enriched = conn.execute(
            "DELETE FROM enriched_articles WHERE raw_id IN "
            "(SELECT id FROM raw_articles WHERE headline IS NULL OR TRIM(headline) = '')"
        ).rowcount
        n_raw = conn.execute(
            "DELETE FROM raw_articles WHERE headline IS NULL OR TRIM(headline) = ''"
        ).rowcount
    else:
        cur = conn.execute(
            "DELETE FROM enriched_articles WHERE raw_id IN "
            "(SELECT id FROM raw_articles WHERE headline IS NULL OR TRIM(headline) = '')"
        )
        n_enriched = cur.rowcount
        cur = conn.execute(
            "DELETE FROM raw_articles WHERE headline IS NULL OR TRIM(headline) = ''"
        )
        n_raw = cur.rowcount
    conn.commit()
    print(f"Deleted {n_enriched} enriched rows and {n_raw} raw rows with empty headlines.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="dev", choices=["dev", "prod"])
    args = parser.parse_args()
    run(args.mode)
