from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
LAYER1_DIR = REPO_ROOT / "layer1"
SQLITE_DB_PATH = LAYER1_DIR / "ioa_dev.db"
TAXONOMY_PATH = REPO_ROOT / "config" / "taxonomy.yaml"
