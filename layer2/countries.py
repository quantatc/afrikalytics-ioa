"""Compatibility imports for older scripts that referenced layer2.countries."""

# ruff: noqa: E402

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ioa_core.countries import (  # noqa: F401
    AFRICAN_COUNTRY_CODE_TO_NAME,
    COUNTRY_NAME_TO_CODE,
    SPECIAL_COUNTRY_CODES,
    VALID_COUNTRY_CODES,
    country_display_name,
    match_country_code,
    normalize_country_codes,
)
