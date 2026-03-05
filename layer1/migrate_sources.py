"""
IOA Intelligence Briefing — Source Migration Script
====================================================
One-time (and repeatable) script that merges insights-tracker-sources.csv
into sources.yaml.

Run whenever analysts add new rows to the CSV:
    uv run python migrate_sources.py
    uv run python migrate_sources.py --csv path/to/other.csv  # custom path
    uv run python migrate_sources.py --dry-run                # preview only

Behaviour:
  - Deduplicates by URL — existing entries in sources.yaml are never overwritten
  - New entries are appended under the correct section (sources / country_specific_sources)
  - Paywall status is mapped to a paywall_status field
  - Country column is mapped to ISO 3166-1 alpha-2 codes automatically
  - Language is inferred from country/source name and added as a field
    (Layer 2 uses this to route non-English articles to translation)
  - A migration log is printed summarising what was added, skipped, and why
"""

import argparse
import csv
import re
from pathlib import Path

import yaml

# ── Paths ─────────────────────────────────────────────────────────────────────
SOURCES_FILE = Path(__file__).parent / "sources.yaml"
DEFAULT_CSV  = Path(__file__).parent / "insights-tracker-sources.csv"

# ── Country name → ISO 3166-1 alpha-2 mapping ────────────────────────────────
# Covers all countries in the CSV. Extend as needed.
COUNTRY_ISO = {
    "pan-african":                  [],
    "algeria":                      ["DZ"],
    "angola":                       ["AO"],
    "benin":                        ["BJ"],
    "botswana":                     ["BW"],
    "burkina faso":                 ["BF"],
    "burundi":                      ["BI"],
    "cabo verde":                   ["CV"],
    "cape verde":                   ["CV"],
    "cameroon":                     ["CM"],
    "central african republic":     ["CF"],
    "car":                          ["CF"],
    "chad":                         ["TD"],
    "comoros":                      ["KM"],
    "congo (democratic republic)":  ["CD"],
    "congo (republic)":             ["CG"],
    "cote d'ivoire":                ["CI"],
    "cote divoire":                 ["CI"],
    "djibouti":                     ["DJ"],
    "egypt":                        ["EG"],
    "equatorial guinea":            ["GQ"],
    "eritrea":                      ["ER"],
    "eswatini":                     ["SZ"],
    "ethiopia":                     ["ET"],
    "gabon":                        ["GA"],
    "gambia":                       ["GM"],
    "ghana":                        ["GH"],
    "guinea":                       ["GN"],
    "guinea-bissau":                ["GW"],
    "kenya":                        ["KE"],
    "lesotho":                      ["LS"],
    "liberia":                      ["LR"],
    "libya":                        ["LY"],
    "madagascar":                   ["MG"],
    "malawi":                       ["MW"],
    "mali":                         ["ML"],
    "mauritania":                   ["MR"],
    "mauritius":                    ["MU"],
    "morocco":                      ["MA"],
    "mozambique":                   ["MZ"],
    "namibia":                      ["NA"],
    "niger":                        ["NE"],
    "nigeria":                      ["NG"],
    "rwanda":                       ["RW"],
    "sao tome and principe":        ["ST"],
    "senegal":                      ["SN"],
    "seychelles":                   ["SC"],
    "sierra leone":                 ["SL"],
    "somalia":                      ["SO"],
    "south africa":                 ["ZA"],
    "south sudan":                  ["SS"],
    "sudan":                        ["SD"],
    "tanzania":                     ["TZ"],
    "togo":                         ["TG"],
    "tunisia":                      ["TN"],
    "uganda":                       ["UG"],
    "zambia":                       ["ZM"],
    "zimbabwe":                     ["ZW"],
}

# ── Country → primary language (for Layer 2 translation routing) ──────────────
COUNTRY_LANGUAGE = {
    # French-speaking
    "BJ": "fr", "BF": "fr", "BI": "fr", "CM": "fr", "CF": "fr",
    "TD": "fr", "KM": "fr", "CG": "fr", "CI": "fr", "DJ": "fr",
    "GQ": "fr", "GA": "fr", "GN": "fr", "MG": "fr", "ML": "fr",
    "MR": "fr", "MU": "fr", "MA": "fr", "NE": "fr", "RW": "fr",
    "SN": "fr", "TG": "fr", "TN": "fr", "CD": "fr",
    # Portuguese-speaking
    "AO": "pt", "CV": "pt", "GW": "pt", "MZ": "pt", "ST": "pt",
    # Arabic-speaking
    "DZ": "ar", "EG": "ar", "LY": "ar", "SD": "ar", "SO": "ar",
    # English default for the rest
}

# ── Region mapping ────────────────────────────────────────────────────────────
ISO_REGION = {
    # North Africa
    "DZ": "north-africa", "EG": "north-africa", "LY": "north-africa",
    "MA": "north-africa", "MR": "north-africa", "SD": "north-africa",
    "TN": "north-africa",
    # West Africa
    "BJ": "west-africa", "BF": "west-africa", "CV": "west-africa",
    "CI": "west-africa", "GM": "west-africa", "GH": "west-africa",
    "GN": "west-africa", "GW": "west-africa", "LR": "west-africa",
    "ML": "west-africa", "MR": "west-africa", "NE": "west-africa",
    "NG": "west-africa", "SN": "west-africa", "SL": "west-africa",
    "TG": "west-africa",
    # East Africa
    "BI": "east-africa", "KM": "east-africa", "DJ": "east-africa",
    "ER": "east-africa", "ET": "east-africa", "KE": "east-africa",
    "MG": "east-africa", "MU": "east-africa", "RW": "east-africa",
    "SC": "east-africa", "SO": "east-africa", "SS": "east-africa",
    "TZ": "east-africa", "UG": "east-africa",
    # Central Africa
    "CM": "central-africa", "CF": "central-africa", "TD": "central-africa",
    "CG": "central-africa", "CD": "central-africa", "GQ": "central-africa",
    "GA": "central-africa", "ST": "central-africa",
    # Southern Africa
    "AO": "southern-africa", "BW": "southern-africa", "SZ": "southern-africa",
    "LS": "southern-africa", "MW": "southern-africa", "MZ": "southern-africa",
    "NA": "southern-africa", "ZA": "southern-africa", "ZM": "southern-africa",
    "ZW": "southern-africa",
}

# ── Paywall normalisation ─────────────────────────────────────────────────────
def normalise_paywall(raw: str) -> str:
    raw = raw.strip().lower()
    if raw in ("yes", "paywalled"):
        return "paywalled"
    if raw in ("restricted",):
        return "restricted"
    return "open"


# ── Sector normalisation ──────────────────────────────────────────────────────
def normalise_sectors(raw: str) -> list:
    mapping = {
        "business and economics":           ["business", "economics", "finance"],
        "politics and security":            ["politics", "security", "governance"],
        "agri, energy and natural resources": ["agriculture", "energy", "mining", "resources"],
        "infrastructure":                   ["infrastructure", "transport", "logistics"],
        "science and technology":           ["tech", "telecoms", "innovation"],
        "multiple sectors":                 ["all"],
    }
    return mapping.get(raw.strip().lower(), [raw.strip().lower()])


# ── URL normalisation ─────────────────────────────────────────────────────────
def normalise_url(url: str) -> str:
    url = url.strip().rstrip("/")
    if not url.startswith("http"):
        url = "https://" + url
    return url


# ── Build YAML entry from CSV row ─────────────────────────────────────────────
def csv_row_to_entry(row: dict) -> dict:
    region_raw  = row.get("region", "").strip().lower()
    country_raw = region_raw  # CSV uses country name in region column

    iso_codes   = COUNTRY_ISO.get(country_raw, [])
    is_pan      = (region_raw == "pan-african" or not iso_codes)
    source_tier = "pan-africa" if is_pan else "country-specific"

    # Region
    if is_pan:
        region = "pan-africa"
    elif iso_codes:
        region = ISO_REGION.get(iso_codes[0], "pan-africa")
    else:
        region = "pan-africa"

    # Language — infer from country; default English
    if iso_codes:
        lang = COUNTRY_LANGUAGE.get(iso_codes[0], "en")
    else:
        lang = "en"

    url = normalise_url(row.get("url", ""))

    entry = {
        "name":           row.get("source_name", "").strip(),
        "url":            url,
        "rss_url":        None,          # unknown at migration time — Layer 1 will probe
        "type":           "rss+scraper", # safe default — tries RSS first
        "source_tier":    source_tier,
        "region":         region,
        "countries":      iso_codes,
        "sectors":        normalise_sectors(row.get("sectors", "Multiple Sectors")),
        "language":       lang,
        "paywall_status": normalise_paywall(row.get("is_paywalled", "no")),
        "active":         True,
    }

    # Carry over analyst comments as notes
    comments = row.get("Comments", "").strip()
    if comments:
        entry["notes"] = comments

    return entry


# ── Load existing YAML ────────────────────────────────────────────────────────
def load_yaml() -> dict:
    with open(SOURCES_FILE, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ── Collect existing URLs for dedup ──────────────────────────────────────────
def existing_urls(config: dict) -> set:
    urls = set()
    for entry in config.get("sources", []):
        urls.add(normalise_url(entry.get("url", "")))
    for entry in config.get("country_specific_sources", []):
        urls.add(normalise_url(entry.get("url", "")))
    return urls


# ── Save YAML preserving header comment ──────────────────────────────────────
def save_yaml(config: dict, dry_run: bool = False):
    if dry_run:
        print("\n[DRY RUN] Would write the following to sources.yaml:")
        print(yaml.dump(config, allow_unicode=True, sort_keys=False)[:2000], "...")
        return

    # Read existing header comments (lines starting with #)
    with open(SOURCES_FILE, encoding="utf-8") as f:
        lines = f.readlines()
    header = "".join(l for l in lines if l.startswith("#") or l.strip() == "")
    header = header.rstrip("\n") + "\n\n"

    body = yaml.dump(config, allow_unicode=True, sort_keys=False, default_flow_style=False)

    with open(SOURCES_FILE, "w", encoding="utf-8") as f:
        f.write(header + body)


# ── Main ──────────────────────────────────────────────────────────────────────
def run(csv_path: Path, dry_run: bool = False):
    if not csv_path.exists():
        print(f"❌  CSV not found: {csv_path}")
        return

    config   = load_yaml()
    known    = existing_urls(config)

    added_pan     = []
    added_country = []
    skipped_dupe  = []
    skipped_empty = []

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Strip whitespace from column headers (analysts may add spaces)
        reader.fieldnames = [h.strip() for h in reader.fieldnames]

        for row in reader:
            # Strip all values
            row = {k: (v.strip() if v else "") for k, v in row.items()}

            url = normalise_url(row.get("url", ""))
            if not url or url == "https://":
                skipped_empty.append(row.get("source_name", "unknown"))
                continue

            if url in known:
                skipped_dupe.append(row.get("source_name", url))
                continue

            entry = csv_row_to_entry(row)
            known.add(url)

            if entry["source_tier"] == "pan-africa":
                config.setdefault("sources", []).append(entry)
                added_pan.append(entry["name"])
            else:
                config.setdefault("country_specific_sources", []).append(entry)
                added_country.append(f"{entry['name']} ({', '.join(entry['countries'])})")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"Migration summary")
    print(f"  Pan-Africa sources added    : {len(added_pan)}")
    print(f"  Country-specific added      : {len(added_country)}")
    print(f"  Duplicates skipped          : {len(skipped_dupe)}")
    print(f"  Empty/invalid URLs skipped  : {len(skipped_empty)}")

    if added_pan:
        print(f"\nNew pan-africa sources:")
        for n in added_pan:
            print(f"  + {n}")

    if added_country:
        print(f"\nNew country-specific sources:")
        for n in added_country:
            print(f"  + {n}")

    if skipped_dupe:
        print(f"\nSkipped (already in sources.yaml):")
        for n in skipped_dupe:
            print(f"  ~ {n}")

    save_yaml(config, dry_run=dry_run)

    if not dry_run:
        print(f"\n✅  sources.yaml updated.")
        print(f"    Next step: run `uv run python collect.py` to test new sources.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge CSV sources into sources.yaml")
    parser.add_argument("--csv",     default=str(DEFAULT_CSV), help="Path to CSV file")
    parser.add_argument("--dry-run", action="store_true",      help="Preview without writing")
    args = parser.parse_args()
    run(csv_path=Path(args.csv), dry_run=args.dry_run)
