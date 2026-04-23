from __future__ import annotations

import json
import re
from functools import lru_cache
from typing import Any

import yaml

from .countries import normalize_country_codes
from .paths import TAXONOMY_PATH


def _slug(value: str) -> str:
    value = value.strip().lower()
    value = value.replace("&", "and")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


@lru_cache(maxsize=1)
def load_taxonomy() -> dict[str, Any]:
    with TAXONOMY_PATH.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def taxonomy_version() -> str:
    return str(load_taxonomy().get("version", "unknown"))


def sector_parents(taxonomy: dict[str, Any] | None = None) -> list[str]:
    taxonomy = taxonomy or load_taxonomy()
    return list((taxonomy.get("sectors") or {}).keys())


def sector_values(taxonomy: dict[str, Any] | None = None) -> list[str]:
    taxonomy = taxonomy or load_taxonomy()
    values: list[str] = []
    for parent, data in (taxonomy.get("sectors") or {}).items():
        values.append(parent)
        values.extend(data.get("children") or [])
    return list(dict.fromkeys(values))


def dimension_values(name: str, taxonomy: dict[str, Any] | None = None) -> list[str]:
    taxonomy = taxonomy or load_taxonomy()
    values = taxonomy.get(name) or []
    if isinstance(values, dict):
        return list(values.keys())
    return list(values)


def regions_for_countries(countries: list[str], taxonomy: dict[str, Any] | None = None) -> list[str]:
    taxonomy = taxonomy or load_taxonomy()
    if not countries or countries == ["PAN"]:
        return ["pan-africa"]

    regions: list[str] = []
    for region, data in (taxonomy.get("regions") or {}).items():
        country_set = set(data.get("countries") or [])
        if any(country in country_set for country in countries):
            regions.append(region)
    return regions or ["pan-africa"]


def _pick_allowed(value: object, allowed: list[str], default: str) -> str:
    allowed_by_slug = {_slug(v): v for v in allowed}
    if isinstance(value, str):
        match = allowed_by_slug.get(_slug(value))
        if match:
            return match
    return default


def _clean_list(values: object, allowed: list[str], limit: int, default: str | None = None) -> list[str]:
    if isinstance(values, str):
        raw = re.split(r"[,\|;/]", values)
    elif isinstance(values, list):
        raw = [str(v) for v in values]
    else:
        raw = []

    allowed_by_slug = {_slug(v): v for v in allowed}
    out: list[str] = []
    for item in raw:
        match = allowed_by_slug.get(_slug(item))
        if match and match not in out:
            out.append(match)
        if len(out) >= limit:
            break

    if not out and default:
        out = [default]
    return out


def _clean_entities(values: object, taxonomy: dict[str, Any], limit: int) -> list[dict[str, str]]:
    allowed_types = dimension_values("entity_types", taxonomy)
    allowed_by_slug = {_slug(v): v for v in allowed_types}
    entities: list[dict[str, str]] = []

    if not isinstance(values, list):
        return entities

    for raw in values:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("name") or "").strip()
        if not name:
            continue
        raw_type = str(raw.get("type") or "").strip()
        entity_type = allowed_by_slug.get(_slug(raw_type), "Company")
        item = {"name": name[:160], "type": entity_type}
        if item not in entities:
            entities.append(item)
        if len(entities) >= limit:
            break

    return entities


def normalize_enrichment_output(
    data: dict[str, Any],
    country_hint: str | None = None,
    taxonomy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    taxonomy = taxonomy or load_taxonomy()
    limits = taxonomy.get("limits") or {}

    countries = normalize_country_codes(
        data.get("countries") or data.get("country") or data.get("primary_country"),
        country_hint=country_hint,
        limit=int(limits.get("countries", 5)),
    )
    primary_country = countries[0] if countries else "PAN"
    regions = regions_for_countries(countries, taxonomy)

    primary_sector = _pick_allowed(
        data.get("primary_sector") or data.get("sector"),
        sector_parents(taxonomy),
        "Other",
    )
    sector_tags = _clean_list(
        data.get("sector_tags") or data.get("sectors") or [primary_sector],
        sector_values(taxonomy),
        int(limits.get("sectors", 4)),
        default=primary_sector,
    )
    if primary_sector not in sector_tags:
        sector_tags.insert(0, primary_sector)

    themes = _clean_list(
        data.get("themes"),
        dimension_values("themes", taxonomy),
        int(limits.get("themes", 5)),
    )
    event_types = _clean_list(
        data.get("event_types") or data.get("event_type"),
        dimension_values("event_types", taxonomy),
        int(limits.get("event_types", 3)),
    )
    entities = _clean_entities(
        data.get("entities"),
        taxonomy,
        int(limits.get("entities", 10)),
    )

    relevance_score = data.get("relevance_score", 3)
    try:
        relevance_score = int(relevance_score)
    except (TypeError, ValueError):
        relevance_score = 3
    relevance_score = max(1, min(5, relevance_score))

    sentiment = _pick_allowed(data.get("sentiment"), dimension_values("sentiment", taxonomy), "Neutral")
    time_horizon = _pick_allowed(
        data.get("time_horizon"),
        dimension_values("time_horizon", taxonomy),
        "Unclear",
    )

    return {
        "taxonomy_version": str(taxonomy.get("version", "unknown")),
        "primary_country": primary_country,
        "countries": countries,
        "regions": regions,
        "primary_sector": primary_sector,
        "sector_tags": sector_tags[: int(limits.get("sectors", 4))],
        "themes": themes,
        "event_types": event_types,
        "entities": entities,
        "sentiment": sentiment,
        "time_horizon": time_horizon,
        "relevance_score": relevance_score,
        "relevance_reason": str(data.get("relevance_reason") or "").strip()[:500] or "No reason provided.",
        "summary": str(data.get("summary") or "").strip()[:1800] or "No summary provided.",
    }


def taxonomy_prompt(taxonomy: dict[str, Any] | None = None) -> str:
    taxonomy = taxonomy or load_taxonomy()
    prompt_payload = {
        "version": taxonomy.get("version"),
        "sector_parent_values": sector_parents(taxonomy),
        "sector_tag_values": sector_values(taxonomy),
        "theme_values": dimension_values("themes", taxonomy),
        "event_type_values": dimension_values("event_types", taxonomy),
        "entity_type_values": dimension_values("entity_types", taxonomy),
        "sentiment_values": dimension_values("sentiment", taxonomy),
        "time_horizon_values": dimension_values("time_horizon", taxonomy),
        "limits": taxonomy.get("limits") or {},
    }
    return json.dumps(prompt_payload, ensure_ascii=True)
