from __future__ import annotations

import re


AFRICAN_COUNTRY_CODE_TO_NAME = {
    "DZ": "Algeria",
    "AO": "Angola",
    "BJ": "Benin",
    "BW": "Botswana",
    "BF": "Burkina Faso",
    "BI": "Burundi",
    "CV": "Cabo Verde",
    "CM": "Cameroon",
    "CF": "Central African Republic",
    "TD": "Chad",
    "KM": "Comoros",
    "CG": "Republic of the Congo",
    "CD": "Democratic Republic of the Congo",
    "CI": "Cote d'Ivoire",
    "DJ": "Djibouti",
    "EG": "Egypt",
    "GQ": "Equatorial Guinea",
    "ER": "Eritrea",
    "SZ": "Eswatini",
    "ET": "Ethiopia",
    "GA": "Gabon",
    "GM": "Gambia",
    "GH": "Ghana",
    "GN": "Guinea",
    "GW": "Guinea-Bissau",
    "KE": "Kenya",
    "LS": "Lesotho",
    "LR": "Liberia",
    "LY": "Libya",
    "MG": "Madagascar",
    "MW": "Malawi",
    "ML": "Mali",
    "MR": "Mauritania",
    "MU": "Mauritius",
    "MA": "Morocco",
    "MZ": "Mozambique",
    "NA": "Namibia",
    "NE": "Niger",
    "NG": "Nigeria",
    "RW": "Rwanda",
    "ST": "Sao Tome and Principe",
    "SN": "Senegal",
    "SC": "Seychelles",
    "SL": "Sierra Leone",
    "SO": "Somalia",
    "ZA": "South Africa",
    "SS": "South Sudan",
    "SD": "Sudan",
    "TZ": "Tanzania",
    "TG": "Togo",
    "TN": "Tunisia",
    "UG": "Uganda",
    "ZM": "Zambia",
    "ZW": "Zimbabwe",
}

SPECIAL_COUNTRY_CODES = {"PAN": "Pan-Africa"}
VALID_COUNTRY_CODES = set(AFRICAN_COUNTRY_CODE_TO_NAME) | set(SPECIAL_COUNTRY_CODES)


def _normalize_text(text: str) -> str:
    text = text.strip().upper()
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


COUNTRY_NAME_TO_CODE = {_normalize_text(v): k for k, v in AFRICAN_COUNTRY_CODE_TO_NAME.items()}
COUNTRY_NAME_TO_CODE.update(
    {
        "CAPE VERDE": "CV",
        "THE GAMBIA": "GM",
        "IVORY COAST": "CI",
        "COTE D IVOIRE": "CI",
        "COTE DIVOIRE": "CI",
        "DRC": "CD",
        "DR CONGO": "CD",
        "CONGO KINSHASA": "CD",
        "DEMOCRATIC REPUBLIC OF CONGO": "CD",
        "CONGO BRAZZAVILLE": "CG",
        "REPUBLIC OF CONGO": "CG",
        "SWAZILAND": "SZ",
        "SAO TOME AND PRINCIPE": "ST",
        "PAN": "PAN",
        "PAN AFRICA": "PAN",
        "PAN AFRICAN": "PAN",
        "MULTI COUNTRY": "PAN",
        "MULTI": "PAN",
        "AFRICA": "PAN",
    }
)


def country_display_name(code: str) -> str:
    code = (code or "").strip().upper()
    if code in AFRICAN_COUNTRY_CODE_TO_NAME:
        return AFRICAN_COUNTRY_CODE_TO_NAME[code]
    if code in SPECIAL_COUNTRY_CODES:
        return SPECIAL_COUNTRY_CODES[code]
    return code or "Unknown"


def match_country_code(value: str | None) -> str | None:
    if not value:
        return None

    value_u = value.strip().upper()
    if value_u in VALID_COUNTRY_CODES:
        return value_u

    norm = _normalize_text(value)
    if norm in COUNTRY_NAME_TO_CODE:
        return COUNTRY_NAME_TO_CODE[norm]

    for token in re.findall(r"\b[A-Z]{2,3}\b", value_u):
        if token in VALID_COUNTRY_CODES:
            return token

    for country_name_norm, code in COUNTRY_NAME_TO_CODE.items():
        if len(country_name_norm) >= 5 and country_name_norm in norm:
            return code

    return None


def normalize_country_codes(raw_values: object, country_hint: str | None = None, limit: int = 5) -> list[str]:
    values: list[str] = []

    if isinstance(raw_values, str):
        pieces = re.split(r"[,\|;/]", raw_values)
    elif isinstance(raw_values, list):
        pieces = [str(v) for v in raw_values]
    else:
        pieces = []

    for piece in pieces:
        code = match_country_code(piece)
        if code and code not in values:
            values.append(code)

    hint = match_country_code(country_hint)
    if hint and hint != "PAN" and hint not in values:
        values.insert(0, hint)

    if not values:
        values = ["PAN"]

    if "PAN" in values and len(values) > 1:
        values = [v for v in values if v != "PAN"]

    return values[:limit]
