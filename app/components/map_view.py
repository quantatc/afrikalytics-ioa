"""Africa choropleth rendered with Plotly."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from ioa_core.countries import AFRICAN_COUNTRY_CODE_TO_NAME, country_display_name

# ISO-2 to ISO-3 for plotly choropleth.
_ISO2_TO_ISO3 = {
    "DZ": "DZA", "AO": "AGO", "BJ": "BEN", "BW": "BWA", "BF": "BFA", "BI": "BDI",
    "CV": "CPV", "CM": "CMR", "CF": "CAF", "TD": "TCD", "KM": "COM", "CG": "COG",
    "CD": "COD", "CI": "CIV", "DJ": "DJI", "EG": "EGY", "GQ": "GNQ", "ER": "ERI",
    "SZ": "SWZ", "ET": "ETH", "GA": "GAB", "GM": "GMB", "GH": "GHA", "GN": "GIN",
    "GW": "GNB", "KE": "KEN", "LS": "LSO", "LR": "LBR", "LY": "LBY", "MG": "MDG",
    "MW": "MWI", "ML": "MLI", "MR": "MRT", "MU": "MUS", "MA": "MAR", "MZ": "MOZ",
    "NA": "NAM", "NE": "NER", "NG": "NGA", "RW": "RWA", "ST": "STP", "SN": "SEN",
    "SC": "SYC", "SL": "SLE", "SO": "SOM", "ZA": "ZAF", "SS": "SSD", "SD": "SDN",
    "TZ": "TZA", "TG": "TGO", "TN": "TUN", "UG": "UGA", "ZM": "ZMB", "ZW": "ZWE",
}


def _aggregate(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    if df.empty or "primary_country" not in df.columns:
        return pd.DataFrame(columns=["iso2", "iso3", "country", "articles", "avg_relevance"])

    rows = []
    for code in AFRICAN_COUNTRY_CODE_TO_NAME:
        mask = df["primary_country"] == code
        if not mask.any():
            continue
        sub = df.loc[mask]
        rows.append(
            {
                "iso2": code,
                "iso3": _ISO2_TO_ISO3.get(code, ""),
                "country": country_display_name(code),
                "articles": int(len(sub)),
                "avg_relevance": float(sub["relevance_score"].fillna(0).astype(float).mean() or 0),
            }
        )
    return pd.DataFrame(rows)


def render_africa_map(df: pd.DataFrame, metric: str = "articles", height: int = 420) -> None:
    data = _aggregate(df, metric)
    if data.empty:
        st.caption("No geotagged articles in the current filter set.")
        return

    color_col = "articles" if metric == "articles" else "avg_relevance"
    hover = {
        "country": True,
        "articles": True,
        "avg_relevance": ":.2f",
        "iso3": False,
    }
    color_label = "Article count" if metric == "articles" else "Avg relevance"

    fig = px.choropleth(
        data,
        locations="iso3",
        color=color_col,
        hover_name="country",
        hover_data=hover,
        color_continuous_scale=["#1A2630", "#4DA3FF", "#E8833A"],
        range_color=(0, max(1, float(data[color_col].max()))),
        labels={color_col: color_label},
        scope="africa",
    )
    fig.update_geos(
        showframe=False,
        showcoastlines=True,
        coastlinecolor="#30363D",
        bgcolor="rgba(0,0,0,0)",
        landcolor="#0E1117",
        projection_type="mercator",
        lataxis_range=[-37, 38],
        lonaxis_range=[-20, 55],
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=6, b=0),
        height=height,
        coloraxis_colorbar=dict(title="", thickness=10, len=0.7, x=1.02, y=0.5),
    )

    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
