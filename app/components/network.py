"""Entity co-occurrence network rendered with Plotly + NetworkX."""

from __future__ import annotations

from collections import Counter
from itertools import combinations

import networkx as nx
import pandas as pd
import plotly.graph_objects as go
import streamlit as st


ENTITY_COLORS = {
    "Company": "#4CAF50",
    "Government Body": "#AB47BC",
    "Multilateral Institution": "#4DA3FF",
    "Key Individual": "#E8833A",
}


def _entity_tuples(entities: list[dict]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for e in entities or []:
        name = str(e.get("name") or "").strip()
        etype = str(e.get("type") or "Company").strip()
        if name:
            out.append((name, etype))
    return out


def build_graph(df: pd.DataFrame, top_n: int = 30, min_mentions: int = 2) -> nx.Graph:
    counts: Counter = Counter()
    types: dict[str, str] = {}
    co_counts: Counter = Counter()

    for entities in df.get("entities", []):
        tuples = _entity_tuples(entities or [])
        for name, etype in tuples:
            counts[name] += 1
            types.setdefault(name, etype)
        for (a, _), (b, _) in combinations(sorted(set(tuples), key=lambda x: x[0]), 2):
            if a == b:
                continue
            co_counts[(a, b)] += 1

    top_nodes = {n for n, _ in counts.most_common(top_n) if counts[n] >= min_mentions}
    graph = nx.Graph()
    for name in top_nodes:
        graph.add_node(name, mentions=counts[name], etype=types.get(name, "Company"))
    for (a, b), w in co_counts.items():
        if a in top_nodes and b in top_nodes and w >= 1:
            graph.add_edge(a, b, weight=w)
    return graph


def render_network(df: pd.DataFrame, top_n: int = 30, min_mentions: int = 2, height: int = 520) -> None:
    if df.empty:
        st.caption("No articles in the current filter set.")
        return

    graph = build_graph(df, top_n=top_n, min_mentions=min_mentions)
    if len(graph.nodes) == 0:
        st.caption("No entities meet the minimum-mention threshold.")
        return

    try:
        pos = nx.spring_layout(graph, k=0.6, iterations=60, seed=42)
    except Exception:
        pos = {n: (i, i) for i, n in enumerate(graph.nodes)}

    edge_x: list[float] = []
    edge_y: list[float] = []
    for a, b in graph.edges:
        x0, y0 = pos[a]
        x1, y1 = pos[b]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        line=dict(width=0.6, color="rgba(255,255,255,0.18)"),
        hoverinfo="none",
        mode="lines",
    )

    node_x: list[float] = []
    node_y: list[float] = []
    text: list[str] = []
    colors: list[str] = []
    sizes: list[float] = []
    hover: list[str] = []
    for name in graph.nodes:
        x, y = pos[name]
        node_x.append(x)
        node_y.append(y)
        mentions = graph.nodes[name]["mentions"]
        etype = graph.nodes[name]["etype"]
        text.append(name if mentions >= max(3, min_mentions + 1) else "")
        colors.append(ENTITY_COLORS.get(etype, "#9AA0A6"))
        sizes.append(10 + min(30, mentions * 2))
        hover.append(f"{name}<br>{etype}<br>{mentions} mentions")

    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode="markers+text",
        text=text,
        textposition="top center",
        textfont=dict(size=10, color="#C9D1D9"),
        hoverinfo="text",
        hovertext=hover,
        marker=dict(
            color=colors,
            size=sizes,
            line=dict(width=1, color="rgba(0,0,0,0.25)"),
        ),
    )

    fig = go.Figure(data=[edge_trace, node_trace])
    fig.update_layout(
        showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, zeroline=False, visible=False),
        yaxis=dict(showgrid=False, zeroline=False, visible=False),
        margin=dict(l=0, r=0, t=10, b=0),
        height=height,
        hoverlabel=dict(bgcolor="#161B22"),
    )
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
