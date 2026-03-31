"""Page 4: Overlap Heatmap — NxN Jaccard weighted overlap + shared holdings."""

from __future__ import annotations

import os
import sys

import streamlit as st

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

st.header("🔥 Overlap Heatmap")

overlap_mat = st.session_state.get("overlap_matrix")
holdings_db: dict = st.session_state.get("holdings_db", {})

if overlap_mat is None or overlap_mat.empty:
    st.info("Inserisci un portafoglio nella pagina **Portfolio Input** e lancia l'analisi. Servono almeno 2 ETF.")
    st.stop()

import plotly.figure_factory as ff

from src.analytics.overlap import shared_holdings

# ── Heatmap ─────────────────────────────────────────────────────────
labels = overlap_mat.columns.tolist()
z = overlap_mat.values.tolist()

# Annotate with % values
annotations = [[f"{v:.1f}%" for v in row] for row in z]

fig = ff.create_annotated_heatmap(
    z=z,
    x=labels,
    y=labels,
    annotation_text=annotations,
    colorscale=[[0, "#2ecc71"], [0.5, "#f1c40f"], [1.0, "#e74c3c"]],
    showscale=True,
)
fig.update_layout(
    height=max(400, len(labels) * 80),
    xaxis=dict(side="bottom"),
)
st.plotly_chart(fig, use_container_width=True)

# ── Shared holdings detail ──────────────────────────────────────────
st.subheader("Dettaglio titoli in comune")
tickers = list(holdings_db.keys())
if len(tickers) >= 2:
    col1, col2 = st.columns(2)
    with col1:
        etf_a = st.selectbox("ETF A", tickers, index=0)
    with col2:
        default_b = 1 if len(tickers) > 1 else 0
        etf_b = st.selectbox("ETF B", tickers, index=default_b)

    if etf_a != etf_b and etf_a in holdings_db and etf_b in holdings_db:
        shared = shared_holdings(holdings_db[etf_a], holdings_db[etf_b])
        if shared.empty:
            st.info("Nessun titolo in comune.")
        else:
            display = shared[["name", "weight_a", "weight_b", "weight_diff"]].copy()
            display.columns = ["Titolo", f"Peso {etf_a} %", f"Peso {etf_b} %", "Delta %"]
            for c in display.columns[1:]:
                display[c] = display[c].map(lambda x: f"{x:.2f}")
            display = display.head(30).reset_index(drop=True)
            display.index = display.index + 1
            st.dataframe(display, use_container_width=True)
    elif etf_a == etf_b:
        st.warning("Seleziona due ETF diversi.")
