"""Page 4: Overlap Heatmap — NxN Jaccard weighted overlap + shared holdings."""

from __future__ import annotations

import os
import sys

import streamlit as st

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

st.header("🔥 Overlap Heatmap")
from src.dashboard.components.global_header import show_global_header
show_global_header()
from src.dashboard.components.observations_box import show_observations
show_observations(st.session_state.get("observations", []), "overlap")

overlap_mat = st.session_state.get("overlap_matrix")
holdings_db: dict = st.session_state.get("holdings_db", {})

if overlap_mat is None or overlap_mat.empty:
    st.info("Inserisci un portafoglio nella pagina **Portfolio Input** e lancia l'analisi. Servono almeno 2 ETF.")
    st.stop()

import pandas as pd
import plotly.figure_factory as ff

aggregated = st.session_state.get("aggregated")
if aggregated is not None and 'real_weight_pct' in aggregated.columns:
    aggregated['real_weight_pct'] = pd.to_numeric(aggregated['real_weight_pct'], errors='coerce').fillna(0.0)

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

with st.expander("ℹ️ Cos'è l'Overlap?"):
    st.markdown(
        "La percentuale di esposizione condivisa tra due ETF. "
        "Un overlap del **53%** tra CSPX e SWDA significa che più della metà del peso "
        "dei due ETF è investita negli stessi titoli.\n\n"
        "Overlap alto (**>50%**) tra due ETF nel tuo portafoglio suggerisce che potresti "
        "semplificare rimuovendo uno dei due."
    )

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

# ── Unique exposure analysis ───────────────────────────────────────
from src.analytics.overlap import compute_unique_exposure

st.subheader("🔍 Analisi: cosa perdi rimuovendo un ETF?")
tickers_all = list(holdings_db.keys())

if len(tickers_all) >= 2:
    target = st.selectbox(
        "Seleziona ETF da analizzare",
        tickers_all,
        key="unique_exposure_target",
    )

    if target:
        ue = compute_unique_exposure(target, holdings_db)

        unique_pct = ue["total_unique_pct"]
        unique_count = ue["unique_holdings_count"]
        total_h = ue["total_holdings"]
        main_etf = ue["main_covering_etf"]

        if unique_pct < 5:
            st.success(
                f"Rimuovendo **{target}**: impatto minimo — "
                f"{target} è ampiamente ridondante. Rimozione suggerita.\n\n"
                f"• Esposizione unica: **{unique_pct:.1f}%** su {unique_count} titoli\n"
                f"• La maggior parte già coperta da: **{main_etf}**"
            )
        elif unique_pct < 15:
            st.warning(
                f"Rimuovendo **{target}**: impatto moderato — "
                f"valuta se l'esposizione unica giustifica il TER.\n\n"
                f"• Esposizione unica: **{unique_pct:.1f}%** su {unique_count} titoli\n"
                f"• La maggior parte già coperta da: **{main_etf}**"
            )
        else:
            st.error(
                f"Rimuovendo **{target}**: impatto significativo — "
                f"{target} contribuisce esposizione difficilmente sostituibile.\n\n"
                f"• Esposizione unica: **{unique_pct:.1f}%** su {unique_count} titoli\n"
                f"• La maggior parte già coperta da: **{main_etf}**"
            )

        detail = ue["holdings_detail"]
        if not detail.empty:
            display_detail = detail.head(20)[
                ["holding_name", "weight_in_target_pct", "covered_weight_pct",
                 "unique_weight_pct", "covered_by_etf"]
            ].copy()
            display_detail.columns = [
                "Titolo", f"Peso in {target} %", "Coperto da altri %",
                "Unico %", "Coperto da",
            ]
            for c in [f"Peso in {target} %", "Coperto da altri %", "Unico %"]:
                display_detail[c] = display_detail[c].map(lambda x: f"{x:.2f}")
            st.dataframe(display_detail, use_container_width=True, hide_index=True)
else:
    st.info("Servono almeno 2 ETF per l'analisi di esposizione unica.")

# ── Footer ─────────────────────────────────────────────────────────
from src.dashboard.components.footer import show_footer
show_footer()
