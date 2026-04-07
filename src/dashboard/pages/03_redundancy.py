"""Page 3: ETF Redundancy — redundancy score, TER wasted, duplicate details."""

from __future__ import annotations

import os
import sys

import streamlit as st

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

st.header("♻️ ETF Redundancy")

redundancy_df = st.session_state.get("redundancy_df")
if redundancy_df is None:
    st.info("Inserisci un portafoglio nella pagina **Portfolio Input** e lancia l'analisi.")
    st.stop()

import pandas as pd
import plotly.express as px

if 'real_weight_pct' in redundancy_df.columns:
    redundancy_df['real_weight_pct'] = pd.to_numeric(redundancy_df['real_weight_pct'], errors='coerce').fillna(0.0)

# ── Horizontal bar chart ────────────────────────────────────────────
color_map = {"green": "#2ecc71", "yellow": "#f39c12", "red": "#e74c3c"}
colors = redundancy_df["verdict"].map(color_map).tolist()

fig = px.bar(
    redundancy_df,
    x="redundancy_pct",
    y="etf_ticker",
    orientation="h",
    labels={"redundancy_pct": "Redundancy (%)", "etf_ticker": "ETF"},
    text=redundancy_df["redundancy_pct"].map(lambda v: f"{v:.1f}%"),
)
fig.update_traces(marker_color=colors, textposition="outside")
fig.update_layout(
    yaxis=dict(autorange="reversed"),
    xaxis=dict(range=[0, 105]),
    height=max(250, len(redundancy_df) * 60),
)
st.plotly_chart(fig, use_container_width=True)

with st.expander("ℹ️ Cos'è il Redundancy Score?"):
    st.markdown(
        "Per ogni ETF, misura quanta percentuale delle sue holdings è già presente "
        "in un altro ETF che hai in portafoglio.\n\n"
        "Se CSPX (S&P 500) è **75% ridondante**, significa che il 75% di quello che "
        "compri con CSPX lo hai già tramite un altro ETF (es. SWDA che include l'S&P 500).\n\n"
        "Quando la ridondanza è alta (>70%): considera di eliminare l'ETF ridondante "
        "e spostare il capitale sull'ETF più ampio che già copre quei titoli."
    )

# ── TER wasted ──────────────────────────────────────────────────────
st.subheader("TER sprecato per ridondanza")
for _, row in redundancy_df.iterrows():
    ticker = row["etf_ticker"]
    ter = row.get("ter_wasted", 0) or 0
    verdict = row["verdict"]
    icon = {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(verdict, "⚪")
    st.write(f"{icon} **{ticker}** — TER sprecato: **€ {ter:,.2f}** /anno")

with st.expander("ℹ️ Cos'è il TER Sprecato?"):
    st.markdown(
        "Il costo annuo che paghi per la parte ridondante di un ETF.\n\n"
        "**Calcolato come:** ridondanza % × TER dell'ETF × capitale investito.\n\n"
        "**Esempio:** se hai €30.000 in un ETF con TER 0.20% e ridondanza 75%, "
        "stai \"sprecando\" €45/anno in commissioni per esposizione che hai già."
    )

# ── Interpretation ──────────────────────────────────────────────────
st.divider()
st.markdown(
    """
**Come leggere il redundancy score:**
- 🟢 **< 30 %** — bassa ridondanza, l'ETF aggiunge esposizione unica al portafoglio.
- 🟡 **30-70 %** — ridondanza moderata, valuta se la diversificazione extra giustifica il TER.
- 🔴 **> 70 %** — alta ridondanza, la maggior parte dei titoli è già coperta da altri ETF.

**Cosa fare?** Considera di rimuovere o ridurre il peso degli ETF con alta ridondanza
per risparmiare sul TER e semplificare il portafoglio.
"""
)
st.info("💡 Suggerimenti basati su questi dati disponibili nella pagina **X-Ray Overview**")
