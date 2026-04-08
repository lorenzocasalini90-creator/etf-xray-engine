"""Page 3: ETF Redundancy — redundancy score, TER wasted, duplicate details."""

from __future__ import annotations

import os
import sys

import streamlit as st

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

st.header("♻️ ETF Redundancy")
from src.dashboard.components.global_header import show_global_header
show_global_header()
from src.dashboard.components.observations_box import show_observations
show_observations(st.session_state.get("observations", []), "redundancy")

redundancy_df = st.session_state.get("redundancy_df")
if redundancy_df is None:
    st.info("Inserisci un portafoglio nella pagina **Portfolio Input** e lancia l'analisi.")
    st.stop()

import pandas as pd
import plotly.express as px

if 'real_weight_pct' in redundancy_df.columns:
    redundancy_df['real_weight_pct'] = pd.to_numeric(redundancy_df['real_weight_pct'], errors='coerce').fillna(0.0)

from src.dashboard.components.display_utils import get_display_name

redundancy_df = redundancy_df.copy()
redundancy_df["display_name"] = redundancy_df["etf_ticker"].map(get_display_name)

# ── Horizontal bar chart ────────────────────────────────────────────
from src.dashboard.styles.colors import GREEN_LIGHT, RED_LIGHT, YELLOW_LIGHT

# Summary box
_red_scores = dict(zip(redundancy_df["display_name"], redundancy_df["redundancy_pct"] / 100))
_ter_wasted_all = dict(zip(redundancy_df["display_name"], redundancy_df["ter_wasted"].fillna(0)))
_total_ter_wasted = sum(_ter_wasted_all.values())
_max_redundant = max(_red_scores, key=_red_scores.get) if _red_scores else ""
_max_r = _red_scores.get(_max_redundant, 0)

_level = "ALTA 🔴" if _max_r > 0.70 else ("MODERATA 🟡" if _max_r > 0.40 else "BASSA 🟢")
_color = RED_LIGHT if _max_r > 0.70 else (YELLOW_LIGHT if _max_r > 0.40 else GREEN_LIGHT)

st.markdown(
    f"""<div style='background:{_color}; border-radius:8px;
    padding:16px 20px; margin-bottom:20px;'>
    <div style='font-size:0.78rem; font-weight:600; color:#374151;
    text-transform:uppercase; letter-spacing:0.04em;'>
    Livello ridondanza portafoglio</div>
    <div style='font-size:1.4rem; font-weight:700; margin:4px 0;'>
    {_level}</div>
    <div style='font-size:0.88rem; color:#374151;'>
    TER inefficienza stimata: <strong>€{_total_ter_wasted:.0f}/anno</strong>
    &nbsp;|&nbsp;
    ETF più ridondante: <strong>{_max_redundant} ({_max_r*100:.0f}%)</strong>
    </div></div>""",
    unsafe_allow_html=True,
)

color_map = {"green": "#2ecc71", "yellow": "#f39c12", "red": "#e74c3c"}
colors = redundancy_df["verdict"].map(color_map).tolist()

fig = px.bar(
    redundancy_df,
    x="redundancy_pct",
    y="display_name",
    orientation="h",
    labels={"redundancy_pct": "Redundancy (%)", "display_name": "ETF"},
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
    ticker = row["display_name"]
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

# ── Footer ─────────────────────────────────────────────────────────
from src.dashboard.components.footer import show_footer
show_footer()
