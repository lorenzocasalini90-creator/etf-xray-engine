"""Page 2: X-Ray Overview — KPI cards, top holdings, active bets."""

from __future__ import annotations

import os
import sys

import streamlit as st

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

st.header("🔍 X-Ray Overview")

aggregated = st.session_state.get("aggregated")
if aggregated is None:
    st.info("Inserisci un portafoglio nella pagina **Portfolio Input** e lancia l'analisi.")
    st.stop()

import pandas as pd
import plotly.express as px

if 'real_weight_pct' in aggregated.columns:
    aggregated['real_weight_pct'] = pd.to_numeric(aggregated['real_weight_pct'], errors='coerce').fillna(0.0)

from src.analytics.overlap import portfolio_hhi

# ── KPI row ─────────────────────────────────────────────────────────
hhi_stats = portfolio_hhi(aggregated)

active_share_result = st.session_state.get("active_share_result")
active_share_pct = active_share_result["active_share_pct"] if active_share_result else None

if active_share_pct is not None:
    k1, k2, k3, k4, k5 = st.columns(5)
    k4.metric("Active Share", f"{active_share_pct:.1f} %")
else:
    k1, k2, k3, k5 = st.columns(4)
k1.metric("Titoli unici", f"{len(aggregated):,}")
k2.metric("HHI", f"{hhi_stats['hhi']:.4f}")
k3.metric("Effective N", f"{hhi_stats['effective_n']:.0f}")
k5.metric("Top-5 Conc.", f"{hhi_stats['top_5_pct']:.2f} %")

# ── Top 30 holdings table ──────────────────────────────────────────
st.subheader("Top 30 titoli per peso reale")
top30 = aggregated.nlargest(30, "real_weight_pct")[
    ["name", "ticker", "real_weight_pct", "n_etf_sources", "sector", "country"]
].copy()
top30.columns = ["Titolo", "Ticker", "Peso Reale %", "N ETF", "Settore", "Paese"]
top30["Peso Reale %"] = top30["Peso Reale %"].map(lambda x: f"{x:.2f}")
top30 = top30.reset_index(drop=True)
top30.index = top30.index + 1
st.dataframe(top30, use_container_width=True)

# ── Bar chart top 20 ───────────────────────────────────────────────
st.subheader("Top 20 titoli — esposizione reale")
top20 = aggregated.nlargest(20, "real_weight_pct").copy()
fig = px.bar(
    top20,
    x="real_weight_pct",
    y="name",
    orientation="h",
    labels={"real_weight_pct": "Peso Reale (%)", "name": ""},
    color="real_weight_pct",
    color_continuous_scale="Blues",
)
fig.update_layout(yaxis=dict(autorange="reversed"), showlegend=False, height=500)
st.plotly_chart(fig, use_container_width=True)

# ── Active Bets vs Benchmark ───────────────────────────────────────
if active_share_result:
    st.subheader("Active Bets vs Benchmark")
    col_over, col_under = st.columns(2)

    top_bets: pd.DataFrame = active_share_result["top_active_bets"]

    if top_bets is not None and not top_bets.empty:
        overweights = top_bets.nlargest(10, "overweight")[
            ["name", "portfolio_weight", "benchmark_weight", "overweight"]
        ].copy()
        overweights.columns = ["Titolo", "Portafoglio %", "Benchmark %", "Sovrappeso %"]
        for c in ["Portafoglio %", "Benchmark %", "Sovrappeso %"]:
            overweights[c] = overweights[c].map(lambda x: f"{x:.2f}")
        overweights = overweights.reset_index(drop=True)

        underweights = top_bets.nsmallest(10, "overweight")[
            ["name", "portfolio_weight", "benchmark_weight", "overweight"]
        ].copy()
        underweights.columns = ["Titolo", "Portafoglio %", "Benchmark %", "Sottopeso %"]
        for c in ["Portafoglio %", "Benchmark %", "Sottopeso %"]:
            underweights[c] = underweights[c].map(lambda x: f"{x:.2f}")
        underweights = underweights.reset_index(drop=True)

        with col_over:
            st.markdown("**Top 10 Sovrappesi**")
            st.dataframe(overweights, use_container_width=True)
        with col_under:
            st.markdown("**Top 10 Sottopesi**")
            st.dataframe(underweights, use_container_width=True)
