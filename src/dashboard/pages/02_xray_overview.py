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

# ── Synthetic replication warning ──────────────────────────────────
KNOWN_SYNTHETIC = {"CW8", "IE00B6YX5D40", "XDWD", "DBXD", "LYX0AG",
                   "LYXMWL", "FR0010315770", "LU0392494562"}

_synthetic_etfs = []
for p in st.session_state.get("portfolio_positions", []):
    _id = p["ticker"].upper()
    if _id in KNOWN_SYNTHETIC:
        _synthetic_etfs.append(_id)
    elif "SWAP" in _id or "SYNTHETIC" in _id:
        _synthetic_etfs.append(_id)

if _synthetic_etfs:
    st.warning(
        f"⚠️ **{', '.join(_synthetic_etfs)}** — replica sintetica (swap-based ETF). "
        "Le holdings mostrate sono il collateral basket del contratto swap, "
        "non i titoli che il fondo replica economicamente. "
        "L'analisi di overlap, sector e country potrebbe non riflettere "
        "l'esposizione reale del fondo."
    )

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
k5.metric("Top-10 Conc.", f"{hhi_stats['top_10_pct']:.2f} %")

# ── KPI explanations ───────────────────────────────────────────────
with st.expander("ℹ️ Cos'è HHI (Indice di Concentrazione)?"):
    st.markdown(
        "Misura quanto il tuo portafoglio dipende da pochi titoli. "
        "Più è basso, meglio è.\n\n"
        "- **Sotto 0.05** = ben diversificato\n"
        "- **Sopra 0.15** = troppo concentrato\n\n"
        "Se i tuoi top titoli crollano, un HHI alto significa che il tuo portafoglio "
        "ne risente pesantemente."
    )

with st.expander("ℹ️ Cos'è Effective N?"):
    st.markdown(
        "Il numero equivalente di titoli nel tuo portafoglio se fossero tutti con lo stesso peso. "
        "Hai 500 titoli ma Effective N è 30? Significa che il portafoglio è dominato da pochi nomi "
        "— si comporta come se ne avessi solo 30."
    )

if active_share_pct is not None:
    with st.expander("ℹ️ Cos'è Active Share?"):
        st.markdown(
            "Quanto il tuo portafoglio è diverso dal benchmark (es. MSCI World).\n\n"
            "- **0%** = identico al mercato\n"
            "- **100%** = completamente diverso\n\n"
            "- **Sotto 20%** = stai pagando più TER per replicare essenzialmente un indice\n"
            "- **Sopra 60%** = portafoglio molto diverso dal mercato, "
            "con rischi e opportunità specifiche"
        )

with st.expander("ℹ️ Cos'è Top-10 Concentration?"):
    st.markdown(
        "La somma dei pesi dei tuoi 10 titoli più grandi. "
        "Se è 35%, un terzo del tuo portafoglio dipende da 10 aziende."
    )

# ── Top 30 holdings table ──────────────────────────────────────────
st.subheader("Top 30 titoli per peso reale")
significant = aggregated[aggregated["real_weight_pct"] >= 0.05]
n_filtered = len(aggregated) - len(significant)
filtered_weight = aggregated[aggregated["real_weight_pct"] < 0.05]["real_weight_pct"].sum()

top30 = significant.nlargest(30, "real_weight_pct")[
    ["name", "ticker", "real_weight_pct", "n_etf_sources", "sector", "country"]
].copy()
top30.columns = ["Titolo", "Ticker", "Peso Reale %", "N ETF", "Settore", "Paese"]
top30["Peso Reale %"] = top30["Peso Reale %"].map(lambda x: f"{x:.2f}")
top30 = top30.reset_index(drop=True)
top30.index = top30.index + 1
st.dataframe(top30, use_container_width=True)
if n_filtered > 0:
    st.caption(f"Titoli con peso < 0.05% non mostrati "
               f"({n_filtered} titoli, {filtered_weight:.2f}% del totale).")

# ── Bar chart top 20 ───────────────────────────────────────────────
with st.expander("📊 Visualizza grafico esposizione (Top 20)", expanded=False):
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

    top_bets: pd.DataFrame = active_share_result["top_active_bets"]
    missed: pd.DataFrame = active_share_result["missed_exposures"]

    st.markdown("**Top 10 Sovrappesi**")
    if top_bets is not None and not top_bets.empty:
        overweights = top_bets.nlargest(10, "overweight")[
            ["name", "portfolio_weight", "benchmark_weight", "overweight"]
        ].copy()
        overweights.columns = ["Titolo", "Portafoglio %", "Benchmark %", "Delta %"]
        overweights["Portafoglio %"] = overweights["Portafoglio %"].map(lambda x: f"{x:.2f}")
        overweights["Benchmark %"] = overweights["Benchmark %"].map(lambda x: f"{x:.2f}")
        overweights["Delta %"] = overweights["Delta %"].map(lambda x: f"+{x:.2f}" if x >= 0 else f"{x:.2f}")
        overweights = overweights.reset_index(drop=True)
        st.dataframe(overweights, use_container_width=True, hide_index=True)
    else:
        st.info("Nessun sovrappeso rilevato.")

    st.markdown("**Top 10 Sottopesi (assenti dal portafoglio)**")
    if missed is not None and not missed.empty:
        underweights = missed.nlargest(10, "benchmark_weight")[
            ["name", "benchmark_weight"]
        ].copy()
        underweights.columns = ["Titolo", "Benchmark %"]
        underweights["Benchmark %"] = underweights["Benchmark %"].map(lambda x: f"{x:.2f}")
        underweights = underweights.reset_index(drop=True)
        st.dataframe(underweights, use_container_width=True, hide_index=True)
    else:
        st.info("Nessun titolo benchmark significativo assente dal portafoglio.")

# ── Sector/Country preview ─────────────────────────────────────────
from src.analytics.aggregator import country_exposure, sector_exposure

sector_df = sector_exposure(aggregated)
country_df = country_exposure(aggregated)

if not sector_df.empty or not country_df.empty:
    st.subheader("🌍 Esposizione geografica e settoriale")
    col_geo, col_sec = st.columns(2)

    with col_geo:
        st.markdown("**Top 5 paesi**")
        if not country_df.empty:
            top5_c = country_df.head(5)
            fig_c = px.bar(
                top5_c,
                x="weight_pct",
                y="country",
                orientation="h",
                labels={"weight_pct": "%", "country": ""},
                text=top5_c["weight_pct"].map(lambda x: f"{x:.1f}%"),
            )
            fig_c.update_traces(marker_color="#2563eb", textposition="outside")
            fig_c.update_layout(
                yaxis=dict(autorange="reversed"),
                showlegend=False,
                height=250,
                margin=dict(l=0, r=40, t=10, b=10),
            )
            st.plotly_chart(fig_c, use_container_width=True)
        else:
            st.info("Dati geografici non disponibili.")

    with col_sec:
        st.markdown("**Top 5 settori**")
        if not sector_df.empty:
            top5_s = sector_df.head(5)
            fig_s = px.bar(
                top5_s,
                x="weight_pct",
                y="sector",
                orientation="h",
                labels={"weight_pct": "%", "sector": ""},
                text=top5_s["weight_pct"].map(lambda x: f"{x:.1f}%"),
            )
            fig_s.update_traces(marker_color="#16a34a", textposition="outside")
            fig_s.update_layout(
                yaxis=dict(autorange="reversed"),
                showlegend=False,
                height=250,
                margin=dict(l=0, r=40, t=10, b=10),
            )
            st.plotly_chart(fig_s, use_container_width=True)
        else:
            st.info("Dati settoriali non disponibili.")

    st.caption("→ Analisi completa con deviazioni vs benchmark: pagina **Sector & Country**")
else:
    st.info("Dati settoriali e geografici non disponibili per questo portafoglio.")
