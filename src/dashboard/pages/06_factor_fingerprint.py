"""Page 6: Factor Fingerprint — radar chart, scores, coverage, drivers."""

from __future__ import annotations

import os
import sys

import streamlit as st

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

st.header("🧬 Factor Fingerprint")

aggregated = st.session_state.get("aggregated")
if aggregated is None:
    st.info("Inserisci un portafoglio nella pagina **Portfolio Input** e lancia l'analisi.")
    st.stop()

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

if 'real_weight_pct' in aggregated.columns:
    aggregated['real_weight_pct'] = pd.to_numeric(aggregated['real_weight_pct'], errors='coerce').fillna(0.0)

# ── Run factor engine on demand ─────────────────────────────────────
factor_result = st.session_state.get("factor_result")

if factor_result is None:
    if st.button("🔬 Calcola Factor Fingerprint", type="primary"):
        from src.factors.factor_engine import FactorEngine
        from src.storage.db import get_session_factory, init_db

        init_db()
        session = get_session_factory()()
        engine = FactorEngine(session)
        benchmark_df = st.session_state.get("benchmark_df")

        with st.spinner("Calcolo fattori in corso (può richiedere qualche minuto per dati yfinance)…"):
            factor_result = engine.analyze(
                aggregated,
                benchmark_df=benchmark_df if benchmark_df is not None else None,
            )
            st.session_state.factor_result = factor_result
        st.rerun()
    else:
        st.info("Clicca il bottone per lanciare l'analisi fattoriale (richiede fetch dati da yfinance).")
        st.stop()

scores = factor_result["factor_scores"]
coverage = factor_result["coverage_report"]
drivers = factor_result.get("factor_drivers", {})
bench_cmp = factor_result.get("benchmark_comparison")

# ── Radar chart ─────────────────────────────────────────────────────
st.subheader("Radar — profilo fattoriale")

# Normalize scores to 0-100 for radar
size_score = scores["size"].get("Large", 0)
vg = scores["value_growth"]
pe_score = max(0, min(100, 100 - (vg.get("weighted_pe", 20) or 20)))  # Lower PE = higher value
roe_raw = scores["quality"].get("weighted_roe", 0) or 0
# yfinance returns ROE as decimal (0.18 = 18%) — convert to percentage
roe_val = roe_raw * 100 if roe_raw < 1 else roe_raw
quality_roe = min(100, roe_val * 0.5)  # 20% ROE → score 10
div_raw = scores["dividend_yield"].get("weighted_yield", 0) or 0
# yfinance returns dividend yield as decimal (0.015 = 1.5%) — convert to percentage
div_yield = div_raw * 100 if div_raw < 1 else div_raw
div_score = min(100, div_yield * 20)  # 2% yield → score 40

dimensions = ["Size (Large Cap)", "Value", "Quality", "Dividend Yield"]
portfolio_vals = [size_score, pe_score, quality_roe, div_score]

fig_radar = go.Figure()
fig_radar.add_trace(go.Scatterpolar(
    r=portfolio_vals + [portfolio_vals[0]],
    theta=dimensions + [dimensions[0]],
    fill="toself",
    name="Portafoglio",
    line_color="#3498db",
))

if bench_cmp and st.session_state.get("benchmark_name") is not None:
    # Benchmark approximation from deltas
    roe_delta = (bench_cmp.get("quality", {}).get("roe_delta", 0) or 0)
    roe_delta_scaled = roe_delta * 100 if abs(roe_delta) < 1 else roe_delta
    dy_delta = (bench_cmp.get("dividend_yield", {}).get("yield_delta", 0) or 0)
    dy_delta_scaled = dy_delta * 100 if abs(dy_delta) < 1 else dy_delta
    bench_vals = [
        max(0, size_score - (bench_cmp.get("size", {}).get("Large_delta", 0) or 0)),
        max(0, pe_score - ((bench_cmp.get("value_growth", {}).get("pe_delta", 0) or 0) * 2)),
        max(0, quality_roe - (roe_delta_scaled * 0.5)),
        max(0, div_score - (dy_delta_scaled * 20)),
    ]
    fig_radar.add_trace(go.Scatterpolar(
        r=bench_vals + [bench_vals[0]],
        theta=dimensions + [dimensions[0]],
        fill="toself",
        name="Benchmark",
        line_color="#e74c3c",
        opacity=0.5,
    ))

fig_radar.update_layout(
    polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
    showlegend=True,
    height=500,
)
st.plotly_chart(fig_radar, use_container_width=True)
st.caption("Nota: il fattore Momentum sarà disponibile in una versione futura.")

with st.expander("ℹ️ Cos'è il Factor Fingerprint?"):
    st.markdown(
        "Il \"DNA\" del tuo portafoglio lungo 4 dimensioni:\n\n"
        "- **Size (Dimensione):** Large-cap (>$10B), Mid-cap ($2-10B) o Small-cap (<$2B). "
        "La maggior parte degli ETF globali è dominata da large-cap.\n\n"
        "- **Value/Growth:** P/E (prezzo/utili) basso = Value (aziende mature, dividendi). "
        "P/E alto = Growth (aziende in crescita, reinvestono utili). "
        "Un P/E medio sopra 25 indica tilt Growth.\n\n"
        "- **Quality:** ROE (ritorno sul patrimonio) alto e debito basso = aziende solide. "
        "ROE basso e debito alto = aziende più rischiose.\n\n"
        "- **Dividend Yield:** Quanto reddito generano le aziende nel tuo portafoglio come "
        "dividendi. Yield sotto 1% è tipico di portafogli Growth, sopra 3% di portafogli Income."
    )

# ── Factor scores table ─────────────────────────────────────────────
st.subheader("Factor Scores")
has_bench = bench_cmp and st.session_state.get("benchmark_name") is not None
rows = []
row_base = [
    ("Size (% Large Cap)", f"{size_score:.1f}%", f"{bench_cmp['size'].get('Large_delta', 'N/A')}" if has_bench else None),
    ("Value (P/E medio)", f"{vg.get('weighted_pe', 'N/A')}", f"{bench_cmp['value_growth'].get('pe_delta', 'N/A')}" if has_bench else None),
    ("Value (P/B medio)", f"{vg.get('weighted_pb', 'N/A')}", f"{bench_cmp['value_growth'].get('pb_delta', 'N/A')}" if has_bench else None),
    ("Quality (ROE %)", f"{roe_val:.1f}%", f"{bench_cmp['quality'].get('roe_delta', 'N/A')}" if has_bench else None),
    ("Dividend Yield %", f"{div_yield:.2f}%", f"{bench_cmp['dividend_yield'].get('yield_delta', 'N/A')}" if has_bench else None),
]
for dim, port, delta in row_base:
    row = {"Dimensione": dim, "Portafoglio": port}
    if has_bench:
        row["Delta Benchmark"] = delta
    rows.append(row)

st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

# ── Coverage disclosure ─────────────────────────────────────────────
st.subheader("Coverage Disclosure")

total_h = coverage.get("total_holdings", 1) or 1
l1 = coverage.get("L1_pct", 0)
l2 = coverage.get("L2_pct", 0)
l3 = coverage.get("L3_pct", 0)
l4 = coverage.get("L4_pct", 0)

cov_cols = st.columns(4)
cov_cols[0].metric("L1 — Sector", f"{l1:.1f}%")
cov_cols[1].metric("L2 — Fundamentals", f"{l2:.1f}%")
cov_cols[2].metric("L3 — Proxy", f"{l3:.1f}%")
cov_cols[3].metric("L4 — Unclassified", f"{l4:.1f}%")

# Stacked bar
fig_cov = go.Figure()
fig_cov.add_trace(go.Bar(name="L1 Sector", x=[l1], y=["Coverage"], orientation="h", marker_color="#2ecc71"))
fig_cov.add_trace(go.Bar(name="L2 Fundamentals", x=[l2], y=["Coverage"], orientation="h", marker_color="#3498db"))
fig_cov.add_trace(go.Bar(name="L3 Proxy", x=[l3], y=["Coverage"], orientation="h", marker_color="#f39c12"))
fig_cov.add_trace(go.Bar(name="L4 Unclassified", x=[l4], y=["Coverage"], orientation="h", marker_color="#e74c3c"))
fig_cov.update_layout(barmode="stack", height=120, xaxis=dict(range=[0, 100], title="%"), yaxis=dict(visible=False))
st.plotly_chart(fig_cov, use_container_width=True)

with st.expander("ℹ️ Cos'è la Coverage?"):
    st.markdown(
        "Non tutti i titoli hanno dati fondamentali disponibili. "
        "La barra mostra quanta percentuale del portafoglio è stata analizzata e con quale fonte:\n\n"
        "- **L1 Sector** — classificazione settoriale disponibile\n"
        "- **L2 Fundamentals** — dati reali (P/E, ROE, etc.) da yfinance\n"
        "- **L3 Proxy** — stima basata sulla media del settore\n"
        "- **L4 Unclassified** — nessun dato disponibile"
    )

# ── Factor Drivers ──────────────────────────────────────────────────
st.subheader("Factor Drivers")

driver_tabs = {
    "Value/Growth": "value_growth",
    "Quality": "quality",
    "Size": "size",
}

for label, key in driver_tabs.items():
    drv = drivers.get(key, [])
    if not drv:
        continue
    with st.expander(f"🔎 {label} — top 5 driver"):
        drv_df = pd.DataFrame(drv[:5])
        st.dataframe(drv_df, use_container_width=True, hide_index=True)
