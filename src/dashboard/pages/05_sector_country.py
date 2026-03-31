"""Page 5: Sector & Country — exposure breakdown + sunburst drill-down."""

from __future__ import annotations

import os
import sys

import streamlit as st

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

st.header("🌍 Sector & Country")

aggregated = st.session_state.get("aggregated")
if aggregated is None:
    st.info("Inserisci un portafoglio nella pagina **Portfolio Input** e lancia l'analisi.")
    st.stop()

import pandas as pd
import plotly.express as px

from src.analytics.aggregator import country_exposure, sector_exposure

sector_df = sector_exposure(aggregated)
country_df = country_exposure(aggregated)
benchmark_df = st.session_state.get("benchmark_df")

# ── Two-column pie/bar ──────────────────────────────────────────────
col_s, col_c = st.columns(2)

with col_s:
    st.subheader("Esposizione per Settore")
    fig_s = px.pie(
        sector_df,
        names="sector",
        values="weight_pct",
        hole=0.35,
    )
    fig_s.update_traces(textinfo="label+percent", textposition="outside")
    fig_s.update_layout(showlegend=False, height=450)
    st.plotly_chart(fig_s, use_container_width=True)

with col_c:
    st.subheader("Esposizione per Paese")
    top_countries = country_df.head(15)
    fig_c = px.bar(
        top_countries,
        x="weight_pct",
        y="country",
        orientation="h",
        labels={"weight_pct": "Peso (%)", "country": ""},
        color="weight_pct",
        color_continuous_scale="Viridis",
    )
    fig_c.update_layout(yaxis=dict(autorange="reversed"), showlegend=False, height=450)
    st.plotly_chart(fig_c, use_container_width=True)

# ── Benchmark deviation bars ────────────────────────────────────────
if benchmark_df is not None and not benchmark_df.empty:
    st.subheader("Deviazione vs Benchmark")
    tab_sec, tab_cou = st.tabs(["Per Settore", "Per Paese"])

    # Sector deviation
    bench_sector = benchmark_df.groupby("sector", as_index=False)["weight_pct"].sum()
    bench_sector.columns = ["sector", "bench_pct"]
    merged_s = sector_df.merge(bench_sector, on="sector", how="outer").fillna(0)
    merged_s["delta"] = merged_s["weight_pct"] - merged_s["bench_pct"]
    merged_s = merged_s.sort_values("delta", ascending=True)

    with tab_sec:
        fig_ds = px.bar(
            merged_s,
            x="delta",
            y="sector",
            orientation="h",
            labels={"delta": "Delta (%)", "sector": ""},
            color="delta",
            color_continuous_scale="RdYlGn",
            color_continuous_midpoint=0,
        )
        fig_ds.update_layout(height=max(300, len(merged_s) * 35))
        st.plotly_chart(fig_ds, use_container_width=True)

    # Country deviation
    bench_country = benchmark_df.groupby("country", as_index=False)["weight_pct"].sum()
    bench_country.columns = ["country", "bench_pct"]
    merged_c = country_df.merge(bench_country, on="country", how="outer").fillna(0)
    merged_c["delta"] = merged_c["weight_pct"] - merged_c["bench_pct"]
    merged_c = merged_c.sort_values("delta", ascending=True).tail(20)

    with tab_cou:
        fig_dc = px.bar(
            merged_c,
            x="delta",
            y="country",
            orientation="h",
            labels={"delta": "Delta (%)", "country": ""},
            color="delta",
            color_continuous_scale="RdYlGn",
            color_continuous_midpoint=0,
        )
        fig_dc.update_layout(height=max(300, len(merged_c) * 35))
        st.plotly_chart(fig_dc, use_container_width=True)

# ── Sunburst chart ──────────────────────────────────────────────────
st.subheader("Drill-down: Paese → Settore → Titolo")

sun_df = aggregated[["country", "sector", "name", "real_weight_pct"]].copy()
sun_df = sun_df.dropna(subset=["country", "sector"])
# Keep top entries for readability
sun_df = sun_df.nlargest(100, "real_weight_pct")

fig_sun = px.sunburst(
    sun_df,
    path=["country", "sector", "name"],
    values="real_weight_pct",
    color="real_weight_pct",
    color_continuous_scale="Blues",
)
fig_sun.update_layout(height=600)
st.plotly_chart(fig_sun, use_container_width=True)
