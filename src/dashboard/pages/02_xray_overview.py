"""Page 2: X-Ray Overview — KPI cards, top holdings, active bets."""

from __future__ import annotations

import os
import sys

import streamlit as st

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

st.header("🔍 X-Ray Overview")
from src.dashboard.components.global_header import show_global_header
show_global_header()
from src.dashboard.components.observations_box import show_observations
show_observations(st.session_state.get("observations", []), "xray")

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

from src.dashboard.components.kpi_card import (
    render_active_share_card, render_effective_n_card,
    render_hhi_card, render_top10_card,
)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Titoli unici", f"{len(aggregated):,}", help="Numero totale di titoli unici nel portafoglio aggregato.")
render_hhi_card(hhi_stats["hhi"], k2)
render_effective_n_card(hhi_stats["effective_n"], k3)
render_active_share_card(active_share_pct, k4)
render_top10_card(hhi_stats["top_10_pct"], k5)

# ── Export PDF ─────────────────────────────────────────────────────
from datetime import datetime

redundancy_df_export = st.session_state.get("redundancy_df")
if redundancy_df_export is not None:
    if st.button("📄 Esporta Report PDF"):
        from src.dashboard.export.pdf_exporter import generate_report_pdf
        from src.analytics.recommendations import generate_recommendations

        positions = st.session_state.get("portfolio_positions", [])
        total_eur = sum(p["capital"] for p in positions)

        # Build xray_data
        top_h = aggregated.nlargest(15, "real_weight_pct")
        xray_data = {
            "n_holdings": len(aggregated),
            "hhi": hhi_stats["hhi"],
            "effective_n": hhi_stats["effective_n"],
            "active_share_pct": active_share_pct,
            "top_10_pct": hhi_stats["top_10_pct"],
            "top_holdings": [
                {"name": r["name"], "ticker": r["ticker"],
                 "weight": r["real_weight_pct"],
                 "sector": r.get("sector", ""), "country": r.get("country", "")}
                for _, r in top_h.iterrows()
            ],
        }

        # Redundancy
        red_list = redundancy_df_export.to_dict("records")

        # Overlap
        overlap_mat = st.session_state.get("overlap_matrix")
        ol_data = overlap_mat.values.tolist() if overlap_mat is not None else None
        ol_labels = overlap_mat.columns.tolist() if overlap_mat is not None else None

        # Recommendations
        red_scores = dict(zip(
            redundancy_df_export["etf_ticker"],
            redundancy_df_export["redundancy_pct"] / 100,
        ))
        ter_wasted = dict(zip(
            redundancy_df_export["etf_ticker"],
            redundancy_df_export["ter_wasted"],
        ))
        top1 = aggregated.nlargest(1, "real_weight_pct").iloc[0] if not aggregated.empty else None
        bench_name = st.session_state.get("benchmark_name") or "mercato"
        bench_labels_map = {"MSCI_WORLD": "MSCI World", "SP500": "S&P 500",
                            "MSCI_EM": "MSCI EM", "FTSE_ALL_WORLD": "FTSE All-World"}
        bench_display = bench_labels_map.get(bench_name, bench_name)

        recs = generate_recommendations(
            redundancy_scores=red_scores,
            ter_wasted_eur=ter_wasted,
            active_share=active_share_pct,
            hhi=hhi_stats["hhi"],
            top1_weight=(top1["real_weight_pct"] / 100) if top1 is not None else 0,
            top1_name=top1["name"] if top1 is not None else "",
            n_etf=len(positions),
            portfolio_total_eur=total_eur,
            benchmark_name=bench_display,
        )

        with st.spinner("Generazione report PDF..."):
            pdf_bytes = generate_report_pdf(
                portfolio=positions,
                benchmark_name=bench_display if bench_name else None,
                xray_data=xray_data,
                redundancy_data=red_list,
                overlap_data=ol_data,
                overlap_labels=ol_labels,
                recommendations=recs,
                factor_data=st.session_state.get("factor_result"),
            )

        st.download_button(
            label="⬇️ Scarica Report PDF",
            data=pdf_bytes,
            file_name=f"xray_report_{datetime.now().strftime('%Y%m%d')}.pdf",
            mime="application/pdf",
        )

# ── Top 30 holdings table ──────────────────────────────────────────
st.subheader("Top 30 titoli per peso reale")
significant = aggregated[aggregated["real_weight_pct"] >= 0.05]
n_filtered = len(aggregated) - len(significant)
filtered_weight = aggregated[aggregated["real_weight_pct"] < 0.05]["real_weight_pct"].sum()

_positions = st.session_state.get("portfolio_positions", [])
_total_eur = sum(p.get("capital", 0) for p in _positions) if _positions else 0

top30 = significant.nlargest(30, "real_weight_pct")[
    ["name", "ticker", "real_weight_pct", "n_etf_sources", "sector", "country"]
].copy()

# Add EUR equivalent column if total invested is available
if _total_eur > 0:
    top30.insert(
        3, "valore_eur",
        (top30["real_weight_pct"] / 100 * _total_eur).round(0).astype(int),
    )
    top30.columns = ["Titolo", "Ticker", "Peso Reale %", "Valore (€)", "N ETF", "Settore", "Paese"]
    top30["Peso Reale %"] = top30["Peso Reale %"].map(lambda x: f"{x:.2f}")
    top30["Valore (€)"] = top30["Valore (€)"].map(lambda x: f"€ {x:,}")
else:
    top30.columns = ["Titolo", "Ticker", "Peso Reale %", "N ETF", "Settore", "Paese"]
    top30["Peso Reale %"] = top30["Peso Reale %"].map(lambda x: f"{x:.2f}")

top30 = top30.reset_index(drop=True)
top30.index = top30.index + 1
st.dataframe(top30, use_container_width=True)
if n_filtered > 0:
    st.caption(f"Titoli con peso < 0.05% non mostrati "
               f"({n_filtered} titoli, {filtered_weight:.2f}% del totale).")

# ── EUR breakdown for retail investors ────────────────────────────
if _total_eur > 0:
    with st.expander("💶 Il tuo portafoglio, titolo per titolo", expanded=True):
        st.markdown("Basandosi sui pesi reali, il tuo portafoglio equivale a:")

        _top20_eur = significant.nlargest(20, "real_weight_pct").copy()
        _top20_eur["valore_eur"] = (
            _top20_eur["real_weight_pct"] / 100 * _total_eur
        ).round(0).astype(int)

        items = []
        for _, r in _top20_eur.iterrows():
            _name = r.get("name", r.get("ticker", "N/D"))
            items.append(f"**€{r['valore_eur']:,}** di {_name}")

        col1, col2 = st.columns(2)
        half = len(items) // 2
        with col1:
            for item in items[:half]:
                st.markdown(f"• {item}")
        with col2:
            for item in items[half:]:
                st.markdown(f"• {item}")

        _top20_pct = _top20_eur["real_weight_pct"].sum()
        _remaining_pct = 100 - _top20_pct
        _remaining_eur = int(_remaining_pct / 100 * _total_eur)
        _n_remaining = len(significant) - 20
        if _n_remaining > 0:
            st.caption(
                f"+ altri {_n_remaining} titoli per circa "
                f"€{_remaining_eur:,} ({_remaining_pct:.1f}% del totale)"
            )

        st.caption(
            "ℹ️ Valori calcolati sui pesi reali aggregati del portafoglio. "
            "Non rappresentano acquisti diretti di singole azioni."
        )

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
    from src.dashboard.components.chart_helpers import apply_bar_chart_style
    apply_bar_chart_style(fig, top20["real_weight_pct"].tolist())
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
            fig_c.update_traces(marker_color="#2563eb")
            fig_c.update_layout(
                yaxis=dict(autorange="reversed"),
                showlegend=False,
                height=250,
            )
            from src.dashboard.components.chart_helpers import apply_bar_chart_style
            apply_bar_chart_style(fig_c, top5_c["weight_pct"].tolist())
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
            fig_s.update_traces(marker_color="#16a34a")
            fig_s.update_layout(
                yaxis=dict(autorange="reversed"),
                showlegend=False,
                height=250,
            )
            apply_bar_chart_style(fig_s, top5_s["weight_pct"].tolist())
            st.plotly_chart(fig_s, use_container_width=True)
        else:
            st.info("Dati settoriali non disponibili.")

    st.caption("→ Analisi completa con deviazioni vs benchmark: pagina **Sector & Country**")
else:
    st.info("Dati settoriali e geografici non disponibili per questo portafoglio.")

# ── Actionable recommendations ─────────────────────────────────────
redundancy_df = st.session_state.get("redundancy_df")

if redundancy_df is not None and not redundancy_df.empty:
    from src.analytics.recommendations import generate_recommendations

    # Build inputs from session_state
    red_scores = dict(zip(
        redundancy_df["etf_ticker"],
        redundancy_df["redundancy_pct"] / 100,
    ))
    ter_wasted = dict(zip(
        redundancy_df["etf_ticker"],
        redundancy_df["ter_wasted"],
    ))

    positions = st.session_state.get("portfolio_positions", [])
    total_eur = sum(p["capital"] for p in positions)

    # Top holding
    top1 = aggregated.nlargest(1, "real_weight_pct").iloc[0] if not aggregated.empty else None
    top1_w = (top1["real_weight_pct"] / 100) if top1 is not None else 0
    top1_n = top1["name"] if top1 is not None else ""

    bench_name = st.session_state.get("benchmark_name") or "mercato"
    bench_labels = {"MSCI_WORLD": "MSCI World", "SP500": "S&P 500",
                    "MSCI_EM": "MSCI EM", "FTSE_ALL_WORLD": "FTSE All-World"}
    bench_display = bench_labels.get(bench_name, bench_name)

    total_ter_eur = sum(ter_wasted.values()) + sum(
        (1 - red_scores.get(p["ticker"], 0)) * 0.002 * p["capital"]
        for p in positions
    )

    recs = generate_recommendations(
        redundancy_scores=red_scores,
        ter_wasted_eur=ter_wasted,
        active_share=active_share_pct,
        hhi=hhi_stats["hhi"],
        top1_weight=top1_w,
        top1_name=top1_n,
        n_etf=len(positions),
        portfolio_total_eur=total_eur,
        benchmark_name=bench_display,
        current_total_ter_eur=total_ter_eur,
    )

    if recs:
        with st.expander("💡 Suggerimenti per il tuo portafoglio", expanded=True):
            for rec in sorted(recs, key=lambda r: {"high": 0, "medium": 1, "low": 2}[r.severity]):
                badge = {"high": "🔴 Alta priorità",
                         "medium": "🟡 Da valutare",
                         "low": "🟢 Nota"}[rec.severity]
                st.markdown(f"**{badge} — {rec.title}**")
                st.write(rec.explanation)
                st.markdown(f"→ *{rec.action}*")
                if rec.saving_eur_annual and rec.saving_eur_annual > 0:
                    st.success(f"💰 Risparmio potenziale: ~€{rec.saving_eur_annual:.0f}/anno")
                st.divider()

            st.caption(
                "ℹ️ Questi suggerimenti sono generati automaticamente dall'analisi "
                "quantitativa del portafoglio. Non costituiscono consulenza "
                "finanziaria. Consulta un professionista per decisioni di investimento."
            )

# ── Footer ─────────────────────────────────────────────────────────
from src.dashboard.components.footer import show_footer
show_footer()
