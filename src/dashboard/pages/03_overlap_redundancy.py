"""Page 3: Overlap & Ridondanza — redundancy breakdown + overlap heatmap."""

from __future__ import annotations

import os
import sys

import streamlit as st

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

st.header("🔄 Overlap & Ridondanza")
from src.dashboard.components.global_header import show_global_header
show_global_header()
from src.dashboard.components.observations_box import show_observations
show_observations(st.session_state.get("observations", []), "overlap_redundancy")

redundancy_df = st.session_state.get("redundancy_df")
overlap_mat = st.session_state.get("overlap_matrix")
holdings_db: dict = st.session_state.get("holdings_db", {})

if redundancy_df is None:
    st.info("Inserisci un portafoglio nella pagina **Portfolio Input** e lancia l'analisi.")
    st.stop()

import pandas as pd
import plotly.figure_factory as ff

from src.dashboard.components.display_utils import get_display_name, map_display_names
from src.dashboard.styles.colors import GREEN_LIGHT, RED_LIGHT, YELLOW_LIGHT

if "real_weight_pct" in redundancy_df.columns:
    redundancy_df["real_weight_pct"] = pd.to_numeric(
        redundancy_df["real_weight_pct"], errors="coerce"
    ).fillna(0.0)

redundancy_df = redundancy_df.copy()
redundancy_df["display_name"] = redundancy_df["etf_ticker"].map(get_display_name)

# ── LAYER 1: Summary ──────────────────────────────────────────────
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

# ── LAYER 2: Per-ETF Breakdown ────────────────────────────────────
st.subheader("Dettaglio ridondanza per ETF")

from src.analytics.redundancy import redundancy_breakdown

for _, row in redundancy_df.iterrows():
    raw_ticker = row["etf_ticker"]
    display = row["display_name"]
    r_pct = row["redundancy_pct"]
    ter = row.get("ter_wasted", 0) or 0
    verdict = row["verdict"]
    icon = {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(verdict, "⚪")

    with st.container():
        col_name, col_bar = st.columns([1, 3])
        with col_name:
            st.markdown(f"**{icon} {display}**")
        with col_bar:
            st.progress(min(r_pct / 100, 1.0))

        # Breakdown: which ETFs contribute to this redundancy?
        breakdown = redundancy_breakdown(raw_ticker, holdings_db)
        if breakdown:
            parts = []
            for other_raw, contrib in sorted(breakdown.items(), key=lambda x: -x[1]):
                if contrib > 0.5:  # Only show meaningful contributions
                    other_display = get_display_name(other_raw)
                    parts.append(f"**{other_display}** {contrib:.0f}%")
            if parts:
                st.caption(f"Coperto da: {', '.join(parts)}")

        st.caption(f"TER sprecato: **€{ter:,.2f}**/anno · Ridondanza: **{r_pct:.1f}%**")
        st.markdown("---")

with st.expander("ℹ️ Cos'è la Ridondanza?"):
    st.markdown(
        "Per ogni ETF, misura quanta percentuale delle sue holdings è già presente "
        "in almeno un altro ETF del tuo portafoglio.\n\n"
        "**Attenzione:** Redundancy 100% **non** significa overlap 100% con un singolo ETF. "
        "Significa che tutti i titoli di questo ETF sono presenti in almeno uno degli altri "
        "ETF nel tuo portafoglio — ma possono essere distribuiti su più ETF diversi.\n\n"
        "Ad esempio, CSPX (S&P 500) può avere ridondanza 99% perché SWDA (MSCI World) "
        "contiene quasi tutti i suoi titoli. Ma l'overlap pairwise tra CSPX e SWDA è solo "
        "~53% perché SWDA contiene anche molti titoli NON presenti in CSPX.\n\n"
        "- 🟢 **< 30%** — bassa ridondanza, aggiunge esposizione unica\n"
        "- 🟡 **30-70%** — moderata, valuta se giustifica il TER\n"
        "- 🔴 **> 70%** — alta ridondanza, considera di rimuoverlo"
    )

# ── LAYER 3: Heatmap drill-down (collapsible) ────────────────────
with st.expander("📊 Dettaglio overlap per coppia"):
    if overlap_mat is not None and not overlap_mat.empty:
        # Heatmap
        st.subheader("Matrice Overlap Pairwise")
        raw_labels = overlap_mat.columns.tolist()
        labels = map_display_names(raw_labels)
        z = overlap_mat.values.tolist()
        annotations = [[f"{v:.1f}%" for v in r] for r in z]

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

        # Shared holdings detail
        from src.analytics.overlap import shared_holdings

        st.subheader("Titoli in comune")
        tickers = list(holdings_db.keys())
        display_tickers = map_display_names(tickers)
        if len(tickers) >= 2:
            col1, col2 = st.columns(2)
            with col1:
                etf_a_display = st.selectbox("ETF A", display_tickers, index=0)
            with col2:
                default_b = 1 if len(display_tickers) > 1 else 0
                etf_b_display = st.selectbox("ETF B", display_tickers, index=default_b)

            _display_to_raw = dict(zip(display_tickers, tickers))
            etf_a = _display_to_raw[etf_a_display]
            etf_b = _display_to_raw[etf_b_display]

            if etf_a != etf_b and etf_a in holdings_db and etf_b in holdings_db:
                shared = shared_holdings(holdings_db[etf_a], holdings_db[etf_b])
                if shared.empty:
                    st.info("Nessun titolo in comune.")
                else:
                    display_sh = shared[["name", "weight_a", "weight_b", "weight_diff"]].copy()
                    display_sh.columns = [
                        "Titolo", f"Peso {etf_a_display} %",
                        f"Peso {etf_b_display} %", "Delta %",
                    ]
                    for c in display_sh.columns[1:]:
                        display_sh[c] = display_sh[c].map(lambda x: f"{x:.2f}")
                    display_sh = display_sh.head(30).reset_index(drop=True)
                    display_sh.index = display_sh.index + 1
                    st.dataframe(display_sh, use_container_width=True)
            elif etf_a_display == etf_b_display:
                st.warning("Seleziona due ETF diversi.")

        # Unique exposure analysis
        from src.analytics.overlap import compute_unique_exposure

        st.subheader("🔍 Cosa perdi rimuovendo un ETF?")
        tickers_all = list(holdings_db.keys())
        display_tickers_all = map_display_names(tickers_all)

        if len(tickers_all) >= 2:
            target_display = st.selectbox(
                "Seleziona ETF da analizzare",
                display_tickers_all,
                key="unique_exposure_target",
            )
            _display_to_raw_all = dict(zip(display_tickers_all, tickers_all))
            target = _display_to_raw_all[target_display]

            if target:
                ue = compute_unique_exposure(target, holdings_db)
                unique_pct = ue["total_unique_pct"]
                unique_count = ue["unique_holdings_count"]
                main_etf = get_display_name(ue["main_covering_etf"])

                if unique_pct < 5:
                    st.success(
                        f"Rimuovendo **{target_display}**: impatto minimo — "
                        f"{target_display} è ampiamente ridondante. Rimozione suggerita.\n\n"
                        f"• Esposizione unica: **{unique_pct:.1f}%** su {unique_count} titoli\n"
                        f"• La maggior parte già coperta da: **{main_etf}**"
                    )
                elif unique_pct < 15:
                    st.warning(
                        f"Rimuovendo **{target_display}**: impatto moderato — "
                        f"valuta se l'esposizione unica giustifica il TER.\n\n"
                        f"• Esposizione unica: **{unique_pct:.1f}%** su {unique_count} titoli\n"
                        f"• La maggior parte già coperta da: **{main_etf}**"
                    )
                else:
                    st.error(
                        f"Rimuovendo **{target_display}**: impatto significativo — "
                        f"{target_display} contribuisce esposizione difficilmente sostituibile.\n\n"
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
                        "Titolo", f"Peso in {target_display} %", "Coperto da altri %",
                        "Unico %", "Coperto da",
                    ]
                    for c in [f"Peso in {target_display} %", "Coperto da altri %", "Unico %"]:
                        display_detail[c] = display_detail[c].map(lambda x: f"{x:.2f}")
                    display_detail["Coperto da"] = display_detail["Coperto da"].map(
                        lambda x: get_display_name(x) if isinstance(x, str) else x
                    )
                    st.dataframe(display_detail, use_container_width=True, hide_index=True)
        else:
            st.info("Servono almeno 2 ETF per l'analisi di esposizione unica.")
    else:
        st.info("Servono almeno 2 ETF per la matrice di overlap.")

# ── Footer ─────────────────────────────────────────────────────────
from src.dashboard.components.footer import show_footer
show_footer()
