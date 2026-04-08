"""Global KPI header bar shown on every page."""

from __future__ import annotations

import streamlit as st


def show_global_header() -> None:
    """Show global KPI bar if analysis is complete, otherwise placeholder."""
    aggregated = st.session_state.get("aggregated")

    if aggregated is None:
        st.info(
            "▶ Inserisci i tuoi ETF nella pagina **Portfolio Input** "
            "e clicca **Analizza Portafoglio** per vedere i risultati."
        )
        return

    from src.analytics.overlap import portfolio_hhi

    hhi_stats = portfolio_hhi(aggregated)
    active_share_result = st.session_state.get("active_share_result")
    active_share = active_share_result["active_share_pct"] if active_share_result else None

    n_holdings = len(aggregated)
    hhi = hhi_stats["hhi"]
    eff_n = hhi_stats["effective_n"]
    top10 = hhi_stats["top_10_pct"]

    bench_name = st.session_state.get("benchmark_name")
    bench_labels = {"MSCI_WORLD": "MSCI World", "SP500": "S&P 500",
                    "MSCI_EM": "MSCI EM", "FTSE_ALL_WORLD": "FTSE All-World"}
    bench_display = bench_labels.get(bench_name, "N/A") if bench_name else "Nessuno"

    hhi_badge = "🟢" if hhi < 0.10 else ("🟡" if hhi < 0.15 else "🔴")
    as_str = f"{active_share:.0f}%" if active_share else "N/D"

    cols = st.columns([1.5, 1, 1, 1, 1, 1, 1.5])
    cols[0].markdown("**Portfolio X-Ray**")
    cols[1].markdown(f"**{n_holdings:,}** titoli")
    cols[2].markdown(f"HHI {hhi:.3f} {hhi_badge}")
    cols[3].markdown(f"Eff.N **{eff_n:.0f}**")
    cols[4].markdown(f"Active Share {as_str}")
    cols[5].markdown(f"Top-10 **{top10:.1f}%**")
    cols[6].markdown(f"📐 vs {bench_display}")

    st.divider()
