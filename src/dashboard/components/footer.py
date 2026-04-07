"""Trust footer component."""

from __future__ import annotations

import streamlit as st


def show_footer() -> None:
    """Show trust footer with data sources and methodology."""
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.caption("📊 **Dati**: iShares · Vanguard · Amundi · Xtrackers")
    with col2:
        st.caption("🔑 **Identity resolution**: OpenFIGI | **Prezzi**: yfinance")
    with col3:
        st.caption("⚙️ Factor fallback hierarchy | Coverage disclosure")
    st.markdown(
        "<div style='text-align:center; font-size:0.7rem; color:#9ca3af; "
        "padding-top:4px;'>ETF X-Ray Engine — Analisi indicativa. "
        "Non costituisce consulenza finanziaria ai sensi MiFID II.</div>",
        unsafe_allow_html=True,
    )
