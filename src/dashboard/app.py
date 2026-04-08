"""ETF X-Ray Engine — Streamlit Dashboard entry point."""

import streamlit as st

st.set_page_config(
    page_title="ETF X-Ray Engine",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
)

import os

def _load_css():
    css_path = os.path.join(os.path.dirname(__file__), "styles", "global.css")
    with open(css_path) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

_load_css()

# ── Session state defaults ──────────────────────────────────────────
_DEFAULTS: dict = {
    "portfolio_positions": [],   # [{"ticker": str, "capital": float}, ...]
    "holdings_db": {},           # ticker -> resolved DataFrame
    "aggregated": None,          # aggregate_portfolio() output
    "benchmark_name": "MSCI_WORLD",
    "benchmark_df": None,
    "overlap_matrix": None,
    "redundancy_df": None,
    "factor_result": None,
    "active_share_result": None,
    "display_names": {},
}

for key, default in _DEFAULTS.items():
    if key not in st.session_state:
        st.session_state[key] = default

# ── Sidebar ─────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🔬 ETF X-Ray Engine")
    st.caption("Analisi trasparente del tuo portafoglio ETF")
    st.divider()

    n_etf = len(st.session_state.portfolio_positions)
    if n_etf:
        display_names = st.session_state.get("display_names", {})
        tickers = ", ".join(
            display_names.get(p["ticker"], p["ticker"])
            for p in st.session_state.portfolio_positions
        )
        st.success(f"**{n_etf} ETF** in portafoglio: {tickers}")
    else:
        st.info("Nessun ETF caricato — vai a **Portfolio Input**")

# ── Landing page ────────────────────────────────────────────────────
st.header("Benvenuto in ETF X-Ray Engine")
st.markdown(
    """
Usa il menu laterale per navigare tra le schermate:

1. **Portfolio Input** — inserisci i tuoi ETF e lancia l'analisi
2. **X-Ray Overview** — KPI, titoli principali, active bets
3. **Overlap & Ridondanza** — ridondanza per ETF, overlap pairwise, esposizione unica
4. **Sector & Country** — esposizione settoriale e geografica
5. **Factor Fingerprint** — profilo fattoriale e confronto benchmark
"""
)
