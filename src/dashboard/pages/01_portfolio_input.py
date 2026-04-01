"""Page 1: Portfolio Input — add ETFs, choose benchmark, run analysis."""

from __future__ import annotations

import os
import sys

import streamlit as st

# Ensure project root is on sys.path for imports
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# ── Session state defaults (safe for direct page navigation) ───────
_DEFAULTS: dict = {
    "portfolio_positions": [],
    "holdings_db": {},
    "aggregated": None,
    "benchmark_name": "MSCI_WORLD",
    "benchmark_df": None,
    "overlap_matrix": None,
    "redundancy_df": None,
    "factor_result": None,
    "active_share_result": None,
}
for _key, _default in _DEFAULTS.items():
    if _key not in st.session_state:
        st.session_state[_key] = _default

st.header("📥 Portfolio Input")

# ── Form: add ETF ───────────────────────────────────────────────────
with st.form("add_etf_form", clear_on_submit=True):
    col1, col2 = st.columns([2, 1])
    with col1:
        ticker_input = st.text_input("Ticker / ISIN", placeholder="es. CSPX, SWDA, VWCE")
    with col2:
        capital_input = st.number_input("Importo (EUR)", min_value=0.0, value=10000.0, step=500.0)
    st.caption("Inserisci il ticker (es. CSPX, SWDA) o l'ISIN (es. IE00B5BMR087). I nomi completi non sono supportati.")
    submitted = st.form_submit_button("➕ Aggiungi ETF")

if submitted and ticker_input.strip():
    ticker = ticker_input.strip().upper()
    existing_tickers = {p["ticker"] for p in st.session_state.portfolio_positions}
    if ticker in existing_tickers:
        st.warning(f"{ticker} è già nel portafoglio.")
    else:
        st.session_state.portfolio_positions.append(
            {"ticker": ticker, "capital": capital_input}
        )
        # Invalidate computed results
        for key in ("aggregated", "overlap_matrix", "redundancy_df",
                     "factor_result", "active_share_result", "benchmark_df"):
            st.session_state[key] = None
        st.session_state.holdings_db.pop(ticker, None)
        st.rerun()

# ── Current portfolio ───────────────────────────────────────────────
positions: list[dict] = st.session_state.portfolio_positions

if not positions:
    st.info("Aggiungi almeno un ETF per iniziare.")
    st.stop()

st.subheader("Portafoglio attuale")

for idx, pos in enumerate(positions):
    col_t, col_c, col_r = st.columns([3, 2, 1])
    col_t.write(f"**{pos['ticker']}**")
    col_c.write(f"€ {pos['capital']:,.0f}")
    if col_r.button("🗑️", key=f"rm_{idx}"):
        st.session_state.portfolio_positions.pop(idx)
        st.session_state.holdings_db.pop(pos["ticker"], None)
        for key in ("aggregated", "overlap_matrix", "redundancy_df",
                     "factor_result", "active_share_result"):
            st.session_state[key] = None
        st.rerun()

total_capital = sum(p["capital"] for p in positions)
st.caption(f"Totale investito: **€ {total_capital:,.0f}**")

# ── Benchmark selector ──────────────────────────────────────────────
st.divider()
BENCHMARK_OPTIONS = {
    "MSCI World (SWDA/IWDA)": "MSCI_WORLD",
    "S&P 500 (CSPX)": "SP500",
    "MSCI EM (EIMI)": "MSCI_EM",
    "FTSE All-World (VWCE)": "FTSE_ALL_WORLD",
}
bench_label = st.selectbox(
    "Benchmark di riferimento",
    options=list(BENCHMARK_OPTIONS.keys()),
    index=0,
)
st.session_state.benchmark_name = BENCHMARK_OPTIONS[bench_label]

# ── Analyse button ──────────────────────────────────────────────────
st.divider()

force_refresh = st.checkbox("🔄 Forza aggiornamento (ignora cache)", value=False)

if st.button("🚀 Analizza Portafoglio", type="primary", use_container_width=True):
    from dotenv import load_dotenv

    load_dotenv()

    from src.analytics.active_share import active_share
    from src.analytics.aggregator import aggregate_portfolio
    from src.analytics.benchmark import BenchmarkManager
    from src.analytics.overlap import overlap_matrix
    from src.analytics.redundancy import redundancy_scores
    from src.ingestion.orchestrator import FetchOrchestrator
    from src.storage.cache import HoldingsCacheManager
    from src.storage.db import get_session_factory, init_db

    init_db()
    session_factory = get_session_factory()
    cache_manager = HoldingsCacheManager(session_factory)
    orchestrator = FetchOrchestrator(cache=cache_manager)

    holdings_db: dict = st.session_state.holdings_db
    n = len(positions)
    status_container = st.status(f"Analisi di {n} ETF…", expanded=True)

    for i, pos in enumerate(positions):
        ticker = pos["ticker"]
        step = f"({i + 1}/{n})"
        if ticker in holdings_db and not force_refresh:
            status_container.write(f"⚡ {ticker} {step} — già in memoria")
            continue
        try:
            status_container.update(label=f"Scaricamento {ticker}… {step}")
            result = orchestrator.fetch(ticker, force_refresh=force_refresh)

            if result.status == "cached":
                status_container.write(f"⚡ {ticker} {step} — cache ({result.message})")
            elif result.status == "success":
                n_h = len(result.holdings) if result.holdings is not None else 0
                status_container.write(f"✅ {ticker} {step} — {n_h} holdings da {result.source}")
            elif result.status == "partial":
                status_container.write(f"⚠️ {ticker} {step} — {result.message}")
            elif result.status == "failed":
                st.error(f"❌ {ticker}: {result.message}")
                continue

            if result.holdings is not None:
                holdings_db[ticker] = result.holdings
        except Exception as exc:
            st.error(f"Errore per {ticker}: {exc}")

    status_container.update(label=f"Analisi di {n} ETF completata", state="complete", expanded=False)

    progress.progress(1.0, text="Aggregazione…")
    st.session_state.holdings_db = holdings_db

    # Aggregate
    try:
        aggregated = aggregate_portfolio(positions, holdings_db)
        st.session_state.aggregated = aggregated
    except Exception as exc:
        st.error(f"Errore aggregazione: {exc}")
        st.stop()

    # Overlap
    try:
        if len(holdings_db) >= 2:
            st.session_state.overlap_matrix = overlap_matrix(holdings_db)
    except Exception as exc:
        st.warning(f"Overlap non calcolato: {exc}")

    # Redundancy
    try:
        st.session_state.redundancy_df = redundancy_scores(positions, holdings_db)
    except Exception as exc:
        st.warning(f"Redundancy non calcolato: {exc}")

    # Benchmark + Active Share
    try:
        bmgr = BenchmarkManager()
        bench_df = bmgr.get_benchmark_holdings(st.session_state.benchmark_name)
        st.session_state.benchmark_df = bench_df
        as_result = active_share(aggregated, bench_df)
        st.session_state.active_share_result = as_result
    except Exception as exc:
        st.warning(f"Benchmark/Active Share non calcolato: {exc}")

    status_container.update(state="complete", expanded=False)
    st.success("✅ Analisi completata! Naviga alle altre pagine per esplorare i risultati.")
