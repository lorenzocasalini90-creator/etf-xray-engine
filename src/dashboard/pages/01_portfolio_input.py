"""Page 1: Portfolio Input — add ETFs, choose benchmark, run analysis."""

from __future__ import annotations

import hashlib
import os
import sys
import time

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
    "analysis_hash": None,
    "analysis_timestamp": None,
    "display_names": {},
    "editing_etf_idx": None,
}
for _key, _default in _DEFAULTS.items():
    if _key not in st.session_state:
        st.session_state[_key] = _default

st.header("📥 Portfolio Input")

tab_manual, tab_upload = st.tabs(["📋 Inserisci manualmente", "📤 Carica da file"])

# ── Tab 1: Manual input with autocomplete ──────────────────────────
with tab_manual:
    from src.dashboard.data.etf_directory import search_etf

    query = st.text_input(
        "Ticker o nome ETF",
        placeholder="Es: SWDA, VWCE, iShares World, Vanguard All...",
        key="etf_search_input",
    )

    selected_ticker = None
    if query and len(query.strip()) >= 2:
        results = search_etf(query)
        if results:
            options = ["— Seleziona —"] + [
                f"{r['ticker']} — {r['name']} (TER {r['ter_pct']}%)"
                for r in results
            ]
            selected = st.selectbox(
                "Risultati trovati:",
                options,
                key="etf_search_select",
                label_visibility="collapsed",
            )
            if selected != "— Seleziona —":
                selected_ticker = selected.split(" — ")[0]
        elif len(query.strip()) >= 3:
            st.caption("Non trovato nella directory. Puoi inserire direttamente ticker o ISIN.")

    with st.form("add_etf_form", clear_on_submit=True):
        col1, col2 = st.columns([2, 1])
        with col1:
            ticker_input = st.text_input(
                "Ticker / ISIN",
                value=selected_ticker or "",
                placeholder="es. CSPX, SWDA, VWCE",
            )
        with col2:
            capital_input = st.number_input(
                "Importo (EUR)", min_value=0.0, value=10000.0, step=500.0,
            )
        st.caption("Inserisci il ticker (es. CSPX, SWDA) o l'ISIN (es. IE00B5BMR087).")
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
            for key in ("aggregated", "overlap_matrix", "redundancy_df",
                         "factor_result", "active_share_result", "benchmark_df",
                         "analysis_hash", "analysis_timestamp"):
                st.session_state[key] = None
            st.session_state.holdings_db.pop(ticker, None)
            st.rerun()

# ── Tab 2: File upload ─────────────────────────────────────────────
with tab_upload:
    from src.dashboard.components.portfolio_uploader import (
        generate_template_xlsx,
        parse_portfolio_file,
    )

    st.download_button(
        label="📥 Scarica template Excel",
        data=generate_template_xlsx(),
        file_name="portafoglio_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    st.caption("Il file deve avere colonne: **Ticker/ISIN** e **Importo (EUR)**")

    uploaded = st.file_uploader(
        "Carica il tuo portafoglio",
        type=["xlsx", "xls", "csv"],
        help="Formati supportati: Excel (.xlsx, .xls) e CSV (.csv)",
    )

    if uploaded is not None:
        positions_parsed, parse_errors = parse_portfolio_file(
            uploaded, filename=uploaded.name,
        )

        for err in parse_errors:
            if "non trovate" in err or "leggere" in err or "vuoto" in err:
                st.error(err)
            else:
                st.warning(err)

        if positions_parsed:
            preview = []
            for p in positions_parsed:
                preview.append({
                    "Ticker/ISIN": p["ticker"],
                    "Importo EUR": f"€ {p['capital']:,.0f}",
                    "Stato": "✅",
                })
            st.dataframe(preview, use_container_width=True, hide_index=True)

            if st.button("✅ Usa questo portafoglio", type="primary"):
                st.session_state.portfolio_positions = positions_parsed
                for key in ("aggregated", "overlap_matrix", "redundancy_df",
                             "factor_result", "active_share_result", "benchmark_df",
                             "analysis_hash", "analysis_timestamp"):
                    st.session_state[key] = None
                st.session_state.holdings_db = {}
                st.session_state.display_names = {}
                st.rerun()

# ── Current portfolio ───────────────────────────────────────────────
positions: list[dict] = st.session_state.portfolio_positions

if not positions:
    st.info("Aggiungi almeno un ETF per iniziare.")
    st.stop()

st.subheader("Portafoglio attuale")

editing_idx = st.session_state.get("editing_etf_idx")

for idx, pos in enumerate(positions):
    if editing_idx == idx:
        # ── Edit mode ──
        col_t, col_c, col_save, col_cancel = st.columns([3, 2, 0.5, 0.5])
        with col_t:
            new_ticker = st.text_input(
                "Ticker", value=pos["ticker"], key=f"edit_ticker_{idx}",
                label_visibility="collapsed",
            )
        with col_c:
            new_capital = st.number_input(
                "Importo", value=pos["capital"], min_value=0.0,
                step=1000.0, key=f"edit_capital_{idx}",
                label_visibility="collapsed",
            )
        with col_save:
            if st.button("✓", key=f"save_{idx}"):
                new_ticker = new_ticker.strip().upper()
                st.session_state.portfolio_positions[idx] = {
                    "ticker": new_ticker, "capital": new_capital,
                }
                if new_ticker != pos["ticker"]:
                    st.session_state.holdings_db.pop(pos["ticker"], None)
                    st.session_state.display_names.pop(pos["ticker"], None)
                for key in ("aggregated", "overlap_matrix", "redundancy_df",
                             "factor_result", "active_share_result",
                             "analysis_hash", "analysis_timestamp"):
                    st.session_state[key] = None
                st.session_state.editing_etf_idx = None
                st.rerun()
        with col_cancel:
            if st.button("✗", key=f"cancel_{idx}"):
                st.session_state.editing_etf_idx = None
                st.rerun()
    else:
        # ── View mode ──
        col_t, col_c, col_edit, col_del = st.columns([3, 2, 0.5, 0.5])
        display = st.session_state.get("display_names", {}).get(pos["ticker"], pos["ticker"])
        col_t.write(f"**{display}**")
        col_c.write(f"€ {pos['capital']:,.0f}")
        if col_edit.button("✏️", key=f"edit_{idx}"):
            st.session_state.editing_etf_idx = idx
            st.rerun()
        if col_del.button("🗑️", key=f"rm_{idx}"):
            st.session_state.portfolio_positions.pop(idx)
            st.session_state.holdings_db.pop(pos["ticker"], None)
            for key in ("aggregated", "overlap_matrix", "redundancy_df",
                         "factor_result", "active_share_result",
                         "analysis_hash", "analysis_timestamp"):
                st.session_state[key] = None
            st.rerun()

total_capital = sum(p["capital"] for p in positions)
st.caption(f"Totale investito: **€ {total_capital:,.0f}**")

# ── Benchmark selector ──────────────────────────────────────────────
st.divider()
BENCHMARK_OPTIONS = {
    "Nessun benchmark (analisi pura)": None,
    "MSCI World (SWDA/IWDA)": "MSCI_WORLD",
    "S&P 500 (CSPX)": "SP500",
    "MSCI EM (EIMI)": "MSCI_EM",
    "FTSE All-World (VWCE)": "FTSE_ALL_WORLD",
}
bench_label = st.selectbox(
    "Benchmark di riferimento",
    options=list(BENCHMARK_OPTIONS.keys()),
    index=1,  # Default: MSCI World
)
st.session_state.benchmark_name = BENCHMARK_OPTIONS[bench_label]
st.caption("Il benchmark serve per confrontare il tuo portafoglio con il mercato. "
           "Se hai solo ETF tematici, potresti non averne bisogno.")

# ── Analyse button ──────────────────────────────────────────────────
st.divider()

def _portfolio_hash(positions: list[dict], benchmark_name: str | None) -> str:
    """Compute a deterministic hash of the portfolio composition."""
    key_parts = sorted(f"{p['ticker']}:{p['capital']}" for p in positions)
    key_parts.append(f"bench:{benchmark_name}")
    return hashlib.sha256("|".join(key_parts).encode()).hexdigest()[:16]

col_main, col_refresh = st.columns([3, 1])
run_analysis = False
force_refresh = False

with col_main:
    if st.button("🚀 Analizza Portafoglio", type="primary", use_container_width=True):
        run_analysis = True

with col_refresh:
    if st.button("↺ Aggiorna dati", use_container_width=True):
        run_analysis = True
        force_refresh = True

if run_analysis:
    # Check aggregation cache
    current_hash = _portfolio_hash(positions, st.session_state.benchmark_name)
    cached_hash = st.session_state.get("analysis_hash")
    cached_time = st.session_state.get("analysis_timestamp")

    if (
        not force_refresh
        and current_hash == cached_hash
        and st.session_state.get("aggregated") is not None
        and cached_time is not None
    ):
        elapsed_min = (time.time() - cached_time) / 60
        st.info(
            f"Usando risultati in cache (analizzato {elapsed_min:.0f} minuti fa). "
            "Premi '↺ Aggiorna dati' per forzare il ricalcolo."
        )
        st.stop()

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
                n_h = len(result.holdings) if result.holdings is not None else 0
                status_container.write(f"⚡ {ticker} {step} — {n_h} holdings dalla cache")
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

    status_container.update(label=f"Aggregazione…")
    st.session_state.holdings_db = holdings_db

    # Resolve display names for ISINs
    display_names: dict[str, str] = st.session_state.get("display_names", {})
    for pos in positions:
        input_id = pos["ticker"]
        if input_id in display_names and not force_refresh:
            continue
        if input_id in holdings_db:
            df = holdings_db[input_id]
            if "etf_ticker" in df.columns:
                resolved = df["etf_ticker"].dropna().unique()
                if len(resolved) > 0 and resolved[0] != input_id:
                    display_names[input_id] = resolved[0]
                    continue
        # ISIN truncation fallback
        if len(input_id) == 12 and input_id[:2].isalpha():
            display_names[input_id] = f"{input_id[:7]}…{input_id[-2:]}"
        else:
            display_names[input_id] = input_id
    st.session_state.display_names = display_names

    # Aggregate
    try:
        aggregated = aggregate_portfolio(positions, holdings_db)
    except Exception as exc:
        st.error(f"Errore aggregazione: {exc}")
        st.stop()

    # Enrich missing sector/country
    try:
        from src.analytics.enrichment import enrich_missing_data

        status_container.write("🔍 Enrichment settore/paese…")
        db_sess = session_factory()
        aggregated = enrich_missing_data(aggregated, db_session=db_sess)
        db_sess.close()
    except Exception as exc:
        st.warning(f"Enrichment parziale: {exc}")

    st.session_state.aggregated = aggregated

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

    # Benchmark + Active Share (skip if no benchmark selected)
    if st.session_state.benchmark_name is not None:
        try:
            bmgr = BenchmarkManager()
            bench_df = bmgr.get_benchmark_holdings(st.session_state.benchmark_name)
            st.session_state.benchmark_df = bench_df
            as_result = active_share(aggregated, bench_df)
            st.session_state.active_share_result = as_result
        except Exception as exc:
            st.warning(f"Benchmark/Active Share non calcolato: {exc}")
    else:
        st.session_state.benchmark_df = None
        st.session_state.active_share_result = None

    # Save cache hash and timestamp
    st.session_state.analysis_hash = current_hash
    st.session_state.analysis_timestamp = time.time()

    status_container.update(state="complete", expanded=False)
    st.success("✅ Analisi completata! Naviga alle altre pagine per esplorare i risultati.")
