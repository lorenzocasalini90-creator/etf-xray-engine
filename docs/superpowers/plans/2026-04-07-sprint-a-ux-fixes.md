# Sprint A — UX Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve dashboard UX with cache feedback, force-refresh button, ticker display normalization, factor progress bar, momentum N/D, top-10 concentration, holdings filter, swap warning, and xray cleanup.

**Architecture:** 5 independent fix groups touching 3 main files (`01_portfolio_input.py`, `02_xray_overview.py`, `06_factor_fingerprint.py`) plus `factor_engine.py` for progress callback and `app.py` for sidebar. Each fix is a self-contained change.

**Tech Stack:** Streamlit, Plotly, pandas, SQLAlchemy, existing FetchResult/FactorEngine.

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `src/dashboard/pages/01_portfolio_input.py` | Modify | FIX 1 (cache feedback, force-refresh button), FIX 2 (ticker display) |
| `src/dashboard/app.py` | Modify | FIX 2 (sidebar ticker display) |
| `src/dashboard/pages/02_xray_overview.py` | Modify | FIX 5A-5E (top-10, filter, swap warning, expander, preview) |
| `src/dashboard/pages/06_factor_fingerprint.py` | Modify | FIX 3 (progress bar), FIX 4 (momentum N/D) |
| `src/factors/factor_engine.py` | Modify | FIX 3 (progress_callback parameter) |

---

### Task 1: FIX 1A+1B — Cache feedback + force-refresh button

**Files:**
- Modify: `src/dashboard/pages/01_portfolio_input.py`

**Current state analysis:**
- Lines 157-183: `st.status()` already wraps the fetch loop with per-ETF messages for cached/success/partial/failed — this is **already working correctly** per the FetchResult fields: `result.status` (str: "success"/"cached"/"partial"/"failed"), `result.source` (str), `result.message` (str), `result.coverage_pct` (float).
- Line 110: `force_refresh = st.checkbox(...)` — the checkbox problem: Streamlit checkboxes reset on rerun, so after clicking "Analizza" the page reruns and the checkbox state may not persist as expected.
- Lines 118-135: The "Analizza" button and cache hash check.

- [ ] **Step 1: Replace checkbox with two buttons**

Replace lines 108-135 of `01_portfolio_input.py`. Remove the checkbox. Add two buttons: primary "Analizza" and secondary "Forza aggiornamento". Both set `force_refresh` in session_state and trigger the same analysis flow.

```python
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
```

The rest of the analysis block (from `from dotenv import load_dotenv` through the end) stays exactly as-is, except change the one reference to `force_refresh` variable (line 162 `if ticker in holdings_db and not force_refresh:` and line 167 `result = orchestrator.fetch(ticker, force_refresh=force_refresh)`) — these already reference the local variable, so they work.

- [ ] **Step 2: Improve status messages with coverage info**

In the existing fetch loop (lines 159-183), the messages already show per-ETF status. Improve the "cached" message to include holdings count:

Replace:
```python
            if result.status == "cached":
                status_container.write(f"⚡ {ticker} {step} — cache ({result.message})")
```
With:
```python
            if result.status == "cached":
                n_h = len(result.holdings) if result.holdings is not None else 0
                status_container.write(f"⚡ {ticker} {step} — {n_h} holdings dalla cache")
```

- [ ] **Step 3: Verify syntax**

```bash
python3 -c "import ast; ast.parse(open('src/dashboard/pages/01_portfolio_input.py').read()); print('OK')"
```

- [ ] **Step 4: Commit**

```bash
git add src/dashboard/pages/01_portfolio_input.py
git commit -m "fix: replace force-refresh checkbox with button, improve cache feedback"
```

---

### Task 2: FIX 2 — Ticker display: ISIN → resolved name

**Files:**
- Modify: `src/dashboard/pages/01_portfolio_input.py`
- Modify: `src/dashboard/app.py`

**Approach:** After fetch, if the input was an ISIN, resolve display_ticker from the holdings DataFrame's `etf_ticker` column (which fetchers populate with the resolved ticker). Store a `display_name` dict in session_state mapping input_identifier → display string.

- [ ] **Step 1: Add display_name resolution after fetch**

In `01_portfolio_input.py`, after the fetch loop stores holdings, add a display_name resolution step. Insert after `st.session_state.holdings_db = holdings_db` (line 186):

```python
    # Resolve display names for ISINs
    display_names: dict[str, str] = st.session_state.get("display_names", {})
    for pos in positions:
        input_id = pos["ticker"]
        if input_id in display_names and not force_refresh:
            continue
        # Check if holdings have an etf_ticker that differs from the input
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
```

- [ ] **Step 2: Use display names in the portfolio list**

Replace the portfolio display loop (lines 73-76):

```python
for idx, pos in enumerate(positions):
    col_t, col_c, col_r = st.columns([3, 2, 1])
    display = st.session_state.get("display_names", {}).get(pos["ticker"], pos["ticker"])
    col_t.write(f"**{display}**")
    col_c.write(f"€ {pos['capital']:,.0f}")
    if col_r.button("🗑️", key=f"rm_{idx}"):
```

- [ ] **Step 3: Update sidebar in app.py**

In `src/dashboard/app.py`, replace lines 36-38:

```python
    if n_etf:
        display_names = st.session_state.get("display_names", {})
        tickers = ", ".join(
            display_names.get(p["ticker"], p["ticker"])
            for p in st.session_state.portfolio_positions
        )
        st.success(f"**{n_etf} ETF** in portafoglio: {tickers}")
```

- [ ] **Step 4: Add display_names to session_state defaults**

In `01_portfolio_input.py`, add to `_DEFAULTS` dict:
```python
    "display_names": {},
```

In `app.py`, add to `_DEFAULTS` dict:
```python
    "display_names": {},
```

- [ ] **Step 5: Verify syntax**

```bash
python3 -c "import ast; ast.parse(open('src/dashboard/pages/01_portfolio_input.py').read()); print('OK')"
python3 -c "import ast; ast.parse(open('src/dashboard/app.py').read()); print('OK')"
```

- [ ] **Step 6: Commit**

```bash
git add src/dashboard/pages/01_portfolio_input.py src/dashboard/app.py
git commit -m "fix: resolve ISIN to display ticker in portfolio list and sidebar"
```

---

### Task 3: FIX 3 — Progress bar for Factor Fingerprint

**Files:**
- Modify: `src/factors/factor_engine.py`
- Modify: `src/dashboard/pages/06_factor_fingerprint.py`

**Approach:** Add optional `progress_callback` to `FactorEngine.analyze()`. Call it at each major step. In the dashboard, use `st.progress()` instead of `st.spinner()`.

- [ ] **Step 1: Add progress_callback to FactorEngine.analyze()**

In `src/factors/factor_engine.py`, modify the `analyze` method signature (line 375) and add callback calls:

```python
    def analyze(
        self,
        portfolio_df: pd.DataFrame,
        benchmark_df: pd.DataFrame | None = None,
        progress_callback: "Callable[[float, str], None] | None" = None,
    ) -> dict:
```

Add a helper and callback calls inside the method body. Replace lines 394-418:

```python
        def _progress(pct: float, msg: str) -> None:
            if progress_callback:
                progress_callback(pct, msg)

        _progress(0.05, "Carico dati fondamentali (P/E, P/B, ROE)…")
        resolved, coverage = self._resolve_fundamentals(portfolio_df)

        _progress(0.45, "Classifico titoli per Size e Value/Growth…")
        factor_scores = self._compute_weighted_factors(resolved)

        _progress(0.55, "Identifico factor drivers…")
        factor_drivers = self._find_factor_drivers(resolved)

        result = {
            "factor_scores": factor_scores,
            "coverage_report": coverage.as_dict(),
            "factor_drivers": factor_drivers,
            "benchmark_comparison": None,
        }

        if benchmark_df is not None and not benchmark_df.empty:
            _progress(0.65, "Analizzo benchmark…")
            # Benchmark DataFrames use 'weight_pct'; normalize to 'real_weight_pct'
            if "real_weight_pct" not in benchmark_df.columns and "weight_pct" in benchmark_df.columns:
                benchmark_df = benchmark_df.copy()
                benchmark_df["real_weight_pct"] = pd.to_numeric(
                    benchmark_df["weight_pct"], errors="coerce"
                ).fillna(0.0)
            bench_resolved, _ = self._resolve_fundamentals(benchmark_df)

            _progress(0.85, "Calcolo delta vs benchmark…")
            bench_factors = self._compute_weighted_factors(bench_resolved)
            result["benchmark_comparison"] = self._compute_delta(
                factor_scores, bench_factors,
            )

        _progress(1.0, "Completato")
        return result
```

Add the import at the top of the file (after line 6):

```python
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from collections.abc import Callable
```

- [ ] **Step 2: Update dashboard page to use progress bar**

In `src/dashboard/pages/06_factor_fingerprint.py`, replace lines 31-50:

```python
if factor_result is None:
    if st.button("🔬 Calcola Factor Fingerprint", type="primary"):
        from src.factors.factor_engine import FactorEngine
        from src.storage.db import get_session_factory, init_db

        init_db()
        session = get_session_factory()()
        engine = FactorEngine(session)
        benchmark_df = st.session_state.get("benchmark_df")

        st.info("⏱ Tempo stimato: 20-40 secondi — "
                "i dati fondamentali vengono scaricati da yfinance")

        progress_bar = st.progress(0, text="Inizializzazione factor engine…")

        def _update_progress(pct: float, msg: str) -> None:
            progress_bar.progress(pct, text=msg)

        factor_result = engine.analyze(
            aggregated,
            benchmark_df=benchmark_df if benchmark_df is not None else None,
            progress_callback=_update_progress,
        )
        st.session_state.factor_result = factor_result

        progress_bar.progress(1.0, text="✅ Factor Fingerprint completato")
        st.rerun()
    else:
        st.info("Clicca il bottone per lanciare l'analisi fattoriale (richiede fetch dati da yfinance).")
        st.stop()
```

- [ ] **Step 3: Verify syntax**

```bash
python3 -c "import ast; ast.parse(open('src/factors/factor_engine.py').read()); print('OK')"
python3 -c "import ast; ast.parse(open('src/dashboard/pages/06_factor_fingerprint.py').read()); print('OK')"
```

- [ ] **Step 4: Run existing tests**

```bash
python3 -m pytest tests/test_factors.py -v
```

Expected: all pass (progress_callback defaults to None so existing callers unaffected).

- [ ] **Step 5: Commit**

```bash
git add src/factors/factor_engine.py src/dashboard/pages/06_factor_fingerprint.py
git commit -m "feat: add progress bar to Factor Fingerprint computation"
```

---

### Task 4: FIX 4 — Momentum placeholder: explicit N/D

**Files:**
- Modify: `src/dashboard/pages/06_factor_fingerprint.py`

**Current state:** The radar chart has 4 dimensions (Size, Value, Quality, Dividend Yield). Momentum is NOT on the radar chart — it was removed in a prior commit. The caption at line 112 says "Nota: il fattore Momentum sarà disponibile in una versione futura." The Factor Scores table (lines 128-145) does NOT include a Momentum row. So Momentum is already absent, not showing "0".

**Fix required per spec:** Add explicit Momentum N/D row to the Factor Scores table, and ensure the caption is clear.

- [ ] **Step 1: Add Momentum row to Factor Scores table**

In `06_factor_fingerprint.py`, after the existing `row_base` list (after the Dividend Yield entry), add the Momentum row. Replace lines 131-143:

```python
rows = []
row_base = [
    ("Size (% Large Cap)", f"{size_score:.1f}%", f"{bench_cmp['size'].get('Large_delta', 'N/A')}" if has_bench else None),
    ("Value (P/E medio)", f"{vg.get('weighted_pe', 'N/A')}", f"{bench_cmp['value_growth'].get('pe_delta', 'N/A')}" if has_bench else None),
    ("Value (P/B medio)", f"{vg.get('weighted_pb', 'N/A')}", f"{bench_cmp['value_growth'].get('pb_delta', 'N/A')}" if has_bench else None),
    ("Quality (ROE %)", f"{roe_val:.1f}%", f"{bench_cmp['quality'].get('roe_delta', 'N/A')}" if has_bench else None),
    ("Dividend Yield %", f"{div_yield:.2f}%", f"{bench_cmp['dividend_yield'].get('yield_delta', 'N/A')}" if has_bench else None),
    ("Momentum *", "N/D", "N/D" if has_bench else None),
]
for dim, port, delta in row_base:
    row = {"Dimensione": dim, "Portafoglio": port}
    if has_bench:
        row["Delta Benchmark"] = delta
    rows.append(row)
```

- [ ] **Step 2: Update the caption to be more explicit**

Replace line 112:
```python
st.caption("* Momentum non disponibile — richiede serie storiche prezzi 6-12 mesi. "
           "Sarà implementato prossimamente.")
```

- [ ] **Step 3: Verify syntax**

```bash
python3 -c "import ast; ast.parse(open('src/dashboard/pages/06_factor_fingerprint.py').read()); print('OK')"
```

- [ ] **Step 4: Commit**

```bash
git add src/dashboard/pages/06_factor_fingerprint.py
git commit -m "fix: Momentum row shows N/D in Factor Scores table"
```

---

### Task 5: FIX 5A — Top-5 → Top-10 concentration

**Files:**
- Modify: `src/dashboard/pages/02_xray_overview.py`

**Current state:** Line 43: `k5.metric("Top-5 Conc.", f"{hhi_stats['top_5_pct']:.2f} %")`. The `portfolio_hhi` function already returns `top_10_pct` (confirmed in overlap.py).

- [ ] **Step 1: Change KPI to Top-10**

Replace line 43:
```python
k5.metric("Top-10 Conc.", f"{hhi_stats['top_10_pct']:.2f} %")
```

- [ ] **Step 2: Update the expander text**

Replace lines 74-79:
```python
with st.expander("ℹ️ Cos'è Top-10 Concentration?"):
    st.markdown(
        "La somma dei pesi dei tuoi 10 titoli più grandi. "
        "Se è 35%, un terzo del tuo portafoglio dipende da 10 aziende."
    )
```

- [ ] **Step 3: Commit**

```bash
git add src/dashboard/pages/02_xray_overview.py
git commit -m "fix: KPI shows Top-10 concentration instead of Top-5"
```

---

### Task 6: FIX 5B — Filter holdings < 0.05%

**Files:**
- Modify: `src/dashboard/pages/02_xray_overview.py`

- [ ] **Step 1: Filter the top holdings table**

Replace lines 82-90:
```python
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
```

- [ ] **Step 2: Commit**

```bash
git add src/dashboard/pages/02_xray_overview.py
git commit -m "fix: filter holdings below 0.05% with disclosure caption"
```

---

### Task 7: FIX 5C — Synthetic replication warning

**Files:**
- Modify: `src/dashboard/pages/02_xray_overview.py`

- [ ] **Step 1: Add swap/synthetic detection and warning**

Insert after line 26 (after `from src.analytics.overlap import portfolio_hhi`), before the KPI row:

```python
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
```

- [ ] **Step 2: Commit**

```bash
git add src/dashboard/pages/02_xray_overview.py
git commit -m "feat: add synthetic replication warning banner"
```

---

### Task 8: FIX 5D — Bar chart in expander

**Files:**
- Modify: `src/dashboard/pages/02_xray_overview.py`

- [ ] **Step 1: Wrap bar chart in collapsed expander**

Replace lines 92-105:
```python
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
```

- [ ] **Step 2: Commit**

```bash
git add src/dashboard/pages/02_xray_overview.py
git commit -m "fix: move Top 20 bar chart into collapsed expander"
```

---

### Task 9: FIX 5E — Sector/country preview in X-Ray

**Files:**
- Modify: `src/dashboard/pages/02_xray_overview.py`

- [ ] **Step 1: Add sector/country preview at bottom of page**

Append at the end of `02_xray_overview.py`, after the Active Bets section:

```python
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
```

- [ ] **Step 2: Verify syntax**

```bash
python3 -c "import ast; ast.parse(open('src/dashboard/pages/02_xray_overview.py').read()); print('OK')"
```

- [ ] **Step 3: Commit**

```bash
git add src/dashboard/pages/02_xray_overview.py
git commit -m "feat: add sector/country preview to X-Ray Overview"
```

---

### Task 10: Final verification and squash

- [ ] **Step 1: Run all tests**

```bash
python3 -m pytest tests/ -x -q
```

Expected: all 261+ tests pass, no regressions.

- [ ] **Step 2: Squash commits and push**

```bash
git log --oneline HEAD~9..HEAD
git reset --soft HEAD~9
git commit -m "fix: cache feedback, force-refresh button, ticker display normalization, factor progress bar, momentum N/D, top10 concentration, holdings filter, swap warning, xray cleanup [sprint-A]"
git push origin main
```
