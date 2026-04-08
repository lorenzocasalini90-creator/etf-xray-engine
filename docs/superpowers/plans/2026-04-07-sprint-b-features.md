# Sprint B — Portfolio UX Features Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add batch Excel/CSV upload, inline ETF editing, ticker autocomplete with ETF directory, unique exposure analysis in Overlap, and actionable recommendations.

**Architecture:** 5 features mapped to 7 tasks (B3 split into data + UI). New files for isolated logic (uploader, directory, recommendations). Existing dashboard pages modified for UI integration. Dependencies: B3-data before B3-UI, B5 after B4 (needs redundancy data shape).

**Tech Stack:** Streamlit, openpyxl (already installed), pandas, Plotly. No new dependencies needed.

---

## File Structure

| File | Action | Feature | ~Lines |
|------|--------|---------|--------|
| `src/dashboard/components/portfolio_uploader.py` | Create | B1 | ~120 |
| `src/dashboard/data/etf_directory.csv` | Create | B3 | ~25 rows |
| `src/dashboard/data/etf_directory.py` | Create | B3 | ~60 |
| `src/analytics/recommendations.py` | Create | B5 | ~120 |
| `src/analytics/overlap.py` | Modify | B4 | +80 |
| `src/dashboard/pages/01_portfolio_input.py` | Modify | B1, B2, B3 | Major rewrite ~350 total |
| `src/dashboard/pages/02_xray_overview.py` | Modify | B5 | +40 |
| `src/dashboard/pages/03_redundancy.py` | Modify | B5 | +2 |
| `src/dashboard/pages/04_overlap.py` | Modify | B4 | +80 |
| `tests/test_portfolio_uploader.py` | Create | B1 | ~80 |
| `tests/test_etf_directory.py` | Create | B3 | ~50 |
| `tests/test_recommendations.py` | Create | B5 | ~100 |
| `tests/test_analytics.py` | Modify | B4 | +40 |

**Risks:**
- `01_portfolio_input.py` is modified by B1, B2, and B3 — tasks must be sequential.
- openpyxl template generation: trivial but needs testing with Excel and LibreOffice.
- Streamlit `st.tabs` + `st.form` interaction: forms inside tabs work but keys must be unique.
- `compute_unique_exposure` reuses `_match_key` infrastructure — well-tested.

---

### Task 1: B3-data — ETF Directory (CSV + search logic + tests)

**Files:**
- Create: `src/dashboard/data/etf_directory.csv`
- Create: `src/dashboard/data/etf_directory.py`
- Create: `tests/test_etf_directory.py`

This task has no UI dependencies — pure data + logic. Must be done before Task 5 (B3 UI).

- [ ] **Step 1: Create the ETF directory CSV**

Create `src/dashboard/data/etf_directory.csv`:

```csv
isin,ticker,name,provider,ter_pct,domicile
IE00B4L5Y983,SWDA,iShares Core MSCI World UCITS ETF USD Acc,iShares,0.20,IE
IE00B5BMR087,CSPX,iShares Core S&P 500 UCITS ETF USD Acc,iShares,0.07,IE
IE00BKM4GZ66,EIMI,iShares Core MSCI EM IMI UCITS ETF USD Acc,iShares,0.18,IE
IE00B6R52259,ISAC,iShares MSCI ACWI UCITS ETF USD Acc,iShares,0.20,IE
IE00B4L5YC18,IWDA,iShares Core MSCI World UCITS ETF USD Acc,iShares,0.20,IE
IE00B52MJY50,IUSA,iShares Core S&P 500 UCITS ETF USD Dist,iShares,0.07,IE
IE00B0M62Q58,IEMA,iShares MSCI Emerging Markets UCITS ETF USD Dist,iShares,0.18,IE
IE00B4WXJJ64,WSML,iShares MSCI World Small Cap UCITS ETF USD Acc,iShares,0.35,IE
IE00B3XXRP09,VUSA,Vanguard S&P 500 UCITS ETF USD Dist,Vanguard,0.07,IE
IE00BK5BQT80,VWCE,Vanguard FTSE All-World UCITS ETF USD Acc,Vanguard,0.22,IE
IE00B8GKDB10,VHYL,Vanguard FTSE All-World High Dividend Yield UCITS ETF,Vanguard,0.29,IE
IE00BKX55T58,VEUR,Vanguard FTSE Developed Europe UCITS ETF USD Dist,Vanguard,0.12,IE
IE00BDD48R20,VFEM,Vanguard FTSE Emerging Markets UCITS ETF USD Dist,Vanguard,0.22,IE
LU1681043599,CW8,Amundi MSCI World UCITS ETF EUR Acc,Amundi,0.12,LU
LU1437016972,PAEEM,Amundi MSCI Emerging Markets UCITS ETF EUR Acc,Amundi,0.20,LU
LU0908500753,CC1,Amundi S&P 500 UCITS ETF EUR Acc,Amundi,0.15,LU
IE00BJ0KDQ92,XDWD,Xtrackers MSCI World Swap UCITS ETF 1C,Xtrackers,0.19,IE
LU0274208692,XMEM,Xtrackers MSCI Emerging Markets Swap UCITS ETF 1C,Xtrackers,0.20,LU
IE00B44Z5B48,SPYW,SPDR S&P Euro Dividend Aristocrats UCITS ETF,SPDR,0.30,IE
```

- [ ] **Step 2: Create the search module**

Create `src/dashboard/data/etf_directory.py`:

```python
"""ETF directory: static CSV lookup with search."""

from __future__ import annotations

import os
from functools import lru_cache

import pandas as pd


_CSV_PATH = os.path.join(os.path.dirname(__file__), "etf_directory.csv")


@lru_cache(maxsize=1)
def load_directory() -> pd.DataFrame:
    """Load the ETF directory CSV (cached in memory)."""
    return pd.read_csv(_CSV_PATH, dtype=str).fillna("")


def search_etf(query: str, limit: int = 6) -> list[dict]:
    """Search ETFs by ticker, ISIN, or partial name.

    Priority: exact ticker > exact ISIN > partial ticker > partial name.
    Returns list of dicts with keys: isin, ticker, name, provider, ter_pct.
    """
    if len(query.strip()) < 2:
        return []

    q = query.strip().upper()
    df = load_directory()

    exact_ticker = df[df["ticker"].str.upper() == q]
    exact_isin = df[df["isin"].str.upper() == q]
    partial_ticker = df[df["ticker"].str.upper().str.startswith(q, na=False)]
    partial_name = df[df["name"].str.upper().str.contains(q, na=False)]

    combined = pd.concat([exact_ticker, exact_isin, partial_ticker, partial_name])
    combined = combined.drop_duplicates(subset="isin").head(limit)

    return combined.to_dict("records")
```

- [ ] **Step 3: Create `__init__.py` for the data package**

Create `src/dashboard/data/__init__.py` (empty file).

- [ ] **Step 4: Write tests**

Create `tests/test_etf_directory.py`:

```python
"""Tests for ETF directory search."""

import pytest

from src.dashboard.data.etf_directory import load_directory, search_etf


class TestLoadDirectory:
    def test_loads_dataframe(self):
        df = load_directory()
        assert len(df) >= 15
        assert "isin" in df.columns
        assert "ticker" in df.columns

    def test_has_required_columns(self):
        df = load_directory()
        for col in ("isin", "ticker", "name", "provider", "ter_pct"):
            assert col in df.columns


class TestSearchETF:
    def test_exact_ticker_swda(self):
        results = search_etf("SWDA")
        assert len(results) >= 1
        assert results[0]["ticker"] == "SWDA"

    def test_partial_name_ishares_world(self):
        results = search_etf("iShares World")
        tickers = {r["ticker"] for r in results}
        assert "SWDA" in tickers or "IWDA" in tickers

    def test_partial_name_vanguard_all(self):
        results = search_etf("Vanguard All")
        tickers = {r["ticker"] for r in results}
        assert "VWCE" in tickers

    def test_exact_isin(self):
        results = search_etf("IE00BK5BQT80")
        assert len(results) >= 1
        assert results[0]["ticker"] == "VWCE"

    def test_short_query_returns_empty(self):
        assert search_etf("X") == []

    def test_nonexistent_returns_empty(self):
        assert search_etf("XXXNOTEXIST") == []

    def test_case_insensitive(self):
        results = search_etf("swda")
        assert len(results) >= 1
        assert results[0]["ticker"] == "SWDA"

    def test_limit_respected(self):
        results = search_etf("iShares", limit=3)
        assert len(results) <= 3
```

- [ ] **Step 5: Run tests**

```bash
python3 -m pytest tests/test_etf_directory.py -v
```

- [ ] **Step 6: Commit**

```bash
git add src/dashboard/data/ tests/test_etf_directory.py
git commit -m "feat: ETF directory CSV + search logic with tests"
```

---

### Task 2: B1 — Portfolio uploader (parse logic + tests)

**Files:**
- Create: `src/dashboard/components/__init__.py`
- Create: `src/dashboard/components/portfolio_uploader.py`
- Create: `tests/test_portfolio_uploader.py`

- [ ] **Step 1: Create the uploader module**

Create `src/dashboard/components/__init__.py` (empty).

Create `src/dashboard/components/portfolio_uploader.py`:

```python
"""Parse portfolio files (Excel/CSV) into positions list."""

from __future__ import annotations

import io
import re

import pandas as pd


# Accepted column names (case-insensitive, stripped)
_TICKER_NAMES = {"ticker", "isin", "etf", "ticker/isin", "ticker_isin", "identificativo"}
_AMOUNT_NAMES = {"importo", "amount", "eur", "value", "importo eur", "amount eur", "importo_eur", "amount_eur"}


def generate_template_xlsx() -> bytes:
    """Generate a template Excel file with example data."""
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Portafoglio"
    ws.append(["Ticker/ISIN", "Importo EUR"])
    ws.append(["CSPX", 30000])
    ws.append(["SWDA", 40000])
    ws.append(["VWCE", 15000])

    # Column widths
    ws.column_dimensions["A"].width = 20
    ws.column_dimensions["B"].width = 15

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def parse_portfolio_file(
    file,
    filename: str = "",
) -> tuple[list[dict], list[str]]:
    """Parse an uploaded portfolio file.

    Args:
        file: File-like object (from st.file_uploader).
        filename: Original filename for format detection.

    Returns:
        Tuple of (positions, errors) where:
        - positions: list of {"ticker": str, "capital": float}
        - errors: list of human-readable error strings
    """
    errors: list[str] = []

    # Read into DataFrame
    try:
        if filename.endswith(".csv"):
            # Try comma first, then semicolon
            content = file.read()
            if isinstance(content, bytes):
                content = content.decode("utf-8-sig")
            try:
                df = pd.read_csv(io.StringIO(content), sep=",")
                if len(df.columns) < 2:
                    df = pd.read_csv(io.StringIO(content), sep=";")
            except Exception:
                df = pd.read_csv(io.StringIO(content), sep=";")
        else:
            df = pd.read_excel(file)
    except Exception as exc:
        return [], [f"Impossibile leggere il file: {exc}"]

    if df.empty:
        return [], ["Il file è vuoto."]

    # Find ticker column
    df.columns = [str(c).strip() for c in df.columns]
    col_map = {c.lower(): c for c in df.columns}

    ticker_col = None
    for name in _TICKER_NAMES:
        if name in col_map:
            ticker_col = col_map[name]
            break

    amount_col = None
    for name in _AMOUNT_NAMES:
        if name in col_map:
            amount_col = col_map[name]
            break

    if ticker_col is None or amount_col is None:
        return [], [
            "Colonne non trovate. Il file deve avere intestazioni "
            "'Ticker/ISIN' e 'Importo EUR'. Scarica il template per un esempio. "
            f"Colonne trovate: {', '.join(df.columns)}"
        ]

    # Parse rows
    positions: list[dict] = []
    seen: dict[str, int] = {}  # ticker -> index in positions

    for i, row in df.iterrows():
        row_num = i + 2  # Excel row (1-indexed + header)
        ticker = str(row[ticker_col]).strip().upper()

        if not ticker or ticker == "NAN":
            continue

        # Parse amount: remove €, spaces, handle locale separators
        raw_amount = str(row[amount_col]).strip()
        amount = _parse_amount(raw_amount)

        if amount is None:
            errors.append(f"Riga {row_num}: importo non valido '{raw_amount}' per {ticker}")
            continue

        if amount <= 0:
            errors.append(f"Riga {row_num}: importo deve essere positivo per {ticker}")
            continue

        # Deduplication
        if ticker in seen:
            idx = seen[ticker]
            old_amount = positions[idx]["capital"]
            positions[idx]["capital"] += amount
            errors.append(
                f"{ticker} trovato 2 volte — importi sommati: "
                f"€{positions[idx]['capital']:,.0f}"
            )
        else:
            seen[ticker] = len(positions)
            positions.append({"ticker": ticker, "capital": amount})

    if len(positions) > 20:
        errors.append(
            f"Attenzione: {len(positions)} ETF caricati. "
            "Portafogli molto grandi possono rallentare l'analisi."
        )

    return positions, errors


def _parse_amount(raw: str) -> float | None:
    """Parse amount string handling €, locale separators."""
    s = raw.replace("€", "").replace(" ", "").strip()
    if not s:
        return None

    # Detect if comma is decimal separator (European: "30.000,50")
    # or thousands separator (US: "30,000.50")
    if "," in s and "." in s:
        last_comma = s.rfind(",")
        last_dot = s.rfind(".")
        if last_comma > last_dot:
            # European: 30.000,50
            s = s.replace(".", "").replace(",", ".")
        else:
            # US: 30,000.50
            s = s.replace(",", "")
    elif "," in s:
        # Could be "30,000" (thousands) or "30,50" (decimal)
        parts = s.split(",")
        if len(parts) == 2 and len(parts[1]) <= 2:
            # Likely decimal: "30,50"
            s = s.replace(",", ".")
        else:
            # Likely thousands: "30,000"
            s = s.replace(",", "")
    # else: "." is decimal or thousands — try as-is

    try:
        return float(s)
    except ValueError:
        return None
```

- [ ] **Step 2: Write tests**

Create `tests/test_portfolio_uploader.py`:

```python
"""Tests for portfolio file uploader."""

import io

import pytest

from src.dashboard.components.portfolio_uploader import (
    generate_template_xlsx,
    parse_portfolio_file,
    _parse_amount,
)


class TestParseAmount:
    def test_simple_integer(self):
        assert _parse_amount("30000") == 30000.0

    def test_with_euro_sign(self):
        assert _parse_amount("€30000") == 30000.0

    def test_european_thousands(self):
        assert _parse_amount("30.000") == 30000.0

    def test_european_decimal(self):
        assert _parse_amount("30.000,50") == 30000.50

    def test_us_thousands(self):
        assert _parse_amount("30,000") == 30000.0

    def test_us_decimal(self):
        assert _parse_amount("30,000.50") == 30000.50

    def test_euro_with_spaces(self):
        assert _parse_amount("€ 30 000") == 30000.0

    def test_empty_returns_none(self):
        assert _parse_amount("") is None

    def test_invalid_returns_none(self):
        assert _parse_amount("abc") is None


class TestGenerateTemplate:
    def test_generates_bytes(self):
        data = generate_template_xlsx()
        assert isinstance(data, bytes)
        assert len(data) > 100

    def test_readable_by_openpyxl(self):
        import openpyxl
        data = generate_template_xlsx()
        wb = openpyxl.load_workbook(io.BytesIO(data))
        ws = wb.active
        assert ws.cell(1, 1).value == "Ticker/ISIN"
        assert ws.cell(2, 1).value == "CSPX"
        assert ws.cell(2, 2).value == 30000


class TestParseCSV:
    def _make_csv(self, content: str) -> io.StringIO:
        return io.StringIO(content)

    def test_basic_csv(self):
        csv = self._make_csv("Ticker/ISIN,Importo EUR\nCSPX,30000\nSWDA,40000\n")
        positions, errors = parse_portfolio_file(csv, filename="test.csv")
        assert len(positions) == 2
        assert positions[0] == {"ticker": "CSPX", "capital": 30000.0}

    def test_semicolon_csv(self):
        csv = self._make_csv("Ticker/ISIN;Importo EUR\nCSPX;30000\nSWDA;40000\n")
        positions, errors = parse_portfolio_file(csv, filename="test.csv")
        assert len(positions) == 2

    def test_euro_amounts(self):
        csv = self._make_csv("ticker,importo\nCSPX,€30.000\nSWDA,€40.000,50\n")
        positions, errors = parse_portfolio_file(csv, filename="test.csv")
        assert positions[0]["capital"] == 30000.0

    def test_duplicate_sums(self):
        csv = self._make_csv("ticker,importo\nCSPX,10000\nCSPX,20000\n")
        positions, errors = parse_portfolio_file(csv, filename="test.csv")
        assert len(positions) == 1
        assert positions[0]["capital"] == 30000.0
        assert any("sommati" in e for e in errors)

    def test_missing_columns_error(self):
        csv = self._make_csv("col1,col2\na,b\n")
        positions, errors = parse_portfolio_file(csv, filename="test.csv")
        assert len(positions) == 0
        assert any("Colonne non trovate" in e for e in errors)

    def test_invalid_amount_row(self):
        csv = self._make_csv("ticker,importo\nCSPX,30000\nSWDA,abc\n")
        positions, errors = parse_portfolio_file(csv, filename="test.csv")
        assert len(positions) == 1  # CSPX parsed, SWDA skipped
        assert any("non valido" in e for e in errors)


class TestParseExcel:
    def test_template_roundtrip(self):
        """Generate template, then parse it back."""
        data = generate_template_xlsx()
        positions, errors = parse_portfolio_file(io.BytesIO(data), filename="test.xlsx")
        assert len(positions) == 3
        assert positions[0] == {"ticker": "CSPX", "capital": 30000.0}
        assert positions[1] == {"ticker": "SWDA", "capital": 40000.0}
        assert positions[2] == {"ticker": "VWCE", "capital": 15000.0}
```

- [ ] **Step 3: Run tests**

```bash
python3 -m pytest tests/test_portfolio_uploader.py -v
```

- [ ] **Step 4: Commit**

```bash
git add src/dashboard/components/ tests/test_portfolio_uploader.py
git commit -m "feat: portfolio file parser with Excel/CSV support and tests"
```

---

### Task 3: B1+B2+B3-UI — Rewrite portfolio_input.py with tabs, upload, edit, autocomplete

**Files:**
- Modify: `src/dashboard/pages/01_portfolio_input.py`

This is the largest task. It restructures the page into tabs and adds inline edit + autocomplete. The upload parsing logic and search logic come from Tasks 1 and 2.

- [ ] **Step 1: Add editing_idx to session_state defaults**

Add to `_DEFAULTS` in `01_portfolio_input.py`:
```python
    "editing_etf_idx": None,
```

- [ ] **Step 2: Restructure page with tabs**

Replace the current form section (lines 38-63) with a two-tab layout. The manual tab gets the autocomplete search + form. The upload tab gets the file uploader.

Replace everything from `st.header("📥 Portfolio Input")` through the form submission handling (lines 36-63) with:

```python
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
            st.caption(
                "Non trovato nella directory. "
                "Puoi inserire direttamente ticker o ISIN."
            )

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
            st.dataframe(
                preview, use_container_width=True, hide_index=True,
            )

            if st.button("✅ Usa questo portafoglio", type="primary"):
                st.session_state.portfolio_positions = positions_parsed
                for key in ("aggregated", "overlap_matrix", "redundancy_df",
                             "factor_result", "active_share_result", "benchmark_df",
                             "analysis_hash", "analysis_timestamp", "holdings_db",
                             "display_names"):
                    st.session_state[key] = {} if key == "holdings_db" else None
                st.session_state.display_names = {}
                st.rerun()
```

- [ ] **Step 3: Replace portfolio list with inline edit support**

Replace the current portfolio display loop (lines 72-86) with the edit-capable version:

```python
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
                # Invalidate if ticker changed
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
```

- [ ] **Step 4: Verify syntax**

```bash
python3 -c "import ast; ast.parse(open('src/dashboard/pages/01_portfolio_input.py').read()); print('OK')"
```

- [ ] **Step 5: Commit**

```bash
git add src/dashboard/pages/01_portfolio_input.py
git commit -m "feat: tabs (manual+upload), inline edit, autocomplete in portfolio input"
```

---

### Task 4: B4 — Unique exposure analysis (analytics + UI)

**Files:**
- Modify: `src/analytics/overlap.py`
- Modify: `src/dashboard/pages/04_overlap.py`
- Modify: `tests/test_analytics.py`

- [ ] **Step 1: Add compute_unique_exposure to overlap.py**

Append to `src/analytics/overlap.py`:

```python
def compute_unique_exposure(
    target_ticker: str,
    all_holdings: dict[str, pd.DataFrame],
) -> dict:
    """Compute what would be lost by removing an ETF from the portfolio.

    Args:
        target_ticker: ETF to analyze.
        all_holdings: All portfolio ETFs {ticker: holdings DataFrame}.

    Returns:
        Dict with total_unique_pct, unique_holdings_count,
        total_holdings, main_covering_etf, holdings_detail (DataFrame).
    """
    build_match_keys_from_holdings(all_holdings)

    target_df = all_holdings.get(target_ticker)
    if target_df is None or target_df.empty:
        return {
            "total_unique_pct": 0.0,
            "unique_holdings_count": 0,
            "total_holdings": 0,
            "main_covering_etf": "",
            "holdings_detail": pd.DataFrame(),
        }

    target_df = add_match_key(target_df)
    if "weight_pct" in target_df.columns:
        target_df["weight_pct"] = pd.to_numeric(
            target_df["weight_pct"], errors="coerce"
        ).fillna(0.0)

    # Build target weights
    target_weights: dict[str, dict] = {}
    for _, row in target_df.iterrows():
        key = row.get("_match_key")
        if not key or (isinstance(key, float) and pd.isna(key)):
            continue
        w = float(row.get("weight_pct", 0) or 0)
        name = row.get("holding_name", "")
        ticker_h = row.get("holding_ticker", "")
        if key in target_weights:
            target_weights[key]["weight"] += w
        else:
            target_weights[key] = {"weight": w, "name": name, "ticker": ticker_h}

    # Build other ETFs weight vectors
    other_weights: dict[str, dict[str, float]] = {}  # key -> {etf: weight}
    for etf_ticker, df in all_holdings.items():
        if etf_ticker == target_ticker:
            continue
        df = add_match_key(df)
        if "weight_pct" in df.columns:
            df["weight_pct"] = pd.to_numeric(df["weight_pct"], errors="coerce").fillna(0.0)
        for _, row in df.iterrows():
            key = row.get("_match_key")
            if not key or (isinstance(key, float) and pd.isna(key)):
                continue
            w = float(row.get("weight_pct", 0) or 0)
            if key not in other_weights:
                other_weights[key] = {}
            other_weights[key][etf_ticker] = (
                other_weights[key].get(etf_ticker, 0) + w
            )

    # Compute per-holding coverage
    total_weight = sum(h["weight"] for h in target_weights.values())
    etf_coverage_total: dict[str, float] = {}
    rows = []

    for key, info in target_weights.items():
        w = info["weight"]
        others = other_weights.get(key, {})
        if others:
            best_etf = max(others, key=others.get)
            covered = min(w, max(others.values()))
        else:
            best_etf = ""
            covered = 0.0

        unique = w - covered
        rows.append({
            "holding_name": info["name"],
            "ticker_holding": info["ticker"],
            "weight_in_target_pct": round(w, 4),
            "covered_weight_pct": round(covered, 4),
            "unique_weight_pct": round(unique, 4),
            "covered_by_etf": best_etf,
        })

        if best_etf:
            etf_coverage_total[best_etf] = (
                etf_coverage_total.get(best_etf, 0) + covered
            )

    detail_df = pd.DataFrame(rows)
    if not detail_df.empty:
        detail_df = detail_df.sort_values(
            "weight_in_target_pct", ascending=False
        ).reset_index(drop=True)

    unique_total = sum(r["unique_weight_pct"] for r in rows)
    unique_count = sum(1 for r in rows if r["covered_weight_pct"] == 0)
    main_etf = max(etf_coverage_total, key=etf_coverage_total.get) if etf_coverage_total else ""

    return {
        "total_unique_pct": round(unique_total, 2),
        "unique_holdings_count": unique_count,
        "total_holdings": len(rows),
        "main_covering_etf": main_etf,
        "holdings_detail": detail_df,
    }
```

- [ ] **Step 2: Add tests**

Append to `tests/test_analytics.py`:

```python
class TestUniqueExposure:
    """Tests for compute_unique_exposure."""

    def test_single_etf_all_unique(self):
        from src.analytics.overlap import compute_unique_exposure
        holdings = {"ETF_A": pd.DataFrame({
            "holding_name": ["AAPL", "MSFT"],
            "holding_ticker": ["AAPL", "MSFT"],
            "holding_isin": ["", ""],
            "weight_pct": [50.0, 50.0],
        })}
        result = compute_unique_exposure("ETF_A", holdings)
        assert result["total_unique_pct"] == 100.0
        assert result["unique_holdings_count"] == 2

    def test_full_overlap_zero_unique(self):
        from src.analytics.overlap import compute_unique_exposure
        df = pd.DataFrame({
            "holding_name": ["AAPL", "MSFT"],
            "holding_ticker": ["AAPL", "MSFT"],
            "holding_isin": ["", ""],
            "weight_pct": [60.0, 40.0],
        })
        holdings = {"ETF_A": df.copy(), "ETF_B": df.copy()}
        result = compute_unique_exposure("ETF_A", holdings)
        assert result["total_unique_pct"] == 0.0
        assert result["main_covering_etf"] == "ETF_B"

    def test_partial_overlap(self):
        from src.analytics.overlap import compute_unique_exposure
        a = pd.DataFrame({
            "holding_name": ["AAPL", "MSFT", "TSLA"],
            "holding_ticker": ["AAPL", "MSFT", "TSLA"],
            "holding_isin": ["", "", ""],
            "weight_pct": [40.0, 30.0, 30.0],
        })
        b = pd.DataFrame({
            "holding_name": ["AAPL", "MSFT"],
            "holding_ticker": ["AAPL", "MSFT"],
            "holding_isin": ["", ""],
            "weight_pct": [50.0, 50.0],
        })
        holdings = {"ETF_A": a, "ETF_B": b}
        result = compute_unique_exposure("ETF_A", holdings)
        assert result["total_unique_pct"] > 0
        assert result["unique_holdings_count"] >= 1  # TSLA is unique
        assert result["total_holdings"] == 3
```

- [ ] **Step 3: Run tests**

```bash
python3 -m pytest tests/test_analytics.py::TestUniqueExposure -v
```

- [ ] **Step 4: Add UI to overlap page**

Append to `src/dashboard/pages/04_overlap.py`:

```python
# ── Unique exposure analysis ───────────────────────────────────────
from src.analytics.overlap import compute_unique_exposure

st.subheader("🔍 Analisi: cosa perdi rimuovendo un ETF?")
tickers_all = list(holdings_db.keys())

if len(tickers_all) >= 2:
    target = st.selectbox(
        "Seleziona ETF da analizzare",
        tickers_all,
        key="unique_exposure_target",
    )

    if target:
        ue = compute_unique_exposure(target, holdings_db)

        unique_pct = ue["total_unique_pct"]
        unique_count = ue["unique_holdings_count"]
        total_h = ue["total_holdings"]
        main_etf = ue["main_covering_etf"]

        # Summary box
        if unique_pct < 5:
            st.success(
                f"Rimuovendo **{target}**: impatto minimo — "
                f"{target} è ampiamente ridondante. Rimozione suggerita.\n\n"
                f"• Esposizione unica: **{unique_pct:.1f}%** su {unique_count} titoli\n"
                f"• La maggior parte già coperta da: **{main_etf}**"
            )
        elif unique_pct < 15:
            st.warning(
                f"Rimuovendo **{target}**: impatto moderato — "
                f"valuta se l'esposizione unica giustifica il TER.\n\n"
                f"• Esposizione unica: **{unique_pct:.1f}%** su {unique_count} titoli\n"
                f"• La maggior parte già coperta da: **{main_etf}**"
            )
        else:
            st.error(
                f"Rimuovendo **{target}**: impatto significativo — "
                f"{target} contribuisce esposizione difficilmente sostituibile.\n\n"
                f"• Esposizione unica: **{unique_pct:.1f}%** su {unique_count} titoli\n"
                f"• La maggior parte già coperta da: **{main_etf}**"
            )

        # Detail table
        detail = ue["holdings_detail"]
        if not detail.empty:
            display_detail = detail.head(20)[
                ["holding_name", "weight_in_target_pct", "covered_weight_pct",
                 "unique_weight_pct", "covered_by_etf"]
            ].copy()
            display_detail.columns = [
                "Titolo", f"Peso in {target} %", "Coperto da altri %",
                "Unico %", "Coperto da",
            ]
            for c in [f"Peso in {target} %", "Coperto da altri %", "Unico %"]:
                display_detail[c] = display_detail[c].map(lambda x: f"{x:.2f}")
            st.dataframe(display_detail, use_container_width=True, hide_index=True)
else:
    st.info("Servono almeno 2 ETF per l'analisi di esposizione unica.")
```

- [ ] **Step 5: Verify syntax**

```bash
python3 -c "import ast; ast.parse(open('src/analytics/overlap.py').read()); print('OK')"
python3 -c "import ast; ast.parse(open('src/dashboard/pages/04_overlap.py').read()); print('OK')"
```

- [ ] **Step 6: Commit**

```bash
git add src/analytics/overlap.py src/dashboard/pages/04_overlap.py tests/test_analytics.py
git commit -m "feat: unique exposure analysis — what you lose removing an ETF"
```

---

### Task 5: B5 — Actionable recommendations (logic + tests)

**Files:**
- Create: `src/analytics/recommendations.py`
- Create: `tests/test_recommendations.py`

- [ ] **Step 1: Create recommendations module**

Create `src/analytics/recommendations.py`:

```python
"""Actionable portfolio recommendations based on quantitative analysis."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass
class Recommendation:
    severity: Literal["high", "medium", "low"]
    title: str
    explanation: str
    action: str
    saving_eur_annual: float | None = None
    etfs_involved: list[str] = field(default_factory=list)
    rule_id: str = ""


def generate_recommendations(
    redundancy_scores: dict[str, float],
    ter_wasted_eur: dict[str, float],
    active_share: float | None,
    hhi: float,
    top1_weight: float,
    top1_name: str,
    n_etf: int,
    portfolio_total_eur: float,
    benchmark_name: str,
    best_single_etf_ter: float = 0.20,
    current_total_ter_eur: float = 0.0,
) -> list[Recommendation]:
    """Generate actionable recommendations from portfolio analysis.

    Args:
        redundancy_scores: {ticker: 0.0-1.0} redundancy fraction.
        ter_wasted_eur: {ticker: EUR/year} wasted TER per ETF.
        active_share: Active Share percentage (0-100) or None.
        hhi: Herfindahl-Hirschman Index.
        top1_weight: Weight of heaviest holding as fraction (0-1).
        top1_name: Name of the heaviest holding.
        n_etf: Number of ETFs in portfolio.
        portfolio_total_eur: Total portfolio value in EUR.
        benchmark_name: Benchmark name for display.
        best_single_etf_ter: TER of cheapest global ETF (default 0.20%).
        current_total_ter_eur: Current total TER cost in EUR/year.

    Returns:
        List of Recommendation objects, sorted by severity.
    """
    recs: list[Recommendation] = []

    # R1 — Highly redundant ETF
    for ticker, score in redundancy_scores.items():
        if score > 0.70:
            wasted = ter_wasted_eur.get(ticker, 0)
            recs.append(Recommendation(
                severity="high",
                title=f"{ticker} duplica quasi interamente il portafoglio esistente",
                explanation=(
                    f"Il {score * 100:.0f}% delle holdings di {ticker} è già presente "
                    f"negli altri ETF. Stai pagando un TER su esposizione già coperta."
                ),
                action=(
                    f"Considera di vendere {ticker} e reinvestire i proventi "
                    f"in un ETF complementare."
                ),
                saving_eur_annual=wasted if wasted > 0 else None,
                etfs_involved=[ticker],
                rule_id="R1",
            ))

    # R2 — Closet indexing
    if active_share is not None and active_share < 20:
        saving = current_total_ter_eur - (portfolio_total_eur * best_single_etf_ter / 100)
        recs.append(Recommendation(
            severity="medium",
            title=f"Il portafoglio replica quasi identicamente {benchmark_name}",
            explanation=(
                f"Con Active Share {active_share:.0f}%, il tuo portafoglio si comporta "
                f"come {benchmark_name} ma con costi più alti. "
                f"Stai pagando {current_total_ter_eur:.0f}€/anno di TER totali."
            ),
            action=(
                f"Valuta se semplificare con un singolo ETF globale: "
                f"SWDA (TER 0.20%) o VWCE (TER 0.22%) costerebbero "
                f"circa {portfolio_total_eur * best_single_etf_ter / 100:.0f}€/anno."
            ),
            saving_eur_annual=saving if saving > 0 else None,
            rule_id="R2",
        ))

    # R3 — Single holding concentration
    if top1_weight > 0.08:
        recs.append(Recommendation(
            severity="medium",
            title=(
                f"Alta concentrazione su {top1_name} "
                f"({top1_weight * 100:.1f}% del portafoglio)"
            ),
            explanation=(
                f"{top1_name} pesa il {top1_weight * 100:.1f}% del portafoglio totale. "
                f"Questo livello è tipico di portafogli con forte overlap su ETF tech US. "
                f"Una correzione su questo titolo avrebbe impatto diretto sul portafoglio."
            ),
            action=(
                "Verifica se questa concentrazione è intenzionale "
                "o effetto dell'overlap tra ETF."
            ),
            rule_id="R3",
        ))

    # R4 — Total TER wasted > 50 EUR/year
    total_wasted = sum(ter_wasted_eur.values())
    if total_wasted > 50:
        recs.append(Recommendation(
            severity="high",
            title=f"Stai sprecando ~{total_wasted:.0f}€/anno in commissioni ridondanti",
            explanation=(
                f"La sovrapposizione tra i tuoi {n_etf} ETF genera "
                f"circa {total_wasted:.0f}€/anno di TER su holdings duplicate. "
                f"Con ETF parzialmente sovrapposti, una parte dei costi non aggiunge valore."
            ),
            action=(
                f"Riduci a 2-3 ETF complementari. "
                f"Risparmio stimato: {total_wasted:.0f}€/anno."
            ),
            saving_eur_annual=total_wasted,
            rule_id="R4",
        ))

    return recs
```

- [ ] **Step 2: Write tests**

Create `tests/test_recommendations.py`:

```python
"""Tests for actionable recommendations engine."""

import pytest

from src.analytics.recommendations import Recommendation, generate_recommendations


_BASE_ARGS = {
    "redundancy_scores": {"CSPX": 0.30, "SWDA": 0.10},
    "ter_wasted_eur": {"CSPX": 10.0, "SWDA": 5.0},
    "active_share": 45.0,
    "hhi": 0.05,
    "top1_weight": 0.04,
    "top1_name": "NVIDIA CORP",
    "n_etf": 2,
    "portfolio_total_eur": 70000.0,
    "benchmark_name": "MSCI World",
    "current_total_ter_eur": 100.0,
}


class TestR1HighRedundancy:
    def test_triggered_above_70(self):
        recs = generate_recommendations(
            **{**_BASE_ARGS, "redundancy_scores": {"CSPX": 0.99, "SWDA": 0.30},
               "ter_wasted_eur": {"CSPX": 74.0, "SWDA": 0.0}},
        )
        r1 = [r for r in recs if r.rule_id == "R1"]
        assert len(r1) >= 1
        assert "CSPX" in r1[0].etfs_involved

    def test_not_triggered_below_70(self):
        recs = generate_recommendations(**_BASE_ARGS)
        assert not any(r.rule_id == "R1" for r in recs)


class TestR2ClosetIndexing:
    def test_triggered_low_active_share(self):
        recs = generate_recommendations(**{**_BASE_ARGS, "active_share": 16.0})
        assert any(r.rule_id == "R2" for r in recs)

    def test_not_triggered_high_active_share(self):
        recs = generate_recommendations(**{**_BASE_ARGS, "active_share": 55.0})
        assert not any(r.rule_id == "R2" for r in recs)

    def test_not_triggered_none_active_share(self):
        recs = generate_recommendations(**{**_BASE_ARGS, "active_share": None})
        assert not any(r.rule_id == "R2" for r in recs)


class TestR3Concentration:
    def test_triggered_high_concentration(self):
        recs = generate_recommendations(
            **{**_BASE_ARGS, "top1_weight": 0.12, "top1_name": "NVIDIA CORP"},
        )
        assert any(r.rule_id == "R3" for r in recs)

    def test_not_triggered_low_concentration(self):
        recs = generate_recommendations(**{**_BASE_ARGS, "top1_weight": 0.04})
        assert not any(r.rule_id == "R3" for r in recs)


class TestR4TERWasted:
    def test_triggered_above_50(self):
        recs = generate_recommendations(
            **{**_BASE_ARGS, "ter_wasted_eur": {"CSPX": 40.0, "SWDA": 20.0}},
        )
        assert any(r.rule_id == "R4" for r in recs)

    def test_not_triggered_below_50(self):
        recs = generate_recommendations(
            **{**_BASE_ARGS, "ter_wasted_eur": {"CSPX": 20.0, "SWDA": 10.0}},
        )
        assert not any(r.rule_id == "R4" for r in recs)


class TestHealthyPortfolio:
    def test_no_high_severity(self):
        recs = generate_recommendations(**_BASE_ARGS)
        assert not any(r.severity == "high" for r in recs)


class TestRecommendationDataclass:
    def test_defaults(self):
        r = Recommendation(
            severity="low", title="test", explanation="x", action="y",
        )
        assert r.saving_eur_annual is None
        assert r.etfs_involved == []
        assert r.rule_id == ""
```

- [ ] **Step 3: Run tests**

```bash
python3 -m pytest tests/test_recommendations.py -v
```

- [ ] **Step 4: Commit**

```bash
git add src/analytics/recommendations.py tests/test_recommendations.py
git commit -m "feat: actionable recommendations engine with 4 rules and tests"
```

---

### Task 6: B5-UI — Recommendations in X-Ray + Redundancy link

**Files:**
- Modify: `src/dashboard/pages/02_xray_overview.py`
- Modify: `src/dashboard/pages/03_redundancy.py`

- [ ] **Step 1: Add recommendations to X-Ray Overview**

Append at the END of `src/dashboard/pages/02_xray_overview.py` (after the sector/country preview):

```python
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
```

- [ ] **Step 2: Add link in Redundancy page**

In `src/dashboard/pages/03_redundancy.py`, append at the very end:

```python
st.info("💡 Suggerimenti basati su questi dati disponibili nella pagina **X-Ray Overview**")
```

- [ ] **Step 3: Verify syntax**

```bash
python3 -c "import ast; ast.parse(open('src/dashboard/pages/02_xray_overview.py').read()); print('OK')"
python3 -c "import ast; ast.parse(open('src/dashboard/pages/03_redundancy.py').read()); print('OK')"
```

- [ ] **Step 4: Commit**

```bash
git add src/dashboard/pages/02_xray_overview.py src/dashboard/pages/03_redundancy.py
git commit -m "feat: recommendations UI in X-Ray + link from Redundancy"
```

---

### Task 7: Final verification + squash + push

- [ ] **Step 1: Run ALL tests**

```bash
python3 -m pytest tests/ -x -q
```

Expected: 261 + ~30 new = ~291 tests pass.

- [ ] **Step 2: Verify syntax on all modified files**

```bash
for f in src/dashboard/pages/01_portfolio_input.py src/dashboard/pages/02_xray_overview.py src/dashboard/pages/03_redundancy.py src/dashboard/pages/04_overlap.py src/analytics/overlap.py src/analytics/recommendations.py src/dashboard/components/portfolio_uploader.py src/dashboard/data/etf_directory.py; do python3 -c "import ast; ast.parse(open('$f').read()); print('$f OK')"; done
```

- [ ] **Step 3: Squash and push**

```bash
git log --oneline HEAD~6..HEAD
git reset --soft HEAD~6
git commit -m "feat: batch upload excel/csv, inline edit portfolio, ticker autocomplete with etf directory, unique exposure analysis in overlap, actionable recommendations [sprint-B]"
git push origin main
```
