# Merge Overlap & Redundancy Pages — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Merge the separate Redundancy (page 03) and Overlap (page 04) pages into a single "Overlap & Ridondanza" page with a 3-layer layout: summary → per-ETF breakdown with contribution sources → collapsible heatmap drill-down.

**Architecture:** Add `redundancy_breakdown()` to `src/analytics/redundancy.py` for per-ETF redundancy contribution decomposition. Create a new merged page `03_overlap_redundancy.py` that combines both pages' content. Delete the old pages and renumber 05→04, 06→05. Update observations to use a single page category.

**Tech Stack:** Streamlit, Plotly, pandas

---

### File Map

| File | Action | Task | Est. lines |
|---|---|---|---|
| `src/analytics/redundancy.py` | Modify | T1 | +40 |
| `tests/test_redundancy.py` | Create | T1 | ~80 |
| `src/analytics/observations.py` | Modify | T2 | ~5 |
| `src/dashboard/pages/03_overlap_redundancy.py` | Create | T3 | ~250 |
| `src/dashboard/pages/03_redundancy.py` | Delete | T4 | — |
| `src/dashboard/pages/04_overlap.py` | Delete | T4 | — |
| `src/dashboard/pages/05_sector_country.py` → `04_sector_country.py` | Rename | T4 | 0 |
| `src/dashboard/pages/06_factor_fingerprint.py` → `05_factor_fingerprint.py` | Rename | T4 | 0 |
| `src/dashboard/app.py` | Modify | T4 | ~3 |

---

### Task 1: Add `redundancy_breakdown()` analytics function (TDD)

**Files:**
- Create: `tests/test_redundancy.py`
- Modify: `src/analytics/redundancy.py`

The new function decomposes "where does the redundancy come from?" For each ETF, it tells you how much each OTHER ETF contributes to its redundancy.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_redundancy.py`:

```python
"""Tests for redundancy analytics."""

import pandas as pd
import pytest

from src.analytics.redundancy import redundancy_breakdown, redundancy_scores


# ---------------------------------------------------------------------------
# Fixtures — two overlapping ETFs + one unique
# ---------------------------------------------------------------------------

def _make_holdings(ticker: str, holdings: list[tuple[str, str, float]]) -> pd.DataFrame:
    """Build a minimal holdings DataFrame.

    Args:
        ticker: ETF ticker for etf_ticker column.
        holdings: List of (name, isin, weight_pct) tuples.
    """
    records = []
    for name, isin, weight in holdings:
        records.append({
            "etf_ticker": ticker,
            "holding_name": name,
            "holding_isin": isin,
            "holding_ticker": None,
            "weight_pct": weight,
        })
    return pd.DataFrame(records)


# BROAD has 4 holdings (AAPL 30%, MSFT 25%, GOOG 20%, AMZN 25%)
BROAD = _make_holdings("BROAD", [
    ("Apple", "US0378331005", 30.0),
    ("Microsoft", "US5949181045", 25.0),
    ("Google", "US02079K1079", 20.0),
    ("Amazon", "US0231351067", 25.0),
])

# NARROW has 2 holdings shared with BROAD + 1 unique
NARROW = _make_holdings("NARROW", [
    ("Apple", "US0378331005", 50.0),
    ("Microsoft", "US5949181045", 30.0),
    ("Tesla", "US88160R1014", 20.0),
])

# UNIQUE has no overlap with anyone
UNIQUE = _make_holdings("UNIQUE", [
    ("Sony", "JP3435000009", 60.0),
    ("Toyota", "JP3633400001", 40.0),
])

HOLDINGS_DB = {"BROAD": BROAD, "NARROW": NARROW, "UNIQUE": UNIQUE}


# ---------------------------------------------------------------------------
# redundancy_breakdown
# ---------------------------------------------------------------------------

class TestRedundancyBreakdown:
    def test_returns_dict_of_contributions(self) -> None:
        result = redundancy_breakdown("NARROW", HOLDINGS_DB)
        assert isinstance(result, dict)
        # NARROW shares AAPL+MSFT with BROAD, nothing with UNIQUE
        assert "BROAD" in result
        assert result.get("UNIQUE", 0) == 0

    def test_contribution_matches_shared_weight(self) -> None:
        result = redundancy_breakdown("NARROW", HOLDINGS_DB)
        # NARROW: AAPL(50%) + MSFT(30%) shared with BROAD = 80% of NARROW's weight
        assert abs(result["BROAD"] - 80.0) < 0.1

    def test_unique_etf_has_zero_contributions(self) -> None:
        result = redundancy_breakdown("UNIQUE", HOLDINGS_DB)
        assert all(v == 0 for v in result.values())

    def test_empty_holdings_returns_empty(self) -> None:
        result = redundancy_breakdown("MISSING", HOLDINGS_DB)
        assert result == {}

    def test_single_etf_portfolio(self) -> None:
        result = redundancy_breakdown("BROAD", {"BROAD": BROAD})
        assert result == {}


# ---------------------------------------------------------------------------
# redundancy_scores (existing function, new tests)
# ---------------------------------------------------------------------------

class TestRedundancyScores:
    def test_returns_dataframe_with_expected_columns(self) -> None:
        positions = [
            {"ticker": "BROAD", "capital": 10000},
            {"ticker": "NARROW", "capital": 5000},
        ]
        df = redundancy_scores(positions, {"BROAD": BROAD, "NARROW": NARROW})
        assert set(df.columns) >= {"etf_ticker", "redundancy_pct", "unique_pct", "ter_wasted", "verdict"}

    def test_narrow_is_more_redundant_than_broad(self) -> None:
        positions = [
            {"ticker": "BROAD", "capital": 10000},
            {"ticker": "NARROW", "capital": 5000},
        ]
        df = redundancy_scores(positions, {"BROAD": BROAD, "NARROW": NARROW})
        broad_r = df.loc[df["etf_ticker"] == "BROAD", "redundancy_pct"].iloc[0]
        narrow_r = df.loc[df["etf_ticker"] == "NARROW", "redundancy_pct"].iloc[0]
        assert narrow_r > broad_r

    def test_unique_etf_has_zero_redundancy(self) -> None:
        positions = [
            {"ticker": "BROAD", "capital": 10000},
            {"ticker": "UNIQUE", "capital": 5000},
        ]
        df = redundancy_scores(positions, {"BROAD": BROAD, "UNIQUE": UNIQUE})
        unique_r = df.loc[df["etf_ticker"] == "UNIQUE", "redundancy_pct"].iloc[0]
        assert unique_r == 0.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_redundancy.py -v`
Expected: `TestRedundancyBreakdown` tests FAIL with `ImportError: cannot import name 'redundancy_breakdown'`; `TestRedundancyScores` tests PASS.

- [ ] **Step 3: Implement `redundancy_breakdown()`**

Add to `src/analytics/redundancy.py` after the `redundancy_scores` function (after line 121):

```python
def redundancy_breakdown(
    etf_ticker: str,
    holdings_db: dict[str, pd.DataFrame],
) -> dict[str, float]:
    """Decompose redundancy: how much does each other ETF contribute?

    For a given ETF, calculates what percentage of its total weight is
    shared with each other ETF in the portfolio. A holding shared with
    multiple ETFs counts towards each (so contributions can sum > 100%).

    Args:
        etf_ticker: The ETF to analyze.
        holdings_db: All portfolio ETFs {ticker: holdings DataFrame}.

    Returns:
        Dict mapping other ETF tickers to their contribution percentage
        (0-100 scale). Empty dict if ETF not found or portfolio has 1 ETF.
    """
    if etf_ticker not in holdings_db or len(holdings_db) < 2:
        return {}

    build_match_keys_from_holdings(holdings_db)

    # Build match key weights for target ETF
    target_df = add_match_key(holdings_db[etf_ticker])
    if "weight_pct" in target_df.columns:
        target_df["weight_pct"] = pd.to_numeric(
            target_df["weight_pct"], errors="coerce"
        ).fillna(0.0)

    target_weights: dict[str, float] = {}
    for _, row in target_df.iterrows():
        key = row.get("_match_key")
        if not key or (isinstance(key, float) and pd.isna(key)):
            continue
        w = float(row.get("weight_pct", 0) or 0)
        target_weights[key] = target_weights.get(key, 0) + w

    total_weight = sum(target_weights.values())
    if total_weight == 0:
        return {}

    target_keys = set(target_weights.keys())

    # For each other ETF, find shared keys and sum shared weight
    contributions: dict[str, float] = {}
    for other_ticker, other_df in holdings_db.items():
        if other_ticker == etf_ticker:
            continue
        other_df = add_match_key(other_df)
        other_keys: set[str] = set()
        for _, row in other_df.iterrows():
            key = row.get("_match_key")
            if key and not (isinstance(key, float) and pd.isna(key)):
                other_keys.add(key)

        shared = target_keys & other_keys
        shared_weight = sum(target_weights[k] for k in shared)
        pct = (shared_weight / total_weight) * 100
        contributions[other_ticker] = round(pct, 2)

    return contributions
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_redundancy.py -v`
Expected: All 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/test_redundancy.py src/analytics/redundancy.py
git commit -m "feat: add redundancy_breakdown() — per-ETF contribution decomposition"
```

---

### Task 2: Update observations for merged page category

**Files:**
- Modify: `src/analytics/observations.py`

Both "redundancy" and "overlap" observations need to use a single page name so `show_observations()` shows them together on the merged page.

- [ ] **Step 1: Change observation page names**

In `src/analytics/observations.py`, replace all `"redundancy"` page values with `"overlap_redundancy"` and all `"overlap"` page values with `"overlap_redundancy"`.

Line 77: change `"high", "redundancy",` to `"high", "overlap_redundancy",`
Line 84: change `"high", "redundancy",` to `"high", "overlap_redundancy",`  
Line 91: change `"medium", "redundancy",` to `"medium", "overlap_redundancy",`
Line 100: change `"high", "overlap",` to `"high", "overlap_redundancy",`
Line 106: change `"medium", "overlap",` to `"medium", "overlap_redundancy",`

- [ ] **Step 2: Verify syntax**

Run: `python3 -c "import ast; ast.parse(open('src/analytics/observations.py').read()); print('OK')"`

- [ ] **Step 3: Commit**

```bash
git add src/analytics/observations.py
git commit -m "refactor: unify observation page category to overlap_redundancy"
```

---

### Task 3: Create merged page `03_overlap_redundancy.py`

**Files:**
- Create: `src/dashboard/pages/03_overlap_redundancy.py`

This is the largest task. The page has 3 layers: summary, per-ETF breakdown, and collapsible heatmap drill-down.

- [ ] **Step 1: Create the merged page**

Create `src/dashboard/pages/03_overlap_redundancy.py`:

```python
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
import plotly.express as px
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
    bar_color = {"green": "#2ecc71", "yellow": "#f39c12", "red": "#e74c3c"}.get(verdict, "#ccc")

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
```

- [ ] **Step 2: Verify syntax**

Run: `python3 -c "import ast; ast.parse(open('src/dashboard/pages/03_overlap_redundancy.py').read()); print('OK')"`

- [ ] **Step 3: Commit**

```bash
git add src/dashboard/pages/03_overlap_redundancy.py
git commit -m "feat: create merged Overlap & Redundancy page with per-ETF breakdown"
```

---

### Task 4: Delete old pages, renumber, update app.py

**Files:**
- Delete: `src/dashboard/pages/03_redundancy.py`
- Delete: `src/dashboard/pages/04_overlap.py`
- Rename: `src/dashboard/pages/05_sector_country.py` → `src/dashboard/pages/04_sector_country.py`
- Rename: `src/dashboard/pages/06_factor_fingerprint.py` → `src/dashboard/pages/05_factor_fingerprint.py`
- Modify: `src/dashboard/app.py` (update page list in landing page text)

- [ ] **Step 1: Delete old page files**

```bash
git rm src/dashboard/pages/03_redundancy.py
git rm src/dashboard/pages/04_overlap.py
```

- [ ] **Step 2: Rename remaining pages**

```bash
git mv src/dashboard/pages/05_sector_country.py src/dashboard/pages/04_sector_country.py
git mv src/dashboard/pages/06_factor_fingerprint.py src/dashboard/pages/05_factor_fingerprint.py
```

- [ ] **Step 3: Update app.py landing page text**

In `src/dashboard/app.py`, replace the page list (lines 60-68):

```python
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
```

- [ ] **Step 4: Run full test suite**

Run: `python3 -m pytest tests/ -v`
Expected: All tests pass. Old tests referencing deleted page files should not exist (pages have no dedicated test files).

- [ ] **Step 5: Verify all page files parse**

```bash
python3 -c "
import ast
for f in ['src/dashboard/pages/01_portfolio_input.py',
          'src/dashboard/pages/02_xray_overview.py',
          'src/dashboard/pages/03_overlap_redundancy.py',
          'src/dashboard/pages/04_sector_country.py',
          'src/dashboard/pages/05_factor_fingerprint.py',
          'src/dashboard/app.py']:
    ast.parse(open(f).read()); print(f'{f} OK')
"
```

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: delete old pages, renumber 05→04 06→05, update app.py navigation"
```

---

### Task 5: Final verification and push

- [ ] **Step 1: Run full test suite**

```bash
python3 -m pytest tests/ -v
```

Expected: All tests pass (386 existing + 8 new redundancy tests).

- [ ] **Step 2: Verify page count**

```bash
ls src/dashboard/pages/*.py | grep -v __pycache__
```

Expected: 5 page files (01 through 05).

- [ ] **Step 3: Squash into single commit and push**

```bash
git reset --soft HEAD~4
git commit -m "feat: merge Overlap+Redundancy into single page with per-ETF breakdown"
git push origin main
```
