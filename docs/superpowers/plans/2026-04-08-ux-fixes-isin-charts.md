# UX Fixes: ISIN→Ticker Display + Chart Readability

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 6 visual bugs in one sprint: ISIN shown instead of resolved ticker across 3 pages, and chart readability issues (pie labels, sunburst overlap, country bar dominance) on page 05.

**Architecture:** Create a shared helper function `get_display_name(identifier)` that reads `st.session_state.display_names` to map ISINs → tickers. Apply it in pages 03, 04, 05. Chart fixes are Plotly layout/trace updates on page 05 only.

**Tech Stack:** Streamlit, Plotly, pandas

---

### File Map

| File | Action | Task |
|---|---|---|
| `src/dashboard/components/display_utils.py` | Create | T1 |
| `src/dashboard/pages/03_redundancy.py` | Modify | T2 |
| `src/dashboard/pages/04_overlap.py` | Modify | T3 |
| `src/dashboard/pages/05_sector_country.py` | Modify | T4 |

---

### Task 1: Create shared display name helper

**Files:**
- Create: `src/dashboard/components/display_utils.py`

- [ ] **Step 1: Create the helper module**

```python
"""Shared display name utilities for dashboard pages."""

from __future__ import annotations

import streamlit as st


def get_display_name(identifier: str) -> str:
    """Map an ETF identifier to its display name.

    Looks up ``st.session_state.display_names`` which is populated
    during analysis in page 01. Falls back to the raw identifier.

    Args:
        identifier: ETF ticker or ISIN.

    Returns:
        Human-friendly display name (resolved ticker or original identifier).
    """
    display_names = st.session_state.get("display_names", {})
    return display_names.get(identifier, identifier)


def map_display_names(identifiers: list[str]) -> list[str]:
    """Map a list of identifiers to display names.

    Args:
        identifiers: List of ETF tickers or ISINs.

    Returns:
        List of display names in the same order.
    """
    return [get_display_name(i) for i in identifiers]
```

- [ ] **Step 2: Commit**

```bash
git add src/dashboard/components/display_utils.py
git commit -m "feat: add shared ISIN→ticker display helper"
```

---

### Task 2: Fix ISIN display in page 03 (Redundancy)

**Files:**
- Modify: `src/dashboard/pages/03_redundancy.py`

The `redundancy_df["etf_ticker"]` column contains the raw identifier the user typed (which may be an ISIN). We need to map it through `display_names` for display purposes.

- [ ] **Step 1: Add import and create display column**

After line 30 (`redundancy_df['real_weight_pct'] = ...`), add:

```python
from src.dashboard.components.display_utils import get_display_name

redundancy_df = redundancy_df.copy()
redundancy_df["display_name"] = redundancy_df["etf_ticker"].map(get_display_name)
```

- [ ] **Step 2: Update summary box to use display_name**

Change line 35:
```python
_red_scores = dict(zip(redundancy_df["etf_ticker"], redundancy_df["redundancy_pct"] / 100))
```
to:
```python
_red_scores = dict(zip(redundancy_df["display_name"], redundancy_df["redundancy_pct"] / 100))
```

Change line 36:
```python
_ter_wasted_all = dict(zip(redundancy_df["etf_ticker"], redundancy_df["ter_wasted"].fillna(0)))
```
to:
```python
_ter_wasted_all = dict(zip(redundancy_df["display_name"], redundancy_df["ter_wasted"].fillna(0)))
```

- [ ] **Step 3: Update bar chart to use display_name on Y-axis**

Change line 66 (`y="etf_ticker"`) to:
```python
    y="display_name",
```

Change line 68 (`labels=...`) to:
```python
    labels={"redundancy_pct": "Redundancy (%)", "display_name": "ETF"},
```

- [ ] **Step 4: Update TER wasted list to use display_name**

Change line 92 (`ticker = row["etf_ticker"]`) to:
```python
    ticker = row["display_name"]
```

- [ ] **Step 5: Run tests and commit**

```bash
python3 -m pytest tests/ -q --tb=line
git add src/dashboard/pages/03_redundancy.py
git commit -m "fix: redundancy page shows ticker instead of ISIN"
```

---

### Task 3: Fix ISIN display in page 04 (Overlap)

**Files:**
- Modify: `src/dashboard/pages/04_overlap.py`

The heatmap axes use `overlap_mat.columns` which are raw identifiers. The selectboxes use `holdings_db.keys()`. Both need mapping.

- [ ] **Step 1: Add import after existing imports**

After line 34 (`from src.analytics.overlap import shared_holdings`), add:

```python
from src.dashboard.components.display_utils import get_display_name, map_display_names
```

- [ ] **Step 2: Map heatmap labels to display names**

Change line 37:
```python
labels = overlap_mat.columns.tolist()
```
to:
```python
raw_labels = overlap_mat.columns.tolist()
labels = map_display_names(raw_labels)
```

- [ ] **Step 3: Map selectbox tickers to display names and back**

Replace lines 68-75 (shared holdings section) with:

```python
tickers = list(holdings_db.keys())
display_tickers = map_display_names(tickers)
if len(tickers) >= 2:
    col1, col2 = st.columns(2)
    with col1:
        etf_a_display = st.selectbox("ETF A", display_tickers, index=0)
    with col2:
        default_b = 1 if len(display_tickers) > 1 else 0
        etf_b_display = st.selectbox("ETF B", display_tickers, index=default_b)

    # Map display names back to raw identifiers for data lookup
    _display_to_raw = dict(zip(display_tickers, tickers))
    etf_a = _display_to_raw[etf_a_display]
    etf_b = _display_to_raw[etf_b_display]
```

Update the shared holdings column headers (line 83) — replace `etf_a`/`etf_b` with display names:

```python
            display.columns = ["Titolo", f"Peso {etf_a_display} %", f"Peso {etf_b_display} %", "Delta %"]
```

Update the equality check (line 89):
```python
    elif etf_a_display == etf_b_display:
```

- [ ] **Step 4: Map unique exposure selectbox and text**

Replace lines 96-101 with:

```python
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
```

Then in the success/warning/error messages (lines 113-133), replace all `{target}` with `{target_display}`.

Also update the column header on line 142:
```python
                "Titolo", f"Peso in {target_display} %", "Coperto da altri %",
```

And in the `"Coperto da"` column, the `main_etf` value comes from the analytics engine and may contain raw identifiers. Map it:
```python
        main_etf = get_display_name(ue["main_covering_etf"])
```

- [ ] **Step 5: Run tests and commit**

```bash
python3 -m pytest tests/ -q --tb=line
git add src/dashboard/pages/04_overlap.py
git commit -m "fix: overlap page shows ticker instead of ISIN in heatmap and selectors"
```

---

### Task 4: Fix chart readability on page 05 (Sector & Country)

**Files:**
- Modify: `src/dashboard/pages/05_sector_country.py`

Three chart fixes: (a) sector pie labels, (b) country bar dominance, (c) sunburst overlap.

- [ ] **Step 1: Fix sector pie chart — increase height, better label positioning**

Replace lines 52-60 with:

```python
    fig_s = px.pie(
        sector_df,
        names="sector",
        values="weight_pct",
        hole=0.35,
    )
    fig_s.update_traces(
        textposition="outside",
        textinfo="label+percent",
        textfont_size=11,
        insidetextorientation="horizontal",
    )
    fig_s.update_layout(
        showlegend=False,
        height=500,
        margin=dict(l=20, r=20, t=40, b=20),
    )
    st.plotly_chart(fig_s, use_container_width=True)
```

- [ ] **Step 2: Fix country bar chart — add value labels, limit to top 10**

Replace lines 64-75 with:

```python
    top_countries = country_df.head(10)
    fig_c = px.bar(
        top_countries,
        x="weight_pct",
        y="country",
        orientation="h",
        labels={"weight_pct": "Peso (%)", "country": ""},
        color="weight_pct",
        color_continuous_scale="Viridis",
        text=top_countries["weight_pct"].round(1).astype(str) + "%",
    )
    fig_c.update_traces(textposition="outside")
    fig_c.update_layout(
        yaxis=dict(autorange="reversed"),
        showlegend=False,
        height=450,
        xaxis=dict(range=[0, top_countries["weight_pct"].max() * 1.15]),
    )
    st.plotly_chart(fig_c, use_container_width=True)
```

- [ ] **Step 3: Fix sunburst chart — radial labels, hide small segments**

Replace lines 141-149 with:

```python
fig_sun = px.sunburst(
    sun_df,
    path=["country", "sector", "name"],
    values="real_weight_pct",
    color="real_weight_pct",
    color_continuous_scale="Blues",
)
fig_sun.update_traces(
    textinfo="label",
    insidetextorientation="radial",
    textfont_size=10,
)
fig_sun.update_layout(
    height=650,
    margin=dict(l=10, r=10, t=40, b=10),
)
st.plotly_chart(fig_sun, use_container_width=True)
```

- [ ] **Step 4: Run tests and commit**

```bash
python3 -m pytest tests/ -q --tb=line
git add src/dashboard/pages/05_sector_country.py
git commit -m "fix: chart readability — pie labels, country bar values, sunburst layout"
```

---

### Task 5: Final verification and combined commit

- [ ] **Step 1: Run full test suite**

```bash
python3 -m pytest tests/ -v
```

Expected: all tests pass (386+).

- [ ] **Step 2: Squash into single commit and push**

```bash
git reset --soft HEAD~4
git commit -m "fix: ISIN→ticker display, chart readability (pie, sunburst, bars)"
git push origin main
```
