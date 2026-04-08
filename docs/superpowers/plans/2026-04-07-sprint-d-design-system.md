# Sprint D — Design System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transform the dashboard from "Python script" to "professional portfolio diagnostics tool" with a consistent design system: Inter font, semantic color system, global KPI header, interpretive KPI cards, auto-generated observations, trust footer, and 3-layer page layouts.

**Architecture:** Task 1 creates the foundation (CSS + colors). Tasks 2-3 create reusable components (global header, KPI cards, observations). Tasks 4-6 apply the design system to each page group. Task 7 adds the trust footer. Each task produces a working, non-breaking change.

**Tech Stack:** Streamlit custom CSS (`st.markdown` with `unsafe_allow_html`), Inter font (Google Fonts CDN), Plotly for charts (unchanged).

**Key Risk:** Streamlit CSS selectors (`data-testid`) change across versions. Plan: test each CSS block, fall back to `st.markdown` HTML if a selector fails. Document failures.

---

## File Structure

| File | Action | Feature | ~Lines |
|------|--------|---------|--------|
| `src/dashboard/styles/__init__.py` | Create | D1 | 0 |
| `src/dashboard/styles/global.css` | Create | D1 | ~60 |
| `src/dashboard/styles/colors.py` | Create | D1 | ~30 |
| `src/dashboard/components/global_header.py` | Create | D2 | ~50 |
| `src/dashboard/components/kpi_card.py` | Create | D3 | ~100 |
| `src/analytics/observations.py` | Create | D4 | ~120 |
| `src/dashboard/components/observations_box.py` | Create | D4 | ~40 |
| `src/dashboard/app.py` | Modify | D1, D5 | +30 |
| `src/dashboard/pages/01_portfolio_input.py` | Modify | D2, D4 | +10 |
| `src/dashboard/pages/02_xray_overview.py` | Modify | D2, D3, D4, D6 | +30 |
| `src/dashboard/pages/03_redundancy.py` | Modify | D2, D4, D6 | +30 |
| `src/dashboard/pages/04_overlap.py` | Modify | D2, D4 | +5 |
| `src/dashboard/pages/05_sector_country.py` | Modify | D2, D4 | +5 |
| `src/dashboard/pages/06_factor_fingerprint.py` | Modify | D2, D4, D6 | +20 |
| `tests/test_observations.py` | Create | D4 | ~80 |

---

### Task 1: D1 — CSS + Color system foundation

**Files:**
- Create: `src/dashboard/styles/__init__.py`
- Create: `src/dashboard/styles/global.css`
- Create: `src/dashboard/styles/colors.py`
- Modify: `src/dashboard/app.py`

- [ ] **Step 1: Create styles directory and files**

Create `src/dashboard/styles/__init__.py` (empty).

Create `src/dashboard/styles/colors.py`:

```python
"""Semantic color constants for the dashboard."""

GREEN = "#16a34a"
GREEN_LIGHT = "#dcfce7"
YELLOW = "#d97706"
YELLOW_LIGHT = "#fef9c3"
RED = "#dc2626"
RED_LIGHT = "#fee2e2"
BLUE = "#2563eb"
BLUE_LIGHT = "#dbeafe"
PURPLE = "#7c3aed"
PURPLE_LIGHT = "#ede9fe"
GRAY = "#6b7280"
GRAY_LIGHT = "#f3f4f6"


def severity_color(severity: str) -> tuple[str, str]:
    """Return (bg_color, text_color) for severity level."""
    return {
        "high": (RED_LIGHT, RED),
        "medium": (YELLOW_LIGHT, YELLOW),
        "low": (GREEN_LIGHT, GREEN),
    }.get(severity, (GRAY_LIGHT, GRAY))
```

Create `src/dashboard/styles/global.css`:

```css
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
}

h1 { font-weight: 700; font-size: 1.5rem; color: #0f1117; }
h2 { font-weight: 600; font-size: 1.15rem; color: #1a1d27; }
h3 { font-weight: 500; font-size: 1.0rem; color: #2d3142; }

[data-testid="stMetricValue"] {
    font-family: 'Inter', sans-serif;
    font-weight: 700;
}

[data-testid="stMetricLabel"] {
    font-size: 0.72rem;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: #6b7280;
}

[data-testid="stExpander"] {
    border: 1px solid #e5e7eb;
    border-radius: 6px;
}

[data-testid="stCaptionContainer"] {
    font-size: 0.75rem;
    color: #9ca3af;
}

[data-testid="stSidebar"] {
    background-color: #f9fafb;
}
```

- [ ] **Step 2: Load CSS in app.py**

In `src/dashboard/app.py`, after `st.set_page_config(...)` and before the session state defaults, add:

```python
import os

def _load_css():
    css_path = os.path.join(os.path.dirname(__file__), "styles", "global.css")
    with open(css_path) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

_load_css()
```

- [ ] **Step 3: Verify**

```bash
python3 -c "import ast; ast.parse(open('src/dashboard/app.py').read()); print('OK')"
python3 -c "from src.dashboard.styles.colors import GREEN, severity_color; print('colors OK')"
```

- [ ] **Step 4: Commit**

```bash
git add src/dashboard/styles/ src/dashboard/app.py
git commit -m "feat(D1): CSS design system + Inter font + semantic colors"
```

---

### Task 2: D2 — Global header component

**Files:**
- Create: `src/dashboard/components/global_header.py`
- Modify: `src/dashboard/pages/01_portfolio_input.py` (add header call)
- Modify: `src/dashboard/pages/02_xray_overview.py` (add header call)
- Modify: `src/dashboard/pages/03_redundancy.py` (add header call)
- Modify: `src/dashboard/pages/04_overlap.py` (add header call)
- Modify: `src/dashboard/pages/05_sector_country.py` (add header call)
- Modify: `src/dashboard/pages/06_factor_fingerprint.py` (add header call)

- [ ] **Step 1: Create global header component**

Create `src/dashboard/components/global_header.py`:

```python
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
    cols[4].markdown(f"AS {as_str}")
    cols[5].markdown(f"Top-10 **{top10:.1f}%**")
    cols[6].markdown(f"📐 vs {bench_display}")

    st.divider()
```

- [ ] **Step 2: Add header call to ALL 6 pages**

In each page file, insert `show_global_header()` as the FIRST thing after imports and `st.header()`. The pattern is the same for each file:

Find the `st.header("...")` line. Insert AFTER it:

```python
from src.dashboard.components.global_header import show_global_header
show_global_header()
```

Do this for:
- `01_portfolio_input.py` (after line 37 `st.header("📥 Portfolio Input")`)
- `02_xray_overview.py` (after line 14 `st.header("🔍 X-Ray Overview")`)
- `03_redundancy.py` (after line 14 `st.header("♻️ ETF Redundancy")`)
- `04_overlap.py` (after line 14 `st.header("🔥 Overlap Heatmap")`)
- `05_sector_country.py` (after line 14 `st.header("🌍 Sector & Country")`)
- `06_factor_fingerprint.py` (after line 14 `st.header("🧬 Factor Fingerprint")`)

- [ ] **Step 3: Verify syntax on all files**

```bash
for f in src/dashboard/pages/*.py; do python3 -c "import ast; ast.parse(open('$f').read()); print('$f OK')"; done
```

- [ ] **Step 4: Commit**

```bash
git add src/dashboard/components/global_header.py src/dashboard/pages/
git commit -m "feat(D2): global KPI header bar on all pages"
```

---

### Task 3: D3 — KPI cards with semantic interpretation

**Files:**
- Create: `src/dashboard/components/kpi_card.py`
- Modify: `src/dashboard/pages/02_xray_overview.py`

- [ ] **Step 1: Create KPI card component**

Create `src/dashboard/components/kpi_card.py`:

```python
"""KPI cards with semantic interpretation badges."""

from __future__ import annotations

import streamlit as st

from src.dashboard.styles.colors import BLUE, GRAY, GREEN, RED, YELLOW


def kpi_card(
    label: str,
    value: str,
    interpretation: str,
    color: str,
    tooltip: str,
    delta_str: str | None = None,
) -> None:
    """Render a KPI card with value + colored interpretation."""
    st.metric(label=label, value=value, delta=delta_str, help=tooltip)
    badge = {"#16a34a": "🟢", "#d97706": "🟡", "#dc2626": "🔴",
             "#2563eb": "🔵", "#6b7280": "⚪"}.get(color, "⚪")
    st.markdown(
        f"<span style='color:{color}; font-size:0.8rem; font-weight:500;'>"
        f"{badge} {interpretation}</span>",
        unsafe_allow_html=True,
    )


def render_hhi_card(hhi: float, col) -> None:
    tooltip = ("Misura quanto il portafoglio dipende da pochi titoli. "
               "Sotto 0.05 = ben diversificato, sopra 0.15 = troppo concentrato.")
    with col:
        if hhi < 0.10:
            kpi_card("HHI", f"{hhi:.4f}", "Low concentration", GREEN, tooltip)
        elif hhi < 0.15:
            kpi_card("HHI", f"{hhi:.4f}", "Moderate concentration", YELLOW, tooltip)
        else:
            kpi_card("HHI", f"{hhi:.4f}", "High concentration", RED, tooltip)


def render_effective_n_card(n: float, col) -> None:
    tooltip = ("Numero equivalente di titoli se fossero tutti con lo stesso peso. "
               "Effective N basso = portafoglio dominato da pochi nomi.")
    n_int = int(n)
    with col:
        if n > 300:
            kpi_card("Effective N", str(n_int), "Well diversified", GREEN, tooltip)
        elif n > 100:
            kpi_card("Effective N", str(n_int), "Moderate diversification", YELLOW, tooltip)
        else:
            kpi_card("Effective N", str(n_int), "Concentrated", RED, tooltip)


def render_active_share_card(active_share: float | None, col) -> None:
    tooltip = ("Quanto il portafoglio è diverso dal benchmark. "
               "0% = identico, 100% = completamente diverso. "
               "Sotto 20% = closet indexing.")
    with col:
        if active_share is None:
            kpi_card("Active Share", "N/D", "Benchmark non selezionato", GRAY, tooltip)
        elif active_share < 20:
            kpi_card("Active Share", f"{active_share:.1f}%", "Closet indexing", RED, tooltip)
        elif active_share < 40:
            kpi_card("Active Share", f"{active_share:.1f}%", "Low active", YELLOW, tooltip)
        else:
            kpi_card("Active Share", f"{active_share:.1f}%", "Active positioning", BLUE, tooltip)


def render_top10_card(top10: float, col) -> None:
    tooltip = ("Somma dei pesi dei 10 titoli più grandi. "
               "Se è 35%, un terzo del portafoglio dipende da 10 aziende.")
    with col:
        if top10 < 15:
            kpi_card("Top-10 Conc.", f"{top10:.1f}%", "Balanced", GREEN, tooltip)
        elif top10 < 25:
            kpi_card("Top-10 Conc.", f"{top10:.1f}%", "Moderate top-heavy", YELLOW, tooltip)
        else:
            kpi_card("Top-10 Conc.", f"{top10:.1f}%", "Top-heavy", RED, tooltip)
```

- [ ] **Step 2: Replace KPI row in 02_xray_overview.py**

Find the current KPI section (lines 50-64, from `hhi_stats = portfolio_hhi(aggregated)` through `k5.metric(...)`). Replace with:

```python
# ── KPI row ─────────────────────────────────────────────────────────
hhi_stats = portfolio_hhi(aggregated)

active_share_result = st.session_state.get("active_share_result")
active_share_pct = active_share_result["active_share_pct"] if active_share_result else None

from src.dashboard.components.kpi_card import (
    render_active_share_card, render_effective_n_card,
    render_hhi_card, render_top10_card,
)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Titoli unici", f"{len(aggregated):,}", help="Numero totale di titoli unici nel portafoglio aggregato.")
render_hhi_card(hhi_stats["hhi"], k2)
render_effective_n_card(hhi_stats["effective_n"], k3)
render_active_share_card(active_share_pct, k4)
render_top10_card(hhi_stats["top_10_pct"], k5)
```

Also remove the old KPI expanders (the `with st.expander("ℹ️ Cos'è HHI...")` blocks through `Cos'è Top-10 Concentration?`) since tooltips now provide that info.

- [ ] **Step 3: Verify syntax**

```bash
python3 -c "import ast; ast.parse(open('src/dashboard/pages/02_xray_overview.py').read()); print('OK')"
```

- [ ] **Step 4: Commit**

```bash
git add src/dashboard/components/kpi_card.py src/dashboard/pages/02_xray_overview.py
git commit -m "feat(D3): semantic KPI cards with interpretation badges"
```

---

### Task 4: D4 — Observations engine + UI component + tests

**Files:**
- Create: `src/analytics/observations.py`
- Create: `src/dashboard/components/observations_box.py`
- Create: `tests/test_observations.py`
- Modify: `src/dashboard/pages/01_portfolio_input.py` (save observations after analysis)
- Modify: `src/dashboard/pages/02_xray_overview.py` (show observations)
- Modify: `src/dashboard/pages/03_redundancy.py` (show observations)
- Modify: `src/dashboard/pages/04_overlap.py` (show observations)
- Modify: `src/dashboard/pages/05_sector_country.py` (show observations)
- Modify: `src/dashboard/pages/06_factor_fingerprint.py` (show observations)

- [ ] **Step 1: Create observations engine**

Create `src/analytics/observations.py`:

```python
"""Auto-generated key observations for portfolio analysis."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass
class Observation:
    text: str
    severity: Literal["high", "medium", "info"]
    page: str


def generate_observations(
    hhi: float,
    effective_n: float,
    active_share: float | None,
    top10_weight: float,
    top1_name: str,
    top1_weight: float,
    redundancy_scores: dict[str, float],
    ter_wasted_eur: dict[str, float],
    overlap_pairs: list[tuple[str, str, float]],
    us_weight: float,
    benchmark_name: str,
) -> list[Observation]:
    """Generate key observations from portfolio analysis data."""
    obs: list[Observation] = []

    # X-Ray observations
    if hhi > 0.15:
        obs.append(Observation(
            "Concentrazione significativa: i titoli principali dominano "
            "l'esposizione del portafoglio.",
            "high", "xray",
        ))
    if top1_weight > 0.08:
        obs.append(Observation(
            f"{top1_name} pesa il {top1_weight * 100:.1f}% del portafoglio "
            "— alta esposizione su un singolo titolo.",
            "high", "xray",
        ))
    if effective_n < 100:
        obs.append(Observation(
            f"Diversificazione effettiva equivalente a {effective_n:.0f} titoli "
            "equi-pesati — portafoglio concentrato.",
            "medium", "xray",
        ))
    if active_share is not None and active_share < 20:
        obs.append(Observation(
            f"Active Share {active_share:.0f}%: il portafoglio replica "
            f"quasi identicamente {benchmark_name}.",
            "high", "xray",
        ))
    if us_weight > 70:
        obs.append(Observation(
            f"Esposizione USA al {us_weight:.0f}% — limitata "
            "diversificazione geografica.",
            "medium", "xray",
        ))
        obs.append(Observation(
            f"Esposizione USA al {us_weight:.0f}% — limitata "
            "diversificazione geografica.",
            "medium", "sector",
        ))

    # Redundancy observations
    if redundancy_scores:
        max_ticker = max(redundancy_scores, key=redundancy_scores.get)
        max_r = redundancy_scores[max_ticker]
        if max_r > 0.80:
            obs.append(Observation(
                f"{max_ticker} è ridondante per il {max_r * 100:.0f}% — quasi tutte "
                "le sue holdings sono già presenti in altri ETF.",
                "high", "redundancy",
            ))
        total_wasted = sum(ter_wasted_eur.values())
        if total_wasted > 100:
            obs.append(Observation(
                f"Stai pagando circa €{total_wasted:.0f}/anno in commissioni "
                "su holdings duplicate.",
                "high", "redundancy",
            ))
        moderate_count = sum(1 for v in redundancy_scores.values() if v > 0.50)
        if moderate_count > 1:
            obs.append(Observation(
                "Più ETF hanno ridondanza superiore al 50% — considera "
                "una consolidazione.",
                "medium", "redundancy",
            ))

    # Overlap observations
    for etf_a, etf_b, overlap in overlap_pairs:
        if overlap > 60:
            obs.append(Observation(
                f"{etf_a} e {etf_b} si sovrappongono per il {overlap:.0f}% "
                "— alta ridondanza pairwise.",
                "high", "overlap",
            ))
        elif overlap > 40:
            obs.append(Observation(
                f"{etf_a} e {etf_b} condividono il {overlap:.0f}% "
                "dell'esposizione.",
                "medium", "overlap",
            ))

    return obs
```

- [ ] **Step 2: Create observations UI component**

Create `src/dashboard/components/observations_box.py`:

```python
"""Observations box UI component."""

from __future__ import annotations

import streamlit as st

from src.analytics.observations import Observation


def show_observations(
    observations: list[Observation],
    page: str,
    max_shown: int = 4,
) -> None:
    """Show key observations for the current page."""
    page_obs = [o for o in observations if o.page == page]
    page_obs.sort(key=lambda o: {"high": 0, "medium": 1, "info": 2}[o.severity])
    page_obs = page_obs[:max_shown]

    if not page_obs:
        return

    icon = {"high": "🔴", "medium": "🟡", "info": "🔵"}

    lines = "<br>".join(
        f"{icon[o.severity]} <span style='font-size:0.9rem;'>{o.text}</span>"
        for o in page_obs
    )
    st.markdown(
        f"""<div style='background:#f8fafc; border:1px solid #e2e8f0;
        border-left:4px solid #2563eb; border-radius:6px;
        padding:12px 16px; margin-bottom:16px;'>
        <div style='font-size:0.78rem; font-weight:600; color:#64748b;
        text-transform:uppercase; letter-spacing:0.05em;
        margin-bottom:8px;'>🔍 Osservazioni chiave</div>
        {lines}
        </div>""",
        unsafe_allow_html=True,
    )
```

- [ ] **Step 3: Create tests**

Create `tests/test_observations.py`:

```python
"""Tests for observations engine."""

import pytest

from src.analytics.observations import generate_observations


_BASE = {
    "hhi": 0.05,
    "effective_n": 350,
    "active_share": 45.0,
    "top10_weight": 20.0,
    "top1_name": "NVIDIA CORP",
    "top1_weight": 0.04,
    "redundancy_scores": {"CSPX": 0.20, "SWDA": 0.10},
    "ter_wasted_eur": {"CSPX": 10.0, "SWDA": 5.0},
    "overlap_pairs": [("CSPX", "SWDA", 30.0)],
    "us_weight": 55.0,
    "benchmark_name": "MSCI World",
}


class TestXrayObservations:
    def test_closet_indexing(self):
        obs = generate_observations(**{**_BASE, "active_share": 16.0})
        xray = [o for o in obs if o.page == "xray"]
        assert any("Active Share" in o.text for o in xray)

    def test_high_concentration(self):
        obs = generate_observations(**{**_BASE, "top1_weight": 0.12})
        xray = [o for o in obs if o.page == "xray"]
        assert any(o.severity == "high" for o in xray)

    def test_high_us_weight(self):
        obs = generate_observations(**{**_BASE, "us_weight": 75.0})
        xray = [o for o in obs if o.page == "xray"]
        assert any("USA" in o.text for o in xray)


class TestRedundancyObservations:
    def test_high_redundancy(self):
        obs = generate_observations(**{
            **_BASE,
            "redundancy_scores": {"CSPX": 0.99, "SWDA": 0.10},
        })
        red = [o for o in obs if o.page == "redundancy"]
        assert any(o.severity == "high" for o in red)

    def test_high_ter_wasted(self):
        obs = generate_observations(**{
            **_BASE,
            "ter_wasted_eur": {"CSPX": 80.0, "SWDA": 50.0},
        })
        red = [o for o in obs if o.page == "redundancy"]
        assert any("commissioni" in o.text for o in red)


class TestOverlapObservations:
    def test_high_overlap(self):
        obs = generate_observations(**{
            **_BASE,
            "overlap_pairs": [("CSPX", "SWDA", 65.0)],
        })
        ovr = [o for o in obs if o.page == "overlap"]
        assert any(o.severity == "high" for o in ovr)


class TestDiversifiedPortfolio:
    def test_no_high_observations(self):
        obs = generate_observations(**_BASE)
        assert not any(o.severity == "high" for o in obs)

    def test_filtered_by_page(self):
        obs = generate_observations(**_BASE)
        for o in obs:
            assert o.page in ("xray", "redundancy", "overlap", "sector", "factor")
```

- [ ] **Step 4: Run tests**

```bash
python3 -m pytest tests/test_observations.py -v
```

- [ ] **Step 5: Add observations computation to portfolio_input.py**

In `src/dashboard/pages/01_portfolio_input.py`, find the line where `last_analyzed_portfolio` is saved (after `st.session_state.analysis_timestamp = time.time()`). Insert BEFORE it:

```python
    # Generate observations for all pages
    from src.analytics.observations import generate_observations
    from src.analytics.aggregator import country_exposure

    country_df = country_exposure(aggregated)
    us_w = 0.0
    if not country_df.empty:
        us_row = country_df[country_df["country"].str.contains("United States", case=False, na=False)]
        us_w = us_row["weight_pct"].sum() if not us_row.empty else 0.0

    red_df = st.session_state.get("redundancy_df")
    red_scores = {}
    ter_wasted = {}
    if red_df is not None and not red_df.empty:
        red_scores = dict(zip(red_df["etf_ticker"], red_df["redundancy_pct"] / 100))
        ter_wasted = dict(zip(red_df["etf_ticker"], red_df["ter_wasted"]))

    overlap_mat = st.session_state.get("overlap_matrix")
    overlap_pairs = []
    if overlap_mat is not None:
        labels = overlap_mat.columns.tolist()
        for i in range(len(labels)):
            for j in range(i + 1, len(labels)):
                overlap_pairs.append((labels[i], labels[j], overlap_mat.iloc[i, j]))

    top1 = aggregated.nlargest(1, "real_weight_pct").iloc[0] if not aggregated.empty else None
    bench_labels = {"MSCI_WORLD": "MSCI World", "SP500": "S&P 500",
                    "MSCI_EM": "MSCI EM", "FTSE_ALL_WORLD": "FTSE All-World"}
    bench_display = bench_labels.get(st.session_state.benchmark_name or "", "mercato")

    as_result = st.session_state.get("active_share_result")
    as_pct = as_result["active_share_pct"] if as_result else None

    from src.analytics.overlap import portfolio_hhi
    hhi_stats_obs = portfolio_hhi(aggregated)

    st.session_state.observations = generate_observations(
        hhi=hhi_stats_obs["hhi"],
        effective_n=hhi_stats_obs["effective_n"],
        active_share=as_pct,
        top10_weight=hhi_stats_obs["top_10_pct"],
        top1_name=top1["name"] if top1 is not None else "",
        top1_weight=(top1["real_weight_pct"] / 100) if top1 is not None else 0,
        redundancy_scores=red_scores,
        ter_wasted_eur=ter_wasted,
        overlap_pairs=overlap_pairs,
        us_weight=us_w,
        benchmark_name=bench_display,
    )
```

- [ ] **Step 6: Add show_observations to pages 02-06**

In each analytical page, add after the `show_global_header()` call:

```python
from src.dashboard.components.observations_box import show_observations
_obs = st.session_state.get("observations", [])
show_observations(_obs, "PAGE_NAME")
```

Where PAGE_NAME is:
- `02_xray_overview.py`: `"xray"`
- `03_redundancy.py`: `"redundancy"`
- `04_overlap.py`: `"overlap"`
- `05_sector_country.py`: `"sector"`
- `06_factor_fingerprint.py`: `"factor"`

- [ ] **Step 7: Verify syntax on all files**

```bash
for f in src/dashboard/pages/*.py; do python3 -c "import ast; ast.parse(open('$f').read()); print('$f OK')"; done
```

- [ ] **Step 8: Commit**

```bash
git add src/analytics/observations.py src/dashboard/components/observations_box.py tests/test_observations.py src/dashboard/pages/
git commit -m "feat(D4): key observations engine + UI component on all pages"
```

---

### Task 5: D5 — Trust footer

**Files:**
- Modify: `src/dashboard/app.py`

- [ ] **Step 1: Add footer function to app.py**

Append at the end of `src/dashboard/app.py`:

```python
# ── Trust footer (rendered by each page via import) ────────────────
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
```

Note: the footer will be called from individual pages, not from app.py itself (since Streamlit multipage apps run each page script independently).

- [ ] **Step 2: Add footer to all analytical pages (02-06)**

In each page file (02-06), append at the very end:

```python
# ── Footer ─────────────────────────────────────────────────────────
from src.dashboard.app import show_footer
show_footer()
```

Note: This import works because app.py is a Python module. If circular import issues arise, move `show_footer` to a separate module like `src/dashboard/components/footer.py` instead.

- [ ] **Step 3: Verify syntax**

```bash
for f in src/dashboard/pages/0[2-6]*.py; do python3 -c "import ast; ast.parse(open('$f').read()); print('$f OK')"; done
```

- [ ] **Step 4: Commit**

```bash
git add src/dashboard/app.py src/dashboard/pages/
git commit -m "feat(D5): trust footer with data sources on all pages"
```

---

### Task 6: D6 — Page layout refinements (Redundancy summary + Factor tilt badges)

**Files:**
- Modify: `src/dashboard/pages/03_redundancy.py`
- Modify: `src/dashboard/pages/06_factor_fingerprint.py`

- [ ] **Step 1: Add redundancy summary box in 03_redundancy.py**

Find the horizontal bar chart section. Insert BEFORE `fig = px.bar(...)`:

```python
from src.dashboard.styles.colors import GREEN_LIGHT, RED_LIGHT, YELLOW_LIGHT

# Summary box
red_scores = dict(zip(redundancy_df["etf_ticker"], redundancy_df["redundancy_pct"] / 100))
ter_wasted_all = dict(zip(redundancy_df["etf_ticker"], redundancy_df["ter_wasted"].fillna(0)))
total_ter_wasted = sum(ter_wasted_all.values())
max_redundant = max(red_scores, key=red_scores.get) if red_scores else ""
max_r = red_scores.get(max_redundant, 0)

level = "ALTA 🔴" if max_r > 0.70 else ("MODERATA 🟡" if max_r > 0.40 else "BASSA 🟢")
color = RED_LIGHT if max_r > 0.70 else (YELLOW_LIGHT if max_r > 0.40 else GREEN_LIGHT)

st.markdown(
    f"""<div style='background:{color}; border-radius:8px;
    padding:16px 20px; margin-bottom:20px;'>
    <div style='font-size:0.78rem; font-weight:600; color:#374151;
    text-transform:uppercase; letter-spacing:0.04em;'>
    Livello ridondanza portafoglio</div>
    <div style='font-size:1.4rem; font-weight:700; margin:4px 0;'>
    {level}</div>
    <div style='font-size:0.88rem; color:#374151;'>
    TER inefficienza stimata: <strong>€{total_ter_wasted:.0f}/anno</strong>
    &nbsp;|&nbsp;
    ETF più ridondante: <strong>{max_redundant} ({max_r*100:.0f}%)</strong>
    </div></div>""",
    unsafe_allow_html=True,
)
```

- [ ] **Step 2: Add factor tilt badges in 06_factor_fingerprint.py**

Find the line `st.subheader("Radar — profilo fattoriale")`. Insert BEFORE it (after the `factor_result` is confirmed to exist):

```python
# Factor tilt summary badges
scores = factor_result["factor_scores"]
vg = scores.get("value_growth", {})
size = scores.get("size", {})
quality = scores.get("quality", {})
dy = scores.get("dividend_yield", {})

pe_val = vg.get("weighted_pe")
roe_raw = quality.get("weighted_roe", 0) or 0
roe_pct = roe_raw * 100 if roe_raw < 1 else roe_raw
large_pct = size.get("Large", 0)
dy_raw = dy.get("weighted_yield", 0) or 0
dy_pct = dy_raw * 100 if dy_raw < 1 else dy_raw

tilts = []
if pe_val and pe_val > 25:
    tilts.append("Growth tilt 🟣")
elif pe_val and pe_val < 15:
    tilts.append("Value tilt 🟣")
if large_pct > 70:
    tilts.append(f"Large-cap {large_pct:.0f}%")
if roe_pct > 20:
    tilts.append("Quality sopra media")
if dy_pct < 1.5:
    tilts.append("Div yield basso")
elif dy_pct > 3:
    tilts.append("Income oriented")

if tilts:
    st.markdown(
        " &nbsp;·&nbsp; ".join(
            f"<span style='background:#ede9fe; color:#5b21b6;"
            f"padding:2px 8px; border-radius:12px;"
            f"font-size:0.8rem; font-weight:500;'>{t}</span>"
            for t in tilts
        ),
        unsafe_allow_html=True,
    )
    st.markdown("")
```

Note: the variable `scores` will shadow the one defined later in the file. To avoid this, use different variable names. Actually, checking the file structure — `scores = factor_result["factor_scores"]` is already defined at line 52. So this badge code should go AFTER that line, using the existing `scores` variable. Place it after `bench_cmp = factor_result.get("benchmark_comparison")` and before `st.subheader("Radar — profilo fattoriale")`.

- [ ] **Step 3: Verify syntax**

```bash
python3 -c "import ast; ast.parse(open('src/dashboard/pages/03_redundancy.py').read()); print('OK')"
python3 -c "import ast; ast.parse(open('src/dashboard/pages/06_factor_fingerprint.py').read()); print('OK')"
```

- [ ] **Step 4: Commit**

```bash
git add src/dashboard/pages/03_redundancy.py src/dashboard/pages/06_factor_fingerprint.py
git commit -m "feat(D6): redundancy summary box + factor tilt badges"
```

---

### Task 7: Final — tests + squash + push

- [ ] **Step 1: Run ALL tests**

```bash
python3 -m pytest tests/ -x -q
```

Expected: 319 + ~8 new = ~327 tests pass, zero regressions.

- [ ] **Step 2: Verify syntax on all modified files**

```bash
for f in src/dashboard/app.py src/dashboard/pages/*.py src/dashboard/components/*.py src/analytics/observations.py; do python3 -c "import ast; ast.parse(open('$f').read()); print('$f OK')"; done
```

- [ ] **Step 3: Squash and push**

```bash
git log --oneline HEAD~6..HEAD
git reset --soft HEAD~6
git commit -m "feat: design system — Inter font, global KPI header, semantic kpi cards, key observations engine, trust footer, 3-layer page layout [sprint-D]"
git push origin main
```
