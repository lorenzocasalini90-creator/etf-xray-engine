# Sprint C — Portfolio Persistence + PDF Export Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add portfolio save/load as JSON, recent portfolio recovery, and PDF report export with reportlab.

**Architecture:** C1 creates a persistence module for JSON serialization + UI for save/load/recent. C2 creates a PDF exporter using reportlab (pure Python, no system deps). Both are independent — C1 modifies portfolio_input.py, C2 modifies xray_overview.py.

**Tech Stack:** reportlab (new dependency), Streamlit, existing analytics modules. No system-level dependencies.

---

## File Structure

| File | Action | Feature | ~Lines |
|------|--------|---------|--------|
| `src/dashboard/components/portfolio_persistence.py` | Create | C1 | ~100 |
| `src/dashboard/export/__init__.py` | Create | C2 | 0 |
| `src/dashboard/export/pdf_exporter.py` | Create | C2 | ~300 |
| `src/dashboard/pages/01_portfolio_input.py` | Modify | C1 | +50 |
| `src/dashboard/pages/02_xray_overview.py` | Modify | C2 | +30 |
| `requirements.txt` | Modify | C2 | +1 line |
| `pyproject.toml` | Modify | C2 | +1 line |
| `tests/test_portfolio_persistence.py` | Create | C1 | ~80 |
| `tests/test_pdf_export.py` | Create | C2 | ~80 |

**Risks:**
- reportlab not installed — must `pip install reportlab` and add to deps.
- Streamlit Cloud deploy: reportlab is pure Python, no system packages needed.
- PDF table column widths: must calculate to fit A4 margins. Risk of overflow with long ETF names.
- `last_analyzed_portfolio` session state: must be populated after analysis completes, not before.

---

### Task 1: Install reportlab + update deps

**Files:**
- Modify: `requirements.txt`
- Modify: `pyproject.toml`

- [ ] **Step 1: Install reportlab**

```bash
pip install reportlab
```

- [ ] **Step 2: Add to requirements.txt**

Append `reportlab>=4.0` to `requirements.txt`.

- [ ] **Step 3: Add to pyproject.toml**

In `pyproject.toml`, add `"reportlab>=4.0"` to the `dashboard` optional dependencies list (after the `plotly` line):

```toml
dashboard = [
    "streamlit>=1.30",
    "plotly>=5.18",
    "reportlab>=4.0",
]
```

- [ ] **Step 4: Verify import**

```bash
python3 -c "from reportlab.lib.pagesizes import A4; print('reportlab OK')"
```

- [ ] **Step 5: Commit**

```bash
git add requirements.txt pyproject.toml
git commit -m "deps: add reportlab for PDF export"
```

---

### Task 2: C1 — Portfolio persistence (serialize/deserialize + tests)

**Files:**
- Create: `src/dashboard/components/portfolio_persistence.py`
- Create: `tests/test_portfolio_persistence.py`

- [ ] **Step 1: Create persistence module**

Create `src/dashboard/components/portfolio_persistence.py`:

```python
"""Portfolio JSON serialization and deserialization."""

from __future__ import annotations

import json
from datetime import datetime


_CURRENT_VERSION = "1.0"


def serialize_portfolio(
    positions: list[dict],
    benchmark: str | None = None,
) -> str:
    """Serialize portfolio positions to JSON string.

    Args:
        positions: List of {"ticker": str, "capital": float}.
        benchmark: Benchmark name (e.g. "MSCI_WORLD").

    Returns:
        JSON string.
    """
    data = {
        "version": _CURRENT_VERSION,
        "positions": [
            {"ticker": p["ticker"], "amount_eur": p["capital"]}
            for p in positions
        ],
        "benchmark": benchmark,
        "saved_at": datetime.now().isoformat(),
    }
    return json.dumps(data, indent=2, ensure_ascii=False)


def deserialize_portfolio(
    json_str: str,
) -> tuple[list[dict], str | None, list[str]]:
    """Deserialize portfolio JSON string.

    Args:
        json_str: JSON string from serialize_portfolio.

    Returns:
        Tuple of (positions, benchmark, warnings).

    Raises:
        ValueError: If JSON is invalid or missing required fields.
    """
    warnings: list[str] = []

    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as exc:
        raise ValueError(f"JSON non valido: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError("Formato JSON non valido: deve essere un oggetto")

    if "positions" not in data:
        raise ValueError("Campo 'positions' mancante nel JSON")

    raw_positions = data["positions"]
    if not isinstance(raw_positions, list):
        raise ValueError("Il campo 'positions' deve essere una lista")

    positions: list[dict] = []
    for i, p in enumerate(raw_positions):
        if not isinstance(p, dict):
            warnings.append(f"Posizione {i + 1}: formato non valido, ignorata")
            continue

        ticker = p.get("ticker") or p.get("input_identifier", "")
        amount = p.get("amount_eur") or p.get("capital", 0)

        if not ticker:
            warnings.append(f"Posizione {i + 1}: ticker mancante, ignorata")
            continue

        try:
            amount = float(amount)
        except (TypeError, ValueError):
            warnings.append(f"Posizione {i + 1}: importo non valido, ignorata")
            continue

        positions.append({"ticker": str(ticker).strip().upper(), "capital": amount})

    if not positions:
        raise ValueError("Nessuna posizione valida trovata nel JSON")

    benchmark = data.get("benchmark")

    version = data.get("version", "unknown")
    if version != _CURRENT_VERSION:
        warnings.append(f"Versione file: {version} (attuale: {_CURRENT_VERSION})")

    return positions, benchmark, warnings


def generate_portfolio_filename(positions: list[dict]) -> str:
    """Generate a filename for the portfolio JSON.

    Uses display_ticker if available, falls back to ticker.
    Shows first 3 tickers, then "e N altri" if more.
    """
    tickers = [
        p.get("display_ticker") or p.get("ticker", "ETF")
        for p in positions
    ]

    if len(tickers) <= 3:
        name_part = "_".join(tickers)
    else:
        name_part = "_".join(tickers[:3]) + f"_e_{len(tickers) - 3}_altri"

    date_str = datetime.now().strftime("%Y%m%d")
    return f"portafoglio_{name_part}_{date_str}.json"
```

- [ ] **Step 2: Create tests**

Create `tests/test_portfolio_persistence.py`:

```python
"""Tests for portfolio JSON persistence."""

import json

import pytest

from src.dashboard.components.portfolio_persistence import (
    deserialize_portfolio,
    generate_portfolio_filename,
    serialize_portfolio,
)


class TestSerializeDeserialize:
    def test_roundtrip(self):
        positions = [{"ticker": "CSPX", "capital": 30000}]
        json_str = serialize_portfolio(positions, benchmark="MSCI_WORLD")
        loaded, benchmark, warnings = deserialize_portfolio(json_str)
        assert benchmark == "MSCI_WORLD"
        assert len(loaded) == 1
        assert loaded[0]["ticker"] == "CSPX"
        assert loaded[0]["capital"] == 30000.0
        assert len(warnings) == 0

    def test_multiple_positions(self):
        positions = [
            {"ticker": "CSPX", "capital": 30000},
            {"ticker": "SWDA", "capital": 40000},
        ]
        json_str = serialize_portfolio(positions)
        loaded, _, _ = deserialize_portfolio(json_str)
        assert len(loaded) == 2

    def test_no_benchmark(self):
        positions = [{"ticker": "CSPX", "capital": 10000}]
        json_str = serialize_portfolio(positions, benchmark=None)
        _, benchmark, _ = deserialize_portfolio(json_str)
        assert benchmark is None


class TestDeserializeEdgeCases:
    def test_missing_optional_fields(self):
        json_str = '{"version":"1.0","positions":[{"ticker":"CSPX","amount_eur":30000}]}'
        positions, _, warnings = deserialize_portfolio(json_str)
        assert len(positions) == 1
        assert positions[0]["ticker"] == "CSPX"

    def test_invalid_json(self):
        with pytest.raises(ValueError, match="JSON"):
            deserialize_portfolio("questo non è json {{{")

    def test_missing_positions(self):
        with pytest.raises(ValueError):
            deserialize_portfolio('{"version":"1.0"}')

    def test_old_format_input_identifier(self):
        json_str = '{"version":"0.9","positions":[{"input_identifier":"CSPX","amount_eur":30000}]}'
        positions, _, warnings = deserialize_portfolio(json_str)
        assert len(positions) == 1
        assert positions[0]["ticker"] == "CSPX"
        assert any("Versione" in w for w in warnings)

    def test_invalid_amount_skipped(self):
        json_str = '{"version":"1.0","positions":[{"ticker":"CSPX","amount_eur":"abc"},{"ticker":"SWDA","amount_eur":40000}]}'
        positions, _, warnings = deserialize_portfolio(json_str)
        assert len(positions) == 1
        assert positions[0]["ticker"] == "SWDA"
        assert any("non valido" in w for w in warnings)


class TestGenerateFilename:
    def test_two_etf(self):
        positions = [{"display_ticker": "CSPX"}, {"display_ticker": "SWDA"}]
        name = generate_portfolio_filename(positions)
        assert "CSPX" in name and "SWDA" in name
        assert name.endswith(".json")

    def test_many_etf(self):
        positions = [{"display_ticker": f"ETF{i}"} for i in range(5)]
        name = generate_portfolio_filename(positions)
        assert "altri" in name

    def test_fallback_to_ticker(self):
        positions = [{"ticker": "CSPX"}]
        name = generate_portfolio_filename(positions)
        assert "CSPX" in name
```

- [ ] **Step 3: Run tests**

```bash
python3 -m pytest tests/test_portfolio_persistence.py -v
```

- [ ] **Step 4: Commit**

```bash
git add src/dashboard/components/portfolio_persistence.py tests/test_portfolio_persistence.py
git commit -m "feat: portfolio JSON serialize/deserialize with tests"
```

---

### Task 3: C1-UI — Save/Load buttons + recent portfolio in portfolio_input.py

**Files:**
- Modify: `src/dashboard/pages/01_portfolio_input.py`

- [ ] **Step 1: Add `last_analyzed_portfolio` to session_state defaults**

Add to `_DEFAULTS`:
```python
    "last_analyzed_portfolio": None,
```

- [ ] **Step 2: Add save/load buttons after the portfolio list**

After the `st.caption(f"Totale investito: ...")` line (line 217) and BEFORE the benchmark selector `st.divider()` (line 220), insert:

```python
# ── Save / Load portfolio ──────────────────────────────────────────
from src.dashboard.components.portfolio_persistence import (
    serialize_portfolio,
    deserialize_portfolio,
    generate_portfolio_filename,
)

col_save, col_load = st.columns(2)

with col_save:
    json_str = serialize_portfolio(positions, benchmark=st.session_state.benchmark_name)
    filename = generate_portfolio_filename(positions)
    st.download_button(
        label="💾 Salva portafoglio",
        data=json_str,
        file_name=filename,
        mime="application/json",
    )

with col_load:
    json_file = st.file_uploader(
        "📂 Carica portafoglio salvato",
        type=["json"],
        key="portfolio_json_loader",
        label_visibility="collapsed",
    )
    if json_file is not None:
        try:
            content = json_file.read().decode("utf-8")
            loaded_positions, loaded_bench, load_warnings = deserialize_portfolio(content)
            for w in load_warnings:
                st.warning(w)
            st.session_state.portfolio_positions = loaded_positions
            if loaded_bench:
                st.session_state.benchmark_name = loaded_bench
            for key in ("aggregated", "overlap_matrix", "redundancy_df",
                         "factor_result", "active_share_result", "benchmark_df",
                         "analysis_hash", "analysis_timestamp"):
                st.session_state[key] = None
            st.session_state.holdings_db = {}
            st.session_state.display_names = {}
            st.rerun()
        except ValueError as exc:
            st.error(str(exc))
```

- [ ] **Step 3: Populate last_analyzed_portfolio after analysis completes**

Find the line at the end of the analysis block:
```python
    st.session_state.analysis_timestamp = time.time()
```

Insert AFTER it:
```python
    # Save last analyzed portfolio for recovery
    st.session_state.last_analyzed_portfolio = {
        "positions": [dict(p) for p in positions],
        "n_etf": len(positions),
        "total_eur": total_capital,
        "analyzed_at": time.strftime("%H:%M"),
        "benchmark": st.session_state.benchmark_name,
    }
```

- [ ] **Step 4: Add recent portfolio recovery at the bottom**

Find the line:
```python
if not positions:
    st.info("Aggiungi almeno un ETF per iniziare.")
    st.stop()
```

Replace with:
```python
if not positions:
    # Show recent portfolio recovery if available
    last = st.session_state.get("last_analyzed_portfolio")
    if last:
        st.info(
            f"📂 **Ultima analisi:** {last['n_etf']} ETF · "
            f"€{last['total_eur']:,.0f} · "
            f"ore {last['analyzed_at']}"
        )
        if st.button("↩ Ricarica portafoglio precedente"):
            st.session_state.portfolio_positions = last["positions"]
            if last.get("benchmark"):
                st.session_state.benchmark_name = last["benchmark"]
            st.rerun()
    else:
        st.info("Aggiungi almeno un ETF per iniziare.")
    st.stop()
```

- [ ] **Step 5: Verify syntax**

```bash
python3 -c "import ast; ast.parse(open('src/dashboard/pages/01_portfolio_input.py').read()); print('OK')"
```

- [ ] **Step 6: Commit**

```bash
git add src/dashboard/pages/01_portfolio_input.py
git commit -m "feat: save/load portfolio JSON + recent portfolio recovery"
```

---

### Task 4: C2 — PDF exporter (reportlab + tests)

**Files:**
- Create: `src/dashboard/export/__init__.py`
- Create: `src/dashboard/export/pdf_exporter.py`
- Create: `tests/test_pdf_export.py`

- [ ] **Step 1: Create export package**

Create `src/dashboard/export/__init__.py` (empty file).

- [ ] **Step 2: Create PDF exporter**

Create `src/dashboard/export/pdf_exporter.py`:

```python
"""PDF report generation for ETF X-Ray Engine."""

from __future__ import annotations

import io
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    HRFlowable,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


_GREY_HEADER = colors.Color(0.953, 0.957, 0.965)  # #F3F4F6
_GREY_BORDER = colors.Color(0.820, 0.835, 0.855)  # #D1D5DB
_GREEN_TEXT = colors.Color(0.086, 0.396, 0.204)    # #166534
_RED_TEXT = colors.Color(0.600, 0.106, 0.106)      # #991B1B

_PAGE_W, _PAGE_H = A4
_MARGIN = 50


def _footer(canvas, doc):
    """Draw footer on every page."""
    canvas.saveState()
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(colors.grey)
    canvas.drawString(
        _MARGIN, 25,
        "Generato da ETF X-Ray Engine — Analisi indicativa, "
        "non costituisce consulenza finanziaria",
    )
    canvas.drawRightString(
        _PAGE_W - _MARGIN, 25, f"Pagina {doc.page}",
    )
    canvas.restoreState()


def _make_table(headers: list[str], rows: list[list[str]], col_widths: list[float] | None = None) -> Table:
    """Build a styled table."""
    data = [headers] + rows
    t = Table(data, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), _GREY_HEADER),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("FONTSIZE", (0, 1), (-1, -1), 8),
        ("TEXTCOLOR", (0, 0), (-1, -1), colors.black),
        ("GRID", (0, 0), (-1, -1), 0.5, _GREY_BORDER),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    return t


def generate_report_pdf(
    portfolio: list[dict],
    benchmark_name: str | None,
    xray_data: dict,
    redundancy_data: list[dict],
    overlap_data: list[list] | None,
    overlap_labels: list[str] | None,
    recommendations: list,
    factor_data: dict | None = None,
) -> bytes:
    """Generate PDF report in memory.

    Args:
        portfolio: List of {"ticker": str, "capital": float}.
        benchmark_name: Benchmark display name or None.
        xray_data: Dict with keys: n_holdings, hhi, effective_n,
            active_share_pct, top_10_pct, top_holdings (list of dicts).
        redundancy_data: List of dicts with etf_ticker, redundancy_pct,
            ter_wasted, verdict.
        overlap_data: NxN matrix as list of lists (or None).
        overlap_labels: List of ETF tickers for overlap matrix.
        recommendations: List of Recommendation objects.
        factor_data: Dict with factor_scores, coverage_report (or None).

    Returns:
        PDF bytes.
    """
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        rightMargin=_MARGIN, leftMargin=_MARGIN,
        topMargin=60, bottomMargin=60,
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "ReportTitle", parent=styles["Title"],
        fontName="Helvetica-Bold", fontSize=22, spaceAfter=6,
    )
    subtitle_style = ParagraphStyle(
        "ReportSubtitle", parent=styles["Normal"],
        fontName="Helvetica", fontSize=11, textColor=colors.grey,
        spaceAfter=12,
    )
    heading_style = ParagraphStyle(
        "SectionHeading", parent=styles["Heading2"],
        fontName="Helvetica-Bold", fontSize=14, spaceBefore=12, spaceAfter=8,
    )
    body_style = ParagraphStyle(
        "BodyText", parent=styles["Normal"],
        fontName="Helvetica", fontSize=9, leading=13,
    )
    small_style = ParagraphStyle(
        "SmallText", parent=styles["Normal"],
        fontName="Helvetica", fontSize=8, leading=11,
        textColor=colors.grey,
    )

    story = []
    usable_w = _PAGE_W - 2 * _MARGIN

    # ── PAGE 1: Cover ──────────────────────────────────────────────
    story.append(Paragraph("Portfolio X-Ray Report", title_style))
    story.append(Paragraph(
        "Analisi della composizione e del rischio del portafoglio",
        subtitle_style,
    ))
    story.append(HRFlowable(width="100%", thickness=1, color=_GREY_BORDER))
    story.append(Spacer(1, 12))

    # Portfolio table
    total_eur = sum(p["capital"] for p in portfolio)
    port_rows = []
    for p in portfolio:
        weight = (p["capital"] / total_eur * 100) if total_eur > 0 else 0
        port_rows.append([
            p["ticker"],
            f"\u20ac {p['capital']:,.0f}",
            f"{weight:.1f}%",
        ])
    port_rows.append(["Totale", f"\u20ac {total_eur:,.0f}", "100.0%"])

    col_w = [usable_w * 0.4, usable_w * 0.3, usable_w * 0.3]
    t = _make_table(["ETF", "Importo EUR", "Peso %"], port_rows, col_w)
    # Bold last row
    t.setStyle(TableStyle([
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
    ]))
    story.append(t)
    story.append(Spacer(1, 12))

    if benchmark_name:
        story.append(Paragraph(
            f"Benchmark di riferimento: <b>{benchmark_name}</b>", body_style,
        ))
    story.append(Paragraph(
        f"Analisi del: {datetime.now().strftime('%d %B %Y')}", body_style,
    ))

    # ── PAGE 2: X-Ray Overview ─────────────────────────────────────
    story.append(PageBreak())
    story.append(Paragraph("X-Ray Overview", heading_style))
    story.append(Spacer(1, 6))

    # KPI grid
    kpi_data = [
        ["Titoli unici", str(xray_data.get("n_holdings", "N/A"))],
        ["HHI", f"{xray_data.get('hhi', 0):.4f}"],
        ["Effective N", f"{xray_data.get('effective_n', 0):.0f}"],
        ["Active Share", f"{xray_data.get('active_share_pct', 'N/A')}%"
         if xray_data.get("active_share_pct") is not None else "N/A"],
        ["Top-10 Conc.", f"{xray_data.get('top_10_pct', 0):.2f}%"],
    ]
    kpi_table = _make_table(["Metrica", "Valore"], kpi_data,
                            [usable_w * 0.5, usable_w * 0.5])
    story.append(kpi_table)
    story.append(Spacer(1, 12))

    # Top holdings
    top_holdings = xray_data.get("top_holdings", [])
    if top_holdings:
        story.append(Paragraph("Top 15 Holdings", heading_style))
        max_show = min(15, len(top_holdings))
        h_rows = []
        for i, h in enumerate(top_holdings[:max_show]):
            h_rows.append([
                str(i + 1),
                str(h.get("name", "")),
                str(h.get("ticker", "")),
                f"{h.get('weight', 0):.2f}%",
                str(h.get("sector", "")),
                str(h.get("country", "")),
            ])
        h_widths = [usable_w * f for f in [0.06, 0.30, 0.12, 0.12, 0.20, 0.20]]
        story.append(_make_table(
            ["#", "Titolo", "Ticker", "Peso%", "Settore", "Paese"],
            h_rows, h_widths,
        ))
        if len(top_holdings) > max_show:
            story.append(Paragraph(
                f"(Mostrate le prime {max_show} su {len(top_holdings)} totali)",
                small_style,
            ))

    # ── PAGE 3: Redundancy & Overlap ───────────────────────────────
    story.append(PageBreak())
    story.append(Paragraph("Redundancy &amp; Overlap", heading_style))
    story.append(Spacer(1, 6))

    if redundancy_data:
        story.append(Paragraph("Redundancy Score per ETF", heading_style))
        r_rows = []
        total_wasted = 0.0
        for r in redundancy_data:
            verdict_map = {"green": "Bassa", "yellow": "Moderata", "red": "Alta"}
            wasted = r.get("ter_wasted", 0) or 0
            total_wasted += wasted
            r_rows.append([
                str(r.get("etf_ticker", "")),
                f"{r.get('redundancy_pct', 0):.1f}%",
                f"\u20ac {wasted:.2f}",
                verdict_map.get(r.get("verdict", ""), ""),
            ])
        r_rows.append(["Totale TER sprecato", "", f"\u20ac {total_wasted:.2f}", ""])
        r_widths = [usable_w * f for f in [0.25, 0.25, 0.25, 0.25]]
        rt = _make_table(
            ["ETF", "Redundancy%", "TER sprecato \u20ac/anno", "Livello"],
            r_rows, r_widths,
        )
        rt.setStyle(TableStyle([
            ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ]))
        story.append(rt)
        story.append(Spacer(1, 12))

    # Overlap matrix (textual)
    if overlap_data and overlap_labels and len(overlap_labels) >= 2:
        story.append(Paragraph("Overlap tra ETF", heading_style))
        o_rows = []
        n = len(overlap_labels)
        for i in range(n):
            for j in range(i + 1, n):
                val = overlap_data[i][j]
                interp = "Alto" if val > 50 else ("Moderato" if val > 25 else "Basso")
                o_rows.append([
                    overlap_labels[i], overlap_labels[j],
                    f"{val:.1f}%", interp,
                ])
        if o_rows:
            o_widths = [usable_w * f for f in [0.25, 0.25, 0.25, 0.25]]
            story.append(_make_table(
                ["ETF A", "ETF B", "Overlap%", "Livello"],
                o_rows, o_widths,
            ))

    # ── PAGE 4: Recommendations & Disclaimer ───────────────────────
    story.append(PageBreak())
    story.append(Paragraph("Raccomandazioni", heading_style))
    story.append(Spacer(1, 6))

    if recommendations:
        severity_order = {"high": 0, "medium": 1, "low": 2}
        for rec in sorted(recommendations, key=lambda r: severity_order.get(r.severity, 3)):
            badge = {"high": "\u26d4 Alta priorit\u00e0",
                     "medium": "\u26a0 Da valutare",
                     "low": "\u2139 Nota"}.get(rec.severity, "")
            story.append(Paragraph(f"<b>{badge} — {rec.title}</b>", body_style))
            story.append(Paragraph(rec.explanation, body_style))
            story.append(Paragraph(f"<i>\u2192 {rec.action}</i>", body_style))
            if rec.saving_eur_annual and rec.saving_eur_annual > 0:
                story.append(Paragraph(
                    f"Risparmio potenziale: ~\u20ac{rec.saving_eur_annual:.0f}/anno",
                    body_style,
                ))
            story.append(Spacer(1, 8))
    else:
        story.append(Paragraph(
            "Nessuna raccomandazione significativa per questo portafoglio.",
            body_style,
        ))

    story.append(Spacer(1, 20))
    story.append(HRFlowable(width="100%", thickness=0.5, color=_GREY_BORDER))
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        "I risultati presentati sono generati automaticamente da ETF X-Ray Engine "
        "sulla base dei dati di holdings degli ETF. L'analisi è indicativa e non "
        "costituisce consulenza finanziaria ai sensi della normativa MiFID II. "
        "Consulta un consulente finanziario abilitato per decisioni di investimento.",
        small_style,
    ))

    # ── PAGE 5: Factor Fingerprint (conditional) ───────────────────
    if factor_data is not None:
        story.append(PageBreak())
        story.append(Paragraph("Factor Fingerprint", heading_style))
        story.append(Spacer(1, 6))

        scores = factor_data.get("factor_scores", {})
        coverage = factor_data.get("coverage_report", {})

        size = scores.get("size", {})
        vg = scores.get("value_growth", {})
        quality = scores.get("quality", {})
        dy = scores.get("dividend_yield", {})

        roe_raw = quality.get("weighted_roe", 0) or 0
        roe_val = roe_raw * 100 if roe_raw < 1 else roe_raw
        dy_raw = dy.get("weighted_yield", 0) or 0
        dy_val = dy_raw * 100 if dy_raw < 1 else dy_raw

        f_rows = [
            ["Size (% Large Cap)", f"{size.get('Large', 0):.1f}%"],
            ["Value (P/E medio)", f"{vg.get('weighted_pe', 'N/A')}"],
            ["Value (P/B medio)", f"{vg.get('weighted_pb', 'N/A')}"],
            ["Quality (ROE %)", f"{roe_val:.1f}%"],
            ["Dividend Yield %", f"{dy_val:.2f}%"],
            ["Momentum *", "N/D"],
        ]
        f_widths = [usable_w * 0.5, usable_w * 0.5]
        story.append(_make_table(["Dimensione", "Portafoglio"], f_rows, f_widths))
        story.append(Spacer(1, 8))

        story.append(Paragraph(
            f"Coverage: L1 Sector {coverage.get('L1_pct', 0):.1f}% · "
            f"L2 Fundamentals {coverage.get('L2_pct', 0):.1f}% · "
            f"L3 Proxy {coverage.get('L3_pct', 0):.1f}% · "
            f"L4 Unclassified {coverage.get('L4_pct', 0):.1f}%",
            small_style,
        ))
        story.append(Paragraph("* Momentum non disponibile in questa versione", small_style))

    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    return buf.getvalue()
```

- [ ] **Step 3: Create tests**

Create `tests/test_pdf_export.py`:

```python
"""Tests for PDF report generation."""

import pytest

from src.dashboard.export.pdf_exporter import generate_report_pdf


@pytest.fixture
def sample_portfolio():
    return [
        {"ticker": "CSPX", "capital": 30000.0},
        {"ticker": "SWDA", "capital": 40000.0},
    ]


@pytest.fixture
def sample_xray_data():
    return {
        "n_holdings": 1329,
        "hhi": 0.0123,
        "effective_n": 81.3,
        "active_share_pct": 13.2,
        "top_10_pct": 28.5,
        "top_holdings": [
            {"name": "NVIDIA CORP", "ticker": "NVDA", "weight": 6.31,
             "sector": "Information Technology", "country": "United States"},
            {"name": "APPLE INC", "ticker": "AAPL", "weight": 5.56,
             "sector": "Information Technology", "country": "United States"},
            {"name": "MICROSOFT CORP", "ticker": "MSFT", "weight": 3.95,
             "sector": "Information Technology", "country": "United States"},
        ],
    }


@pytest.fixture
def sample_redundancy():
    return [
        {"etf_ticker": "CSPX", "redundancy_pct": 98.5, "ter_wasted": 20.6, "verdict": "red"},
        {"etf_ticker": "SWDA", "redundancy_pct": 25.3, "ter_wasted": 0.0, "verdict": "green"},
    ]


@pytest.fixture
def sample_overlap():
    return [[100.0, 53.2], [53.2, 100.0]]


@pytest.fixture
def sample_factor():
    return {
        "factor_scores": {
            "size": {"Large": 85.2, "Mid": 10.1, "Small": 3.5, "Unknown": 1.2},
            "value_growth": {"weighted_pe": 24.5, "weighted_pb": 4.2, "style": "Blend"},
            "quality": {"weighted_roe": 0.22, "weighted_debt_equity": 1.5},
            "dividend_yield": {"weighted_yield": 0.015},
        },
        "coverage_report": {
            "L1_pct": 95.0, "L2_pct": 80.0, "L3_pct": 10.0, "L4_pct": 5.0,
        },
    }


class TestPDFGeneration:
    def test_generates_valid_pdf(self, sample_portfolio, sample_xray_data, sample_redundancy, sample_overlap):
        pdf_bytes = generate_report_pdf(
            portfolio=sample_portfolio,
            benchmark_name="MSCI World (SWDA)",
            xray_data=sample_xray_data,
            redundancy_data=sample_redundancy,
            overlap_data=sample_overlap,
            overlap_labels=["CSPX", "SWDA"],
            recommendations=[],
        )
        assert isinstance(pdf_bytes, bytes)
        assert len(pdf_bytes) > 1000
        assert pdf_bytes[:4] == b"%PDF"

    def test_with_factor_data(self, sample_portfolio, sample_xray_data, sample_redundancy, sample_overlap, sample_factor):
        pdf_bytes = generate_report_pdf(
            portfolio=sample_portfolio,
            benchmark_name="MSCI World",
            xray_data=sample_xray_data,
            redundancy_data=sample_redundancy,
            overlap_data=sample_overlap,
            overlap_labels=["CSPX", "SWDA"],
            recommendations=[],
            factor_data=sample_factor,
        )
        assert pdf_bytes[:4] == b"%PDF"
        assert len(pdf_bytes) > 1000

    def test_without_recommendations(self, sample_portfolio, sample_xray_data, sample_redundancy, sample_overlap):
        pdf_bytes = generate_report_pdf(
            portfolio=sample_portfolio,
            benchmark_name=None,
            xray_data=sample_xray_data,
            redundancy_data=sample_redundancy,
            overlap_data=None,
            overlap_labels=None,
            recommendations=[],
        )
        assert pdf_bytes[:4] == b"%PDF"

    def test_with_recommendations(self, sample_portfolio, sample_xray_data, sample_redundancy, sample_overlap):
        from src.analytics.recommendations import Recommendation
        recs = [
            Recommendation(
                severity="high",
                title="CSPX duplica il portafoglio",
                explanation="Il 98% delle holdings è ridondante.",
                action="Considera di vendere CSPX.",
                saving_eur_annual=20.6,
                rule_id="R1",
            ),
        ]
        pdf_bytes = generate_report_pdf(
            portfolio=sample_portfolio,
            benchmark_name="MSCI World",
            xray_data=sample_xray_data,
            redundancy_data=sample_redundancy,
            overlap_data=sample_overlap,
            overlap_labels=["CSPX", "SWDA"],
            recommendations=recs,
        )
        assert pdf_bytes[:4] == b"%PDF"
        assert len(pdf_bytes) > 1000

    def test_many_holdings_no_crash(self, sample_redundancy, sample_overlap):
        big_portfolio = [{"ticker": f"ETF{i}", "capital": 1000.0} for i in range(20)]
        big_holdings = [
            {"name": f"HOLDING_{i}", "ticker": f"T{i}", "weight": 0.5,
             "sector": "Tech", "country": "US"}
            for i in range(500)
        ]
        pdf_bytes = generate_report_pdf(
            portfolio=big_portfolio,
            benchmark_name="MSCI World",
            xray_data={"n_holdings": 500, "hhi": 0.01, "effective_n": 100,
                       "active_share_pct": 30.0, "top_10_pct": 10.0,
                       "top_holdings": big_holdings},
            redundancy_data=sample_redundancy,
            overlap_data=sample_overlap,
            overlap_labels=["CSPX", "SWDA"],
            recommendations=[],
        )
        assert pdf_bytes[:4] == b"%PDF"
```

- [ ] **Step 4: Run tests**

```bash
python3 -m pytest tests/test_pdf_export.py -v
```

- [ ] **Step 5: Commit**

```bash
git add src/dashboard/export/ tests/test_pdf_export.py
git commit -m "feat: PDF report generation with reportlab"
```

---

### Task 5: C2-UI — PDF export button in X-Ray Overview

**Files:**
- Modify: `src/dashboard/pages/02_xray_overview.py`

- [ ] **Step 1: Add PDF export button after KPI row**

In `02_xray_overview.py`, find the line:
```python
k5.metric("Top-10 Conc.", f"{hhi_stats['top_10_pct']:.2f} %")
```

Insert AFTER it (before the KPI explanations):

```python
# ── Export PDF ─────────────────────────────────────────────────────
from datetime import datetime

redundancy_df_export = st.session_state.get("redundancy_df")
if redundancy_df_export is not None:
    if st.button("📄 Esporta Report PDF"):
        from src.dashboard.export.pdf_exporter import generate_report_pdf
        from src.analytics.recommendations import generate_recommendations

        positions = st.session_state.get("portfolio_positions", [])
        total_eur = sum(p["capital"] for p in positions)

        # Build xray_data
        top_h = aggregated.nlargest(15, "real_weight_pct")
        xray_data = {
            "n_holdings": len(aggregated),
            "hhi": hhi_stats["hhi"],
            "effective_n": hhi_stats["effective_n"],
            "active_share_pct": active_share_pct,
            "top_10_pct": hhi_stats["top_10_pct"],
            "top_holdings": [
                {"name": r["name"], "ticker": r["ticker"],
                 "weight": r["real_weight_pct"],
                 "sector": r.get("sector", ""), "country": r.get("country", "")}
                for _, r in top_h.iterrows()
            ],
        }

        # Redundancy
        red_list = redundancy_df_export.to_dict("records")

        # Overlap
        overlap_mat = st.session_state.get("overlap_matrix")
        ol_data = overlap_mat.values.tolist() if overlap_mat is not None else None
        ol_labels = overlap_mat.columns.tolist() if overlap_mat is not None else None

        # Recommendations
        red_scores = dict(zip(
            redundancy_df_export["etf_ticker"],
            redundancy_df_export["redundancy_pct"] / 100,
        ))
        ter_wasted = dict(zip(
            redundancy_df_export["etf_ticker"],
            redundancy_df_export["ter_wasted"],
        ))
        top1 = aggregated.nlargest(1, "real_weight_pct").iloc[0] if not aggregated.empty else None
        bench_name = st.session_state.get("benchmark_name") or "mercato"
        bench_labels_map = {"MSCI_WORLD": "MSCI World", "SP500": "S&P 500",
                            "MSCI_EM": "MSCI EM", "FTSE_ALL_WORLD": "FTSE All-World"}
        bench_display = bench_labels_map.get(bench_name, bench_name)

        recs = generate_recommendations(
            redundancy_scores=red_scores,
            ter_wasted_eur=ter_wasted,
            active_share=active_share_pct,
            hhi=hhi_stats["hhi"],
            top1_weight=(top1["real_weight_pct"] / 100) if top1 is not None else 0,
            top1_name=top1["name"] if top1 is not None else "",
            n_etf=len(positions),
            portfolio_total_eur=total_eur,
            benchmark_name=bench_display,
        )

        with st.spinner("Generazione report PDF..."):
            pdf_bytes = generate_report_pdf(
                portfolio=positions,
                benchmark_name=bench_display if bench_name else None,
                xray_data=xray_data,
                redundancy_data=red_list,
                overlap_data=ol_data,
                overlap_labels=ol_labels,
                recommendations=recs,
                factor_data=st.session_state.get("factor_result"),
            )

        st.download_button(
            label="⬇️ Scarica Report PDF",
            data=pdf_bytes,
            file_name=f"xray_report_{datetime.now().strftime('%Y%m%d')}.pdf",
            mime="application/pdf",
        )
```

- [ ] **Step 2: Verify syntax**

```bash
python3 -c "import ast; ast.parse(open('src/dashboard/pages/02_xray_overview.py').read()); print('OK')"
```

- [ ] **Step 3: Commit**

```bash
git add src/dashboard/pages/02_xray_overview.py
git commit -m "feat: PDF export button in X-Ray Overview"
```

---

### Task 6: Final verification + squash + push

- [ ] **Step 1: Run ALL tests**

```bash
python3 -m pytest tests/ -x -q
```

Expected: 303 + ~17 new = ~320 tests pass.

- [ ] **Step 2: Squash and push**

```bash
git log --oneline HEAD~5..HEAD
git reset --soft HEAD~5
git commit -m "feat: portfolio save/load JSON, PDF report export with reportlab [sprint-C]"
git push origin main
```
