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


_GREY_HEADER = colors.Color(0.953, 0.957, 0.965)
_GREY_BORDER = colors.Color(0.820, 0.835, 0.855)
_GREEN_TEXT = colors.Color(0.086, 0.396, 0.204)
_RED_TEXT = colors.Color(0.600, 0.106, 0.106)

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
        "BodyText2", parent=styles["Normal"],
        fontName="Helvetica", fontSize=9, leading=13,
    )
    small_style = ParagraphStyle(
        "SmallText2", parent=styles["Normal"],
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

    as_pct = xray_data.get("active_share_pct")
    kpi_data = [
        ["Titoli unici", str(xray_data.get("n_holdings", "N/A"))],
        ["HHI", f"{xray_data.get('hhi', 0):.4f}"],
        ["Effective N", f"{xray_data.get('effective_n', 0):.0f}"],
        ["Active Share", f"{as_pct:.1f}%" if as_pct is not None else "N/A"],
        ["Top-10 Conc.", f"{xray_data.get('top_10_pct', 0):.2f}%"],
    ]
    kpi_table = _make_table(["Metrica", "Valore"], kpi_data,
                            [usable_w * 0.5, usable_w * 0.5])
    story.append(kpi_table)
    story.append(Spacer(1, 12))

    top_holdings = xray_data.get("top_holdings", [])
    if top_holdings:
        story.append(Paragraph("Top 15 Holdings", heading_style))
        max_show = min(15, len(top_holdings))
        h_rows = []
        for i, h in enumerate(top_holdings[:max_show]):
            h_rows.append([
                str(i + 1),
                str(h.get("name", ""))[:40],
                str(h.get("ticker", "")),
                f"{h.get('weight', 0):.2f}%",
                str(h.get("sector", ""))[:20],
                str(h.get("country", ""))[:15],
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
        "sulla base dei dati di holdings degli ETF. L'analisi \u00e8 indicativa e non "
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
            f"Coverage: L1 Sector {coverage.get('L1_pct', 0):.1f}% \u00b7 "
            f"L2 Fundamentals {coverage.get('L2_pct', 0):.1f}% \u00b7 "
            f"L3 Proxy {coverage.get('L3_pct', 0):.1f}% \u00b7 "
            f"L4 Unclassified {coverage.get('L4_pct', 0):.1f}%",
            small_style,
        ))
        story.append(Paragraph("* Momentum non disponibile in questa versione", small_style))

    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    return buf.getvalue()
