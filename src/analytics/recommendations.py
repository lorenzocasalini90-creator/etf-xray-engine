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
        List of Recommendation objects.
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
