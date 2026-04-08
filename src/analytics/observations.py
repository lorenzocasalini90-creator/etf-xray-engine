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
                "high", "overlap_redundancy",
            ))
        total_wasted = sum(ter_wasted_eur.values())
        if total_wasted > 100:
            obs.append(Observation(
                f"Stai pagando circa €{total_wasted:.0f}/anno in commissioni "
                "su holdings duplicate.",
                "high", "overlap_redundancy",
            ))
        moderate_count = sum(1 for v in redundancy_scores.values() if v > 0.50)
        if moderate_count > 1:
            obs.append(Observation(
                "Più ETF hanno ridondanza superiore al 50% — considera "
                "una consolidazione.",
                "medium", "overlap_redundancy",
            ))

    # Overlap observations
    for etf_a, etf_b, overlap in overlap_pairs:
        if overlap > 60:
            obs.append(Observation(
                f"{etf_a} e {etf_b} si sovrappongono per il {overlap:.0f}% "
                "— alta ridondanza pairwise.",
                "high", "overlap_redundancy",
            ))
        elif overlap > 40:
            obs.append(Observation(
                f"{etf_a} e {etf_b} condividono il {overlap:.0f}% "
                "dell'esposizione.",
                "medium", "overlap_redundancy",
            ))

    return obs
