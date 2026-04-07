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
    """Render a KPI card with value + colored interpretation.

    Args:
        label: Card label displayed above the value.
        value: Main metric value as a string.
        interpretation: Short semantic label (e.g. "Low concentration").
        color: Hex color string for the interpretation badge.
        tooltip: Help text shown on hover.
        delta_str: Optional delta string for st.metric.
    """
    st.metric(label=label, value=value, delta=delta_str, help=tooltip)
    badge = {"#16a34a": "🟢", "#d97706": "🟡", "#dc2626": "🔴",
             "#2563eb": "🔵", "#6b7280": "⚪"}.get(color, "⚪")
    st.markdown(
        f"<span style='color:{color}; font-size:0.8rem; font-weight:500;'>"
        f"{badge} {interpretation}</span>",
        unsafe_allow_html=True,
    )


def render_hhi_card(hhi: float, col) -> None:
    """Render the HHI concentration KPI card.

    Args:
        hhi: Herfindahl-Hirschman Index value.
        col: Streamlit column context to render into.
    """
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
    """Render the Effective N diversification KPI card.

    Args:
        n: Effective N value (equivalent number of equal-weight holdings).
        col: Streamlit column context to render into.
    """
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
    """Render the Active Share vs benchmark KPI card.

    Args:
        active_share: Active share percentage, or None if no benchmark selected.
        col: Streamlit column context to render into.
    """
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
    """Render the Top-10 concentration KPI card.

    Args:
        top10: Sum of weights of the 10 largest holdings, as a percentage.
        col: Streamlit column context to render into.
    """
    tooltip = ("Somma dei pesi dei 10 titoli più grandi. "
               "Se è 35%, un terzo del portafoglio dipende da 10 aziende.")
    with col:
        if top10 < 15:
            kpi_card("Top-10 Conc.", f"{top10:.1f}%", "Balanced", GREEN, tooltip)
        elif top10 < 25:
            kpi_card("Top-10 Conc.", f"{top10:.1f}%", "Moderate top-heavy", YELLOW, tooltip)
        else:
            kpi_card("Top-10 Conc.", f"{top10:.1f}%", "Top-heavy", RED, tooltip)
