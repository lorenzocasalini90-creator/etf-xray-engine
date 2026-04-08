"""Shared Plotly chart styling helpers for the dashboard."""

from __future__ import annotations

import plotly.graph_objects as go


def apply_bar_chart_style(
    fig: go.Figure,
    values: list[float],
    unit: str = "%",
    threshold_inside: float = 3.0,
) -> go.Figure:
    """Apply standard style to a horizontal bar chart.

    Adds dotted vertical gridlines, auto-positioned value labels
    (inside if bar is wide enough, outside otherwise), and consistent
    font/background styling matching the design system.

    Args:
        fig: Plotly figure to modify (horizontal bar chart).
        values: List of numeric values for text labels.
        unit: Unit suffix for labels (default ``%``).
        threshold_inside: Minimum value for inside text positioning.
            Values below this threshold show text outside the bar.

    Returns:
        The modified figure.
    """
    # Dotted vertical gridlines
    fig.update_xaxes(
        showgrid=True,
        gridwidth=0.5,
        gridcolor="#E0E0E0",
        griddash="dot",
        zeroline=False,
    )
    fig.update_yaxes(showgrid=False)

    # Auto-positioned text labels
    text_labels = [
        f"{v:.1f}{unit}" if v is not None else ""
        for v in values
    ]
    fig.update_traces(
        text=text_labels,
        textposition="auto",
        textfont=dict(family="Inter, sans-serif", size=11, color="white"),
        insidetextfont=dict(color="white"),
        outsidetextfont=dict(color="#374151"),
        cliponaxis=False,
    )

    # Consistent layout
    fig.update_layout(
        font=dict(family="Inter, sans-serif", size=12),
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(l=10, r=60, t=20, b=10),
    )

    return fig


def apply_deviation_bar_style(
    fig: go.Figure,
    values: list[float],
) -> go.Figure:
    """Apply style to a deviation (positive/negative) bar chart.

    Similar to ``apply_bar_chart_style`` but formats values with +/- signs
    and uses semantic green/red colors for the text.

    Args:
        fig: Plotly figure to modify.
        values: List of delta values (can be negative).

    Returns:
        The modified figure.
    """
    fig.update_xaxes(
        showgrid=True,
        gridwidth=0.5,
        gridcolor="#E0E0E0",
        griddash="dot",
        zeroline=True,
        zerolinewidth=1,
        zerolinecolor="#9ca3af",
    )
    fig.update_yaxes(showgrid=False)

    text_labels = [
        f"+{v:.1f}%" if v > 0 else f"{v:.1f}%"
        for v in values
    ]
    text_colors = ["#16a34a" if v >= 0 else "#dc2626" for v in values]

    fig.update_traces(
        text=text_labels,
        textposition="outside",
        textfont=dict(family="Inter, sans-serif", size=11),
        cliponaxis=False,
    )

    # Per-point text color via marker
    fig.for_each_trace(
        lambda t: t.update(textfont_color=text_colors)
    )

    fig.update_layout(
        font=dict(family="Inter, sans-serif", size=12),
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(l=10, r=60, t=20, b=10),
    )

    return fig
