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
