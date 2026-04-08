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
