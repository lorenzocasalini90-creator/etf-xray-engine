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
