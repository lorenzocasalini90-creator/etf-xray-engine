"""ETF directory: static CSV lookup with search."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st


_CSV_PATH = Path(__file__).parent / "etf_directory.csv"


@st.cache_data
def load_directory() -> pd.DataFrame:
    """Load the ETF directory CSV (cached via Streamlit)."""
    return pd.read_csv(_CSV_PATH, dtype=str).fillna("")


def search_etf(query: str, limit: int = 6) -> list[dict]:
    """Search ETFs by ticker, ISIN, or partial name.

    Priority: exact ticker > exact ISIN > partial ticker > partial name.
    Returns list of dicts with keys: isin, ticker, name, provider, ter_pct.
    """
    if len(query.strip()) < 2:
        return []

    q = query.strip().upper()
    df = load_directory()

    exact_ticker = df[df["ticker"].str.upper() == q]
    exact_isin = df[df["isin"].str.upper() == q]
    partial_ticker = df[df["ticker"].str.upper().str.startswith(q, na=False)]

    # Word-based name matching: all query words must appear in the name
    words = q.split()
    name_upper = df["name"].str.upper()
    mask = name_upper.str.contains(words[0], na=False)
    for word in words[1:]:
        mask = mask & name_upper.str.contains(word, na=False)
    partial_name = df[mask]

    combined = pd.concat([exact_ticker, exact_isin, partial_ticker, partial_name])
    combined = combined.drop_duplicates(subset="isin").head(limit)

    return combined.to_dict("records")
