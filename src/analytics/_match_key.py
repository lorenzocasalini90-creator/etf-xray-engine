"""Shared utility: derive a match key for holdings identity matching.

Uses holding_ticker as the primary key (most universal — every fetcher
provides tickers). Normalizes Bloomberg format ("SAP GY" → "SAP").

For holdings without tickers, falls back to holding_isin then composite_figi.
"""

import pandas as pd

# Well-known US ticker → ISIN mappings for cross-source matching.
# iShares provides tickers without ISINs; Xtrackers provides ISINs without tickers.
# This table enables overlap detection between them.
_TICKER_TO_ISIN: dict[str, str] = {}
_ISIN_TO_TICKER: dict[str, str] = {}


def _build_ticker_isin_map(df: pd.DataFrame) -> None:
    """Learn ticker↔ISIN mappings from a DataFrame that has both."""
    if "holding_ticker" not in df.columns or "holding_isin" not in df.columns:
        return
    for _, row in df.iterrows():
        ticker = row.get("holding_ticker")
        isin = row.get("holding_isin")
        if (ticker and isinstance(ticker, str) and ticker.strip()
                and isin and isinstance(isin, str) and isin.strip()):
            t = ticker.strip().upper().split(" ")[0]  # strip Bloomberg suffix
            i = isin.strip().upper()
            _TICKER_TO_ISIN[t] = i
            _ISIN_TO_TICKER[i] = t


def build_match_keys_from_holdings(holdings_db: dict[str, pd.DataFrame]) -> None:
    """Pre-scan all holdings to build the global ticker↔ISIN lookup.

    Call this once before running analytics on a portfolio.

    Args:
        holdings_db: Dict mapping ETF ticker to its holdings DataFrame.
    """
    _TICKER_TO_ISIN.clear()
    _ISIN_TO_TICKER.clear()
    for df in holdings_db.values():
        _build_ticker_isin_map(df)


def add_match_key(df: pd.DataFrame) -> pd.DataFrame:
    """Add a ``_match_key`` column for identity matching.

    Uses a normalized ticker as the primary key. For holdings with
    only ISIN (no ticker), looks up the global ``_ISIN_TO_TICKER`` map
    built by ``build_match_keys_from_holdings()``.

    Args:
        df: Holdings DataFrame (not modified in place).

    Returns:
        Copy of df with ``_match_key`` column added.
    """
    df = df.copy()
    keys = []

    for _, row in df.iterrows():
        key = None

        # Try ticker first
        ticker = row.get("holding_ticker")
        if ticker and isinstance(ticker, str) and ticker.strip():
            key = ticker.strip().upper().split(" ")[0]  # strip Bloomberg suffix

        # If no ticker, try ISIN → ticker lookup, or use ISIN directly
        if key is None:
            isin = row.get("holding_isin")
            if isin and isinstance(isin, str) and isin.strip():
                isin_clean = isin.strip().upper()
                # Try to resolve ISIN to ticker for cross-matching
                key = _ISIN_TO_TICKER.get(isin_clean, isin_clean)

        # Last resort: composite_figi
        if key is None:
            figi = row.get("composite_figi") if "composite_figi" in row.index else None
            if figi and isinstance(figi, str) and figi.strip():
                key = figi.strip()

        keys.append(key)

    df["_match_key"] = keys
    return df
