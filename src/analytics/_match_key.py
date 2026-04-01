"""Shared utility: derive a match key for holdings identity matching.

Cross-source matching problem:
  - iShares: ticker (AAPL) but no ISIN
  - Xtrackers: ISIN (US0378331005) but no ticker
  - Amundi: Bloomberg ticker (AAPL UW) + ISIN

Solution: build a global ticker↔ISIN mapping from ETFs that have both,
then use it to unify match keys across all ETFs. When no mapping exists,
fall back to normalized holding_name.
"""

import re

import pandas as pd


# Global lookups — populated by build_match_keys_from_holdings()
_TICKER_TO_ISIN: dict[str, str] = {}
_ISIN_TO_TICKER: dict[str, str] = {}
_NAME_TO_KEY: dict[str, str] = {}


def _normalize_ticker(raw: str) -> str:
    """Normalize a ticker: uppercase, strip Bloomberg exchange suffix.

    ``"AAPL UW"`` → ``"AAPL"``, ``"SAP GY"`` → ``"SAP"``.
    """
    return raw.strip().upper().split()[0] if raw and raw.strip() else ""


def _normalize_name(raw: str) -> str:
    """Normalize a holding name for fuzzy matching.

    Uppercase, strip suffixes like INC, CORP, PLC, SE, NV, AG, LTD,
    punctuation, and extra whitespace.
    """
    if not raw or not isinstance(raw, str):
        return ""
    name = raw.upper().strip()
    # Remove common suffixes
    name = re.sub(
        r"\b(INC|CORP|PLC|SE|NV|AG|LTD|CO|CLASS [A-Z]|CL [A-Z]|/\s*\w+)\b",
        "", name,
    )
    # Remove punctuation and collapse whitespace
    name = re.sub(r"[.,/\\()'-]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def build_match_keys_from_holdings(holdings_db: dict[str, pd.DataFrame]) -> None:
    """Pre-scan all holdings to build global ticker↔ISIN and name lookups.

    Must be called once before running analytics on a portfolio.
    ETFs that have both ticker and ISIN (like Amundi) provide the bridge
    between ticker-only (iShares) and ISIN-only (Xtrackers) ETFs.

    Args:
        holdings_db: Dict mapping ETF ticker to its holdings DataFrame.
    """
    _TICKER_TO_ISIN.clear()
    _ISIN_TO_TICKER.clear()
    _NAME_TO_KEY.clear()

    # Pass 1: learn ticker↔ISIN mappings from ETFs that have both
    for df in holdings_db.values():
        if "holding_ticker" not in df.columns or "holding_isin" not in df.columns:
            continue
        for _, row in df.iterrows():
            ticker_raw = row.get("holding_ticker")
            isin_raw = row.get("holding_isin")
            if (ticker_raw and isinstance(ticker_raw, str) and ticker_raw.strip()
                    and isin_raw and isinstance(isin_raw, str) and isin_raw.strip()):
                t = _normalize_ticker(ticker_raw)
                i = isin_raw.strip().upper()
                if t and i:
                    _TICKER_TO_ISIN[t] = i
                    _ISIN_TO_TICKER[i] = t

    # Pass 2: build name→key mapping for holdings that have a ticker or ISIN
    # This allows name-based fallback matching
    for df in holdings_db.values():
        for _, row in df.iterrows():
            name_raw = row.get("holding_name")
            if not name_raw or not isinstance(name_raw, str):
                continue
            norm_name = _normalize_name(name_raw)
            if not norm_name:
                continue

            # Prefer ticker as the canonical key
            ticker_raw = row.get("holding_ticker")
            if ticker_raw and isinstance(ticker_raw, str) and ticker_raw.strip():
                t = _normalize_ticker(ticker_raw)
                if t and norm_name not in _NAME_TO_KEY:
                    _NAME_TO_KEY[norm_name] = t
                continue

            # If no ticker, use ISIN → try to resolve to ticker, else use ISIN
            isin_raw = row.get("holding_isin")
            if isin_raw and isinstance(isin_raw, str) and isin_raw.strip():
                i = isin_raw.strip().upper()
                resolved = _ISIN_TO_TICKER.get(i, i)
                if norm_name not in _NAME_TO_KEY:
                    _NAME_TO_KEY[norm_name] = resolved


def _get_match_key_for_row(row) -> str | None:
    """Derive match key for a single row.

    Priority:
    1. Ticker (normalized) — if available
    2. ISIN → resolve to ticker via lookup, else use ISIN
    3. Name → resolve via name lookup
    """
    # Try ticker
    ticker_raw = row.get("holding_ticker")
    if ticker_raw and isinstance(ticker_raw, str) and ticker_raw.strip():
        t = _normalize_ticker(ticker_raw)
        if t:
            return t

    # Try ISIN → resolve to ticker if possible
    isin_raw = row.get("holding_isin")
    if isin_raw and isinstance(isin_raw, str) and isin_raw.strip():
        i = isin_raw.strip().upper()
        return _ISIN_TO_TICKER.get(i, i)

    # Try composite_figi
    if "composite_figi" in row.index:
        figi = row.get("composite_figi")
        if figi and isinstance(figi, str) and figi.strip():
            return figi.strip()

    # Last resort: normalized name lookup
    name_raw = row.get("holding_name")
    if name_raw and isinstance(name_raw, str):
        norm = _normalize_name(name_raw)
        if norm in _NAME_TO_KEY:
            return _NAME_TO_KEY[norm]
        if norm:
            return f"NAME:{norm}"

    return None


def add_match_key(df: pd.DataFrame) -> pd.DataFrame:
    """Add a ``_match_key`` column for identity matching.

    Args:
        df: Holdings DataFrame (not modified in place).

    Returns:
        Copy of df with ``_match_key`` column added.
    """
    df = df.copy()
    df["_match_key"] = [_get_match_key_for_row(row) for _, row in df.iterrows()]
    return df
