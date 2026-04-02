"""Shared utility: derive a match key for holdings identity matching.

Cross-source matching problem:
  - iShares: ticker (AAPL) but no ISIN
  - Xtrackers: ISIN (US0378331005) but no ticker
  - Amundi: Bloomberg ticker (AAPL UW) + ISIN

Solution:
1. Static lookup table (src/data/ticker_isin_map.json) with ~500 mappings
   for the most common holdings — works without any bridge ETF.
2. Dynamic learning from ETFs that have both ticker + ISIN (e.g. Amundi).
3. Name-based fallback for anything not in the lookup.
"""

import json
import logging
import re
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Static lookup — loaded once at import time
# ---------------------------------------------------------------------------

_STATIC_MAP_PATH = Path(__file__).resolve().parent.parent / "data" / "ticker_isin_map.json"

_STATIC_TICKER_TO_ISIN: dict[str, str] = {}
_STATIC_ISIN_TO_TICKER: dict[str, str] = {}

try:
    with open(_STATIC_MAP_PATH) as f:
        _data = json.load(f)
        _STATIC_TICKER_TO_ISIN = _data.get("ticker_to_isin", {})
        _STATIC_ISIN_TO_TICKER = _data.get("isin_to_ticker", {})
    logger.info(
        "Loaded static ticker-ISIN map: %d entries", len(_STATIC_TICKER_TO_ISIN)
    )
except FileNotFoundError:
    logger.warning("Static ticker-ISIN map not found at %s", _STATIC_MAP_PATH)
except Exception as exc:
    logger.warning("Failed to load static ticker-ISIN map: %s", exc)

# ---------------------------------------------------------------------------
# Dynamic lookups — populated per-analysis by build_match_keys_from_holdings()
# ---------------------------------------------------------------------------

_DYN_TICKER_TO_ISIN: dict[str, str] = {}
_DYN_ISIN_TO_TICKER: dict[str, str] = {}
_NAME_TO_KEY: dict[str, str] = {}


def _normalize_ticker(raw: str) -> str:
    """Normalize a ticker: uppercase, strip Bloomberg exchange suffix.

    ``"AAPL UW"`` -> ``"AAPL"``, ``"SAP GY"`` -> ``"SAP"``.
    """
    return raw.strip().upper().split()[0] if raw and raw.strip() else ""


def _normalize_name(raw: str) -> str:
    """Normalize a holding name for fuzzy matching."""
    if not raw or not isinstance(raw, str):
        return ""
    name = raw.upper().strip()
    name = re.sub(
        r"\b(INC|CORP|PLC|SE|NV|AG|LTD|CO|CLASS [A-Z]|CL [A-Z]|/\s*\w+)\b",
        "", name,
    )
    name = re.sub(r"[.,/\\()'-]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _resolve_ticker(ticker: str) -> str | None:
    """Resolve a ticker to ISIN via dynamic then static lookup."""
    return _DYN_TICKER_TO_ISIN.get(ticker) or _STATIC_TICKER_TO_ISIN.get(ticker)


def _resolve_isin(isin: str) -> str | None:
    """Resolve an ISIN to ticker via dynamic then static lookup."""
    return _DYN_ISIN_TO_TICKER.get(isin) or _STATIC_ISIN_TO_TICKER.get(isin)


def build_match_keys_from_holdings(holdings_db: dict[str, pd.DataFrame]) -> None:
    """Pre-scan all holdings to build dynamic ticker<->ISIN and name lookups.

    Must be called once before running analytics on a portfolio.

    Args:
        holdings_db: Dict mapping ETF ticker to its holdings DataFrame.
    """
    _DYN_TICKER_TO_ISIN.clear()
    _DYN_ISIN_TO_TICKER.clear()
    _NAME_TO_KEY.clear()

    # Pass 1: learn ticker<->ISIN from ETFs that have both
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
                    _DYN_TICKER_TO_ISIN[t] = i
                    _DYN_ISIN_TO_TICKER[i] = t

    # Pass 2: name->key for fallback
    for df in holdings_db.values():
        for _, row in df.iterrows():
            name_raw = row.get("holding_name")
            if not name_raw or not isinstance(name_raw, str):
                continue
            norm_name = _normalize_name(name_raw)
            if not norm_name or norm_name in _NAME_TO_KEY:
                continue

            ticker_raw = row.get("holding_ticker")
            if ticker_raw and isinstance(ticker_raw, str) and ticker_raw.strip():
                _NAME_TO_KEY[norm_name] = _normalize_ticker(ticker_raw)
                continue

            isin_raw = row.get("holding_isin")
            if isin_raw and isinstance(isin_raw, str) and isin_raw.strip():
                i = isin_raw.strip().upper()
                resolved = _resolve_isin(i)
                _NAME_TO_KEY[norm_name] = resolved if resolved else i


def _get_match_key_for_row(row) -> str | None:
    """Derive match key for a single row.

    Handles both standard schema (``holding_ticker``, ``holding_isin``,
    ``holding_name``) and aggregated format (``ticker``, ``name``).

    Priority:
    1. Ticker (normalized) — if available
    2. ISIN -> resolve to ticker via lookup, else use ISIN
    3. composite_figi
    4. Normalized name -> resolve via name lookup
    """
    # Try ticker — standard schema or aggregated format
    ticker_raw = row.get("holding_ticker") or row.get("ticker")
    if ticker_raw and isinstance(ticker_raw, str) and ticker_raw.strip():
        return _normalize_ticker(ticker_raw)

    # Try ISIN -> resolve to ticker if possible
    isin_raw = row.get("holding_isin")
    if isin_raw and isinstance(isin_raw, str) and isin_raw.strip():
        i = isin_raw.strip().upper()
        resolved = _resolve_isin(i)
        return resolved if resolved else i

    # Try composite_figi
    if "composite_figi" in row.index:
        figi = row.get("composite_figi")
        if figi and isinstance(figi, str) and figi.strip():
            return figi.strip()

    # Last resort: normalized name — standard schema or aggregated format
    name_raw = row.get("holding_name") or row.get("name")
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
