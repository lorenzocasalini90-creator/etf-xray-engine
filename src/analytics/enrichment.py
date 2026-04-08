"""Enrich aggregated holdings with missing sector and country data.

Sources (in priority order):
1. Cross-reference from other holdings in the same portfolio
2. Exchange code → country mapping (from figi_mapping table)
3. Static mapping for well-known securities (defense, energy, tech)
4. yfinance (top 50 holdings by weight only, to avoid rate limits)
"""

from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# File-based cache for yfinance lookups (persists across sessions)
_YFINANCE_CACHE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "src", "data", "yfinance_cache.json",
)


def _load_yfinance_cache() -> dict[str, dict]:
    """Load cached yfinance sector/country data from disk."""
    if os.path.exists(_YFINANCE_CACHE_PATH):
        try:
            with open(_YFINANCE_CACHE_PATH) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_yfinance_cache(cache: dict[str, dict]) -> None:
    """Persist yfinance cache to disk."""
    try:
        with open(_YFINANCE_CACHE_PATH, "w") as f:
            json.dump(cache, f, indent=2)
    except OSError as exc:
        logger.warning("Failed to save yfinance cache: %s", exc)


# Static mapping for well-known securities where yfinance may fail
STATIC_SECTOR_COUNTRY: dict[str, tuple[str, str]] = {
    # Defense / Aerospace
    "RTX": ("Industrials", "United States"),
    "RTX CORP": ("Industrials", "United States"),
    "RAYTHEON": ("Industrials", "United States"),
    "THALES": ("Industrials", "France"),
    "THALES SA": ("Industrials", "France"),
    "LEONARDO": ("Industrials", "Italy"),
    "LEONARDO SPA": ("Industrials", "Italy"),
    "SAAB": ("Industrials", "Sweden"),
    "SAAB AB": ("Industrials", "Sweden"),
    "HANWHA": ("Industrials", "South Korea"),
    "HANWHA AEROSPACE": ("Industrials", "South Korea"),
    "HANWHA SYSTEMS": ("Industrials", "South Korea"),
    "RHEINMETALL": ("Industrials", "Germany"),
    "RHEINMETALL AG": ("Industrials", "Germany"),
    "BAE SYSTEMS": ("Industrials", "United Kingdom"),
    "BAE SYSTEMS PLC": ("Industrials", "United Kingdom"),
    "NORTHROP GRUMMAN": ("Industrials", "United States"),
    "NORTHROP GRUMMAN CORP": ("Industrials", "United States"),
    "LOCKHEED MARTIN": ("Industrials", "United States"),
    "LOCKHEED MARTIN CORP": ("Industrials", "United States"),
    "GENERAL DYNAMICS": ("Industrials", "United States"),
    "GENERAL DYNAMICS CORP": ("Industrials", "United States"),
    "L3HARRIS": ("Industrials", "United States"),
    "L3HARRIS TECHNOLOGIES": ("Industrials", "United States"),
    "ELBIT SYSTEMS": ("Industrials", "Israel"),
    "PALANTIR": ("Technology", "United States"),
    "PALANTIR TECHNOLOGIES": ("Technology", "United States"),
    "BOOZ ALLEN": ("Industrials", "United States"),
    "BOOZ ALLEN HAMILTON": ("Industrials", "United States"),
    "ROLLS-ROYCE": ("Industrials", "United Kingdom"),
    "ROLLS-ROYCE HOLDINGS": ("Industrials", "United Kingdom"),
    "SAFRAN": ("Industrials", "France"),
    "SAFRAN SA": ("Industrials", "France"),
    "KONGSBERG": ("Industrials", "Norway"),
    "KONGSBERG GRUPPEN": ("Industrials", "Norway"),
    "CURTISS-WRIGHT": ("Industrials", "United States"),
    "CURTISS-WRIGHT CORP": ("Industrials", "United States"),
    "TEXTRON": ("Industrials", "United States"),
    "TEXTRON INC": ("Industrials", "United States"),
    "HOWMET AEROSPACE": ("Industrials", "United States"),
    "LEIDOS": ("Industrials", "United States"),
    "LEIDOS HOLDINGS": ("Industrials", "United States"),
    "HENSOLDT": ("Industrials", "Germany"),
    "HENSOLDT AG": ("Industrials", "Germany"),
    "DASSAULT AVIATION": ("Industrials", "France"),
    "DASSAULT AVIATION SA": ("Industrials", "France"),
    "BABCOCK": ("Industrials", "United Kingdom"),
    "BABCOCK INTERNATIONAL": ("Industrials", "United Kingdom"),
    "CAE": ("Industrials", "Canada"),
    "CAE INC": ("Industrials", "Canada"),
    "CHEMRING": ("Industrials", "United Kingdom"),
    "CHEMRING GROUP": ("Industrials", "United Kingdom"),
    "QINETIQ": ("Industrials", "United Kingdom"),
    "QINETIQ GROUP": ("Industrials", "United Kingdom"),
    # Energy / Oil & Gas
    "EXXON MOBIL": ("Energy", "United States"),
    "EXXON MOBIL CORP": ("Energy", "United States"),
    "CHEVRON": ("Energy", "United States"),
    "CHEVRON CORP": ("Energy", "United States"),
    "SHELL": ("Energy", "United Kingdom"),
    "SHELL PLC": ("Energy", "United Kingdom"),
    "TOTALENERGIES": ("Energy", "France"),
    "TOTALENERGIES SE": ("Energy", "France"),
    "BP": ("Energy", "United Kingdom"),
    "BP PLC": ("Energy", "United Kingdom"),
    "CONOCOPHILLIPS": ("Energy", "United States"),
    "ENI": ("Energy", "Italy"),
    "ENI SPA": ("Energy", "Italy"),
    "EQUINOR": ("Energy", "Norway"),
    "EQUINOR ASA": ("Energy", "Norway"),
    "SCHLUMBERGER": ("Energy", "United States"),
    "SLB": ("Energy", "United States"),
    # Tech
    "APPLE": ("Technology", "United States"),
    "APPLE INC": ("Technology", "United States"),
    "MICROSOFT": ("Technology", "United States"),
    "MICROSOFT CORP": ("Technology", "United States"),
    "NVIDIA": ("Technology", "United States"),
    "NVIDIA CORP": ("Technology", "United States"),
    "ALPHABET": ("Communication Services", "United States"),
    "AMAZON": ("Consumer Cyclical", "United States"),
    "AMAZON.COM": ("Consumer Cyclical", "United States"),
    "META PLATFORMS": ("Communication Services", "United States"),
    "TESLA": ("Consumer Cyclical", "United States"),
    "TESLA INC": ("Consumer Cyclical", "United States"),
    "BROADCOM": ("Technology", "United States"),
    "BROADCOM INC": ("Technology", "United States"),
    "TAIWAN SEMICONDUCTOR": ("Technology", "Taiwan"),
    "TSMC": ("Technology", "Taiwan"),
    "SAMSUNG ELECTRONICS": ("Technology", "South Korea"),
    "ASML": ("Technology", "Netherlands"),
    "ASML HOLDING": ("Technology", "Netherlands"),
}

# Hardcoded exchange code → country mapping for major markets
EXCHANGE_COUNTRY_MAP: dict[str, str] = {
    "US": "United States",
    "UN": "United States",
    "UW": "United States",
    "UA": "United States",
    "UP": "United States",
    "UQ": "United States",
    "LN": "United Kingdom",
    "GY": "Germany",
    "GR": "Germany",
    "FP": "France",
    "NA": "Netherlands",
    "SM": "Spain",
    "IM": "Italy",
    "SS": "Sweden",
    "DC": "Denmark",
    "NO": "Norway",
    "HB": "Hong Kong",
    "HK": "Hong Kong",
    "JT": "Japan",
    "JP": "Japan",
    "AT": "Australia",
    "AU": "Australia",
    "CN": "Canada",
    "CT": "Canada",
    "SW": "Switzerland",
    "VX": "Switzerland",
    "SJ": "South Africa",
    "KS": "South Korea",
    "KP": "South Korea",
    "TT": "Taiwan",
    "SP": "Singapore",
    "IB": "Ireland",
    "ID": "Ireland",
    "PL": "Poland",
    "FH": "Finland",
    "BB": "Belgium",
    "AV": "Austria",
    "NZ": "New Zealand",
}


def enrich_missing_data(
    df: pd.DataFrame,
    db_session: Session | None = None,
    *,
    yfinance_top_n: int = 50,
) -> pd.DataFrame:
    """Enrich holdings DataFrame with missing sector and country data.

    Args:
        df: Aggregated holdings DataFrame with columns: name, ticker, sector,
            country, real_weight_pct.
        db_session: Optional SQLAlchemy session for DB lookups.
        yfinance_top_n: Max holdings to enrich via yfinance (by weight).

    Returns:
        Enriched DataFrame with fewer missing sector/country values.
    """
    if df.empty:
        return df

    result = df.copy()

    # Ensure sector/country are string columns
    for col in ("sector", "country"):
        if col in result.columns:
            result[col] = result[col].fillna("").astype(str)

    # Step 1: Cross-reference from other holdings in the portfolio
    _enrich_from_portfolio_cross_ref(result)

    # Step 2: DB lookup (figi_mapping + security_fundamentals)
    if db_session is not None:
        _enrich_from_db(result, db_session)

    # Step 3: Static mapping for well-known securities
    _enrich_from_static_mapping(result)

    # Step 4: yfinance for top holdings still missing data
    _enrich_from_yfinance(result, top_n=yfinance_top_n)

    # Normalize empty strings back to "Unknown" for display
    for col in ("sector", "country"):
        if col in result.columns:
            result[col] = result[col].replace("", "Unknown")

    return result


def _enrich_from_portfolio_cross_ref(df: pd.DataFrame) -> None:
    """Fill missing sector/country from other rows with the same ticker/name."""
    # Build lookup from rows that HAVE data
    ticker_sector: dict[str, str] = {}
    ticker_country: dict[str, str] = {}
    name_sector: dict[str, str] = {}
    name_country: dict[str, str] = {}

    for _, row in df.iterrows():
        ticker = str(row.get("ticker", "")).strip().upper()
        name = str(row.get("name", "")).strip().upper()
        sector = str(row.get("sector", "")).strip()
        country = str(row.get("country", "")).strip()

        if sector and sector != "Unknown":
            if ticker:
                ticker_sector[ticker] = sector
            if name:
                name_sector[name] = sector
        if country and country != "Unknown":
            if ticker:
                ticker_country[ticker] = country
            if name:
                name_country[name] = country

    # Fill missing values
    filled_sector = 0
    filled_country = 0
    for idx, row in df.iterrows():
        sector = str(row.get("sector", "")).strip()
        country = str(row.get("country", "")).strip()
        ticker = str(row.get("ticker", "")).strip().upper()
        name = str(row.get("name", "")).strip().upper()

        if not sector or sector == "Unknown":
            found = ticker_sector.get(ticker) or name_sector.get(name)
            if found:
                df.at[idx, "sector"] = found
                filled_sector += 1

        if not country or country == "Unknown":
            found = ticker_country.get(ticker) or name_country.get(name)
            if found:
                df.at[idx, "country"] = found
                filled_country += 1

    if filled_sector or filled_country:
        logger.info(
            "Cross-ref enrichment: filled %d sectors, %d countries",
            filled_sector, filled_country,
        )


def _enrich_from_db(df: pd.DataFrame, session: Session) -> None:
    """Fill missing sector/country from figi_mapping exchange codes."""
    from src.storage.models import FigiMapping

    # Find rows still missing country
    missing_country = df[
        (df["country"] == "") | (df["country"] == "Unknown")
    ]
    if missing_country.empty:
        return

    # Collect tickers to look up
    tickers = missing_country["ticker"].dropna().unique().tolist()
    tickers = [t for t in tickers if t.strip()]
    if not tickers:
        return

    try:
        mappings = session.query(FigiMapping).filter(
            FigiMapping.ticker.in_(tickers)
        ).all()
    except Exception as exc:
        logger.warning("DB lookup failed: %s", exc)
        return

    ticker_exchange: dict[str, str] = {}
    for m in mappings:
        if m.ticker and m.exchange:
            ticker_exchange[m.ticker.upper()] = m.exchange.upper()

    filled = 0
    for idx, row in missing_country.iterrows():
        ticker = str(row.get("ticker", "")).strip().upper()
        exchange = ticker_exchange.get(ticker, "")
        country = EXCHANGE_COUNTRY_MAP.get(exchange, "")
        if country:
            df.at[idx, "country"] = country
            filled += 1

    if filled:
        logger.info("DB enrichment: filled %d countries from exchange codes", filled)


def _enrich_from_static_mapping(df: pd.DataFrame) -> None:
    """Fill missing sector/country from static mapping of well-known securities.

    When sector is missing, we also overwrite country even if set, because
    fetchers often report the exchange country (e.g. "Germany" for Xetra)
    instead of the company's domicile country.
    """
    filled_sector = 0
    filled_country = 0

    for idx, row in df.iterrows():
        sector = str(row.get("sector", "")).strip()
        country = str(row.get("country", "")).strip()
        needs_sector = not sector or sector == "Unknown"
        needs_country = not country or country == "Unknown"

        if not needs_sector and not needs_country:
            continue

        # Try matching by ticker, then by name (exact and partial)
        name = str(row.get("name", "")).strip().upper()
        ticker = str(row.get("ticker", "")).strip().upper()

        match = None
        if ticker and ticker in STATIC_SECTOR_COUNTRY:
            match = STATIC_SECTOR_COUNTRY[ticker]
        elif name and name in STATIC_SECTOR_COUNTRY:
            match = STATIC_SECTOR_COUNTRY[name]
        else:
            # Partial name match: check if any key is contained in the name
            for key, val in STATIC_SECTOR_COUNTRY.items():
                if len(key) >= 4 and key in name:
                    match = val
                    break

        if match:
            if needs_sector:
                df.at[idx, "sector"] = match[0]
                filled_sector += 1
            # When sector was missing, also fix country (likely exchange-based)
            if needs_sector or needs_country:
                df.at[idx, "country"] = match[1]
                filled_country += 1

    if filled_sector or filled_country:
        logger.info(
            "Static mapping enrichment: filled %d sectors, %d countries",
            filled_sector, filled_country,
        )


def _normalize_name_for_yfinance(name: str) -> str | None:
    """Convert a holding name to a plausible yfinance ticker symbol."""
    if not name:
        return None
    # Remove common suffixes
    clean = name.upper().strip()
    for suffix in (" INC", " CORP", " PLC", " LTD", " SA", " AG", " SE",
                   " SPA", " NV", " AB", " ASA", " CO", " GROUP",
                   " HOLDINGS", " CLASS A", " CLASS B", " CLASS C",
                   " CL A", " CL B", " CL C", " ORD", " REGISTERED"):
        clean = clean.replace(suffix, "")
    clean = clean.strip()
    # Replace spaces with hyphens (yfinance convention for some stocks)
    if " " in clean:
        return clean.replace(" ", "-")
    return clean if clean else None


def _enrich_from_yfinance(df: pd.DataFrame, top_n: int = 50) -> None:
    """Fill missing sector/country from yfinance for top holdings by weight.

    Handles holdings without tickers by attempting name-based lookups.
    Results are cached to disk to avoid repeated API calls.
    """
    missing = df[
        ((df["sector"] == "") | (df["sector"] == "Unknown"))
        | ((df["country"] == "") | (df["country"] == "Unknown"))
    ]

    if missing.empty:
        return

    # Only process top N by weight to avoid rate limits
    if "real_weight_pct" in missing.columns:
        missing = missing.nlargest(top_n, "real_weight_pct")

    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not installed, skipping yfinance enrichment")
        return

    # Load persistent cache
    yf_cache = _load_yfinance_cache()

    filled_sector = 0
    filled_country = 0

    # Build list of (index, lookup_key) pairs — try ticker first, then name
    lookups: list[tuple[int, str]] = []
    for idx, row in missing.iterrows():
        ticker = str(row.get("ticker", "")).strip()
        name = str(row.get("name", "")).strip()

        if ticker and len(ticker) <= 10:
            lookups.append((idx, ticker))
        elif name:
            derived = _normalize_name_for_yfinance(name)
            if derived and len(derived) <= 20:
                lookups.append((idx, derived))

    if not lookups:
        return

    # Deduplicate lookup keys
    seen_keys: set[str] = set()
    unique_lookups: list[tuple[int, str]] = []
    for idx, key in lookups:
        if key.upper() not in seen_keys:
            seen_keys.add(key.upper())
            unique_lookups.append((idx, key))
        else:
            unique_lookups.append((idx, key))

    # Fetch from yfinance (or cache)
    fetched: dict[str, dict[str, str]] = {}
    cache_dirty = False

    for _, key in unique_lookups:
        upper_key = key.upper()
        if upper_key in fetched:
            continue

        # Check cache first
        if upper_key in yf_cache:
            fetched[upper_key] = yf_cache[upper_key]
            continue

        # Fetch from yfinance
        try:
            info = yf.Ticker(key).info
            yf_sector = info.get("sector", "")
            yf_country = info.get("country", "")

            result = {"sector": yf_sector, "country": yf_country}
            fetched[upper_key] = result

            # Persist to cache (even empty results to avoid retrying)
            yf_cache[upper_key] = result
            cache_dirty = True
        except Exception:
            # Cache the miss too
            yf_cache[upper_key] = {"sector": "", "country": ""}
            cache_dirty = True
            continue

    # Apply fetched data
    for idx, key in unique_lookups:
        data = fetched.get(key.upper())
        if not data:
            continue

        cur_sector = str(df.at[idx, "sector"]).strip()
        cur_country = str(df.at[idx, "country"]).strip()
        needs_sector = not cur_sector or cur_sector == "Unknown"

        if needs_sector and data.get("sector"):
            df.at[idx, "sector"] = data["sector"]
            filled_sector += 1
        # When sector was missing, also overwrite country — fetcher country
        # is often the exchange country (e.g. "Germany" for Xetra), not the
        # company's domicile.
        if data.get("country") and (needs_sector or not cur_country or cur_country == "Unknown"):
            df.at[idx, "country"] = data["country"]
            filled_country += 1

    # Save cache if changed
    if cache_dirty:
        _save_yfinance_cache(yf_cache)

    if filled_sector or filled_country:
        logger.info(
            "yfinance enrichment: filled %d sectors, %d countries",
            filled_sector, filled_country,
        )
