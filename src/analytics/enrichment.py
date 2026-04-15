"""Enrich aggregated holdings with missing sector and country data.

Sources (in priority order):
1. Cross-reference from other holdings in the same portfolio
2. Exchange code → country mapping (from figi_mapping table)
3. Static mapping for well-known securities (defense, energy, tech)
4. yfinance (top 50 holdings by weight only, to avoid rate limits)
5. API Ninjas (for remaining unknowns, if API_NINJAS_KEY is set)
"""

from __future__ import annotations

import logging
import os
import time
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

def _load_yfinance_cache(db_session: Session | None) -> dict[str, dict]:
    """Load cached yfinance sector/country data from the database."""
    if db_session is None:
        return {}
    try:
        from src.storage.models import YfinanceCache
        rows = db_session.query(YfinanceCache).all()
        return {
            row.ticker: {"sector": row.sector or "", "country": row.country or ""}
            for row in rows
        }
    except Exception as exc:
        logger.warning("Failed to load yfinance cache from DB: %s", exc)
        return {}


def _save_yfinance_cache(cache: dict[str, dict], db_session: Session | None) -> None:
    """Persist yfinance cache to the database."""
    if db_session is None:
        return
    try:
        from src.storage.models import YfinanceCache
        for ticker, data in cache.items():
            existing = db_session.get(YfinanceCache, ticker)
            if existing:
                existing.sector = data.get("sector", "")
                existing.country = data.get("country", "")
            else:
                db_session.add(YfinanceCache(
                    ticker=ticker,
                    sector=data.get("sector", ""),
                    country=data.get("country", ""),
                ))
        db_session.commit()
    except Exception as exc:
        logger.warning("Failed to save yfinance cache to DB: %s", exc)
        try:
            db_session.rollback()
        except Exception:
            pass


# Common suffixes to strip when normalizing holding names for matching
_NAME_SUFFIXES = (
    " INC.", " INC", " CORP.", " CORP", " PLC", " LTD.", " LTD",
    " SA", " AG", " SE", " SPA", " NV", " AB", " ASA",
    " CO.", " CO", " GROUP", " HOLDINGS", " HOLDING",
    " CLASS A", " CLASS B", " CLASS C", " CL A", " CL B", " CL C",
    " ORD", " REGISTERED", " COMMON STOCK", ", INC.", ", INC",
    ", LTD.", ", LTD", ",", ".",
)


def _normalize_holding_name(name: str) -> str:
    """Normalize a holding name for matching: uppercase, strip suffixes."""
    clean = name.upper().strip()
    # Iteratively strip suffixes (order matters — strip longer first)
    changed = True
    while changed:
        changed = False
        for suffix in _NAME_SUFFIXES:
            if clean.endswith(suffix):
                clean = clean[: -len(suffix)].strip()
                changed = True
    return clean


# Static mapping for well-known securities where yfinance may fail.
# Keys are NORMALIZED (uppercase, no suffixes). Matching is done via
# _normalize_holding_name() so "Thales SA", "THALES", "Thales" all match.
STATIC_SECTOR_COUNTRY: dict[str, tuple[str, str]] = {
    # Defense / Aerospace
    "RTX": ("Industrials", "United States"),
    "RAYTHEON": ("Industrials", "United States"),
    "THALES": ("Industrials", "France"),
    "LEONARDO": ("Industrials", "Italy"),
    "SAAB": ("Industrials", "Sweden"),
    "HANWHA AEROSPACE": ("Industrials", "South Korea"),
    "HANWHA SYSTEMS": ("Industrials", "South Korea"),
    "HANWHA": ("Industrials", "South Korea"),
    "RHEINMETALL": ("Industrials", "Germany"),
    "BAE SYSTEMS": ("Industrials", "United Kingdom"),
    "NORTHROP GRUMMAN": ("Industrials", "United States"),
    "LOCKHEED MARTIN": ("Industrials", "United States"),
    "GENERAL DYNAMICS": ("Industrials", "United States"),
    "L3HARRIS": ("Industrials", "United States"),
    "L3HARRIS TECHNOLOGIES": ("Industrials", "United States"),
    "ELBIT SYSTEMS": ("Industrials", "Israel"),
    "PALANTIR": ("Information Technology", "United States"),
    "PALANTIR TECHNOLOGIES": ("Information Technology", "United States"),
    "BOOZ ALLEN": ("Industrials", "United States"),
    "BOOZ ALLEN HAMILTON": ("Industrials", "United States"),
    "ROLLS-ROYCE": ("Industrials", "United Kingdom"),
    "SAFRAN": ("Industrials", "France"),
    "KONGSBERG": ("Industrials", "Norway"),
    "KONGSBERG GRUPPEN": ("Industrials", "Norway"),
    "CURTISS-WRIGHT": ("Industrials", "United States"),
    "TEXTRON": ("Industrials", "United States"),
    "HOWMET AEROSPACE": ("Industrials", "United States"),
    "LEIDOS": ("Industrials", "United States"),
    "LEIDOS HOLDINGS": ("Industrials", "United States"),
    "HENSOLDT": ("Industrials", "Germany"),
    "DASSAULT AVIATION": ("Industrials", "France"),
    "BABCOCK": ("Industrials", "United Kingdom"),
    "BABCOCK INTERNATIONAL": ("Industrials", "United Kingdom"),
    "CAE": ("Industrials", "Canada"),
    "CHEMRING": ("Industrials", "United Kingdom"),
    "QINETIQ": ("Industrials", "United Kingdom"),
    "LUMENTUM": ("Information Technology", "United States"),
    "LUMENTUM HOLDINGS": ("Information Technology", "United States"),
    "PALO ALTO NETWORKS": ("Information Technology", "United States"),
    # Energy / Oil & Gas
    "EXXON MOBIL": ("Energy", "United States"),
    "CHEVRON": ("Energy", "United States"),
    "SHELL": ("Energy", "United Kingdom"),
    "TOTALENERGIES": ("Energy", "France"),
    "BP": ("Energy", "United Kingdom"),
    "CONOCOPHILLIPS": ("Energy", "United States"),
    "ENI": ("Energy", "Italy"),
    "EQUINOR": ("Energy", "Norway"),
    "SCHLUMBERGER": ("Energy", "United States"),
    "SLB": ("Energy", "United States"),
    "CANADIAN NATURAL": ("Energy", "Canada"),
    "CANADIAN NATURAL RESOURCES": ("Energy", "Canada"),
    "EOG RESOURCES": ("Energy", "United States"),
    "CORTEVA": ("Materials", "United States"),
    "CORTEVA AGRISCIENCE": ("Materials", "United States"),
    "ARCHER-DANIELS-MIDLAND": ("Consumer Staples", "United States"),
    "ARCHER DANIELS MIDLAND": ("Consumer Staples", "United States"),
    # Tech / Information Technology
    "APPLE": ("Information Technology", "United States"),
    "MICROSOFT": ("Information Technology", "United States"),
    "NVIDIA": ("Information Technology", "United States"),
    "ALPHABET": ("Communication Services", "United States"),
    "AMAZON": ("Consumer Discretionary", "United States"),
    "AMAZON.COM": ("Consumer Discretionary", "United States"),
    "META PLATFORMS": ("Communication Services", "United States"),
    "TESLA": ("Consumer Discretionary", "United States"),
    "BROADCOM": ("Information Technology", "United States"),
    "TAIWAN SEMICONDUCTOR": ("Information Technology", "Taiwan"),
    "TAIWAN SEMICONDUCTOR MANUFACTURING": ("Information Technology", "Taiwan"),
    "TSMC": ("Information Technology", "Taiwan"),
    "SAMSUNG ELECTRONICS": ("Information Technology", "South Korea"),
    "ASML": ("Information Technology", "Netherlands"),
    "ASML HOLDING": ("Information Technology", "Netherlands"),
    "LAM RESEARCH": ("Information Technology", "United States"),
    "ANALOG DEVICES": ("Information Technology", "United States"),
    "TERADYNE": ("Information Technology", "United States"),
    "INFINEON": ("Information Technology", "Germany"),
    "INFINEON TECHNOLOGIES": ("Information Technology", "Germany"),
    "COGNEX": ("Information Technology", "United States"),
    # Defense / Aerospace — additional
    "BOEING": ("Industrials", "United States"),
    "AIRBUS": ("Industrials", "France"),
    # European banks
    "DEUTSCHE BANK": ("Financials", "Germany"),
    "SOCIETE GENERALE": ("Financials", "France"),
    "CREDIT AGRICOLE": ("Financials", "France"),
    "BARCLAYS": ("Financials", "United Kingdom"),
    "HSBC": ("Financials", "United Kingdom"),
    "STANDARD CHARTERED": ("Financials", "United Kingdom"),
    "NORDEA": ("Financials", "Finland"),
    "DANSKE BANK": ("Financials", "Denmark"),
    "KBC GROUP": ("Financials", "Belgium"),
    "ERSTE GROUP": ("Financials", "Austria"),
    "RAIFFEISEN": ("Financials", "Austria"),
    "COMMERZBANK": ("Financials", "Germany"),
    # European banks — additional
    "BANCO SANTANDER": ("Financials", "Spain"),
    "SANTANDER": ("Financials", "Spain"),
    "UNICREDIT": ("Financials", "Italy"),
    "BANCO BILBAO": ("Financials", "Spain"),
    "BBVA": ("Financials", "Spain"),
    "BNP PARIBAS": ("Financials", "France"),
    "ING GROUP": ("Financials", "Netherlands"),
    "ING GROEP": ("Financials", "Netherlands"),
    "INTESA SANPAOLO": ("Financials", "Italy"),
    "CAIXABANK": ("Financials", "Spain"),
    # Consumer / Industrials — European
    "THULE GROUP": ("Consumer Discretionary", "Sweden"),
    "THULE": ("Consumer Discretionary", "Sweden"),
    "MTU AERO ENGINES": ("Industrials", "Germany"),
    "MTU AERO": ("Industrials", "Germany"),
    "GENERAL ELECTRIC": ("Industrials", "United States"),
    "GE AEROSPACE": ("Industrials", "United States"),
    "MELROSE INDUSTRIES": ("Industrials", "United Kingdom"),
    "CHEMRING GROUP": ("Industrials", "United Kingdom"),
    "INDRA SISTEMAS": ("Information Technology", "Spain"),
    "INDRA": ("Information Technology", "Spain"),
    "LEONARDO DRS": ("Industrials", "United States"),
    "ASELSAN": ("Industrials", "Turkey"),
    "ISRAEL AEROSPACE": ("Industrials", "Israel"),
    "KOREAN AIR": ("Industrials", "South Korea"),
    "TURKISH AEROSPACE": ("Industrials", "Turkey"),
    "FINCANTIERI": ("Industrials", "Italy"),
    "DIEHL": ("Industrials", "Germany"),
    "TRASTOR": ("Industrials", "Greece"),
    "THYSSENKRUPP": ("Industrials", "Germany"),
    "NAVAL GROUP": ("Industrials", "France"),
    "MBDA": ("Industrials", "France"),
    # Energy — additional
    "REPSOL": ("Energy", "Spain"),
    "GALP": ("Energy", "Portugal"),
    "OMV": ("Energy", "Austria"),
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

    # Diagnostic: log top 20 holdings BEFORE enrichment
    if logger.isEnabledFor(logging.DEBUG):
        top20_before = result.nlargest(20, "real_weight_pct")[
            ["name", "ticker", "sector", "country"]
        ] if "real_weight_pct" in result.columns else result.head(20)
        logger.debug("BEFORE enrichment (top 20):\n%s", top20_before.to_string())

    # Step 1: Cross-reference from other holdings in the portfolio
    _enrich_from_portfolio_cross_ref(result)

    # Step 2: Static mapping for well-known securities
    _enrich_from_static_mapping(result)

    # Step 3: yfinance for top holdings still missing data
    _enrich_from_yfinance(result, top_n=yfinance_top_n, db_session=db_session)

    # Step 4: API Ninjas for remaining unknowns
    _enrich_from_api_ninjas(result, top_n=yfinance_top_n, db_session=db_session)

    # Normalize empty strings back to "Unknown" for display
    for col in ("sector", "country"):
        if col in result.columns:
            result[col] = result[col].replace("", "Unknown")

    # Diagnostic: log top 20 holdings AFTER enrichment
    if logger.isEnabledFor(logging.DEBUG):
        top20_after = result.nlargest(20, "real_weight_pct")[
            ["name", "ticker", "sector", "country"]
        ] if "real_weight_pct" in result.columns else result.head(20)
        logger.debug("AFTER enrichment (top 20):\n%s", top20_after.to_string())

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

    Matching strategy (in order):
    1. Exact match on ticker (uppercase)
    2. Exact match on normalized name (uppercase, suffixes stripped)
    3. Substring: any mapping key contained in the normalized name

    When sector is missing, we also overwrite country even if already set,
    because fetchers often report the exchange country (e.g. "Germany" for
    Xetra listings) instead of the company's domicile country.
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

        raw_name = str(row.get("name", "")).strip()
        ticker = str(row.get("ticker", "")).strip().upper()
        norm_name = _normalize_holding_name(raw_name)

        # 1. Exact ticker match
        match = STATIC_SECTOR_COUNTRY.get(ticker) if ticker else None

        # 2. Exact normalized-name match
        if match is None and norm_name:
            match = STATIC_SECTOR_COUNTRY.get(norm_name)

        # 3. Substring: any mapping key found inside the normalized name
        if match is None and norm_name and len(norm_name) >= 3:
            # Sort keys longest-first so more specific keys win
            for key in sorted(STATIC_SECTOR_COUNTRY, key=len, reverse=True):
                if len(key) >= 3 and key in norm_name:
                    match = STATIC_SECTOR_COUNTRY[key]
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


def _enrich_from_yfinance(
    df: pd.DataFrame, top_n: int = 50, db_session: Session | None = None,
) -> None:
    """Fill missing sector/country from yfinance for top holdings by weight.

    Handles holdings without tickers by attempting name-based lookups.
    Results are cached in the database to persist across deploys.
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

    # Load persistent cache from DB
    yf_cache = _load_yfinance_cache(db_session)

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

    # Fetch from yfinance (or cache) with time budget
    fetched: dict[str, dict[str, str]] = {}
    cache_dirty = False
    yf_time_budget = 30.0  # max seconds for all yfinance calls
    yf_start = time.time()

    for _, key in unique_lookups:
        upper_key = key.upper()
        if upper_key in fetched:
            continue

        # Check cache first
        if upper_key in yf_cache:
            fetched[upper_key] = yf_cache[upper_key]
            continue

        # Bail out if time budget exceeded
        if time.time() - yf_start > yf_time_budget:
            logger.info(
                "yfinance time budget (%.0fs) exceeded, skipping remaining lookups",
                yf_time_budget,
            )
            break

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
        _save_yfinance_cache(yf_cache, db_session)

    if filled_sector or filled_country:
        logger.info(
            "yfinance enrichment: filled %d sectors, %d countries",
            filled_sector, filled_country,
        )


def _enrich_from_api_ninjas(
    df: pd.DataFrame, top_n: int = 50, db_session: Session | None = None,
) -> None:
    """Fill missing sector/country via API Ninjas for top holdings still unknown.

    Skips silently if API_NINJAS_KEY env var is not set.
    Results are cached in the yfinance_cache DB table (same mechanism).
    Rate limited to 1 request/second.
    """
    api_key = os.environ.get("API_NINJAS_KEY")
    if not api_key:
        return

    missing = df[
        ((df["sector"] == "") | (df["sector"] == "Unknown"))
        | ((df["country"] == "") | (df["country"] == "Unknown"))
    ]
    if missing.empty:
        return

    if "real_weight_pct" in missing.columns:
        missing = missing.nlargest(top_n, "real_weight_pct")

    import requests

    yf_cache = _load_yfinance_cache(db_session)
    filled_sector = 0
    filled_country = 0
    cache_dirty = False

    # Build lookups — only tickers (API Ninjas needs a ticker symbol)
    lookups: list[tuple[int, str]] = []
    seen: set[str] = set()
    for idx, row in missing.iterrows():
        ticker = str(row.get("ticker", "")).strip()
        if ticker and len(ticker) <= 10:
            upper = ticker.upper()
            if upper not in seen:
                seen.add(upper)
            lookups.append((idx, ticker))

    if not lookups:
        return

    fetched: dict[str, dict[str, str]] = {}

    for _, key in lookups:
        upper_key = key.upper()
        if upper_key in fetched:
            continue

        # Check cache first (shared with yfinance cache)
        if upper_key in yf_cache:
            cached = yf_cache[upper_key]
            if cached.get("sector") or cached.get("country"):
                fetched[upper_key] = cached
                continue

        # Fetch from API Ninjas
        try:
            time.sleep(1)  # Rate limit: 1 req/sec
            resp = requests.get(
                f"https://api.api-ninjas.com/v1/stockprice?ticker={key}",
                headers={"X-Api-Key": api_key},
                timeout=5,
            )
            if resp.status_code == 404 or not resp.text.strip():
                yf_cache[upper_key] = {"sector": "", "country": ""}
                cache_dirty = True
                continue
            resp.raise_for_status()
            data = resp.json()

            # API Ninjas stockprice returns a dict or list
            if isinstance(data, list):
                data = data[0] if data else {}

            sector = data.get("sector", "") or ""
            country = data.get("country", "") or ""

            result = {"sector": sector, "country": country}
            fetched[upper_key] = result
            yf_cache[upper_key] = result
            cache_dirty = True
        except Exception as exc:
            logger.debug("API Ninjas lookup failed for %s: %s", key, exc)
            yf_cache[upper_key] = {"sector": "", "country": ""}
            cache_dirty = True
            continue

    # Apply fetched data
    for idx, key in lookups:
        data = fetched.get(key.upper())
        if not data:
            continue

        cur_sector = str(df.at[idx, "sector"]).strip()
        cur_country = str(df.at[idx, "country"]).strip()
        needs_sector = not cur_sector or cur_sector == "Unknown"

        if needs_sector and data.get("sector"):
            df.at[idx, "sector"] = data["sector"]
            filled_sector += 1
        if data.get("country") and (needs_sector or not cur_country or cur_country == "Unknown"):
            df.at[idx, "country"] = data["country"]
            filled_country += 1

    if cache_dirty:
        _save_yfinance_cache(yf_cache, db_session)

    if filled_sector or filled_country:
        logger.info(
            "API Ninjas enrichment: filled %d sectors, %d countries",
            filled_sector, filled_country,
        )
