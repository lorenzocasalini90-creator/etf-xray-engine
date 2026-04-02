"""Enrich aggregated holdings with missing sector and country data.

Sources (in priority order):
1. Cross-reference from other holdings in the same portfolio
2. Exchange code → country mapping (from figi_mapping table)
3. yfinance (top 50 holdings by weight only, to avoid rate limits)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

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

    # Step 3: yfinance for top holdings still missing data
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


def _enrich_from_yfinance(df: pd.DataFrame, top_n: int = 50) -> None:
    """Fill missing sector/country from yfinance for top holdings by weight."""
    missing = df[
        ((df["sector"] == "") | (df["sector"] == "Unknown"))
        | ((df["country"] == "") | (df["country"] == "Unknown"))
    ]

    if missing.empty:
        return

    # Only process top N by weight to avoid rate limits
    if "real_weight_pct" in missing.columns:
        missing = missing.nlargest(top_n, "real_weight_pct")

    tickers_to_fetch = missing["ticker"].dropna().unique().tolist()
    tickers_to_fetch = [t for t in tickers_to_fetch if t.strip() and len(t) <= 10]

    if not tickers_to_fetch:
        return

    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not installed, skipping yfinance enrichment")
        return

    filled_sector = 0
    filled_country = 0

    for ticker in tickers_to_fetch:
        try:
            info = yf.Ticker(ticker).info
            yf_sector = info.get("sector", "")
            yf_country = info.get("country", "")

            mask = df["ticker"].str.upper() == ticker.upper()
            rows = df[mask]

            for idx in rows.index:
                cur_sector = str(df.at[idx, "sector"]).strip()
                cur_country = str(df.at[idx, "country"]).strip()

                if (not cur_sector or cur_sector == "Unknown") and yf_sector:
                    df.at[idx, "sector"] = yf_sector
                    filled_sector += 1
                if (not cur_country or cur_country == "Unknown") and yf_country:
                    df.at[idx, "country"] = yf_country
                    filled_country += 1
        except Exception:
            continue

    if filled_sector or filled_country:
        logger.info(
            "yfinance enrichment: filled %d sectors, %d countries",
            filled_sector, filled_country,
        )
