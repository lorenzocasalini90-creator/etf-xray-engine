"""Fundamentals provider: fetch and cache security fundamentals via yfinance."""

import logging
import re
import time
from datetime import date, datetime, timedelta

import yfinance as yf
from sqlalchemy.orm import Session

from src.storage.models import FigiMapping, SecurityFundamental

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Bloomberg → Yahoo Finance ticker conversion
# ---------------------------------------------------------------------------

BLOOMBERG_TO_YAHOO: dict[str, str] = {
    "SIE GY": "SIE.DE", "SAP GY": "SAP.DE", "ALV GY": "ALV.DE",
    "DTE GY": "DTE.DE", "BAS GY": "BAS.DE", "BAYN GY": "BAYN.DE",
    "MBG GY": "MBG.DE", "BMW GY": "BMW.DE", "MUV2 GY": "MUV2.DE",
    "NVDA UW": "NVDA", "MSFT UW": "MSFT", "AAPL UW": "AAPL",
    "AMZN UW": "AMZN", "GOOGL UW": "GOOGL", "GOOG UW": "GOOG",
    "META UW": "META", "AVGO UW": "AVGO", "TSLA UW": "TSLA",
    "JPM UN": "JPM", "V UN": "V", "MA UN": "MA", "UNH UN": "UNH",
    "JNJ UN": "JNJ", "PG UN": "PG", "HD UN": "HD", "TSM UN": "TSM",
    "MC FP": "MC.PA", "OR FP": "OR.PA", "SAN FP": "SAN.PA",
    "AI FP": "AI.PA", "BNP FP": "BNP.PA", "SU FP": "SU.PA",
    "ASML NA": "ASML.AS", "RDSA NA": "SHELL.AS", "INGA NA": "INGA.AS",
    "ROG SW": "ROG.SW", "NESN SW": "NESN.SW", "NOVN SW": "NOVN.SW",
    "AZN LN": "AZN.L", "HSBA LN": "HSBA.L", "SHEL LN": "SHEL.L",
    "ULVR LN": "ULVR.L", "RIO LN": "RIO.L", "BP/ LN": "BP.L",
    "7203 JT": "7203.T", "6758 JT": "6758.T", "9984 JT": "9984.T",
}

_VALID_TICKER_RE = re.compile(r"^[A-Za-z0-9.\-^/]+$")


def normalize_ticker_for_yfinance(ticker: str) -> str:
    """Convert Bloomberg-format ticker to Yahoo Finance format."""
    if not ticker:
        return ticker
    clean = ticker.strip()
    return BLOOMBERG_TO_YAHOO.get(clean, clean)


def is_valid_yfinance_ticker(ticker: str) -> bool:
    """Return False for tickers yfinance cannot resolve (Bloomberg format etc)."""
    if not ticker or not isinstance(ticker, str):
        return False
    t = ticker.strip()
    if not t or len(t) > 20:
        return False
    # Bloomberg tickers contain a space (e.g. "SIE GY")
    if " " in t:
        return False
    return bool(_VALID_TICKER_RE.match(t))

# Cache validity: 7 days
CACHE_TTL_DAYS = 7

# yfinance field mapping → our DB fields
YF_FIELD_MAP = {
    "trailingPE": "pe_ratio",
    "priceToBook": "pb_ratio",
    "returnOnEquity": "roe",
    "debtToEquity": "debt_to_equity",
    "dividendYield": "dividend_yield",
    "marketCap": "market_cap",
}


class FundamentalsProvider:
    """Fetch and cache security fundamentals from yfinance.

    Checks DB cache first (7-day TTL). Falls back to yfinance API.
    Saves results with data_source='L2'.

    Args:
        session: SQLAlchemy session for DB access.
        max_retries: Max retries per ticker on yfinance failure.
    """

    def __init__(self, session: Session, max_retries: int = 1) -> None:
        self.session = session
        self.max_retries = max_retries

    def _get_cached(self, figi_id: int) -> dict | None:
        """Return cached fundamentals if fresh (< 7 days), else None."""
        row = (
            self.session.query(SecurityFundamental)
            .filter(SecurityFundamental.figi_id == figi_id)
            .order_by(SecurityFundamental.updated_at.desc())
            .first()
        )
        if row is None:
            return None
        cutoff = datetime.utcnow() - timedelta(days=CACHE_TTL_DAYS)
        if row.updated_at < cutoff:
            return None
        return {
            "pe_ratio": row.pe_ratio,
            "pb_ratio": row.pb_ratio,
            "roe": row.roe,
            "debt_to_equity": row.debt_to_equity,
            "dividend_yield": row.dividend_yield,
            "market_cap": row.market_cap,
        }

    def _save_to_db(self, figi_id: int, data: dict) -> None:
        """Upsert fundamentals into security_fundamentals table."""
        existing = (
            self.session.query(SecurityFundamental)
            .filter(SecurityFundamental.figi_id == figi_id)
            .first()
        )
        now = datetime.utcnow()
        if existing:
            for field, value in data.items():
                setattr(existing, field, value)
            existing.updated_at = now
            existing.data_source = "L2"
            existing.as_of_date = date.today()
        else:
            row = SecurityFundamental(
                figi_id=figi_id,
                pe_ratio=data.get("pe_ratio"),
                pb_ratio=data.get("pb_ratio"),
                roe=data.get("roe"),
                debt_to_equity=data.get("debt_to_equity"),
                dividend_yield=data.get("dividend_yield"),
                market_cap=data.get("market_cap"),
                data_source="L2",
                as_of_date=date.today(),
                updated_at=now,
            )
            self.session.add(row)
        self.session.flush()

    def _fetch_from_yfinance(self, ticker: str) -> dict | None:
        """Fetch fundamentals from yfinance. Returns dict or None on failure."""
        # Normalize Bloomberg → Yahoo format, then validate
        ticker = normalize_ticker_for_yfinance(ticker)
        if not is_valid_yfinance_ticker(ticker):
            logger.debug("Skipping invalid yfinance ticker: %s", ticker)
            return None

        for attempt in range(1, self.max_retries + 1):
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                if not info or info.get("regularMarketPrice") is None:
                    logger.debug("yfinance: no data for %s", ticker)
                    return None

                result = {}
                for yf_key, db_key in YF_FIELD_MAP.items():
                    val = info.get(yf_key)
                    if val is not None:
                        if yf_key == "debtToEquity":
                            val = val / 100.0
                        result[db_key] = float(val)
                    else:
                        result[db_key] = None

                return result
            except Exception:
                logger.warning("yfinance error for %s (attempt %d/%d)",
                               ticker, attempt, self.max_retries)
                if attempt < self.max_retries:
                    time.sleep(0.5)
        return None

    def fetch(self, ticker: str, figi_id: int) -> dict | None:
        """Fetch fundamentals for a single ticker.

        Checks DB cache first. If stale or missing, fetches from yfinance.

        Args:
            ticker: Stock ticker symbol.
            figi_id: FK to figi_mapping.id for DB storage.

        Returns:
            Dict with pe_ratio, pb_ratio, roe, debt_to_equity,
            dividend_yield, market_cap — or None if unavailable.
        """
        cached = self._get_cached(figi_id)
        if cached is not None:
            logger.debug("Cache hit for %s (figi_id=%d)", ticker, figi_id)
            return cached

        data = self._fetch_from_yfinance(ticker)
        if data is None:
            return None

        self._save_to_db(figi_id, data)
        return data

    def fetch_batch(
        self,
        tickers: list[dict],
        sleep_between: float = 0.2,
    ) -> dict[str, dict]:
        """Fetch fundamentals for multiple tickers with rate limiting.

        Args:
            tickers: List of dicts with keys 'ticker' and 'figi_id'.
            sleep_between: Seconds to sleep between yfinance calls.

        Returns:
            Dict mapping ticker → fundamentals dict. Tickers with no data
            are omitted from the result.
        """
        results: dict[str, dict] = {}
        yf_calls = 0
        for item in tickers:
            ticker = item["ticker"]
            figi_id = item["figi_id"]
            data = self.fetch(ticker, figi_id)
            if data is not None:
                results[ticker] = data
            # Only sleep between actual yfinance API calls (not cache hits)
            normalized = normalize_ticker_for_yfinance(ticker)
            if is_valid_yfinance_ticker(normalized):
                yf_calls += 1
                if yf_calls < len(tickers):
                    time.sleep(sleep_between)
        return results
