"""Fundamentals provider: fetch and cache security fundamentals via yfinance."""

import logging
import time
from datetime import date, datetime, timedelta

import yfinance as yf
from sqlalchemy.orm import Session

from src.storage.models import FigiMapping, SecurityFundamental

logger = logging.getLogger(__name__)

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

    def __init__(self, session: Session, max_retries: int = 3) -> None:
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
        for attempt in range(1, self.max_retries + 1):
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                if not info or info.get("regularMarketPrice") is None:
                    logger.warning("yfinance: no data for %s (attempt %d)", ticker, attempt)
                    if attempt < self.max_retries:
                        time.sleep(1)
                        continue
                    return None

                result = {}
                for yf_key, db_key in YF_FIELD_MAP.items():
                    val = info.get(yf_key)
                    if val is not None:
                        # debt_to_equity from yfinance is in %, convert to ratio
                        if yf_key == "debtToEquity":
                            val = val / 100.0
                        result[db_key] = float(val)
                    else:
                        result[db_key] = None

                return result
            except Exception:
                logger.warning("yfinance error for %s (attempt %d/%d)",
                               ticker, attempt, self.max_retries, exc_info=True)
                if attempt < self.max_retries:
                    time.sleep(1)
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
        sleep_between: float = 0.5,
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
        for i, item in enumerate(tickers):
            ticker = item["ticker"]
            figi_id = item["figi_id"]
            data = self.fetch(ticker, figi_id)
            if data is not None:
                results[ticker] = data
            if i < len(tickers) - 1:
                time.sleep(sleep_between)
        return results
