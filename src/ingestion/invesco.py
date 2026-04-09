"""Invesco ETF holdings fetcher.

Invesco is the 6th largest ETF issuer in Europe (~3% market share,
130+ ETFs). ETFs are domiciled primarily in IE.

Data source: etf-scraper for US-listed Invesco tickers. UCITS tickers
are recognised but not yet fetchable via etf-scraper (US-only listings).

Known limitation: etf-scraper only covers US-listed tickers (QQQ, SPLG,
etc.). UCITS equivalents (EQQQ, SC0K, etc.) are NOT available via
etf-scraper and will return status="failed".
"""

from __future__ import annotations

import logging
import time
from datetime import date

import pandas as pd

from src.ingestion.base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
BACKOFF_BASE = 2.0

# ---------------------------------------------------------------------------
# Known Invesco ETF tickers
# ---------------------------------------------------------------------------

# US-listed tickers available via etf-scraper
US_TICKERS: frozenset[str] = frozenset({
    "QQQ", "QQQM", "RSP", "SPLG", "PHO", "PBW", "PRFZ",
    "PGX", "BKLN", "SPHD", "RDVY",
})

# UCITS tickers — recognised but NOT fetchable via etf-scraper
# TODO: find a data source for UCITS Invesco holdings (invesco.com
# renders holdings client-side via JS, no public API found yet)
UCITS_TICKERS: frozenset[str] = frozenset({
    "EQQQ",   # Nasdaq-100 UCITS ETF
    "SC0K",   # S&P 500 Equal Weight UCITS ETF
    "RQFI",   # Euro Corporate Bond UCITS ETF
    "MXWO",   # MSCI World UCITS ETF
    "PSWD",   # MSCI World UCITS ETF (EUR Hedged)
    "SXRV",   # S&P 500 UCITS ETF
    "SMEA",   # STOXX Europe 600 UCITS ETF
    "MXUS",   # S&P 500 UCITS ETF Acc
})

ALL_TICKERS: frozenset[str] = US_TICKERS | UCITS_TICKERS

# UCITS ISIN → US-equivalent ticker for proxy fetching via etf-scraper
UCITS_TO_US: dict[str, str] = {
    "IE00B60SX394": "SPLG",   # Invesco S&P 500 UCITS → SPLG (S&P 500)
    "IE00BQYABZ44": "QQQ",    # Invesco Nasdaq-100 UCITS → QQQ
    "IE00B3YCGJ38": "RSP",    # Invesco S&P 500 Equal Weight UCITS → RSP
    "IE00B6R52143": "RSP",    # Invesco MSCI Europe Equal Weight → RSP proxy
}

# Reverse: UCITS ticker → US ticker
_UCITS_TICKER_TO_US: dict[str, str] = {
    "EQQQ": "QQQ",
    "SC0K": "RSP",
    "SXRV": "SPLG",
    "MXUS": "SPLG",
}

# Asset classes to exclude
_NON_EQUITY_CLASSES = frozenset({
    "Cash", "Money Market", "Cash Collateral and Margins",
    "Futures", "FX Forwards", "Rights/Warrants",
    "Net Other Assets/Cash", "Cash Collateral",
})


class InvescoFetcher(BaseFetcher):
    """Fetcher for Invesco ETFs via etf-scraper.

    Strategy:
    1. US-listed tickers (QQQ, RSP, …) → etf-scraper.
    2. UCITS tickers (EQQQ, SC0K, …) → not yet supported.
    """

    def __init__(self) -> None:
        self._scraper = None
        self._scraper_tickers: set[str] = set()
        self._init_scraper()

    def _init_scraper(self) -> None:
        """Initialise etf-scraper and cache the Invesco ticker set."""
        try:
            from etf_scraper import ETFScraper

            self._scraper = ETFScraper()
            df = self._scraper.listings_df
            self._scraper_tickers = set(
                df.loc[df["provider"] == "Invesco", "ticker"].tolist()
            )
            logger.info(
                "etf-scraper loaded: %d Invesco tickers",
                len(self._scraper_tickers),
            )
        except Exception:
            logger.warning("etf-scraper unavailable — no Invesco support")

    # ------------------------------------------------------------------
    # BaseFetcher interface
    # ------------------------------------------------------------------

    def can_handle(self, identifier: str) -> float:
        """Return confidence score (0.0–1.0) for handling *identifier*.

        Args:
            identifier: ETF ticker or ISIN string.
        """
        clean = identifier.upper().strip()
        if not clean:
            return 0.0
        if clean in self._scraper_tickers or clean in ALL_TICKERS:
            return 0.9
        if clean in UCITS_TO_US:
            return 0.9
        # IE-domiciled ISINs — shared with iShares/Vanguard/Xtrackers
        if len(clean) == 12 and clean.startswith("IE") and clean.isalnum():
            return 0.3
        return 0.2

    def fetch_holdings(
        self, identifier: str, as_of_date: date | None = None
    ) -> pd.DataFrame:
        """Fetch and normalise Invesco holdings.

        Args:
            identifier: ETF ticker.
            as_of_date: Optional reference date.

        Returns:
            Validated DataFrame conforming to the standard schema.

        Raises:
            NotImplementedError: For UCITS tickers not in etf-scraper.
            ConnectionError: If etf-scraper API is unreachable.
        """
        ticker = identifier.upper().strip()

        # Check UCITS→US proxy mapping (ISIN or ticker)
        us_proxy = UCITS_TO_US.get(ticker) or _UCITS_TICKER_TO_US.get(ticker)

        if us_proxy and self._scraper:
            logger.info("Proxying UCITS %s via US ticker %s", ticker, us_proxy)
            df = self._fetch_via_scraper(us_proxy, as_of_date)
            df["etf_ticker"] = ticker  # Label with original UCITS identifier
        elif ticker in self._scraper_tickers:
            df = self._fetch_via_scraper(ticker, as_of_date)
        else:
            # Try etf-scraper anyway — it may know tickers we don't
            df = self._fetch_via_scraper(ticker, as_of_date)

        df = self._filter_non_equity(df)
        return self.validate_output(df)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch_via_scraper(
        self, ticker: str, as_of_date: date | None
    ) -> pd.DataFrame:
        """Fetch holdings via etf-scraper (US-listed Invesco).

        Args:
            ticker: US-listed Invesco ticker.
            as_of_date: Optional date.

        Returns:
            Raw DataFrame with column names mapped to standard schema.
        """
        assert self._scraper is not None, (
            "etf-scraper is not available — cannot fetch Invesco holdings"
        )
        last_exc: Exception | None = None
        for attempt in range(MAX_RETRIES):
            try:
                logger.info("Fetching %s via etf-scraper (attempt %d)", ticker, attempt + 1)
                raw = self._scraper.query_holdings(ticker, holdings_date=as_of_date)
                return self._normalise_scraper(raw, ticker)
            except Exception as exc:
                last_exc = exc
                wait = BACKOFF_BASE ** attempt
                logger.warning(
                    "Attempt %d/%d for %s failed: %s — retrying in %.1fs",
                    attempt + 1, MAX_RETRIES, ticker, exc, wait,
                )
                time.sleep(wait)
        raise ConnectionError(
            f"Failed to fetch {ticker} after {MAX_RETRIES} attempts. "
            f"Last error: {last_exc}"
        )

    @staticmethod
    def _normalise_scraper(raw: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """Map etf-scraper columns to the standard schema.

        Args:
            raw: DataFrame from ``ETFScraper.query_holdings``.
            ticker: ETF ticker.

        Returns:
            DataFrame with standard column names.
        """
        df = raw.copy()
        col_map = {
            "ticker": "holding_ticker",
            "name": "holding_name",
            "sector": "sector",
            "asset_class": "asset_class",
            "market_value": "market_value",
            "weight": "weight_pct",
            "amount": "shares",
            "location": "country",
            "currency": "currency",
            "fund_ticker": "etf_ticker",
            "as_of_date": "as_of_date",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        df["etf_ticker"] = ticker
        return df

    @staticmethod
    def _filter_non_equity(df: pd.DataFrame) -> pd.DataFrame:
        """Remove non-equity rows (cash, derivatives, futures).

        Args:
            df: Holdings DataFrame with optional ``asset_class`` column.

        Returns:
            Filtered DataFrame.
        """
        if "asset_class" not in df.columns:
            return df
        mask = ~df["asset_class"].astype(str).str.strip().isin(_NON_EQUITY_CLASSES)
        filtered = df.loc[mask].copy()
        logger.info(
            "Filtered %d non-equity rows (%d remaining)",
            len(df) - len(filtered),
            len(filtered),
        )
        return filtered
