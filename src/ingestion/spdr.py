"""SPDR (State Street) ETF holdings fetcher.

SPDR/State Street is the 7th largest ETF issuer in Europe (~2.5% market
share, 80+ ETFs). ETFs are domiciled primarily in IE.

Data source: etf-scraper for US-listed SPDR tickers. UCITS tickers are
recognised but not yet fetchable via etf-scraper (US-only listings).

Known limitation: etf-scraper only covers US-listed tickers (SPY, XLF,
etc.). UCITS equivalents (SPY5, SPPW, etc.) are NOT available via
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
# Known SPDR ETF tickers
# ---------------------------------------------------------------------------

# US-listed tickers available via etf-scraper
US_TICKERS: frozenset[str] = frozenset({
    "SPY", "XLF", "XLK", "XLE", "XLV", "XLI", "XLP",
    "XLY", "XLB", "XLU", "XLRE", "XLC", "GLD", "SDY",
})

# UCITS tickers — recognised but NOT fetchable via etf-scraper
# TODO: find a data source for UCITS SPDR holdings (ssga.com has
# CSV downloads but requires cookies/JS for download links)
UCITS_TICKERS: frozenset[str] = frozenset({
    "SPY5",   # S&P 500 UCITS ETF
    "SPYD",   # S&P US Dividend Aristocrats UCITS ETF
    "SPPW",   # MSCI World UCITS ETF
    "SPYX",   # S&P 500 ESG Leaders UCITS ETF
    "SPPE",   # S&P 500 EUR Hedged UCITS ETF
    "SPYV",   # MSCI ACWI UCITS ETF
    "SPYJ",   # MSCI ACWI IMI UCITS ETF
    "SYBM",   # Bloomberg Barclays Euro Aggregate Bond UCITS ETF
})

ALL_TICKERS: frozenset[str] = US_TICKERS | UCITS_TICKERS

# UCITS ISIN → US-equivalent ticker for proxy fetching via etf-scraper
UCITS_TO_US: dict[str, str] = {
    "IE00B6YX5D40": "SPY",    # SPDR S&P 500 UCITS → SPY
    "IE00BF2B0P08": "SDY",    # SPDR S&P US Dividend Aristocrats UCITS → SDY
    "IE00BFY0GT14": "SPDW",   # SPDR MSCI World UCITS → SPDW
    "IE00BFMXXD54": "SPDW",   # SPDR MSCI ACWI UCITS → SPDW proxy
}

# Reverse: UCITS ticker → US ticker
_UCITS_TICKER_TO_US: dict[str, str] = {
    "SPY5": "SPY",
    "SPYD": "SDY",
    "SPPW": "SPDW",
    "SPYV": "SPDW",
}

# Asset classes to exclude
_NON_EQUITY_CLASSES = frozenset({
    "Cash", "Money Market", "Cash Collateral and Margins",
    "Futures", "FX Forwards", "Rights/Warrants",
    "Net Other Assets/Cash", "Cash Collateral",
})


class SPDRFetcher(BaseFetcher):
    """Fetcher for SPDR (State Street) ETFs via etf-scraper.

    Strategy:
    1. US-listed tickers (SPY, XLF, …) → etf-scraper.
    2. UCITS tickers (SPY5, SPPW, …) → not yet supported.
    """

    def __init__(self) -> None:
        self._scraper = None
        self._scraper_tickers: set[str] = set()
        self._init_scraper()

    def _init_scraper(self) -> None:
        """Initialise etf-scraper and cache the SPDR ticker set."""
        try:
            from etf_scraper import ETFScraper

            self._scraper = ETFScraper()
            df = self._scraper.listings_df
            # etf-scraper lists State Street as "StateStreet"
            self._scraper_tickers = set(
                df.loc[df["provider"] == "StateStreet", "ticker"].tolist()
            )
            logger.info(
                "etf-scraper loaded: %d SPDR tickers",
                len(self._scraper_tickers),
            )
        except Exception as exc:
            logger.warning("etf-scraper unavailable — no SPDR support: %s", exc)

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
        # IE-domiciled ISINs — shared with many issuers
        if len(clean) == 12 and clean.startswith("IE") and clean.isalnum():
            return 0.3
        return 0.2

    def fetch_holdings(
        self, identifier: str, as_of_date: date | None = None
    ) -> pd.DataFrame:
        """Fetch and normalise SPDR holdings.

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
        """Fetch holdings via etf-scraper (US-listed SPDR).

        Args:
            ticker: US-listed SPDR ticker.
            as_of_date: Optional date.

        Returns:
            Raw DataFrame with column names mapped to standard schema.
        """
        assert self._scraper is not None, (
            "etf-scraper is not available — cannot fetch SPDR holdings"
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
