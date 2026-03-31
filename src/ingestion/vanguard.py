"""Vanguard ETF holdings fetcher.

Supports US-listed Vanguard ETFs via etf-scraper (when the Vanguard API
endpoint is reachable) and recognises UCITS tickers for future support.

Known limitation: etf-scraper's Vanguard backend
(eds.ecs.gisp.c1.vanguard.com) may be unreachable from some networks.
The fetcher wraps calls with retry and clear error messages.
"""

import logging
import time
from datetime import date

import pandas as pd

from src.ingestion.base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
BACKOFF_BASE = 2.0

# Known UCITS tickers — recognised by can_handle but not yet fetchable
UCITS_TICKERS: frozenset[str] = frozenset({
    "VWCE", "VUSA", "VEVE", "VFEM", "VUAA", "VWRL",
    "VHYL", "VNRT", "VGVF", "VVAL", "VMOM",
})

# Asset classes to exclude
_NON_EQUITY_CLASSES = frozenset({
    "Cash", "Money Market", "Cash Collateral and Margins",
    "Futures", "FX Forwards", "Rights/Warrants",
    "Net Other Assets/Cash", "Cash Collateral",
})


class VanguardFetcher(BaseFetcher):
    """Fetcher for Vanguard ETFs.

    Strategy:
    1. US-listed tickers (VOO, VTI, VGT, …) → etf-scraper.
    2. UCITS tickers (VWCE, VUSA, …) → not yet implemented.
    """

    def __init__(self) -> None:
        self._scraper = None
        self._scraper_tickers: set[str] = set()
        self._init_scraper()

    def _init_scraper(self) -> None:
        """Initialise etf-scraper and cache the Vanguard ticker set."""
        try:
            from etf_scraper import ETFScraper

            self._scraper = ETFScraper()
            df = self._scraper.listings_df
            self._scraper_tickers = set(
                df.loc[df["provider"] == "Vanguard", "ticker"].tolist()
            )
            logger.info(
                "etf-scraper loaded: %d Vanguard tickers",
                len(self._scraper_tickers),
            )
        except Exception:
            logger.warning("etf-scraper unavailable — no Vanguard support")

    def can_handle(self, identifier: str) -> bool:
        """Return True if *identifier* is a known Vanguard ticker or an IE-domiciled ISIN.

        Args:
            identifier: ETF ticker or ISIN string.
        """
        ticker = identifier.upper().strip()
        if ticker in self._scraper_tickers or ticker in UCITS_TICKERS:
            return True
        # Accept IE-domiciled ISINs as fallback (could be Vanguard UCITS)
        if len(ticker) == 12 and ticker.startswith("IE") and ticker.isalnum():
            return True
        return False

    def fetch_holdings(
        self, identifier: str, as_of_date: date | None = None
    ) -> pd.DataFrame:
        """Fetch and normalise Vanguard holdings.

        Args:
            identifier: ETF ticker.
            as_of_date: Optional reference date.

        Returns:
            Validated DataFrame conforming to the standard schema.
        """
        ticker = identifier.upper().strip()

        if ticker in self._scraper_tickers:
            df = self._fetch_via_scraper(ticker, as_of_date)
        elif ticker in UCITS_TICKERS:
            # TODO: Vanguard UCITS holdings download.
            # The Vanguard UK/IE sites have aggressive anti-scraping
            # protections (Akamai bot manager, dynamic JS-rendered pages).
            # Potential future approaches:
            #   - fund-docs.vanguard.com CSV if a stable URL is found
            #   - justETF or another aggregator as fallback
            #   - Selenium/Playwright for JS-rendered pages
            raise NotImplementedError(
                f"UCITS Vanguard ETF {ticker!r} is not yet supported. "
                "Only US-listed Vanguard ETFs work via etf-scraper."
            )
        else:
            raise ValueError(f"Cannot handle identifier: {identifier!r}")

        df = self._filter_non_equity(df)
        return self.validate_output(df)

    def _fetch_via_scraper(
        self, ticker: str, as_of_date: date | None
    ) -> pd.DataFrame:
        """Fetch holdings via etf-scraper (US-listed Vanguard).

        Retries with exponential backoff since the Vanguard API endpoint
        (eds.ecs.gisp.c1.vanguard.com) can be unreachable from some networks.

        Args:
            ticker: US-listed Vanguard ticker.
            as_of_date: Optional date.

        Returns:
            Raw DataFrame with column names mapped to standard schema.

        Raises:
            ConnectionError: If the Vanguard API is unreachable after retries.
        """
        assert self._scraper is not None
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
            f"The Vanguard API (eds.ecs.gisp.c1.vanguard.com) may be "
            f"unreachable from this network. Last error: {last_exc}"
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
