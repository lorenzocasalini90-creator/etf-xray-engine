"""iShares ETF holdings fetcher.

Supports both US-listed iShares (via etf-scraper) and UCITS iShares
(via direct CSV download from ishares.com).
"""

import csv
import io
import logging
import time
from datetime import date, datetime

import pandas as pd
import requests

from src.ingestion.base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Known UCITS ETF product mappings: ticker -> (product_id, slug)
# ---------------------------------------------------------------------------
UCITS_PRODUCTS: dict[str, tuple[int, str]] = {
    "CSPX": (253743, "ishares-core-sp-500-ucits-etf"),
    "IWDA": (251882, "ishares-msci-world-ucits-etf"),
    "SWDA": (251882, "ishares-msci-world-ucits-etf"),
    "EIMI": (264659, "ishares-core-msci-emerging-markets-imi-ucits-etf"),
    "ISAC": (251850, "ishares-msci-acwi-ucits-etf"),
    "IEMA": (251858, "ishares-msci-em-ucits-etf"),
    "CSNDX": (253741, "ishares-nasdaq-100-ucits-etf"),
    "IUIT": (280510, "ishares-sp-500-information-technology-sector-ucits-etf"),
    "SEMB": (251824, "ishares-jp-morgan-em-local-govt-bond-ucits-etf"),
    "IEAC": (251726, "ishares-core-euro-corporate-bond-ucits-etf"),
}

# iShares.com CSV config token (site-wide, stable)
_AJAX_TIMESTAMP = "1506575576011"
_ISHARES_BASE = "https://www.ishares.com/uk/individual/en/products"

# Asset classes to exclude from equity holdings
_NON_EQUITY_CLASSES = frozenset({
    "Cash",
    "Money Market",
    "Cash Collateral and Margins",
    "Futures",
    "FX Forwards",
    "Rights/Warrants",
    "Net Other Assets/Cash",
    "Cash Collateral",
})

MAX_RETRIES = 3
BACKOFF_BASE = 2.0


def _retry_request(
    session: requests.Session, url: str, timeout: int = 30
) -> requests.Response:
    """GET with exponential backoff retry.

    Args:
        session: Requests session.
        url: Target URL.
        timeout: Request timeout in seconds.

    Returns:
        Response object.

    Raises:
        requests.HTTPError: After all retries exhausted.
    """
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp
        except (requests.RequestException, requests.HTTPError) as exc:
            last_exc = exc
            wait = BACKOFF_BASE ** attempt
            logger.warning(
                "Attempt %d/%d failed for %s: %s — retrying in %.1fs",
                attempt + 1, MAX_RETRIES, url, exc, wait,
            )
            time.sleep(wait)
    raise last_exc  # type: ignore[misc]


class ISharesFetcher(BaseFetcher):
    """Fetcher for iShares ETFs (US-listed and UCITS).

    Strategy:
    1. For US-listed tickers available in etf-scraper → use etf-scraper.
    2. For UCITS tickers in ``UCITS_PRODUCTS`` → scrape CSV from ishares.com.
    """

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers["User-Agent"] = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        # Lazy-load etf-scraper listings
        self._scraper = None
        self._scraper_tickers: set[str] = set()
        self._init_scraper()

    def _init_scraper(self) -> None:
        """Initialise etf-scraper and cache the iShares ticker set."""
        try:
            from etf_scraper import ETFScraper

            self._scraper = ETFScraper()
            df = self._scraper.listings_df
            self._scraper_tickers = set(
                df.loc[df["provider"] == "IShares", "ticker"].tolist()
            )
            logger.info(
                "etf-scraper loaded: %d iShares tickers", len(self._scraper_tickers)
            )
        except Exception:
            logger.warning("etf-scraper unavailable — UCITS-only mode")

    # ------------------------------------------------------------------
    # BaseFetcher interface
    # ------------------------------------------------------------------

    def can_handle(self, identifier: str) -> bool:
        """Return True if *identifier* is a known iShares ticker or an IE-domiciled ISIN.

        Args:
            identifier: ETF ticker or ISIN string.
        """
        ticker = identifier.upper().strip()
        if ticker in self._scraper_tickers or ticker in UCITS_PRODUCTS:
            return True
        # Accept any Irish-domiciled ISIN (IE prefix, 12 chars) — most are iShares UCITS
        if len(ticker) == 12 and ticker.startswith("IE") and ticker.isalnum():
            return True
        return False

    def fetch_holdings(
        self, identifier: str, as_of_date: date | None = None
    ) -> pd.DataFrame:
        """Fetch and normalise iShares holdings.

        Args:
            identifier: ETF ticker.
            as_of_date: Optional reference date.

        Returns:
            Validated DataFrame conforming to the standard schema.
        """
        ticker = identifier.upper().strip()

        if ticker in UCITS_PRODUCTS:
            df = self._fetch_ucits(ticker, as_of_date)
        elif ticker in self._scraper_tickers:
            df = self._fetch_via_scraper(ticker, as_of_date)
        elif len(ticker) == 12 and ticker.startswith("IE") and ticker.isalnum():
            # Try to resolve ISIN by searching UCITS_PRODUCTS by known patterns,
            # otherwise attempt a direct iShares.com download by ISIN
            df = self._fetch_by_isin(ticker, as_of_date)
        else:
            raise ValueError(f"Cannot handle identifier: {identifier!r}")

        df = self._filter_non_equity(df)
        return self.validate_output(df)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch_via_scraper(
        self, ticker: str, as_of_date: date | None
    ) -> pd.DataFrame:
        """Fetch holdings via etf-scraper (US-listed iShares).

        Args:
            ticker: US-listed iShares ticker.
            as_of_date: Optional date.

        Returns:
            Raw DataFrame with column names mapped to standard schema.
        """
        assert self._scraper is not None
        logger.info("Fetching %s via etf-scraper", ticker)
        raw = self._scraper.query_holdings(ticker, holdings_date=as_of_date)
        return self._normalise_scraper(raw, ticker)

    def _fetch_by_isin(
        self, isin: str, as_of_date: date | None
    ) -> pd.DataFrame:
        """Try to fetch holdings for an arbitrary ISIN from iShares.com.

        Uses the iShares search/product page pattern. If it fails,
        raises a clear error.

        Args:
            isin: 12-character ISIN (e.g. IE00BK5BCD43).
            as_of_date: Optional reference date.

        Returns:
            DataFrame with standard schema columns.

        Raises:
            ValueError: If the ISIN cannot be fetched from iShares.
        """
        # Try searching iShares.com for the ISIN via the screener API
        search_url = (
            f"https://www.ishares.com/uk/individual/en/search#/"
            f"q={isin}&type=fund"
        )
        logger.info("Attempting iShares ISIN lookup for %s", isin)

        # Use the product search JSON endpoint
        api_url = (
            f"https://www.ishares.com/uk/individual/en/products/"
            f"{_AJAX_TIMESTAMP}.ajax?fileType=csv&dataType=fund"
            f"&isin={isin}"
        )
        try:
            resp = _retry_request(self._session, api_url)
            df = self._parse_ucits_csv(resp.text, isin)
            if df.empty:
                raise ValueError(
                    f"iShares returned empty data for ISIN {isin}. "
                    f"This ISIN may not be an iShares product."
                )
            return df
        except Exception as exc:
            raise ValueError(
                f"Could not fetch holdings for ISIN {isin} from iShares.com. "
                f"This ISIN may not be an iShares product or the product page "
                f"format is different. Error: {exc}"
            ) from exc

    def _fetch_ucits(
        self, ticker: str, as_of_date: date | None
    ) -> pd.DataFrame:
        """Fetch holdings CSV from ishares.com (UCITS ETFs).

        Args:
            ticker: UCITS iShares ticker.
            as_of_date: Ignored (always returns latest available).

        Returns:
            Raw DataFrame with column names mapped to standard schema.
        """
        product_id, slug = UCITS_PRODUCTS[ticker]
        url = (
            f"{_ISHARES_BASE}/{product_id}/{slug}/"
            f"{_AJAX_TIMESTAMP}.ajax?fileType=csv&dataType=fund"
        )
        logger.info("Fetching %s from %s", ticker, url)
        resp = _retry_request(self._session, url)
        return self._parse_ucits_csv(resp.text, ticker)

    def _parse_ucits_csv(self, text: str, ticker: str) -> pd.DataFrame:
        """Parse the iShares UCITS CSV format.

        The CSV has metadata rows before the actual header:
        - Row 0: ``Fund Holdings as of,"27/Mar/2026"``
        - Row 1: blank
        - Row 2: column headers
        - Row 3+: data

        Args:
            text: Raw CSV text from iShares.
            ticker: ETF ticker for the ``etf_ticker`` column.

        Returns:
            DataFrame mapped to standard schema columns.
        """
        lines = text.strip().splitlines()

        # Extract as_of_date from first row
        as_of_date_str = None
        if lines and "," in lines[0]:
            raw_date = lines[0].split(",", 1)[1].strip().strip('"')
            try:
                as_of_date_str = datetime.strptime(raw_date, "%d/%b/%Y").strftime(
                    "%Y-%m-%d"
                )
            except ValueError:
                logger.warning("Could not parse as_of_date from header: %r", raw_date)

        # Find the header row (first row with "Ticker" and "Name")
        header_idx = 2
        for i, line in enumerate(lines):
            if "Ticker" in line and "Name" in line:
                header_idx = i
                break

        reader = csv.DictReader(io.StringIO("\n".join(lines[header_idx:])))
        rows = list(reader)

        if not rows:
            return pd.DataFrame(columns=["etf_ticker"])

        df = pd.DataFrame(rows)

        col_map = {
            "Ticker": "holding_ticker",
            "Name": "holding_name",
            "Sector": "sector",
            "Asset Class": "asset_class",
            "Market Value": "market_value",
            "Weight (%)": "weight_pct",
            "Shares": "shares",
            "Location": "country",
            "Market Currency": "currency",
            "ISIN": "holding_isin",
            "SEDOL": "holding_sedol",
            "CUSIP": "holding_cusip",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

        df["etf_ticker"] = ticker
        df["as_of_date"] = as_of_date_str

        # Convert numeric columns
        for col in ("weight_pct", "market_value", "shares"):
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(",", "", regex=False)
                    .str.strip()
                )
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

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
