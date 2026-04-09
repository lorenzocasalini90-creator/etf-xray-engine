"""L&G (Legal & General / LGIM) ETF holdings fetcher.

L&G is a mid-tier European ETF issuer with thematic products
(Cyber Security, Battery Value Chain, Clean Energy, etc.).

Data source:
  - **CSV download** (primary):
    ``https://fundcentres.landg.com/srp/api/fund-holdings-csv-download/{fund_id}/?as_at_date={YYYY-MM-DD}``
  - The ``fund_id`` is a numeric ID specific to L&G's internal system.
    Mapped statically for known ETFs; resolved dynamically for unknowns
    via the ETF page at ``fundcentres.landg.com/srp/fund-centre/ETF/``.

CSV columns (as of April 2026):
  ETF Name, ISIN, Security Description, Security Type, Broad Type,
  Currency Code, Price, Cash Value CCY, Collateral Value CCY, Percentage
"""

from __future__ import annotations

import io
import logging
import re
import time
from datetime import date, timedelta

import pandas as pd
import requests

from src.ingestion.base_fetcher import BaseFetcher, FetchResult

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Known L&G UCITS ETFs (ticker → ISIN)
# ---------------------------------------------------------------------------
LGIM_PRODUCTS: dict[str, str] = {
    "ISPY": "IE00BYPLS672",   # Cyber Security UCITS ETF
    "BATT": "IE00BF0M2Z96",   # Battery Value-Chain UCITS ETF
    "ECOM": "IE00BF0M6N54",   # Ecommerce Logistics UCITS ETF
    "APTS": "IE000RDRMSD1",   # Artificial Intelligence UCITS ETF
    "XMLD": "IE000YYE6WK5",   # Multi-Strategy Enhanced Commodities
}

_ISIN_TO_TICKER: dict[str, str] = {v: k for k, v in LGIM_PRODUCTS.items()}

# Static ISIN → fund_id mapping (from L&G fund centre)
_ISIN_TO_FUND_ID: dict[str, int] = {
    "IE00BYPLS672": 228,   # Cyber Security
    "IE00BF0M2Z96": 229,   # Battery Value-Chain
    "IE00BF0M6N54": 231,   # Ecommerce Logistics
    "IE000RDRMSD1": 1653,  # Artificial Intelligence
    "IE000YYE6WK5": 1821,  # Multi-Strategy Enhanced Commodities
}

_CSV_URL = (
    "https://fundcentres.landg.com/srp/api/fund-holdings-csv-download"
    "/{fund_id}/?as_at_date={as_at_date}"
)

_ETF_PAGE_URL = (
    "https://fundcentres.landg.com/srp/fund-centre/ETF/{isin}"
)

MAX_RETRIES = 2
BACKOFF_BASE = 2.0


class LGIMFetcher(BaseFetcher):
    """Fetcher for L&G (Legal & General) UCITS ETFs.

    Strategy:
    1. Resolve ticker → ISIN via LGIM_PRODUCTS.
    2. Resolve ISIN → fund_id via static map or ETF page scrape.
    3. Download holdings CSV from fundcentres.landg.com.
    4. If download fails (404/500), let orchestrator cascade to JustETF.
    """

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers["User-Agent"] = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

    def can_handle(self, identifier: str) -> float:
        """Return confidence score for handling *identifier*."""
        clean = identifier.upper().strip()
        if not clean:
            return 0.0

        if clean in LGIM_PRODUCTS or clean in _ISIN_TO_TICKER:
            return 0.9

        if len(clean) == 12 and clean.isalnum() and clean.startswith("IE"):
            return 0.3

        return 0.0

    def fetch_holdings(
        self, identifier: str, as_of_date: date | None = None,
    ) -> pd.DataFrame:
        """Fetch and normalise L&G holdings CSV."""
        isin = self._resolve_isin(identifier)
        ticker = self._resolve_ticker(identifier)

        fund_id = self._resolve_fund_id(isin)
        if fund_id is None:
            raise ValueError(f"Cannot resolve L&G fund_id for ISIN {isin}")

        # Try recent month-end dates if no specific date given
        dates_to_try = self._candidate_dates(as_of_date)

        last_exc: Exception | None = None
        for try_date in dates_to_try:
            url = _CSV_URL.format(fund_id=fund_id, as_at_date=try_date)
            logger.info("Fetching LGIM CSV for %s (fund_id=%d, date=%s)", isin, fund_id, try_date)
            try:
                resp = self._request_with_retry(url)
                # Check for JSON error response (API returns 404 JSON, not HTML)
                if resp.headers.get("Content-Type", "").startswith("application/json"):
                    data = resp.json()
                    if "detail" in data:
                        logger.debug("LGIM API: %s for date %s", data["detail"], try_date)
                        continue
                df = pd.read_csv(io.StringIO(resp.text))
                if df.empty:
                    continue
                df = self._normalise(df, ticker)
                return self.validate_output(df)
            except requests.HTTPError as exc:
                last_exc = exc
                # 404 means data not available for this date — try next
                if exc.response is not None and exc.response.status_code == 404:
                    continue
                raise
            except Exception as exc:
                last_exc = exc
                continue

        if last_exc:
            raise last_exc
        raise ValueError(f"No holdings data found for {isin} (fund_id={fund_id})")

    # ------------------------------------------------------------------
    # Private helpers — identifier resolution
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_isin(identifier: str) -> str:
        clean = identifier.upper().strip()
        if clean in LGIM_PRODUCTS:
            return LGIM_PRODUCTS[clean]
        return clean

    @staticmethod
    def _resolve_ticker(identifier: str) -> str:
        clean = identifier.upper().strip()
        if clean in LGIM_PRODUCTS:
            return clean
        if clean in _ISIN_TO_TICKER:
            return _ISIN_TO_TICKER[clean]
        return clean

    def _resolve_fund_id(self, isin: str) -> int | None:
        """Resolve ISIN to L&G fund_id — static map first, then ETF page."""
        fund_id = _ISIN_TO_FUND_ID.get(isin)
        if fund_id is not None:
            return fund_id

        # Try to scrape fund_id from the ETF page
        try:
            url = _ETF_PAGE_URL.format(isin=isin)
            resp = self._session.get(url, timeout=10, allow_redirects=True)
            resp.raise_for_status()
            # Look for fund_id in page content or redirect URL
            match = re.search(r"fund[_-]?id[\"'\s:=]+(\d+)", resp.text)
            if match:
                return int(match.group(1))
            # Check for numeric ID in the final URL path
            match = re.search(r"/(\d+)/?$", resp.url)
            if match:
                return int(match.group(1))
        except Exception as exc:
            logger.debug("Failed to resolve fund_id for %s: %s", isin, exc)

        return None

    @staticmethod
    def _candidate_dates(as_of_date: date | None) -> list[str]:
        """Generate candidate month-end dates to try for the CSV download."""
        if as_of_date:
            return [as_of_date.strftime("%Y-%m-%d")]

        today = date.today()
        dates = []
        # Try last 6 month-ends
        for months_back in range(0, 6):
            d = today.replace(day=1) - timedelta(days=1 + 30 * months_back)
            # Snap to last day of month
            if months_back > 0:
                d = d.replace(day=1) - timedelta(days=1)
            dates.append(d.strftime("%Y-%m-%d"))
        return dates

    # ------------------------------------------------------------------
    # Private helpers — HTTP
    # ------------------------------------------------------------------

    def _request_with_retry(self, url: str) -> requests.Response:
        """GET with retry, fail-fast on 404/500."""
        last_exc: Exception | None = None
        for attempt in range(MAX_RETRIES):
            try:
                resp = self._session.get(url, timeout=15, allow_redirects=True)
                resp.raise_for_status()
                return resp
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else 0
                if status_code in (404, 500):
                    raise  # Not available — no retry
                last_exc = exc
                wait = BACKOFF_BASE ** attempt
                logger.warning(
                    "LGIM attempt %d/%d failed for %s: %s — retrying in %.1fs",
                    attempt + 1, MAX_RETRIES, url, exc, wait,
                )
                time.sleep(wait)
            except requests.RequestException as exc:
                last_exc = exc
                wait = BACKOFF_BASE ** attempt
                logger.warning(
                    "LGIM attempt %d/%d failed for %s: %s — retrying in %.1fs",
                    attempt + 1, MAX_RETRIES, url, exc, wait,
                )
                time.sleep(wait)
        raise last_exc  # type: ignore[misc]

    # ------------------------------------------------------------------
    # Private helpers — normalisation
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """Map L&G CSV columns to the standard schema.

        Expected columns from L&G:
        ETF Name, ISIN, Security Description, Security Type, Broad Type,
        Currency Code, Price, Cash Value CCY, Collateral Value CCY, Percentage
        """
        col_map = {
            "security description": "holding_name",
            "isin": "holding_isin",
            "percentage": "weight_pct",
            "currency code": "currency",
            "security type": "sector",
            "broad type": "asset_class",
            # Fallback names (in case format varies)
            "name": "holding_name",
            "security name": "holding_name",
            "holding name": "holding_name",
            "ticker": "holding_ticker",
            "weight": "weight_pct",
            "weight (%)": "weight_pct",
            "% weight": "weight_pct",
            "weighting": "weight_pct",
            "sector": "sector",
            "country": "country",
            "market value": "market_value",
            "cash value ccy": "market_value",
        }

        renamed = {}
        for orig_col in df.columns:
            key = orig_col.strip().lower()
            if key in col_map and col_map[key] not in renamed.values():
                renamed[orig_col] = col_map[key]
        df = df.rename(columns=renamed)

        if "weight_pct" in df.columns:
            df["weight_pct"] = pd.to_numeric(df["weight_pct"], errors="coerce")
            # If weights look like decimals (max < 1), convert to percentage
            max_w = df["weight_pct"].max()
            if pd.notna(max_w) and 0 < max_w < 1.0:
                df["weight_pct"] = df["weight_pct"] * 100

        # Filter out non-equity rows if asset_class is available
        if "asset_class" in df.columns:
            non_equity = {"Cash", "Futures", "FX Forwards", "Swap", "Money Market"}
            mask = ~df["asset_class"].astype(str).str.strip().isin(non_equity)
            df = df.loc[mask].copy()

        df["etf_ticker"] = ticker
        df["as_of_date"] = None
        return df
