"""L&G (Legal & General / LGIM) ETF holdings fetcher.

L&G is a mid-tier European ETF issuer with thematic products
(Cyber Security, Battery Value Chain, Clean Energy, etc.).

Data source:
  - **CSV download** (primary):
    ``https://fundcentres.lgim.com/srp/fund-centre/download/Holdings?isin={ISIN}&lang=en``
  - Note: as of April 2026 this endpoint redirects to fundcentres.landg.com
    and may return 404. When that happens, the orchestrator cascade falls
    through to JustETF top-10 fallback.
"""

from __future__ import annotations

import io
import logging
import time
from datetime import date

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
    "RENW": "IE00BF0M2Z96",   # Clean Energy UCITS ETF (alias)
    "ECOM": "IE00BF0M6N54",   # Ecommerce Logistics UCITS ETF
    "DGTL": "IE00BF0M6N54",   # Digital Payments UCITS ETF (alias)
    "APTS": "IE000RDRMSD1",   # Artificial Intelligence UCITS ETF
    "XMLD": "IE000YYE6WK5",   # Multi-Strategy Enhanced Commodities
}

_ISIN_TO_TICKER: dict[str, str] = {v: k for k, v in LGIM_PRODUCTS.items()}

_LGIM_ISIN_PREFIXES: tuple[str, ...] = ("IE",)

_DOWNLOAD_URL = (
    "https://fundcentres.lgim.com/srp/fund-centre/download/Holdings"
    "?isin={isin}&lang=en"
)

MAX_RETRIES = 2
BACKOFF_BASE = 2.0


class LGIMFetcher(BaseFetcher):
    """Fetcher for L&G (Legal & General) UCITS ETFs.

    Strategy:
    1. Resolve ticker → ISIN via LGIM_PRODUCTS.
    2. Download holdings CSV from fundcentres.lgim.com.
    3. If download fails (404/500), let orchestrator cascade to JustETF.
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

        url = _DOWNLOAD_URL.format(isin=isin)
        logger.info("Fetching LGIM holdings for %s from %s", isin, url)

        resp = self._request_with_retry(url)
        content_type = resp.headers.get("Content-Type", "")

        if "text/csv" in content_type or "application/octet" in content_type:
            df = pd.read_csv(io.StringIO(resp.text))
        elif "spreadsheet" in content_type or "excel" in content_type:
            df = pd.read_excel(io.BytesIO(resp.content), engine="openpyxl")
        else:
            # Try CSV first, fall back to Excel
            try:
                df = pd.read_csv(io.StringIO(resp.text))
            except Exception:
                df = pd.read_excel(io.BytesIO(resp.content), engine="openpyxl")

        df = self._normalise(df, ticker)
        return self.validate_output(df)

    # ------------------------------------------------------------------
    # Private helpers
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

    def _request_with_retry(self, url: str) -> requests.Response:
        """GET with retry, fail-fast on 404."""
        last_exc: Exception | None = None
        for attempt in range(MAX_RETRIES):
            try:
                resp = self._session.get(url, timeout=15, allow_redirects=True)
                resp.raise_for_status()
                return resp
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else 0
                if status in (404, 500):
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

    @staticmethod
    def _normalise(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """Map CSV columns to the standard schema."""
        col_map = {
            "Name": "holding_name",
            "Security Name": "holding_name",
            "Holding Name": "holding_name",
            "ISIN": "holding_isin",
            "Security ISIN": "holding_isin",
            "Ticker": "holding_ticker",
            "Security Ticker": "holding_ticker",
            "Weight": "weight_pct",
            "Weight (%)": "weight_pct",
            "% Weight": "weight_pct",
            "Weighting": "weight_pct",
            "Sector": "sector",
            "Country": "country",
            "Currency": "currency",
            "Market Value": "market_value",
        }

        renamed = {}
        for orig_col in df.columns:
            for pattern, target in col_map.items():
                if orig_col.strip().lower() == pattern.lower():
                    renamed[orig_col] = target
                    break
        df = df.rename(columns=renamed)

        if "weight_pct" in df.columns:
            df["weight_pct"] = pd.to_numeric(df["weight_pct"], errors="coerce")
            # If weights look like decimals (max < 1), convert to percentage
            max_w = df["weight_pct"].max()
            if pd.notna(max_w) and max_w < 1.0:
                df["weight_pct"] = df["weight_pct"] * 100

        df["etf_ticker"] = ticker
        df["as_of_date"] = None
        return df
