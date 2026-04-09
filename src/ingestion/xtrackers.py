"""Xtrackers (DWS) ETF holdings fetcher.

Xtrackers is the 3rd largest ETF issuer in Europe (~10.4% market share,
~336 ETFs). ETFs are domiciled in IE, LU, and DE.

Data sources (discovered via etf.dws.com):
  - **Excel endpoint** (primary): full holdings with ISIN, currency, sector.
    ``https://etf.dws.com/etfdata/export/GBR/ENG/excel/product/constituent/{ISIN}/``
  - **JSON API** (fallback): same data, lighter payload.
    ``https://etf.dws.com/api/pdp/en-gb/etf/{ISIN}/holdings``

Excel format notes:
  - Sheet name is the as-of date (e.g. ``2026-04-01``).
  - Row 1–3: metadata/disclaimer. Row 4: header. Row 5+: data.
  - Columns: Row number, Name, ISIN, Country, Currency, Exchange,
    Type of Security, Rating, Primary Listing, Industry Classification,
    Weighting (decimal, e.g. 0.0514 = 5.14%).
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
# Known Xtrackers tickers → ISIN mapping
# ---------------------------------------------------------------------------
XTRACKERS_PRODUCTS: dict[str, str] = {
    # Global / World
    "XDWD": "IE00BK1PV551",   # MSCI World 1C
    "XDWL": "IE00BL25JN58",   # MSCI World 1D
    "XMWO": "IE00BJ0KDQ92",   # MSCI World Swap 1C
    # US
    "XDPG": "IE00BM67HT60",   # S&P 500 Swap
    "XMUS": "IE00BJ0KDR00",   # MSCI USA Swap 1C
    # Europe
    "XMEU": "LU0274209237",   # MSCI Europe 1C
    "DBXE": "LU0274211217",   # Euro Stoxx 50 1C
    "XESX": "LU0380865021",   # Euro Stoxx 50 Short Daily
    # Emerging Markets
    "XMME": "IE00BTJRMP35",   # MSCI Emerging Markets 1C
    "XDET": "IE00BK5BCD43",   # MSCI EM ESG Leaders
    # ESG
    "XZW0": "IE00BZ02LR44",   # MSCI World ESG 1C
    "XZMU": "IE00BFMNHK08",   # MSCI USA ESG Leaders
    # Thematic
    "XAIX": "IE00BGV5VN51",   # Artificial Intelligence & Big Data
    "XDWH": "IE00BM67HK77",   # MSCI World Health Care
    "XDWT": "IE00BM67HT60",   # MSCI World IT
    # Germany
    "XDAX": "LU0274211480",   # DAX 1C
    # Japan
    "XDJP": "LU0274209740",   # MSCI Japan 1C
}

# Reverse lookup: ISIN → ticker
_ISIN_TO_TICKER: dict[str, str] = {v: k for k, v in XTRACKERS_PRODUCTS.items()}

# ISIN prefixes known to host Xtrackers
_XTRACKERS_ISIN_PREFIXES: tuple[str, ...] = ("IE", "LU", "DE")

# ---------------------------------------------------------------------------
# URL templates
# ---------------------------------------------------------------------------
_EXCEL_URL = (
    "https://etf.dws.com/etfdata/export/GBR/ENG/excel/product/constituent/{isin}/"
)
_JSON_API_URL = (
    "https://etf.dws.com/api/pdp/en-gb/etf/{isin}/holdings"
)

MAX_RETRIES = 3
BACKOFF_BASE = 2.0

# Asset classes to exclude from equity holdings
_NON_EQUITY_CLASSES = frozenset({
    "Cash",
    "Money Market",
    "Cash and/or Derivatives",
    "Cash Collateral",
    "Futures",
    "FX Forwards",
    "Swap",
})


def _retry_request(
    session: requests.Session,
    url: str,
    timeout: int = 30,
    stream: bool = False,
) -> requests.Response:
    """GET with exponential backoff retry.

    Args:
        session: Requests session.
        url: Target URL.
        timeout: Request timeout in seconds.
        stream: Whether to stream the response.

    Returns:
        Response object.

    Raises:
        requests.HTTPError: After all retries exhausted.
    """
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=timeout, stream=stream)
            resp.raise_for_status()
            return resp
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            if status in (404, 500):
                raise  # 404/500 from DWS = not available, no retry
            last_exc = exc
            wait = BACKOFF_BASE ** attempt
            logger.warning(
                "Attempt %d/%d failed for %s: %s — retrying in %.1fs",
                attempt + 1, MAX_RETRIES, url, exc, wait,
            )
            time.sleep(wait)
        except requests.RequestException as exc:
            last_exc = exc
            wait = BACKOFF_BASE ** attempt
            logger.warning(
                "Attempt %d/%d failed for %s: %s — retrying in %.1fs",
                attempt + 1, MAX_RETRIES, url, exc, wait,
            )
            time.sleep(wait)
    raise last_exc  # type: ignore[misc]


class XtrackersFetcher(BaseFetcher):
    """Fetcher for Xtrackers (DWS) ETFs.

    Strategy:
    1. Resolve ticker → ISIN via ``XTRACKERS_PRODUCTS``.
    2. Download holdings Excel from etf.dws.com.
    3. Fall back to JSON API if Excel fails.
    """

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers["User-Agent"] = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

    # ------------------------------------------------------------------
    # BaseFetcher interface
    # ------------------------------------------------------------------

    def can_handle(self, identifier: str) -> float:
        """Return confidence score (0.0–1.0) for handling *identifier*.

        Args:
            identifier: ETF ticker or ISIN string.

        Returns:
            0.95 for known Xtrackers ISINs/tickers, 0.8 for DE-domiciled
            ISINs, 0.5 for LU-domiciled ISINs, 0.3 for unknown tickers.
        """
        clean = identifier.upper().strip()
        if not clean:
            return 0.0

        # Known ticker or known ISIN
        if clean in XTRACKERS_PRODUCTS or clean in _ISIN_TO_TICKER:
            return 0.95

        # ISIN-shaped (12 alnum chars)
        if len(clean) == 12 and clean.isalnum():
            if clean.startswith("DE"):
                return 0.8
            if clean.startswith("LU"):
                return 0.5
            # IE ISINs could be iShares or Xtrackers — moderate confidence
            if clean.startswith("IE"):
                return 0.4

        # Unknown ticker — low confidence speculative attempt
        return 0.3

    def fetch_holdings(
        self, identifier: str, as_of_date: date | None = None
    ) -> pd.DataFrame:
        """Fetch and normalise Xtrackers holdings.

        Args:
            identifier: ETF ticker or ISIN.
            as_of_date: Optional reference date (ignored — always returns latest).

        Returns:
            Validated DataFrame conforming to the standard schema.
        """
        isin = self._resolve_isin(identifier)
        ticker = self._resolve_ticker(identifier)

        # Try Excel first, then JSON API as fallback
        try:
            df = self._fetch_excel(isin, ticker)
        except Exception as excel_exc:
            logger.warning(
                "Excel download failed for %s, trying JSON API: %s",
                isin, excel_exc,
            )
            df = self._fetch_json(isin, ticker)

        df = self._filter_non_equity(df)
        return self.validate_output(df)

    # ------------------------------------------------------------------
    # Private helpers — identifier resolution
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_isin(identifier: str) -> str:
        """Resolve *identifier* to an ISIN.

        Args:
            identifier: Ticker or ISIN.

        Returns:
            12-character ISIN string.
        """
        clean = identifier.upper().strip()
        if clean in XTRACKERS_PRODUCTS:
            return XTRACKERS_PRODUCTS[clean]
        # Assume it's already an ISIN
        return clean

    @staticmethod
    def _resolve_ticker(identifier: str) -> str:
        """Resolve *identifier* to a ticker for the etf_ticker column.

        Args:
            identifier: Ticker or ISIN.

        Returns:
            Ticker string (original identifier if no mapping found).
        """
        clean = identifier.upper().strip()
        if clean in XTRACKERS_PRODUCTS:
            return clean
        if clean in _ISIN_TO_TICKER:
            return _ISIN_TO_TICKER[clean]
        return clean

    # ------------------------------------------------------------------
    # Private helpers — Excel download
    # ------------------------------------------------------------------

    def _fetch_excel(self, isin: str, ticker: str) -> pd.DataFrame:
        """Download and parse holdings Excel from etf.dws.com.

        Args:
            isin: 12-character ISIN.
            ticker: ETF ticker for the ``etf_ticker`` column.

        Returns:
            DataFrame mapped to standard schema columns.
        """
        url = _EXCEL_URL.format(isin=isin)
        logger.info("Fetching Xtrackers Excel for %s from %s", isin, url)
        resp = _retry_request(self._session, url)

        # Parse Excel — header is on row 4 (0-indexed: 3), data starts row 5
        df = pd.read_excel(
            io.BytesIO(resp.content),
            header=3,
            engine="openpyxl",
        )

        # Extract as_of_date from sheet name
        wb_sheets = pd.ExcelFile(io.BytesIO(resp.content), engine="openpyxl").sheet_names
        as_of_date_str = wb_sheets[0] if wb_sheets else None

        return self._normalise_excel(df, ticker, as_of_date_str)

    @staticmethod
    def _normalise_excel(
        df: pd.DataFrame, ticker: str, as_of_date_str: str | None
    ) -> pd.DataFrame:
        """Map Excel columns to the standard schema.

        Args:
            df: Raw DataFrame from ``pd.read_excel``.
            ticker: ETF ticker.
            as_of_date_str: As-of date string from the sheet name.

        Returns:
            DataFrame with standard column names.
        """
        col_map = {
            "Name": "holding_name",
            "ISIN": "holding_isin",
            "Country": "country",
            "Currency": "currency",
            "Industry Classification": "sector",
            "Weighting": "weight_pct",
            "Type of Security": "asset_class",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

        # Weighting is a decimal (0.0514 = 5.14%) — convert to percentage
        if "weight_pct" in df.columns:
            df["weight_pct"] = pd.to_numeric(df["weight_pct"], errors="coerce") * 100

        df["etf_ticker"] = ticker
        df["as_of_date"] = as_of_date_str
        return df

    # ------------------------------------------------------------------
    # Private helpers — JSON API fallback
    # ------------------------------------------------------------------

    def _fetch_json(self, isin: str, ticker: str) -> pd.DataFrame:
        """Fetch holdings via DWS JSON API as fallback.

        Args:
            isin: 12-character ISIN.
            ticker: ETF ticker for the ``etf_ticker`` column.

        Returns:
            DataFrame mapped to standard schema columns.
        """
        url = _JSON_API_URL.format(isin=isin)
        logger.info("Fetching Xtrackers JSON for %s from %s", isin, url)
        resp = _retry_request(self._session, url)
        data = resp.json()

        rows = data.get("body", [])
        if not rows:
            return pd.DataFrame(columns=["etf_ticker"])

        records = []
        for row in rows:
            records.append({
                "holding_isin": row.get("header", {}).get("value"),
                "holding_name": row.get("column_0", {}).get("value"),
                "weight_pct": row.get("column_1", {}).get("sortValue"),
                "market_value": row.get("column_2", {}).get("sortValue"),
                "country": row.get("column_3", {}).get("value"),
                "sector": row.get("column_4", {}).get("value"),
                "asset_class": row.get("column_5", {}).get("value"),
            })

        df = pd.DataFrame(records)

        # Extract as_of_date from response metadata
        as_of_date_str = data.get("asOfDate") or data.get("date")

        df["etf_ticker"] = ticker
        df["as_of_date"] = as_of_date_str
        return df

    # ------------------------------------------------------------------
    # Private helpers — filtering
    # ------------------------------------------------------------------

    @staticmethod
    def _filter_non_equity(df: pd.DataFrame) -> pd.DataFrame:
        """Remove non-equity rows (cash, derivatives, swaps).

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
