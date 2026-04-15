"""Amundi ETF holdings fetcher.

Amundi is the 2nd largest ETF issuer in Europe (~12.5% market share,
~336 UCITS ETFs). ETFs are domiciled in LU and FR.

Data source: POST API on ``www.amundietf.fr/mapi/ProductAPI/getProductsData``
which returns full holdings composition (no authentication required).

Note: Many Amundi ETFs (e.g. CW8) use **swap replication**. The holdings
returned by the API are the **substitute basket** (physical collateral),
not the full index constituents. This is documented in each FetchResult.
"""

from __future__ import annotations

import logging
import time
from datetime import date

import pandas as pd
import requests

from src.ingestion.base_fetcher import BaseFetcher, FetchResult

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Known Amundi ETF tickers → ISIN mapping
# ---------------------------------------------------------------------------
AMUNDI_PRODUCTS: dict[str, str] = {
    # MSCI World variants
    "CW8": "LU1681043599",
    "MWRD": "LU1437016972",
    "LCWD": "LU1681043672",
    # Emerging Markets
    "PAEEM": "LU1681045370",
    "AEEM": "LU1681045453",
    # US / S&P 500
    "PANX": "LU1681038326",
    "PE500": "LU1681048804",
    "500U": "LU1681049018",
    # Europe
    "PCEU": "LU1681042609",
    "MEUD": "LU2572257124",
    "CE8": "LU1681042781",
    # Thematic / Sector
    "ANEW": "LU1681041544",
    "CL2": "LU0252634307",
    "LQQ": "LU1681038243",
    # Prime (low-cost range)
    "PRIA": "LU2089238203",
    "PRIJ": "LU2090063673",
    "PRIM": "LU2089238039",
    "PRIE": "LU2089238112",
    # ESG
    "LWCR": "LU1681043599",
}

# Reverse lookup: ISIN → ticker (first match)
_ISIN_TO_TICKER: dict[str, str] = {}
for _t, _i in AMUNDI_PRODUCTS.items():
    if _i not in _ISIN_TO_TICKER:
        _ISIN_TO_TICKER[_i] = _t

# ISIN prefix patterns characteristic of Amundi
AMUNDI_ISIN_PREFIXES: tuple[str, ...] = ("LU1681", "LU1829", "LU2090", "LU2089", "LU2572", "LU1437", "LU0252", "FR0010")

# ---------------------------------------------------------------------------
# API configuration
# ---------------------------------------------------------------------------
_API_URL = "https://www.amundietf.fr/mapi/ProductAPI/getProductsData"

_API_CONTEXT = {
    "countryCode": "FRA",
    "languageCode": "fr",
    "userProfileName": "INSTIT",
}

_COMPOSITION_FIELDS = [
    "date", "type", "bbg", "isin", "name", "weight",
    "quantity", "currency", "sector", "country", "countryOfRisk",
]

MAX_RETRIES = 3
BACKOFF_BASE = 2.0

# Asset classes to exclude
_NON_EQUITY_TYPES = frozenset({
    "CASH",
    "FX_FORWARD",
    "FUTURES",
    "SWAP",
    "MONEY_MARKET",
})


class AmundiFetcher(BaseFetcher):
    """Fetcher for Amundi ETFs via the amundietf.fr API.

    Strategy:
    1. Resolve ticker → ISIN via ``AMUNDI_PRODUCTS``.
    2. POST to ``/mapi/ProductAPI/getProductsData`` with composition fields.
    3. Parse holdings from JSON response.

    Note: Swap-based ETFs return the substitute basket, not index constituents.
    """

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    # ------------------------------------------------------------------
    # BaseFetcher interface
    # ------------------------------------------------------------------

    def can_handle(self, identifier: str) -> float:
        """Return confidence score (0.0–1.0) for handling *identifier*.

        Args:
            identifier: ETF ticker or ISIN string.

        Returns:
            0.95 for known Amundi ISINs/tickers, 0.85 for FR-domiciled ISINs,
            0.6 for LU-domiciled ISINs (shared with Xtrackers), 0.3 for unknown.
        """
        clean = identifier.upper().strip()
        if not clean:
            return 0.0

        # Known ticker or known ISIN
        if clean in AMUNDI_PRODUCTS or clean in _ISIN_TO_TICKER:
            return 0.95

        # Known Amundi ISIN prefixes
        if any(clean.startswith(p) for p in AMUNDI_ISIN_PREFIXES):
            return 0.95

        # ISIN-shaped (12 alnum chars)
        if len(clean) == 12 and clean.isalnum():
            if clean.startswith("FR"):
                return 0.85
            if clean.startswith("LU"):
                return 0.6

        # Unknown ticker — low confidence
        return 0.3

    def fetch_holdings(
        self, identifier: str, as_of_date: date | None = None
    ) -> pd.DataFrame:
        """Fetch and normalise Amundi holdings via the mapi API.

        Args:
            identifier: ETF ticker or ISIN.
            as_of_date: Optional reference date (ignored — API returns latest).

        Returns:
            Validated DataFrame conforming to the standard schema.
        """
        isin = self._resolve_isin(identifier)
        ticker = self._resolve_ticker(identifier)

        data = self._fetch_api(isin)
        df = self._parse_holdings(data, ticker)
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
        if clean in AMUNDI_PRODUCTS:
            return AMUNDI_PRODUCTS[clean]
        return clean

    @staticmethod
    def _resolve_ticker(identifier: str) -> str:
        """Resolve *identifier* to a ticker for the etf_ticker column.

        Args:
            identifier: Ticker or ISIN.

        Returns:
            Ticker string.
        """
        clean = identifier.upper().strip()
        if clean in AMUNDI_PRODUCTS:
            return clean
        if clean in _ISIN_TO_TICKER:
            return _ISIN_TO_TICKER[clean]
        return clean

    # ------------------------------------------------------------------
    # Private helpers — API
    # ------------------------------------------------------------------

    def _fetch_api(self, isin: str) -> dict:
        """POST to the Amundi mapi API and return the product data.

        Args:
            isin: 12-character ISIN.

        Returns:
            Product dict from the API response.

        Raises:
            ValueError: If the API returns no products or an error.
        """
        payload = {
            "characteristics": ["ISIN", "SHARE_MARKETING_NAME", "TICKER",
                                "FUND_REPLICATION_METHODOLOGY"],
            "metrics": [],
            "context": _API_CONTEXT,
            "productType": "ALL",
            "productIds": [isin],
            "composition": {
                "compositionFields": _COMPOSITION_FIELDS,
            },
        }

        last_exc: Exception | None = None
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(
                    "Fetching Amundi holdings for %s (attempt %d/%d)",
                    isin, attempt + 1, MAX_RETRIES,
                )
                resp = self._session.post(_API_URL, json=payload, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                products = data.get("products", [])
                if not products:
                    raise ValueError(
                        f"Amundi API returned no products for ISIN {isin}. "
                        f"This ISIN may not be an Amundi ETF."
                    )
                return products[0]
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    raise  # 404 = wrong ETF, no retry
                last_exc = exc
                wait = BACKOFF_BASE ** attempt
                logger.warning(
                    "Attempt %d/%d failed for %s: %s — retrying in %.1fs",
                    attempt + 1, MAX_RETRIES, isin, exc, wait,
                )
                time.sleep(wait)
            except (requests.RequestException, ValueError) as exc:
                last_exc = exc
                wait = BACKOFF_BASE ** attempt
                logger.warning(
                    "Attempt %d/%d failed for %s: %s — retrying in %.1fs",
                    attempt + 1, MAX_RETRIES, isin, exc, wait,
                )
                time.sleep(wait)

        raise last_exc  # type: ignore[misc]

    @staticmethod
    def _parse_holdings(product: dict, ticker: str) -> pd.DataFrame:
        """Parse holdings from the API product response.

        Args:
            product: Single product dict from the API.
            ticker: ETF ticker for the ``etf_ticker`` column.

        Returns:
            DataFrame mapped to standard schema columns.
        """
        composition = product.get("composition", {})
        items = composition.get("compositionData", [])

        if not items:
            return pd.DataFrame(columns=["etf_ticker"])

        records = []
        for item in items:
            chars = item.get("compositionCharacteristics", {})
            weight_raw = chars.get("weight")
            # API returns weight as decimal (0.0514 = 5.14%)
            weight_pct = weight_raw * 100 if weight_raw is not None else None

            records.append({
                "holding_name": chars.get("name"),
                "holding_isin": chars.get("isin"),
                "holding_ticker": chars.get("bbg"),
                "weight_pct": weight_pct,
                "shares": chars.get("quantity"),
                "currency": chars.get("currency"),
                "sector": chars.get("sector"),
                "country": chars.get("countryOfRisk"),
                "as_of_date": chars.get("date"),
                "asset_class": chars.get("type"),
            })

        df = pd.DataFrame(records)
        df["etf_ticker"] = ticker
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
        mask = ~df["asset_class"].astype(str).str.strip().str.upper().isin(_NON_EQUITY_TYPES)
        filtered = df.loc[mask].copy()
        logger.info(
            "Filtered %d non-equity rows (%d remaining)",
            len(df) - len(filtered),
            len(filtered),
        )
        return filtered
