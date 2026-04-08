"""UBS ETF holdings fetcher (stub — site not accessible).

UBS is the 5th largest ETF issuer in Europe (~3-4% market share, 200+ UCITS).
ETFs are domiciled in IE and LU.

Data source status (explored 8 April 2026):
  - ubs.com uses **Akamai WAF** (bot detection blocks direct requests) and
    **Adobe Experience Manager** (SPA — holdings loaded via XHR post-render).
  - No public API endpoint discovered. Page source contains no inline holdings
    data or download links.
  - ``etf-scraper`` library does not support UBS.

Current strategy:
  - ``can_handle()`` identifies UBS ETFs with high confidence via known
    ISINs/tickers, so the orchestrator can log the issuer correctly.
  - ``fetch_holdings()`` raises ``NotImplementedError`` so the orchestrator
    falls through to **JustETF fallback** (top ~10 holdings, partial coverage).

TODO: If a UBS API or holdings download URL is discovered (e.g. via browser
DevTools Network tab inspection), implement ``fetch_holdings()`` here.
Candidate patterns to investigate:
  - AEM content fragments: ``/content/dam/ubs/.../*.json``
  - Fund data API: ``/api/fund-data/...``
  - Third-party provider (FE fundinfo / Morningstar) used by UBS
"""

from __future__ import annotations

import logging
from datetime import date

import pandas as pd

from src.ingestion.base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Known UBS UCITS products: ticker → ISIN
# UBS ETFs often lack simple exchange tickers; users typically use ISIN.
# Where exchange tickers exist (e.g. Borsa Italiana), they're mapped here.
# ---------------------------------------------------------------------------
UBS_PRODUCTS: dict[str, str] = {
    # MSCI World
    "WRDA": "IE00B7KQ7B66",   # UBS MSCI World UCITS ETF USD A-dis
    "WRDU": "IE00BD4TXV59",   # UBS MSCI World UCITS ETF USD A-acc
    # S&P 500
    "SP5U": "IE00BD4TXS21",   # UBS S&P 500 UCITS ETF USD A-dis
    "SP5A": "IE00BD4TXT67",   # UBS S&P 500 UCITS ETF USD A-acc
    # MSCI EMU
    "EMUL": "LU0147308422",   # UBS MSCI EMU UCITS ETF EUR A-dis
    "EMUA": "LU0950668870",   # UBS MSCI EMU UCITS ETF EUR A-acc
    # MSCI ACWI
    "ACWU": "IE00BYM11H29",   # UBS MSCI ACWI UCITS ETF USD A-acc
    # MSCI EM
    "EMMU": "LU0480132876",   # UBS MSCI Emerging Markets UCITS ETF USD A-dis
    # MSCI Japan
    "JPNU": "LU0136240974",   # UBS MSCI Japan UCITS ETF JPY A-dis
    # MSCI Europe
    "EURU": "LU0446734526",   # UBS MSCI Europe UCITS ETF EUR A-dis
    # SRI / ESG
    "WSRI": "IE00BK72HJ67",   # UBS MSCI World SRI UCITS ETF USD A-acc
    "SSRI": "IE00BKT6FV49",   # UBS S&P 500 ESG UCITS ETF USD A-acc
    "ESRI": "LU1280300853",   # UBS MSCI EMU SRI UCITS ETF EUR A-dis
    # MSCI USA
    "USAU": "IE00BD4TXY86",   # UBS MSCI USA UCITS ETF USD A-dis
    # MSCI Pacific
    "PACU": "LU0464839820",   # UBS MSCI Pacific ex Japan UCITS ETF
    # MSCI Canada
    "CANU": "LU0446734104",   # UBS MSCI Canada UCITS ETF CAD A-dis
}

# Reverse lookup: ISIN → ticker
_ISIN_TO_TICKER: dict[str, str] = {v: k for k, v in UBS_PRODUCTS.items()}

# All known UBS ISINs
_KNOWN_ISINS: frozenset[str] = frozenset(_ISIN_TO_TICKER.keys())


class UBSFetcher(BaseFetcher):
    """Fetcher for UBS UCITS ETFs.

    Currently a stub — UBS's website (Akamai WAF + AEM SPA) does not
    expose a public holdings API. The orchestrator falls through to
    JustETF for partial holdings.

    This fetcher exists so that:
    1. ``can_handle()`` correctly identifies UBS ETFs in the registry.
    2. The issuer mapping in the orchestrator routes to this fetcher.
    3. When a UBS API is discovered, only ``fetch_holdings()`` needs updating.
    """

    # ------------------------------------------------------------------
    # BaseFetcher interface
    # ------------------------------------------------------------------

    def can_handle(self, identifier: str) -> float:
        """Return confidence score (0.0–1.0) for handling *identifier*.

        Args:
            identifier: ETF ticker or ISIN string.

        Returns:
            0.95 for known UBS tickers/ISINs, 0.4 for IE/LU ISINs
            (shared with many issuers), 0.2 for unknown tickers.
        """
        clean = identifier.upper().strip()
        if not clean:
            return 0.0

        # Known UBS ticker or ISIN
        if clean in UBS_PRODUCTS or clean in _KNOWN_ISINS:
            return 0.95

        # ISIN-shaped (12 alnum chars) — IE and LU are shared
        if len(clean) == 12 and clean.isalnum():
            if clean.startswith(("IE", "LU")):
                return 0.4

        # Unknown ticker
        return 0.2

    def fetch_holdings(
        self, identifier: str, as_of_date: date | None = None
    ) -> pd.DataFrame:
        """Fetch UBS ETF holdings.

        Currently raises ``NotImplementedError`` — UBS's website does not
        expose a public holdings API. The orchestrator will fall through
        to JustETF fallback for partial holdings.

        Args:
            identifier: ETF ticker or ISIN.
            as_of_date: Optional reference date.

        Raises:
            NotImplementedError: Always — no UBS API available yet.
        """
        # TODO: Implement when a UBS holdings API or download URL is discovered.
        # Candidates to investigate (via browser DevTools Network tab):
        #   - AEM content fragments under /content/dam/ubs/
        #   - Third-party fund data provider (FE fundinfo, Morningstar)
        #   - Direct Excel/CSV download from the fund page
        raise NotImplementedError(
            f"UBS fetcher not yet implemented for {identifier}. "
            "UBS website uses Akamai WAF + AEM SPA with no public API. "
            "JustETF fallback will provide partial holdings."
        )

    # ------------------------------------------------------------------
    # Helper — identifier resolution (ready for when fetch is implemented)
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
        if clean in UBS_PRODUCTS:
            return UBS_PRODUCTS[clean]
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
        if clean in UBS_PRODUCTS:
            return clean
        if clean in _ISIN_TO_TICKER:
            return _ISIN_TO_TICKER[clean]
        return clean
