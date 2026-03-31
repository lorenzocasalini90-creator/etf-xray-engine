"""Amundi ETF holdings fetcher.

Amundi ETFs are UCITS-only (domiciled in LU/FR). etf-scraper does not
cover Amundi. The amundietf.com site does not expose a stable public
REST API for holdings data — tested endpoints return 404.

Current status: can_handle recognises Amundi tickers/ISINs.
fetch_holdings raises NotImplementedError pending a working data source.

Potential future approaches:
  - Monitor amundietf.com for new API endpoints after site redesign
  - Parse the factsheet PDF (monthly, top-10 only)
  - Use a third-party aggregator (justETF, Morningstar) as data source
"""

import logging
from datetime import date

import pandas as pd

from src.ingestion.base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Known Amundi ETF tickers and ISIN prefixes
# ---------------------------------------------------------------------------
AMUNDI_TICKERS: frozenset[str] = frozenset({
    # MSCI World variants
    "CW8", "MWRD", "LCWD",
    # Emerging Markets
    "PAEEM", "AEEM",
    # US / S&P 500
    "PANX", "PE500", "500U",
    # Europe
    "PCEU", "CE8",
    # Thematic / Sector
    "ANEW", "CL2", "LQQ",
    # Prime (low-cost range)
    "PRIA", "PRIJ", "PRIM", "PRIE",
    # ESG
    "LWCR",
})

# Amundi ISINs start with LU or FR
AMUNDI_ISIN_PREFIXES: tuple[str, ...] = ("LU1681", "LU2090", "LU1437", "FR0010")


class AmundiFetcher(BaseFetcher):
    """Fetcher for Amundi ETFs.

    Recognises Amundi tickers and LU/FR ISINs. Holdings download is
    not yet implemented — amundietf.com does not expose a stable API.
    """

    def can_handle(self, identifier: str) -> bool:
        """Return True if *identifier* is a known Amundi ticker or ISIN.

        Args:
            identifier: ETF ticker or ISIN string.
        """
        clean = identifier.upper().strip()
        if clean in AMUNDI_TICKERS:
            return True
        return any(clean.startswith(p) for p in AMUNDI_ISIN_PREFIXES)

    def fetch_holdings(
        self, identifier: str, as_of_date: date | None = None
    ) -> pd.DataFrame:
        """Fetch Amundi ETF holdings.

        Args:
            identifier: ETF ticker or ISIN.
            as_of_date: Optional reference date.

        Raises:
            NotImplementedError: Always — no working data source yet.
        """
        # TODO: Amundi holdings download.
        # amundietf.com does not expose a stable public REST API.
        # All tested endpoints (api/fund/composition, direct product
        # pages, CSV download) return 404 as of 2026-03.
        # The site appears to render holdings client-side via JS.
        raise NotImplementedError(
            f"Amundi ETF {identifier!r} is recognised but holdings download "
            "is not yet implemented. amundietf.com does not expose a stable "
            "public API for holdings data."
        )
