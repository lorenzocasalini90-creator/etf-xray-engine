"""VanEck ETF holdings fetcher.

VanEck is a major ETF issuer with ~40 UCITS ETFs popular among EU retail
investors (DFNS Defense, SMH/VVSM Semiconductor, NUCL Nuclear, TDIV Dividend,
GDXJ Gold Miners). ETFs are domiciled in IE and NL.

Data source (discovered via vaneck.com):
  - **Holdings API** (two-step):
    1. Scrape ``/uk/en/{slug}/holdings/`` to extract ``blockId`` and ``pageId``
       from the ``<ve-holdingsblock>`` web component.
    2. Call ``/Main/HoldingsBlock/GetDataset/?blockId=...&pageId=...&ticker=...``
       which returns JSON with full holdings including ISIN, CUSIP, FIGI,
       sector, country, weight, market value, and shares.

  Requires cookies (site redirects to /corp/en/disabled-cookies/ without them).

JSON response format::

    {
      "AsOfDate": "2026-04-06T00:00:00",
      "Holdings": [
        {
          "Label": "NVDA",           # holding ticker
          "HoldingName": "Nvidia Corp",
          "ISIN": "US67066G1040",
          "CUSIP": "67066G104",
          "FIGI": "BBG000BBJQV0",
          "Weight": "19.36",         # percentage string
          "MV": "8,415,615,950",     # market value with commas
          "Shares": "47,374,555",    # shares with commas
          "Sector": "Information Technology",
          "Country": "United States",
          "CurrencyCode": "USD",
          "AsOfDate": "04/06/2026 00:00:00",
          "Ticker": "SMH"            # ETF ticker
        }, ...
      ]
    }
"""

from __future__ import annotations

import logging
import re
import time
from datetime import date

import pandas as pd
import requests

from src.ingestion.base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Known VanEck UCITS products: ticker → (ISIN, URL slug)
# ---------------------------------------------------------------------------
VANECK_PRODUCTS: dict[str, tuple[str, str]] = {
    # Semiconductor
    "SMH":  ("IE00BMC38736", "semiconductor-etf"),
    "VVSM": ("IE00BMC38736", "semiconductor-etf"),
    # Defense
    "DFNS": ("IE000YYE6WK5", "defense-etf"),
    # Nuclear
    "NUCL": ("IE000M7V94E1", "uranium-nuclear-etf"),
    # Dividends
    "TDIV": ("NL0011683594", "sustainable-world-equal-weight-etf"),
    "VDIV": ("NL0011683594", "sustainable-world-equal-weight-etf"),
    # Gold Miners
    "GDX":  ("IE00BQQP9F84", "gold-miners-etf"),
    "GDXJ": ("IE00BQQP9G91", "junior-gold-miners-etf"),
    # Wide Moat
    "MOAT": ("IE00BQQP9H09", "morningstar-wide-moat-etf"),
    # ESG
    "ESGU": ("IE00BFNM3P36", "sustainable-world-equal-weight-etf"),
    # Crypto / Digital Assets
    "DAPP": ("IE00BMDKNW35", "crypto-and-blockchain-innovators-etf"),
    # Rare Earth / Strategic Metals
    "REMX": ("IE0002PG6CA6", "rare-earth-and-strategic-metals-etf"),
    # Video Gaming & eSports
    "ESPO": ("IE00BYWQWR46", "video-gaming-and-esports-etf"),
    # Hydrogen Economy
    "HDRO": ("IE00BMDH1538", "hydrogen-economy-etf"),
}

# ISIN → ticker reverse lookup
_ISIN_TO_TICKER: dict[str, str] = {}
for _t, (_isin, _slug) in VANECK_PRODUCTS.items():
    if _isin not in _ISIN_TO_TICKER:
        _ISIN_TO_TICKER[_isin] = _t

# ISIN → slug lookup (for when user passes ISIN directly)
_ISIN_TO_SLUG: dict[str, str] = {isin: slug for _, (isin, slug) in VANECK_PRODUCTS.items()}

# All known VanEck ISINs
_KNOWN_ISINS: frozenset[str] = frozenset(_ISIN_TO_TICKER.keys())

# ---------------------------------------------------------------------------
# URL templates
# ---------------------------------------------------------------------------
_HOLDINGS_PAGE_URL = "https://www.vaneck.com/uk/en/{slug}/holdings/"
_DATASET_API_URL = (
    "https://www.vaneck.com/Main/HoldingsBlock/GetDataset/"
    "?blockId={block_id}&pageId={page_id}&ticker={ticker}"
)

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

MAX_RETRIES = 3
BACKOFF_BASE = 2.0

# Regex to extract blockId and pageId from <ve-holdingsblock> web component
_BLOCK_RE = re.compile(
    r'<ve-holdingsblock[^>]*'
    r'data-blockid="(\d+)"[^>]*'
    r'data-pageid="(\d+)"',
    re.IGNORECASE,
)

# Non-equity asset types to filter out
_NON_EQUITY_LABELS = frozenset({
    "Cash",
    "Cash and/or Derivatives",
    "Cash Collateral",
    "Futures",
    "FX Forwards",
    "Swap",
    "Money Market",
})


def _retry_request(
    session: requests.Session,
    url: str,
    timeout: int = 30,
) -> requests.Response:
    """GET with exponential backoff retry.

    Args:
        session: Requests session (must have cookies enabled).
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
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                raise  # 404 = wrong ETF, no retry
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


class VanEckFetcher(BaseFetcher):
    """Fetcher for VanEck UCITS ETFs.

    Strategy:
    1. Resolve ticker → (ISIN, URL slug) via ``VANECK_PRODUCTS``.
    2. Scrape the holdings page to extract ``blockId`` and ``pageId``.
    3. Call the Holdings Dataset API to get full JSON holdings.
    """

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers["User-Agent"] = _USER_AGENT

    # ------------------------------------------------------------------
    # BaseFetcher interface
    # ------------------------------------------------------------------

    def can_handle(self, identifier: str) -> float:
        """Return confidence score (0.0–1.0) for handling *identifier*.

        Args:
            identifier: ETF ticker or ISIN string.

        Returns:
            0.95 for known VanEck tickers/ISINs, 0.7 for NL-domiciled ISINs
            (almost exclusively VanEck in EU), 0.3 for IE ISINs (shared with
            many issuers), 0.2 for unknown tickers.
        """
        clean = identifier.upper().strip()
        if not clean:
            return 0.0

        # Known VanEck ticker or ISIN
        if clean in VANECK_PRODUCTS or clean in _KNOWN_ISINS:
            return 0.95

        # ISIN-shaped (12 alnum chars)
        if len(clean) == 12 and clean.isalnum():
            if clean.startswith("NL"):
                return 0.7
            if clean.startswith("IE"):
                return 0.3

        # Unknown ticker
        return 0.2

    def fetch_holdings(
        self, identifier: str, as_of_date: date | None = None
    ) -> pd.DataFrame:
        """Fetch and normalise VanEck holdings.

        Args:
            identifier: ETF ticker or ISIN.
            as_of_date: Optional reference date (ignored — always returns latest).

        Returns:
            Validated DataFrame conforming to the standard schema.
        """
        ticker = self._resolve_ticker(identifier)
        slug = self._resolve_slug(identifier)

        # Step 1: Scrape holdings page to get blockId/pageId
        block_id, page_id = self._scrape_block_ids(slug)

        # Step 2: Fetch holdings JSON via dataset API
        holdings = self._fetch_dataset(block_id, page_id, ticker)

        # Step 3: Normalise to standard schema
        df = self._normalise(holdings, ticker)

        # Step 4: Filter non-equity rows
        df = self._filter_non_equity(df)

        return self.validate_output(df)

    # ------------------------------------------------------------------
    # Private helpers — identifier resolution
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_ticker(identifier: str) -> str:
        """Resolve *identifier* to a ticker for the etf_ticker column.

        Args:
            identifier: Ticker or ISIN.

        Returns:
            Ticker string.
        """
        clean = identifier.upper().strip()
        if clean in VANECK_PRODUCTS:
            return clean
        if clean in _ISIN_TO_TICKER:
            return _ISIN_TO_TICKER[clean]
        return clean

    @staticmethod
    def _resolve_slug(identifier: str) -> str:
        """Resolve *identifier* to a VanEck URL slug.

        Args:
            identifier: Ticker or ISIN.

        Returns:
            URL slug string.

        Raises:
            ValueError: If no slug mapping exists for the identifier.
        """
        clean = identifier.upper().strip()
        if clean in VANECK_PRODUCTS:
            return VANECK_PRODUCTS[clean][1]
        if clean in _ISIN_TO_SLUG:
            return _ISIN_TO_SLUG[clean]
        raise ValueError(
            f"No VanEck URL slug for '{identifier}'. "
            "Add it to VANECK_PRODUCTS mapping."
        )

    # ------------------------------------------------------------------
    # Private helpers — page scraping and API calls
    # ------------------------------------------------------------------

    def _scrape_block_ids(self, slug: str) -> tuple[str, str]:
        """Scrape the holdings page to extract blockId and pageId.

        Args:
            slug: URL slug for the ETF (e.g. ``semiconductor-etf``).

        Returns:
            Tuple of (blockId, pageId) strings.

        Raises:
            ValueError: If block IDs cannot be found in the page.
        """
        url = _HOLDINGS_PAGE_URL.format(slug=slug)
        logger.info("Scraping VanEck holdings page: %s", url)
        resp = _retry_request(self._session, url)

        match = _BLOCK_RE.search(resp.text)
        if not match:
            raise ValueError(
                f"Could not find <ve-holdingsblock> in {url}. "
                "Page structure may have changed."
            )

        block_id, page_id = match.group(1), match.group(2)
        logger.info("Found blockId=%s, pageId=%s for slug=%s", block_id, page_id, slug)
        return block_id, page_id

    def _fetch_dataset(
        self, block_id: str, page_id: str, ticker: str
    ) -> list[dict]:
        """Fetch holdings dataset via the VanEck API.

        Args:
            block_id: Block ID from the holdings page.
            page_id: Page ID from the holdings page.
            ticker: ETF ticker to pass to the API.

        Returns:
            List of holding dicts from the API response.

        Raises:
            ValueError: If the API response contains no holdings.
        """
        url = _DATASET_API_URL.format(
            block_id=block_id, page_id=page_id, ticker=ticker,
        )
        logger.info("Fetching VanEck dataset API: %s", url)
        resp = _retry_request(self._session, url)
        data = resp.json()

        holdings = data.get("Holdings", [])
        if not holdings:
            raise ValueError(
                f"VanEck API returned no holdings for ticker={ticker} "
                f"(blockId={block_id}, pageId={page_id})"
            )

        logger.info(
            "VanEck API returned %d holdings for %s (as_of: %s)",
            len(holdings), ticker, data.get("AsOfDate", "unknown"),
        )
        return holdings

    # ------------------------------------------------------------------
    # Private helpers — normalisation
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise(holdings: list[dict], ticker: str) -> pd.DataFrame:
        """Map VanEck API holdings to the standard schema.

        Args:
            holdings: List of holding dicts from the API.
            ticker: ETF ticker for the ``etf_ticker`` column.

        Returns:
            DataFrame with standard column names.
        """
        records = []
        for h in holdings:
            # Parse weight: string like "19.36" → float
            weight_raw = h.get("Weight", "0")
            try:
                weight = float(str(weight_raw).replace(",", ""))
            except (ValueError, TypeError):
                weight = None

            # Parse market value: string like "8,415,615,950" → float
            mv_raw = h.get("MV", "")
            try:
                mv = float(str(mv_raw).replace(",", ""))
            except (ValueError, TypeError):
                mv = None

            # Parse shares: string like "47,374,555" → float
            shares_raw = h.get("Shares", "")
            try:
                shares = float(str(shares_raw).replace(",", ""))
            except (ValueError, TypeError):
                shares = None

            # Extract as_of_date from holding-level field or skip
            as_of_raw = h.get("AsOfDate", "")
            # Format: "04/06/2026 00:00:00" → "2026-04-06"
            as_of_date = None
            if as_of_raw:
                try:
                    parts = str(as_of_raw).split(" ")[0].split("/")
                    if len(parts) == 3:
                        as_of_date = f"{parts[2]}-{parts[0]:>02}-{parts[1]:>02}"
                except (IndexError, ValueError):
                    as_of_date = str(as_of_raw)

            records.append({
                "etf_ticker": ticker,
                "holding_name": h.get("HoldingName"),
                "holding_isin": h.get("ISIN") or None,
                "holding_ticker": h.get("Label") or None,
                "holding_cusip": h.get("CUSIP") or None,
                "weight_pct": weight,
                "market_value": mv,
                "shares": shares,
                "sector": h.get("Sector") or None,
                "country": h.get("Country") or None,
                "currency": h.get("CurrencyCode") or None,
                "as_of_date": as_of_date,
            })

        return pd.DataFrame(records)

    # ------------------------------------------------------------------
    # Private helpers — filtering
    # ------------------------------------------------------------------

    @staticmethod
    def _filter_non_equity(df: pd.DataFrame) -> pd.DataFrame:
        """Remove non-equity rows (cash, derivatives, swaps).

        Args:
            df: Holdings DataFrame.

        Returns:
            Filtered DataFrame.
        """
        if "sector" not in df.columns:
            return df
        mask = ~df["sector"].astype(str).str.strip().isin(_NON_EQUITY_LABELS)
        filtered = df.loc[mask].copy()
        n_removed = len(df) - len(filtered)
        if n_removed > 0:
            logger.info(
                "Filtered %d non-equity rows (%d remaining)",
                n_removed, len(filtered),
            )
        return filtered
