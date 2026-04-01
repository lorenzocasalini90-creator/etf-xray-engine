"""JustETF fallback fetcher — universal UCITS ETF coverage via justetf-scraping.

This is the last-resort fetcher in the cascade: when no issuer-specific
fetcher (iShares, Xtrackers, Amundi, Invesco, SPDR) succeeds, JustETF
provides at least **top 10 holdings + metadata** for virtually any
European UCITS ETF.

Limitations:
  - Only ~10 holdings (the top positions), not full holdings.
  - Coverage is typically 30-40% of total weight.
  - FetchResult always has ``status="partial"``.

Depends on ``justetf-scraping`` (optional dependency). If not installed,
``can_handle()`` returns 0.0 and the fetcher is effectively disabled.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import pandas as pd

from src.ingestion.base_fetcher import BaseFetcher, FetchResult

logger = logging.getLogger(__name__)


def _justetf_available() -> bool:
    """Check if justetf-scraping is importable."""
    try:
        import justetf_scraping  # type: ignore[import-untyped]  # noqa: F401
        return True
    except ImportError:
        return False


@dataclass
class JustETFMetadata:
    """Metadata extracted from JustETF for an ETF.

    Attributes:
        name: Fund name.
        isin: ISIN code.
        issuer: Issuer name (e.g. ``iShares``, ``Xtrackers``).
        ter: Total Expense Ratio as percentage.
        fund_size_eur: Fund size in EUR.
        description: Fund description.
        countries: Country allocation dict (country → weight%).
        sectors: Sector allocation dict (sector → weight%).
    """

    name: str | None = None
    isin: str | None = None
    issuer: str | None = None
    ter: float | None = None
    fund_size_eur: float | None = None
    description: str | None = None
    countries: dict[str, float] | None = None
    sectors: dict[str, float] | None = None


class JustETFFetcher(BaseFetcher):
    """Universal fallback fetcher for European UCITS ETFs via JustETF.

    Returns partial holdings (top ~10) with ``status="partial"``.
    Should always have the lowest priority in the registry.
    """

    def __init__(self) -> None:
        self._available = _justetf_available()
        if not self._available:
            logger.info("justetf-scraping not installed — JustETFFetcher disabled")

    # ------------------------------------------------------------------
    # BaseFetcher interface
    # ------------------------------------------------------------------

    def can_handle(self, identifier: str) -> float:
        """Return 0.1 for any non-empty input if justetf-scraping is installed.

        This is the universal fallback — lowest priority of all fetchers.

        Args:
            identifier: ETF ticker, ISIN, or name.
        """
        if not self._available:
            return 0.0
        if not identifier or not identifier.strip():
            return 0.0
        return 0.1

    def fetch_holdings(
        self, identifier: str, as_of_date: date | None = None
    ) -> pd.DataFrame:
        """Fetch top holdings from JustETF.

        Args:
            identifier: ETF ISIN (preferred) or ticker.
            as_of_date: Ignored — JustETF always returns latest.

        Returns:
            DataFrame with top ~10 holdings in standard schema.

        Raises:
            ValueError: If no holdings data is available.
        """
        overview = self._get_overview(identifier)
        top_holdings = overview.get("top_holdings", [])

        if not top_holdings:
            raise ValueError(
                f"JustETF returned no holdings for {identifier}. "
                f"This identifier may not be a valid UCITS ETF."
            )

        df = self._normalise_holdings(top_holdings, identifier)
        return df

    def try_fetch(
        self, identifier: str, as_of_date: date | None = None
    ) -> FetchResult:
        """Fetch top holdings and return a ``FetchResult`` with status="partial".

        Overrides ``BaseFetcher.try_fetch`` to always return ``partial``
        on success (not ``success``), since JustETF only provides top ~10.

        Args:
            identifier: ETF ISIN or ticker.
            as_of_date: Ignored.

        Returns:
            ``FetchResult`` with status ``partial`` or ``failed``.
        """
        try:
            overview = self._get_overview(identifier)
            top_holdings = overview.get("top_holdings", [])

            if not top_holdings:
                return FetchResult(
                    status="failed",
                    message=f"JustETF returned no holdings for {identifier}",
                    source="JustETFFetcher",
                )

            df = self._normalise_holdings(top_holdings, identifier)
            df = self.validate_output(df)

            coverage_pct = df["weight_pct"].sum() if "weight_pct" in df.columns else 0.0
            coverage_pct = min(coverage_pct, 100.0)

            # Extract metadata for logging
            name = overview.get("name", identifier)
            ter = overview.get("ter")
            ter_str = f", TER {ter}%" if ter is not None else ""

            return FetchResult(
                status="partial",
                holdings=df,
                message=(
                    f"Solo top {len(df)} holdings disponibili "
                    f"({coverage_pct:.1f}% del peso). Analisi parziale."
                    f" [{name}{ter_str}]"
                ),
                coverage_pct=coverage_pct,
                source="JustETFFetcher",
            )
        except Exception as exc:
            logger.warning("JustETFFetcher failed for %s: %s", identifier, exc)
            return FetchResult(
                status="failed",
                message=f"JustETFFetcher failed for {identifier}: {exc}",
                source="JustETFFetcher",
            )

    def get_metadata(self, identifier: str) -> JustETFMetadata | None:
        """Extract metadata from JustETF overview.

        Args:
            identifier: ETF ISIN or ticker.

        Returns:
            ``JustETFMetadata`` or ``None`` on failure.
        """
        try:
            overview = self._get_overview(identifier)
            return JustETFMetadata(
                name=overview.get("name"),
                isin=overview.get("isin"),
                issuer=overview.get("issuer"),
                ter=overview.get("ter"),
                fund_size_eur=overview.get("fund_size_eur"),
                description=overview.get("description"),
                countries=overview.get("countries"),
                sectors=overview.get("sectors"),
            )
        except Exception as exc:
            logger.warning("JustETF metadata failed for %s: %s", identifier, exc)
            return None

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_overview(identifier: str) -> dict:
        """Call justetf_scraping to get ETF overview data.

        Tries ``get_etf_overview`` first (newer API), falls back to
        ``load_etf`` (older API).

        Args:
            identifier: ETF ISIN or ticker.

        Returns:
            Dict with overview data.

        Raises:
            ImportError: If justetf-scraping is not installed.
            Exception: If the API call fails.
        """
        import justetf_scraping  # type: ignore[import-untyped]

        # Try newer API first
        if hasattr(justetf_scraping, "get_etf_overview"):
            return justetf_scraping.get_etf_overview(identifier.strip())

        # Fallback to older API
        if hasattr(justetf_scraping, "load_etf"):
            return justetf_scraping.load_etf(identifier.strip())

        raise ImportError(
            "justetf-scraping has neither get_etf_overview nor load_etf"
        )

    @staticmethod
    def _normalise_holdings(
        top_holdings: list[dict], identifier: str
    ) -> pd.DataFrame:
        """Normalise JustETF top holdings to the standard schema.

        Args:
            top_holdings: List of dicts with holding data from JustETF.
                Expected keys: ``name``, ``isin``, ``percentage``/``weight``.
            identifier: ETF identifier for the ``etf_ticker`` column.

        Returns:
            DataFrame with standard schema columns.
        """
        records = []
        for h in top_holdings:
            # JustETF may use 'percentage' or 'weight' for the weight field
            weight = h.get("percentage") or h.get("weight")
            if weight is not None:
                weight = float(weight)

            records.append({
                "holding_name": h.get("name"),
                "holding_isin": h.get("isin"),
                "weight_pct": weight,
                "country": h.get("country"),
                "sector": h.get("sector"),
            })

        df = pd.DataFrame(records)
        df["etf_ticker"] = identifier
        return df
