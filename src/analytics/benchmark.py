"""Benchmark proxy manager.

Uses iShares ETFs as proxies for standard benchmarks.
Downloads and normalizes benchmark holdings for comparison.
Also resolves holdings to Composite FIGI so that Active Share
comparison works correctly against the portfolio.
"""

import logging
from datetime import date

import pandas as pd

from src.ingestion.ishares import ISharesFetcher

logger = logging.getLogger(__name__)

# Pre-configured benchmark proxies: benchmark_name -> (etf_ticker, index_name)
BENCHMARK_PROXIES: dict[str, tuple[str, str]] = {
    "MSCI_WORLD": ("SWDA", "MSCI World"),
    "SP500": ("CSPX", "S&P 500"),
    "MSCI_EM": ("EIMI", "MSCI Emerging Markets"),
    "FTSE_ALL_WORLD": ("VWCE", "FTSE All-World"),
}

# Aliases: allow lookup by ETF ticker too
_TICKER_ALIASES: dict[str, str] = {
    "SWDA": "MSCI_WORLD",
    "CSPX": "SP500",
    "EIMI": "MSCI_EM",
    "VWCE": "FTSE_ALL_WORLD",
}


class BenchmarkManager:
    """Manage benchmark proxy ETFs for portfolio comparison.

    Downloads holdings from iShares ETFs that track major indices
    and provides them as normalized DataFrames with FIGI resolution.

    Args:
        fetcher: Optional ISharesFetcher instance. Creates one if not provided.
        resolver: Optional FigiResolver for FIGI resolution of benchmark
            holdings. If ``None``, benchmark holdings will lack ``composite_figi``
            and Active Share comparison will not work.
    """

    def __init__(self, fetcher: ISharesFetcher | None = None, resolver=None) -> None:
        self._fetcher = fetcher or ISharesFetcher()
        self._resolver = resolver
        self._cache: dict[str, pd.DataFrame] = {}

    def list_benchmarks(self) -> list[dict[str, str]]:
        """Return available benchmark names and their proxy ETFs.

        Returns:
            List of dicts with name, ticker, and index_name.
        """
        return [
            {"name": name, "ticker": info[0], "index_name": info[1]}
            for name, info in BENCHMARK_PROXIES.items()
        ]

    def get_benchmark_holdings(
        self, benchmark_name: str, as_of_date: date | None = None
    ) -> pd.DataFrame:
        """Fetch holdings for a benchmark proxy ETF.

        If a ``resolver`` was provided, also resolves holdings to
        Composite FIGI (required for Active Share comparison).

        Args:
            benchmark_name: Benchmark name (e.g. "MSCI_WORLD") or ETF ticker (e.g. "SWDA").
            as_of_date: Optional reference date.

        Returns:
            DataFrame with standard holdings schema columns
            (+ ``composite_figi`` if resolver is available).

        Raises:
            ValueError: If benchmark_name is not recognized.
        """
        # Resolve aliases
        key = benchmark_name.upper().strip()
        if key in _TICKER_ALIASES:
            key = _TICKER_ALIASES[key]
        if key not in BENCHMARK_PROXIES:
            raise ValueError(
                f"Unknown benchmark: {benchmark_name!r}. "
                f"Available: {list(BENCHMARK_PROXIES.keys())}"
            )

        cache_key = f"{key}_{as_of_date}"
        if cache_key in self._cache:
            logger.info("Using cached holdings for benchmark %s", key)
            return self._cache[cache_key].copy()

        ticker = BENCHMARK_PROXIES[key][0]
        logger.info("Fetching benchmark %s via proxy ETF %s", key, ticker)
        df = self._fetcher.fetch_holdings(ticker, as_of_date=as_of_date)

        # Resolve FIGI so Active Share can match holdings by composite_figi
        if self._resolver is not None:
            try:
                logger.info("Resolving FIGI for benchmark %s (%d holdings)", key, len(df))
                df = self._resolver.resolve_batch(df)
            except Exception as exc:
                logger.warning("FIGI resolution failed for benchmark %s: %s", key, exc)

        self._cache[cache_key] = df
        return df.copy()
