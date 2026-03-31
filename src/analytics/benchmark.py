"""Benchmark proxy manager.

Uses iShares ETFs as proxies for standard benchmarks.
Downloads and normalizes benchmark holdings for comparison.
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
    and provides them as normalized DataFrames.

    Args:
        fetcher: Optional ISharesFetcher instance. Creates one if not provided.
    """

    def __init__(self, fetcher: ISharesFetcher | None = None) -> None:
        self._fetcher = fetcher or ISharesFetcher()
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

        Args:
            benchmark_name: Benchmark name (e.g. "MSCI_WORLD") or ETF ticker (e.g. "SWDA").
            as_of_date: Optional reference date.

        Returns:
            DataFrame with standard holdings schema columns.

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
        self._cache[cache_key] = df
        return df.copy()
