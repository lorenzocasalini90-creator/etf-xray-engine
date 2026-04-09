"""Tests for the SPDR (State Street) fetcher."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingestion.base_fetcher import HOLDINGS_SCHEMA
from src.ingestion.spdr import SPDRFetcher


# ---------------------------------------------------------------------------
# Mock etf-scraper output
# ---------------------------------------------------------------------------

MOCK_SCRAPER_DF = pd.DataFrame({
    "ticker": ["AAPL", "MSFT", "AMZN", "CASH_USD"],
    "name": ["APPLE INC", "MICROSOFT CORP", "AMAZON COM", "US DOLLAR"],
    "sector": [
        "Information Technology", "Information Technology",
        "Consumer Discretionary", "Cash",
    ],
    "asset_class": ["Equity", "Equity", "Equity", "Cash"],
    "market_value": [46e9, 34e9, 25e9, 1.5e6],
    "weight": [6.65, 4.90, 3.61, 0.02],
    "amount": [188_000_000, 95_000_000, 125_000_000, 1_500_000],
    "location": ["United States"] * 4,
    "currency": ["USD"] * 4,
    "fund_ticker": ["SPY"] * 4,
    "as_of_date": ["2026-03-28"] * 4,
})


@pytest.fixture
def fetcher() -> SPDRFetcher:
    """Create SPDRFetcher with mocked scraper."""
    with patch("src.ingestion.spdr.SPDRFetcher._init_scraper"):
        f = SPDRFetcher()
        f._scraper = MagicMock()
        f._scraper_tickers = {"SPY", "XLF", "XLK", "XLE", "GLD"}
        return f


# ---------------------------------------------------------------------------
# can_handle
# ---------------------------------------------------------------------------


class TestCanHandle:
    def test_known_us_tickers(self, fetcher: SPDRFetcher) -> None:
        for t in ("SPY", "XLF", "XLK"):
            assert fetcher.can_handle(t) == 0.9, f"Should handle {t}"

    def test_known_ucits_tickers(self, fetcher: SPDRFetcher) -> None:
        for t in ("SPY5", "SPYD", "SPPW"):
            assert fetcher.can_handle(t) == 0.9, f"Should handle {t}"

    def test_ie_isin(self, fetcher: SPDRFetcher) -> None:
        assert fetcher.can_handle("IE00B5BMR087") == 0.3

    def test_unknown_ticker(self, fetcher: SPDRFetcher) -> None:
        assert fetcher.can_handle("ZZZZZ") == 0.2

    def test_empty_string(self, fetcher: SPDRFetcher) -> None:
        assert fetcher.can_handle("") == 0.0

    def test_case_insensitive(self, fetcher: SPDRFetcher) -> None:
        assert fetcher.can_handle("spy") == 0.9
        assert fetcher.can_handle("  SPY  ") == 0.9


# ---------------------------------------------------------------------------
# fetch_holdings
# ---------------------------------------------------------------------------


class TestFetchHoldings:
    def test_schema_matches(self, fetcher: SPDRFetcher) -> None:
        fetcher._scraper.query_holdings.return_value = MOCK_SCRAPER_DF.copy()

        df = fetcher.fetch_holdings("SPY")

        assert list(df.columns) == HOLDINGS_SCHEMA

    def test_parses_holdings(self, fetcher: SPDRFetcher) -> None:
        fetcher._scraper.query_holdings.return_value = MOCK_SCRAPER_DF.copy()

        df = fetcher.fetch_holdings("SPY")

        # 3 equity rows (cash filtered)
        assert len(df) == 3
        assert "APPLE INC" in df["holding_name"].values

    def test_weight_column(self, fetcher: SPDRFetcher) -> None:
        fetcher._scraper.query_holdings.return_value = MOCK_SCRAPER_DF.copy()

        df = fetcher.fetch_holdings("SPY")

        assert (df["weight_pct"] > 0).all()

    def test_etf_ticker_column(self, fetcher: SPDRFetcher) -> None:
        fetcher._scraper.query_holdings.return_value = MOCK_SCRAPER_DF.copy()

        df = fetcher.fetch_holdings("SPY")

        assert (df["etf_ticker"] == "SPY").all()

    def test_ucits_proxied_via_us_ticker(self, fetcher: SPDRFetcher) -> None:
        fetcher._scraper.query_holdings.return_value = MOCK_SCRAPER_DF.copy()
        df = fetcher.fetch_holdings("SPY5")
        assert (df["etf_ticker"] == "SPY5").all()


# ---------------------------------------------------------------------------
# try_fetch
# ---------------------------------------------------------------------------


class TestTryFetch:
    def test_try_fetch_success(self, fetcher: SPDRFetcher) -> None:
        fetcher._scraper.query_holdings.return_value = MOCK_SCRAPER_DF.copy()

        result = fetcher.try_fetch("SPY")

        assert result.status == "success"
        assert result.holdings is not None
        assert result.source == "SPDRFetcher"

    def test_try_fetch_ucits_proxied(self, fetcher: SPDRFetcher) -> None:
        fetcher._scraper.query_holdings.return_value = MOCK_SCRAPER_DF.copy()
        result = fetcher.try_fetch("SPY5")
        assert result.status == "success"
        assert result.holdings is not None

    def test_try_fetch_network_error(self, fetcher: SPDRFetcher) -> None:
        fetcher._scraper.query_holdings.side_effect = ConnectionError("timeout")

        result = fetcher.try_fetch("SPY")

        assert result.status == "failed"
        assert result.holdings is None


# ---------------------------------------------------------------------------
# Non-equity filtering
# ---------------------------------------------------------------------------


class TestFilterNonEquity:
    def test_cash_removed(self, fetcher: SPDRFetcher) -> None:
        fetcher._scraper.query_holdings.return_value = MOCK_SCRAPER_DF.copy()

        df = fetcher.fetch_holdings("SPY")

        assert "US DOLLAR" not in df["holding_name"].tolist()


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_spdr_discovered(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        names = {type(f).__name__ for f in registry.fetchers}
        assert "SPDRFetcher" in names

    def test_routes_spdr_ticker(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        fetcher = registry.get_fetcher("SPY5")
        assert type(fetcher).__name__ == "SPDRFetcher"
