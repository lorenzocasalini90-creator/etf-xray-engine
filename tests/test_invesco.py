"""Tests for the Invesco fetcher."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingestion.base_fetcher import HOLDINGS_SCHEMA
from src.ingestion.invesco import InvescoFetcher


# ---------------------------------------------------------------------------
# Mock etf-scraper output (matches real etf-scraper column names)
# ---------------------------------------------------------------------------

MOCK_SCRAPER_DF = pd.DataFrame({
    "ticker": ["AAPL", "MSFT", "NVDA", "CASH_USD", "FUT_ES"],
    "name": ["APPLE INC", "MICROSOFT CORP", "NVIDIA CORP", "US DOLLAR", "S&P FUTURE"],
    "sector": [
        "Information Technology", "Information Technology",
        "Information Technology", "Cash", "Futures",
    ],
    "asset_class": ["Equity", "Equity", "Equity", "Cash", "Futures"],
    "market_value": [46e9, 34e9, 51e9, 1.5e6, 5e5],
    "weight": [6.65, 4.90, 7.37, 0.02, 0.01],
    "amount": [188_000_000, 95_000_000, 312_000_000, 1_500_000, 10],
    "location": ["United States"] * 5,
    "currency": ["USD"] * 5,
    "fund_ticker": ["QQQ"] * 5,
    "as_of_date": ["2026-03-28"] * 5,
})


@pytest.fixture
def fetcher() -> InvescoFetcher:
    """Create InvescoFetcher with mocked scraper."""
    with patch("src.ingestion.invesco.InvescoFetcher._init_scraper"):
        f = InvescoFetcher()
        f._scraper = MagicMock()
        f._scraper_tickers = {"QQQ", "QQQM", "RSP", "SPLG"}
        return f


# ---------------------------------------------------------------------------
# can_handle
# ---------------------------------------------------------------------------


class TestCanHandle:
    def test_known_us_tickers(self, fetcher: InvescoFetcher) -> None:
        for t in ("QQQ", "QQQM", "RSP"):
            assert fetcher.can_handle(t) == 0.9, f"Should handle {t}"

    def test_known_ucits_tickers(self, fetcher: InvescoFetcher) -> None:
        for t in ("EQQQ", "SC0K", "MXWO"):
            assert fetcher.can_handle(t) == 0.9, f"Should handle {t}"

    def test_ie_isin(self, fetcher: InvescoFetcher) -> None:
        assert fetcher.can_handle("IE00B5BMR087") == 0.3

    def test_unknown_ticker(self, fetcher: InvescoFetcher) -> None:
        assert fetcher.can_handle("ZZZZZ") == 0.2

    def test_empty_string(self, fetcher: InvescoFetcher) -> None:
        assert fetcher.can_handle("") == 0.0

    def test_case_insensitive(self, fetcher: InvescoFetcher) -> None:
        assert fetcher.can_handle("qqq") == 0.9
        assert fetcher.can_handle("  QQQ  ") == 0.9


# ---------------------------------------------------------------------------
# fetch_holdings
# ---------------------------------------------------------------------------


class TestFetchHoldings:
    def test_schema_matches(self, fetcher: InvescoFetcher) -> None:
        fetcher._scraper.query_holdings.return_value = MOCK_SCRAPER_DF.copy()

        df = fetcher.fetch_holdings("QQQ")

        assert list(df.columns) == HOLDINGS_SCHEMA

    def test_parses_holdings(self, fetcher: InvescoFetcher) -> None:
        fetcher._scraper.query_holdings.return_value = MOCK_SCRAPER_DF.copy()

        df = fetcher.fetch_holdings("QQQ")

        # 3 equity rows (cash + futures filtered)
        assert len(df) == 3
        assert "APPLE INC" in df["holding_name"].values

    def test_weight_column(self, fetcher: InvescoFetcher) -> None:
        fetcher._scraper.query_holdings.return_value = MOCK_SCRAPER_DF.copy()

        df = fetcher.fetch_holdings("QQQ")

        assert (df["weight_pct"] > 0).all()

    def test_etf_ticker_column(self, fetcher: InvescoFetcher) -> None:
        fetcher._scraper.query_holdings.return_value = MOCK_SCRAPER_DF.copy()

        df = fetcher.fetch_holdings("QQQ")

        assert (df["etf_ticker"] == "QQQ").all()

    def test_ucits_raises_not_implemented(self, fetcher: InvescoFetcher) -> None:
        with pytest.raises(NotImplementedError, match="UCITS"):
            fetcher.fetch_holdings("EQQQ")


# ---------------------------------------------------------------------------
# try_fetch
# ---------------------------------------------------------------------------


class TestTryFetch:
    def test_try_fetch_success(self, fetcher: InvescoFetcher) -> None:
        fetcher._scraper.query_holdings.return_value = MOCK_SCRAPER_DF.copy()

        result = fetcher.try_fetch("QQQ")

        assert result.status == "success"
        assert result.holdings is not None
        assert result.source == "InvescoFetcher"

    def test_try_fetch_ucits_fails_gracefully(self, fetcher: InvescoFetcher) -> None:
        result = fetcher.try_fetch("EQQQ")

        assert result.status == "failed"
        assert result.holdings is None
        assert "UCITS" in result.message

    def test_try_fetch_network_error(self, fetcher: InvescoFetcher) -> None:
        fetcher._scraper.query_holdings.side_effect = ConnectionError("timeout")

        result = fetcher.try_fetch("QQQ")

        assert result.status == "failed"
        assert result.holdings is None


# ---------------------------------------------------------------------------
# Non-equity filtering
# ---------------------------------------------------------------------------


class TestFilterNonEquity:
    def test_cash_and_futures_removed(self, fetcher: InvescoFetcher) -> None:
        fetcher._scraper.query_holdings.return_value = MOCK_SCRAPER_DF.copy()

        df = fetcher.fetch_holdings("QQQ")

        names = df["holding_name"].tolist()
        assert "US DOLLAR" not in names
        assert "S&P FUTURE" not in names


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_invesco_discovered(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        names = {type(f).__name__ for f in registry.fetchers}
        assert "InvescoFetcher" in names

    def test_routes_invesco_ticker(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        fetcher = registry.get_fetcher("EQQQ")
        assert type(fetcher).__name__ == "InvescoFetcher"
