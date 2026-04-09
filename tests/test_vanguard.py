"""Tests for the Vanguard fetcher."""

import pandas as pd
import pytest

from src.ingestion.base_fetcher import HOLDINGS_SCHEMA
from src.ingestion.vanguard import VanguardFetcher

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_SCRAPER_DF = pd.DataFrame(
    {
        "ticker": ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "CASH_USD", "FUT_ES"],
        "name": [
            "APPLE INC", "MICROSOFT CORP", "NVIDIA CORP",
            "AMAZON COM INC", "ALPHABET INC CLASS A",
            "US DOLLAR", "S&P FUTURE",
        ],
        "sector": [
            "Information Technology", "Information Technology",
            "Information Technology", "Consumer Discretionary",
            "Communication", "Cash", "Futures",
        ],
        "asset_class": [
            "Equity", "Equity", "Equity", "Equity", "Equity",
            "Cash", "Futures",
        ],
        "market_value": [46e9, 34e9, 51e9, 25e9, 14e9, 1.5e6, 5e5],
        "weight": [6.65, 4.90, 7.37, 3.61, 2.03, 0.02, 0.01],
        "amount": [188e6, 95e6, 312e6, 125e6, 85e6, 1.5e6, 10],
        "location": ["United States"] * 7,
        "currency": ["USD"] * 7,
        "fund_ticker": ["VOO"] * 7,
        "as_of_date": [pd.Timestamp("2026-03-28")] * 7,
    }
)


@pytest.fixture
def fetcher() -> VanguardFetcher:
    return VanguardFetcher()


# ---------------------------------------------------------------------------
# can_handle
# ---------------------------------------------------------------------------


class TestCanHandle:
    def test_recognises_us_tickers(self, fetcher: VanguardFetcher) -> None:
        for t in ("VOO", "VTI", "VGT", "VEA", "VWO"):
            assert fetcher.can_handle(t), f"Should handle {t}"

    def test_recognises_ucits_tickers(self, fetcher: VanguardFetcher) -> None:
        for t in ("VWCE", "VUSA", "VEVE", "VFEM"):
            assert fetcher.can_handle(t), f"Should handle {t}"

    def test_rejects_unknown_tickers(self, fetcher: VanguardFetcher) -> None:
        for t in ("SPY", "CSPX", "IWDA", "QQQ"):
            assert not fetcher.can_handle(t), f"Should NOT handle {t}"

    def test_case_insensitive(self, fetcher: VanguardFetcher) -> None:
        assert fetcher.can_handle("voo")
        assert fetcher.can_handle("  VOO  ")


# ---------------------------------------------------------------------------
# fetch_holdings — schema
# ---------------------------------------------------------------------------


class TestFetchHoldingsSchema:
    def test_scraper_schema_matches(
        self, fetcher: VanguardFetcher, mocker
    ) -> None:
        mocker.patch.object(
            fetcher,
            "_fetch_via_scraper",
            return_value=VanguardFetcher._normalise_scraper(
                SAMPLE_SCRAPER_DF, "VOO"
            ),
        )
        fetcher._scraper_tickers.add("VOO")
        df = fetcher.fetch_holdings("VOO")
        assert list(df.columns) == HOLDINGS_SCHEMA

    def test_all_schema_columns_present(
        self, fetcher: VanguardFetcher, mocker
    ) -> None:
        mocker.patch.object(
            fetcher,
            "_fetch_via_scraper",
            return_value=VanguardFetcher._normalise_scraper(
                SAMPLE_SCRAPER_DF, "VTI"
            ),
        )
        fetcher._scraper_tickers.add("VTI")
        df = fetcher.fetch_holdings("VTI")
        for col in HOLDINGS_SCHEMA:
            assert col in df.columns, f"Missing column: {col}"


# ---------------------------------------------------------------------------
# Non-equity filtering
# ---------------------------------------------------------------------------


class TestFilterNonEquity:
    def test_cash_and_futures_removed(
        self, fetcher: VanguardFetcher, mocker
    ) -> None:
        mocker.patch.object(
            fetcher,
            "_fetch_via_scraper",
            return_value=VanguardFetcher._normalise_scraper(
                SAMPLE_SCRAPER_DF, "VOO"
            ),
        )
        fetcher._scraper_tickers.add("VOO")
        df = fetcher.fetch_holdings("VOO")
        names = df["holding_name"].tolist()
        assert "US DOLLAR" not in names
        assert "S&P FUTURE" not in names
        assert "APPLE INC" in names
        assert len(df) == 5


# ---------------------------------------------------------------------------
# Weights sanity
# ---------------------------------------------------------------------------


class TestWeightsSum:
    def test_weights_sum_positive(
        self, fetcher: VanguardFetcher, mocker
    ) -> None:
        mocker.patch.object(
            fetcher,
            "_fetch_via_scraper",
            return_value=VanguardFetcher._normalise_scraper(
                SAMPLE_SCRAPER_DF, "VOO"
            ),
        )
        fetcher._scraper_tickers.add("VOO")
        df = fetcher.fetch_holdings("VOO")
        total = df["weight_pct"].sum()
        assert total > 0
        assert total <= 100.0


# ---------------------------------------------------------------------------
# UCITS not implemented
# ---------------------------------------------------------------------------


class TestUcitsFallback:
    def test_ucits_proxied_via_us_ticker(
        self, fetcher: VanguardFetcher
    ) -> None:
        from unittest.mock import MagicMock
        fetcher._scraper = MagicMock()
        fetcher._scraper.query_holdings.return_value = SAMPLE_SCRAPER_DF.copy()
        df = fetcher.fetch_holdings("VWCE")
        assert (df["etf_ticker"] == "VWCE").all()

    def test_unknown_raises_value_error(
        self, fetcher: VanguardFetcher
    ) -> None:
        with pytest.raises(ValueError, match="Cannot handle"):
            fetcher.fetch_holdings("XYZNOTREAL")


# ---------------------------------------------------------------------------
# Connection error handling
# ---------------------------------------------------------------------------


class TestConnectionError:
    def test_scraper_failure_raises_connection_error(
        self, fetcher: VanguardFetcher, mocker
    ) -> None:
        """Verify graceful error when Vanguard API is unreachable."""
        fetcher._scraper_tickers.add("VOO")
        fetcher._scraper = mocker.MagicMock()
        fetcher._scraper.query_holdings.side_effect = Exception("DNS resolution failed")
        mocker.patch("src.ingestion.vanguard.time.sleep")  # skip waits

        with pytest.raises(ConnectionError, match="unreachable"):
            fetcher.fetch_holdings("VOO")
        assert fetcher._scraper.query_holdings.call_count == 3  # retried


# ---------------------------------------------------------------------------
# Registry integration
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_auto_discovery(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        names = [type(f).__name__ for f in registry.fetchers]
        assert "VanguardFetcher" in names
        assert "ISharesFetcher" in names

    def test_routes_correctly(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        assert type(registry.get_fetcher("CSPX")).__name__ == "ISharesFetcher"
        assert type(registry.get_fetcher("VWCE")).__name__ == "VanguardFetcher"
