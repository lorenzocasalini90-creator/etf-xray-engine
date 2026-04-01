"""Tests for the JustETF fallback fetcher."""

from unittest.mock import MagicMock, patch
import sys

import pandas as pd
import pytest

from src.ingestion.base_fetcher import HOLDINGS_SCHEMA


# ---------------------------------------------------------------------------
# Realistic mock data (matches typical JustETF get_etf_overview output)
# ---------------------------------------------------------------------------

MOCK_OVERVIEW_CSPX = {
    "name": "iShares Core S&P 500 UCITS ETF USD (Acc)",
    "isin": "IE00B5BMR087",
    "issuer": "iShares",
    "ter": 0.07,
    "fund_size_eur": 95_000_000_000,
    "description": "The fund invests in the S&P 500 index constituents.",
    "top_holdings": [
        {"name": "APPLE INC", "isin": "US0378331005", "percentage": 7.12},
        {"name": "NVIDIA CORP", "isin": "US67066G1040", "percentage": 6.53},
        {"name": "MICROSOFT CORP", "isin": "US5949181045", "percentage": 5.87},
        {"name": "AMAZON.COM INC", "isin": "US0231351067", "percentage": 3.94},
        {"name": "META PLATFORMS INC", "isin": "US30303M1027", "percentage": 2.78},
        {"name": "ALPHABET INC A", "isin": "US02079K3059", "percentage": 2.21},
        {"name": "BROADCOM INC", "isin": "US11135F1012", "percentage": 2.08},
        {"name": "BERKSHIRE HATHAWAY B", "isin": "US0846707026", "percentage": 1.94},
        {"name": "TESLA INC", "isin": "US88160R1014", "percentage": 1.82},
        {"name": "ELI LILLY AND CO", "isin": "US5324571083", "percentage": 1.71},
    ],
    "countries": {
        "United States": 97.8,
        "Ireland": 0.7,
        "United Kingdom": 0.5,
        "Switzerland": 0.3,
        "Other": 0.7,
    },
    "sectors": {
        "Information Technology": 31.2,
        "Financials": 13.8,
        "Health Care": 11.5,
        "Consumer Discretionary": 10.3,
        "Communication Services": 9.1,
        "Industrials": 8.4,
        "Consumer Staples": 5.9,
        "Energy": 3.4,
        "Utilities": 2.6,
        "Real Estate": 2.1,
        "Materials": 1.7,
    },
}

MOCK_OVERVIEW_EMPTY = {
    "name": "Unknown ETF",
    "isin": "XX0000000000",
    "top_holdings": [],
    "countries": {},
    "sectors": {},
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_justetf_module():
    """Create a mock justetf_scraping module and inject it into sys.modules."""
    mock_mod = MagicMock()
    mock_mod.get_etf_overview.return_value = MOCK_OVERVIEW_CSPX
    with patch.dict(sys.modules, {"justetf_scraping": mock_mod}):
        yield mock_mod


@pytest.fixture
def fetcher(mock_justetf_module):
    """Create JustETFFetcher with mocked justetf-scraping."""
    from src.ingestion.justetf import JustETFFetcher
    return JustETFFetcher()


@pytest.fixture
def fetcher_no_lib():
    """Create JustETFFetcher when justetf-scraping is NOT available."""
    with patch.dict(sys.modules, {"justetf_scraping": None}):
        # Need to reimport to pick up the unavailability
        from src.ingestion.justetf import JustETFFetcher
        f = JustETFFetcher()
        f._available = False
        return f


# ---------------------------------------------------------------------------
# can_handle
# ---------------------------------------------------------------------------


class TestCanHandle:
    def test_returns_0_1_for_any_input(self, fetcher) -> None:
        assert fetcher.can_handle("CSPX") == 0.1
        assert fetcher.can_handle("IE00B5BMR087") == 0.1
        assert fetcher.can_handle("ANYTHING") == 0.1

    def test_returns_0_for_empty(self, fetcher) -> None:
        assert fetcher.can_handle("") == 0.0
        assert fetcher.can_handle("   ") == 0.0

    def test_returns_0_when_not_installed(self, fetcher_no_lib) -> None:
        assert fetcher_no_lib.can_handle("CSPX") == 0.0
        assert fetcher_no_lib.can_handle("IE00B5BMR087") == 0.0

    def test_lowest_priority(self, fetcher) -> None:
        """0.1 must be lower than any other fetcher's score for known tickers."""
        score = fetcher.can_handle("CSPX")
        assert score < 0.2  # lower than Invesco/SPDR unknown (0.2)


# ---------------------------------------------------------------------------
# try_fetch — partial results
# ---------------------------------------------------------------------------


class TestTryFetch:
    def test_returns_partial_status(self, fetcher, mock_justetf_module) -> None:
        result = fetcher.try_fetch("IE00B5BMR087")

        assert result.status == "partial"

    def test_returns_top_holdings(self, fetcher, mock_justetf_module) -> None:
        result = fetcher.try_fetch("IE00B5BMR087")

        assert result.holdings is not None
        assert len(result.holdings) == 10

    def test_coverage_pct_calculated(self, fetcher, mock_justetf_module) -> None:
        result = fetcher.try_fetch("IE00B5BMR087")

        # Sum of top 10 weights: 7.12+6.53+5.87+3.94+2.78+2.21+2.08+1.94+1.82+1.71 = 36.0
        assert abs(result.coverage_pct - 36.0) < 0.1

    def test_message_includes_coverage(self, fetcher, mock_justetf_module) -> None:
        result = fetcher.try_fetch("IE00B5BMR087")

        assert "top 10" in result.message.lower() or "top 10" in result.message
        assert "36.0%" in result.message
        assert "parziale" in result.message.lower()

    def test_message_includes_metadata(self, fetcher, mock_justetf_module) -> None:
        result = fetcher.try_fetch("IE00B5BMR087")

        assert "iShares Core S&P 500" in result.message
        assert "TER 0.07%" in result.message

    def test_source_is_justetf(self, fetcher, mock_justetf_module) -> None:
        result = fetcher.try_fetch("IE00B5BMR087")

        assert result.source == "JustETFFetcher"

    def test_schema_matches(self, fetcher, mock_justetf_module) -> None:
        result = fetcher.try_fetch("IE00B5BMR087")

        assert list(result.holdings.columns) == HOLDINGS_SCHEMA

    def test_isins_present(self, fetcher, mock_justetf_module) -> None:
        result = fetcher.try_fetch("IE00B5BMR087")

        assert "US0378331005" in result.holdings["holding_isin"].values
        assert "US67066G1040" in result.holdings["holding_isin"].values

    def test_holding_names(self, fetcher, mock_justetf_module) -> None:
        result = fetcher.try_fetch("IE00B5BMR087")

        assert "APPLE INC" in result.holdings["holding_name"].values
        assert "NVIDIA CORP" in result.holdings["holding_name"].values

    def test_etf_ticker_column(self, fetcher, mock_justetf_module) -> None:
        result = fetcher.try_fetch("IE00B5BMR087")

        assert (result.holdings["etf_ticker"] == "IE00B5BMR087").all()


# ---------------------------------------------------------------------------
# try_fetch — failure cases
# ---------------------------------------------------------------------------


class TestTryFetchFailures:
    def test_empty_holdings_returns_failed(self, fetcher, mock_justetf_module) -> None:
        mock_justetf_module.get_etf_overview.return_value = MOCK_OVERVIEW_EMPTY

        result = fetcher.try_fetch("XX0000000000")

        assert result.status == "failed"
        assert result.holdings is None

    def test_api_error_returns_failed(self, fetcher, mock_justetf_module) -> None:
        mock_justetf_module.get_etf_overview.side_effect = Exception("API error")

        result = fetcher.try_fetch("IE00B5BMR087")

        assert result.status == "failed"
        assert "failed" in result.message.lower() or "API error" in result.message

    def test_not_installed_returns_failed(self, fetcher_no_lib) -> None:
        result = fetcher_no_lib.try_fetch("IE00B5BMR087")

        assert result.status == "failed"


# ---------------------------------------------------------------------------
# get_metadata
# ---------------------------------------------------------------------------


class TestGetMetadata:
    def test_returns_metadata(self, fetcher, mock_justetf_module) -> None:
        meta = fetcher.get_metadata("IE00B5BMR087")

        assert meta is not None
        assert meta.name == "iShares Core S&P 500 UCITS ETF USD (Acc)"
        assert meta.isin == "IE00B5BMR087"
        assert meta.issuer == "iShares"
        assert meta.ter == 0.07
        assert meta.fund_size_eur == 95_000_000_000

    def test_countries(self, fetcher, mock_justetf_module) -> None:
        meta = fetcher.get_metadata("IE00B5BMR087")

        assert meta.countries is not None
        assert meta.countries["United States"] == 97.8

    def test_sectors(self, fetcher, mock_justetf_module) -> None:
        meta = fetcher.get_metadata("IE00B5BMR087")

        assert meta.sectors is not None
        assert meta.sectors["Information Technology"] == 31.2

    def test_api_failure_returns_none(self, fetcher, mock_justetf_module) -> None:
        mock_justetf_module.get_etf_overview.side_effect = Exception("fail")

        meta = fetcher.get_metadata("IE00B5BMR087")

        assert meta is None


# ---------------------------------------------------------------------------
# Weight field compatibility
# ---------------------------------------------------------------------------


class TestWeightFieldCompat:
    def test_percentage_field(self, fetcher, mock_justetf_module) -> None:
        """Standard JustETF output uses 'percentage' key."""
        result = fetcher.try_fetch("IE00B5BMR087")

        apple = result.holdings.loc[result.holdings["holding_name"] == "APPLE INC"]
        assert abs(apple["weight_pct"].iloc[0] - 7.12) < 0.01

    def test_weight_field_alternative(self, fetcher, mock_justetf_module) -> None:
        """Some versions may use 'weight' instead of 'percentage'."""
        alt_holdings = [
            {"name": "APPLE INC", "isin": "US0378331005", "weight": 7.12},
            {"name": "NVIDIA CORP", "isin": "US67066G1040", "weight": 6.53},
        ]
        mock_justetf_module.get_etf_overview.return_value = {
            **MOCK_OVERVIEW_CSPX,
            "top_holdings": alt_holdings,
        }

        result = fetcher.try_fetch("IE00B5BMR087")

        assert result.status == "partial"
        apple = result.holdings.loc[result.holdings["holding_name"] == "APPLE INC"]
        assert abs(apple["weight_pct"].iloc[0] - 7.12) < 0.01


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_justetf_discovered(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        names = {type(f).__name__ for f in registry.fetchers}
        assert "JustETFFetcher" in names

    def test_lowest_priority_in_registry(self) -> None:
        """JustETFFetcher should have the lowest score for any identifier."""
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        ranked = registry.get_fetchers_ranked("CSPX")
        if ranked:
            # JustETFFetcher should be last (or not present if lib missing)
            justetf_entries = [
                (f, s) for f, s in ranked
                if type(f).__name__ == "JustETFFetcher"
            ]
            if justetf_entries:
                _, score = justetf_entries[0]
                assert score == 0.1
                # All other fetchers should score higher
                for f, s in ranked:
                    if type(f).__name__ != "JustETFFetcher":
                        assert s > score
