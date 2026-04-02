"""Tests for src.analytics.enrichment module."""

import pandas as pd
import pytest

from src.analytics.enrichment import (
    EXCHANGE_COUNTRY_MAP,
    enrich_missing_data,
    _enrich_from_portfolio_cross_ref,
)


def _make_df(rows: list[dict]) -> pd.DataFrame:
    """Helper to create a holdings-like DataFrame."""
    cols = ["name", "ticker", "sector", "country", "real_weight_pct"]
    df = pd.DataFrame(rows, columns=cols)
    return df


class TestCrossRefEnrichment:
    """Test cross-reference enrichment from portfolio data."""

    def test_fills_sector_from_same_ticker(self):
        df = _make_df([
            {"name": "Apple Inc", "ticker": "AAPL", "sector": "Technology",
             "country": "United States", "real_weight_pct": 5.0},
            {"name": "Apple Inc", "ticker": "AAPL", "sector": "",
             "country": "", "real_weight_pct": 3.0},
        ])
        _enrich_from_portfolio_cross_ref(df)
        assert df.iloc[1]["sector"] == "Technology"
        assert df.iloc[1]["country"] == "United States"

    def test_fills_from_name_match(self):
        df = _make_df([
            {"name": "Microsoft Corp", "ticker": "MSFT", "sector": "Technology",
             "country": "United States", "real_weight_pct": 5.0},
            {"name": "Microsoft Corp", "ticker": "", "sector": "",
             "country": "", "real_weight_pct": 2.0},
        ])
        _enrich_from_portfolio_cross_ref(df)
        assert df.iloc[1]["sector"] == "Technology"
        assert df.iloc[1]["country"] == "United States"

    def test_does_not_overwrite_existing(self):
        df = _make_df([
            {"name": "Apple", "ticker": "AAPL", "sector": "Technology",
             "country": "United States", "real_weight_pct": 5.0},
            {"name": "Apple", "ticker": "AAPL", "sector": "Consumer Electronics",
             "country": "US", "real_weight_pct": 3.0},
        ])
        _enrich_from_portfolio_cross_ref(df)
        assert df.iloc[1]["sector"] == "Consumer Electronics"
        assert df.iloc[1]["country"] == "US"

    def test_no_data_available(self):
        df = _make_df([
            {"name": "Unknown Co", "ticker": "UNK", "sector": "",
             "country": "", "real_weight_pct": 1.0},
        ])
        _enrich_from_portfolio_cross_ref(df)
        assert df.iloc[0]["sector"] == ""
        assert df.iloc[0]["country"] == ""


class TestEnrichMissingData:
    """Test the main enrich_missing_data function."""

    def test_empty_df(self):
        df = pd.DataFrame(columns=["name", "ticker", "sector", "country", "real_weight_pct"])
        result = enrich_missing_data(df)
        assert result.empty

    def test_all_populated_no_change(self):
        df = _make_df([
            {"name": "Apple", "ticker": "AAPL", "sector": "Technology",
             "country": "United States", "real_weight_pct": 5.0},
        ])
        result = enrich_missing_data(df)
        assert result.iloc[0]["sector"] == "Technology"
        assert result.iloc[0]["country"] == "United States"

    def test_missing_becomes_unknown(self):
        """Holdings with no data available should get 'Unknown' not empty string."""
        df = _make_df([
            {"name": "Mystery Co", "ticker": "XYZ123", "sector": "",
             "country": "", "real_weight_pct": 0.1},
        ])
        result = enrich_missing_data(df)
        assert result.iloc[0]["sector"] == "Unknown"
        assert result.iloc[0]["country"] == "Unknown"

    def test_none_values_handled(self):
        df = _make_df([
            {"name": "Test", "ticker": "TST", "sector": None,
             "country": None, "real_weight_pct": 1.0},
        ])
        result = enrich_missing_data(df)
        assert result.iloc[0]["sector"] == "Unknown"
        assert result.iloc[0]["country"] == "Unknown"

    def test_cross_ref_fills_gaps(self):
        df = _make_df([
            {"name": "Apple Inc", "ticker": "AAPL", "sector": "Technology",
             "country": "United States", "real_weight_pct": 5.0},
            {"name": "Apple Inc", "ticker": "AAPL", "sector": None,
             "country": None, "real_weight_pct": 3.0},
        ])
        result = enrich_missing_data(df)
        assert result.iloc[1]["sector"] == "Technology"
        assert result.iloc[1]["country"] == "United States"


class TestExchangeCountryMap:
    """Test the exchange code mapping."""

    def test_major_exchanges(self):
        assert EXCHANGE_COUNTRY_MAP["US"] == "United States"
        assert EXCHANGE_COUNTRY_MAP["LN"] == "United Kingdom"
        assert EXCHANGE_COUNTRY_MAP["GY"] == "Germany"
        assert EXCHANGE_COUNTRY_MAP["FP"] == "France"
        assert EXCHANGE_COUNTRY_MAP["JT"] == "Japan"
        assert EXCHANGE_COUNTRY_MAP["HK"] == "Hong Kong"
        assert EXCHANGE_COUNTRY_MAP["AT"] == "Australia"

    def test_european_exchanges(self):
        assert EXCHANGE_COUNTRY_MAP["NA"] == "Netherlands"
        assert EXCHANGE_COUNTRY_MAP["SM"] == "Spain"
        assert EXCHANGE_COUNTRY_MAP["IM"] == "Italy"
        assert EXCHANGE_COUNTRY_MAP["SS"] == "Sweden"
        assert EXCHANGE_COUNTRY_MAP["DC"] == "Denmark"
