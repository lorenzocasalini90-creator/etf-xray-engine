"""Tests for L&G (LGIM) fetcher."""

import io

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from src.ingestion.fetcher_lgim import (
    LGIMFetcher,
    LGIM_PRODUCTS,
    _ISIN_TO_FUND_ID,
)


class TestCanHandle:
    def test_known_ticker(self):
        f = LGIMFetcher()
        assert f.can_handle("ISPY") == 0.9

    def test_known_isin(self):
        f = LGIMFetcher()
        assert f.can_handle("IE00BYPLS672") == 0.9

    def test_unknown_ie_isin(self):
        f = LGIMFetcher()
        assert f.can_handle("IE00BK5BCD43") == 0.3

    def test_non_ie_isin(self):
        f = LGIMFetcher()
        assert f.can_handle("LU0274211217") == 0.0

    def test_empty(self):
        f = LGIMFetcher()
        assert f.can_handle("") == 0.0

    def test_case_insensitive(self):
        f = LGIMFetcher()
        assert f.can_handle("ispy") == 0.9


class TestResolveIsin:
    def test_ticker_to_isin(self):
        assert LGIMFetcher._resolve_isin("ISPY") == "IE00BYPLS672"

    def test_isin_passthrough(self):
        assert LGIMFetcher._resolve_isin("IE00BYPLS672") == "IE00BYPLS672"

    def test_unknown_passthrough(self):
        assert LGIMFetcher._resolve_isin("UNKNOWN") == "UNKNOWN"


class TestResolveTicker:
    def test_ticker_passthrough(self):
        assert LGIMFetcher._resolve_ticker("ISPY") == "ISPY"

    def test_isin_to_ticker(self):
        assert LGIMFetcher._resolve_ticker("IE00BYPLS672") == "ISPY"


class TestResolveFundId:
    def test_known_isin(self):
        f = LGIMFetcher()
        assert f._resolve_fund_id("IE00BYPLS672") == 228

    def test_all_known_isins_mapped(self):
        for isin in LGIM_PRODUCTS.values():
            assert isin in _ISIN_TO_FUND_ID, f"Missing fund_id for {isin}"


class TestNormalise:
    def test_maps_lgim_csv_columns(self):
        """Test with actual L&G CSV column names."""
        raw = pd.DataFrame({
            "ETF Name": ["L&G Cyber", "L&G Cyber"],
            "ISIN": ["US22788C1053", "US6974351057"],
            "Security Description": ["CROWDSTRIKE HOLDINGS INC", "PALO ALTO NETWORKS INC"],
            "Security Type": ["Equity", "Equity"],
            "Broad Type": ["Equity", "Equity"],
            "Currency Code": ["USD", "USD"],
            "Price": [350.0, 180.0],
            "Cash Value CCY": [1000000, 800000],
            "Collateral Value CCY": [0, 0],
            "Percentage": [8.5, 7.2],
        })
        df = LGIMFetcher._normalise(raw, "ISPY")
        assert "holding_name" in df.columns
        assert "holding_isin" in df.columns
        assert "weight_pct" in df.columns
        assert df["holding_name"].iloc[0] == "CROWDSTRIKE HOLDINGS INC"
        assert df["holding_isin"].iloc[0] == "US22788C1053"
        assert df["weight_pct"].iloc[0] == 8.5
        assert df["etf_ticker"].iloc[0] == "ISPY"

    def test_decimal_weights_converted(self):
        raw = pd.DataFrame({
            "Security Description": ["TEST"],
            "Percentage": [0.085],
        })
        df = LGIMFetcher._normalise(raw, "ISPY")
        assert abs(df["weight_pct"].iloc[0] - 8.5) < 0.01

    def test_filters_non_equity(self):
        raw = pd.DataFrame({
            "Security Description": ["STOCK", "CASH COLLATERAL"],
            "Percentage": [5.0, 1.0],
            "Broad Type": ["Equity", "Cash"],
        })
        df = LGIMFetcher._normalise(raw, "ISPY")
        assert len(df) == 1
        assert df["holding_name"].iloc[0] == "STOCK"


class TestCandidateDates:
    def test_with_specific_date(self):
        from datetime import date
        dates = LGIMFetcher._candidate_dates(date(2026, 3, 15))
        assert dates == ["2026-03-15"]

    def test_without_date_returns_multiple(self):
        dates = LGIMFetcher._candidate_dates(None)
        assert len(dates) >= 3
        # All should be valid date strings
        for d in dates:
            assert len(d) == 10  # YYYY-MM-DD


class TestRegistry:
    def test_lgim_discovered(self):
        from src.ingestion.registry import FetcherRegistry
        registry = FetcherRegistry()
        names = [type(f).__name__ for f in registry.fetchers]
        assert "LGIMFetcher" in names

    def test_routes_known_ticker(self):
        from src.ingestion.registry import FetcherRegistry
        registry = FetcherRegistry()
        ranked = registry.get_fetchers_ranked("ISPY")
        top_names = [type(f).__name__ for f, _ in ranked[:3]]
        assert "LGIMFetcher" in top_names


class TestTryFetch:
    def test_try_fetch_404_returns_failed(self):
        f = LGIMFetcher()
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_resp.raise_for_status.side_effect = __import__("requests").HTTPError(
            response=mock_resp
        )

        with patch.object(f._session, "get", return_value=mock_resp):
            result = f.try_fetch("IE00BYPLS672")
            assert result.status == "failed"

    def test_try_fetch_csv_success(self):
        """Test successful CSV parse flow."""
        csv_content = (
            "ETF Name,ISIN,Security Description,Security Type,Broad Type,"
            "Currency Code,Price,Cash Value CCY,Collateral Value CCY,Percentage\n"
            "L&G Cyber,US22788C1053,CROWDSTRIKE,Equity,Equity,USD,350,1000000,0,8.5\n"
            "L&G Cyber,US6974351057,PALO ALTO,Equity,Equity,USD,180,800000,0,7.2\n"
        )
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Content-Type": "text/csv"}
        mock_resp.text = csv_content
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.side_effect = ValueError("not json")

        with patch.object(f := LGIMFetcher(), "_session") as mock_session:
            mock_session.get.return_value = mock_resp
            result = f.try_fetch("IE00BYPLS672")
            assert result.status == "success"
            assert len(result.holdings) == 2
            assert "CROWDSTRIKE" in result.holdings["holding_name"].values
