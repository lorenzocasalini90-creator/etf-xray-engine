"""Tests for L&G (LGIM) fetcher."""

import io

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from src.ingestion.fetcher_lgim import LGIMFetcher, LGIM_PRODUCTS


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


class TestNormalise:
    def test_maps_columns(self):
        raw = pd.DataFrame({
            "Name": ["CROWDSTRIKE", "PALO ALTO"],
            "ISIN": ["US22788C1053", "US6974351057"],
            "Weight (%)": [8.5, 7.2],
            "Sector": ["Technology", "Technology"],
            "Country": ["US", "US"],
        })
        df = LGIMFetcher._normalise(raw, "ISPY")
        assert "holding_name" in df.columns
        assert "weight_pct" in df.columns
        assert df["etf_ticker"].iloc[0] == "ISPY"
        assert df["weight_pct"].iloc[0] == 8.5

    def test_decimal_weights_converted(self):
        raw = pd.DataFrame({
            "Name": ["TEST"],
            "Weighting": [0.085],
        })
        df = LGIMFetcher._normalise(raw, "ISPY")
        assert abs(df["weight_pct"].iloc[0] - 8.5) < 0.01


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
