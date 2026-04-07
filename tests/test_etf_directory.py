"""Tests for ETF directory search."""

import pytest

from src.dashboard.data.etf_directory import load_directory, search_etf


class TestLoadDirectory:
    def test_loads_dataframe(self):
        df = load_directory()
        assert len(df) >= 15
        assert "isin" in df.columns
        assert "ticker" in df.columns

    def test_has_required_columns(self):
        df = load_directory()
        for col in ("isin", "ticker", "name", "provider", "ter_pct"):
            assert col in df.columns


class TestSearchETF:
    def test_exact_ticker_swda(self):
        results = search_etf("SWDA")
        assert len(results) >= 1
        assert results[0]["ticker"] == "SWDA"

    def test_partial_name_ishares_world(self):
        results = search_etf("iShares World")
        tickers = {r["ticker"] for r in results}
        assert "SWDA" in tickers or "IWDA" in tickers

    def test_partial_name_vanguard_all(self):
        results = search_etf("Vanguard All")
        tickers = {r["ticker"] for r in results}
        assert "VWCE" in tickers

    def test_exact_isin(self):
        results = search_etf("IE00BK5BQT80")
        assert len(results) >= 1
        assert results[0]["ticker"] == "VWCE"

    def test_short_query_returns_empty(self):
        assert search_etf("X") == []

    def test_nonexistent_returns_empty(self):
        assert search_etf("XXXNOTEXIST") == []

    def test_case_insensitive(self):
        results = search_etf("swda")
        assert len(results) >= 1
        assert results[0]["ticker"] == "SWDA"

    def test_limit_respected(self):
        results = search_etf("iShares", limit=3)
        assert len(results) <= 3
