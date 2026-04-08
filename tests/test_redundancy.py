"""Tests for redundancy analytics."""

import pandas as pd
import pytest

from src.analytics.redundancy import redundancy_breakdown, redundancy_scores


def _make_holdings(ticker: str, holdings: list[tuple[str, str, float]]) -> pd.DataFrame:
    records = []
    for name, isin, weight in holdings:
        records.append({
            "etf_ticker": ticker,
            "holding_name": name,
            "holding_isin": isin,
            "holding_ticker": None,
            "weight_pct": weight,
        })
    return pd.DataFrame(records)


BROAD = _make_holdings("BROAD", [
    ("Apple", "US0378331005", 30.0),
    ("Microsoft", "US5949181045", 25.0),
    ("Google", "US02079K1079", 20.0),
    ("Amazon", "US0231351067", 25.0),
])

NARROW = _make_holdings("NARROW", [
    ("Apple", "US0378331005", 50.0),
    ("Microsoft", "US5949181045", 30.0),
    ("Tesla", "US88160R1014", 20.0),
])

UNIQUE = _make_holdings("UNIQUE", [
    ("Sony", "JP3435000009", 60.0),
    ("Toyota", "JP3633400001", 40.0),
])

HOLDINGS_DB = {"BROAD": BROAD, "NARROW": NARROW, "UNIQUE": UNIQUE}


class TestRedundancyBreakdown:
    def test_returns_dict_of_contributions(self) -> None:
        result = redundancy_breakdown("NARROW", HOLDINGS_DB)
        assert isinstance(result, dict)
        assert "BROAD" in result
        assert result.get("UNIQUE", 0) == 0

    def test_contribution_matches_shared_weight(self) -> None:
        result = redundancy_breakdown("NARROW", HOLDINGS_DB)
        # NARROW: AAPL(50%) + MSFT(30%) shared with BROAD = 80% of NARROW's weight
        assert abs(result["BROAD"] - 80.0) < 0.1

    def test_unique_etf_has_zero_contributions(self) -> None:
        result = redundancy_breakdown("UNIQUE", HOLDINGS_DB)
        assert all(v == 0 for v in result.values())

    def test_empty_holdings_returns_empty(self) -> None:
        result = redundancy_breakdown("MISSING", HOLDINGS_DB)
        assert result == {}

    def test_single_etf_portfolio(self) -> None:
        result = redundancy_breakdown("BROAD", {"BROAD": BROAD})
        assert result == {}


class TestRedundancyScores:
    def test_returns_dataframe_with_expected_columns(self) -> None:
        positions = [
            {"ticker": "BROAD", "capital": 10000},
            {"ticker": "NARROW", "capital": 5000},
        ]
        df = redundancy_scores(positions, {"BROAD": BROAD, "NARROW": NARROW})
        assert set(df.columns) >= {"etf_ticker", "redundancy_pct", "unique_pct", "ter_wasted", "verdict"}

    def test_narrow_is_more_redundant_than_broad(self) -> None:
        positions = [
            {"ticker": "BROAD", "capital": 10000},
            {"ticker": "NARROW", "capital": 5000},
        ]
        df = redundancy_scores(positions, {"BROAD": BROAD, "NARROW": NARROW})
        broad_r = df.loc[df["etf_ticker"] == "BROAD", "redundancy_pct"].iloc[0]
        narrow_r = df.loc[df["etf_ticker"] == "NARROW", "redundancy_pct"].iloc[0]
        assert narrow_r > broad_r

    def test_unique_etf_has_zero_redundancy(self) -> None:
        positions = [
            {"ticker": "BROAD", "capital": 10000},
            {"ticker": "UNIQUE", "capital": 5000},
        ]
        df = redundancy_scores(positions, {"BROAD": BROAD, "UNIQUE": UNIQUE})
        unique_r = df.loc[df["etf_ticker"] == "UNIQUE", "redundancy_pct"].iloc[0]
        assert unique_r == 0.0
