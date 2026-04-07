"""Tests for actionable recommendations engine."""

import pytest

from src.analytics.recommendations import Recommendation, generate_recommendations


_BASE_ARGS = {
    "redundancy_scores": {"CSPX": 0.30, "SWDA": 0.10},
    "ter_wasted_eur": {"CSPX": 10.0, "SWDA": 5.0},
    "active_share": 45.0,
    "hhi": 0.05,
    "top1_weight": 0.04,
    "top1_name": "NVIDIA CORP",
    "n_etf": 2,
    "portfolio_total_eur": 70000.0,
    "benchmark_name": "MSCI World",
    "current_total_ter_eur": 100.0,
}


class TestR1HighRedundancy:
    def test_triggered_above_70(self):
        recs = generate_recommendations(
            **{**_BASE_ARGS, "redundancy_scores": {"CSPX": 0.99, "SWDA": 0.30},
               "ter_wasted_eur": {"CSPX": 74.0, "SWDA": 0.0}},
        )
        r1 = [r for r in recs if r.rule_id == "R1"]
        assert len(r1) >= 1
        assert "CSPX" in r1[0].etfs_involved

    def test_not_triggered_below_70(self):
        recs = generate_recommendations(**_BASE_ARGS)
        assert not any(r.rule_id == "R1" for r in recs)


class TestR2ClosetIndexing:
    def test_triggered_low_active_share(self):
        recs = generate_recommendations(**{**_BASE_ARGS, "active_share": 16.0})
        assert any(r.rule_id == "R2" for r in recs)

    def test_not_triggered_high_active_share(self):
        recs = generate_recommendations(**{**_BASE_ARGS, "active_share": 55.0})
        assert not any(r.rule_id == "R2" for r in recs)

    def test_not_triggered_none_active_share(self):
        recs = generate_recommendations(**{**_BASE_ARGS, "active_share": None})
        assert not any(r.rule_id == "R2" for r in recs)


class TestR3Concentration:
    def test_triggered_high_concentration(self):
        recs = generate_recommendations(
            **{**_BASE_ARGS, "top1_weight": 0.12, "top1_name": "NVIDIA CORP"},
        )
        assert any(r.rule_id == "R3" for r in recs)

    def test_not_triggered_low_concentration(self):
        recs = generate_recommendations(**{**_BASE_ARGS, "top1_weight": 0.04})
        assert not any(r.rule_id == "R3" for r in recs)


class TestR4TERWasted:
    def test_triggered_above_50(self):
        recs = generate_recommendations(
            **{**_BASE_ARGS, "ter_wasted_eur": {"CSPX": 40.0, "SWDA": 20.0}},
        )
        assert any(r.rule_id == "R4" for r in recs)

    def test_not_triggered_below_50(self):
        recs = generate_recommendations(
            **{**_BASE_ARGS, "ter_wasted_eur": {"CSPX": 20.0, "SWDA": 10.0}},
        )
        assert not any(r.rule_id == "R4" for r in recs)


class TestHealthyPortfolio:
    def test_no_high_severity(self):
        recs = generate_recommendations(**_BASE_ARGS)
        assert not any(r.severity == "high" for r in recs)


class TestRecommendationDataclass:
    def test_defaults(self):
        r = Recommendation(
            severity="low", title="test", explanation="x", action="y",
        )
        assert r.saving_eur_annual is None
        assert r.etfs_involved == []
        assert r.rule_id == ""
