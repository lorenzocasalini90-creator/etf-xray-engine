"""Tests for observations engine."""

import pytest

from src.analytics.observations import generate_observations


_BASE = {
    "hhi": 0.05,
    "effective_n": 350,
    "active_share": 45.0,
    "top10_weight": 20.0,
    "top1_name": "NVIDIA CORP",
    "top1_weight": 0.04,
    "redundancy_scores": {"CSPX": 0.20, "SWDA": 0.10},
    "ter_wasted_eur": {"CSPX": 10.0, "SWDA": 5.0},
    "overlap_pairs": [("CSPX", "SWDA", 30.0)],
    "us_weight": 55.0,
    "benchmark_name": "MSCI World",
}


class TestXrayObservations:
    def test_closet_indexing(self):
        obs = generate_observations(**{**_BASE, "active_share": 16.0})
        xray = [o for o in obs if o.page == "xray"]
        assert any("Active Share" in o.text for o in xray)

    def test_high_concentration(self):
        obs = generate_observations(**{**_BASE, "top1_weight": 0.12})
        xray = [o for o in obs if o.page == "xray"]
        assert any(o.severity == "high" for o in xray)

    def test_high_us_weight(self):
        obs = generate_observations(**{**_BASE, "us_weight": 75.0})
        xray = [o for o in obs if o.page == "xray"]
        assert any("USA" in o.text for o in xray)


class TestRedundancyObservations:
    def test_high_redundancy(self):
        obs = generate_observations(**{
            **_BASE,
            "redundancy_scores": {"CSPX": 0.99, "SWDA": 0.10},
        })
        red = [o for o in obs if o.page == "redundancy"]
        assert any(o.severity == "high" for o in red)

    def test_high_ter_wasted(self):
        obs = generate_observations(**{
            **_BASE,
            "ter_wasted_eur": {"CSPX": 80.0, "SWDA": 50.0},
        })
        red = [o for o in obs if o.page == "redundancy"]
        assert any("commissioni" in o.text for o in red)


class TestOverlapObservations:
    def test_high_overlap(self):
        obs = generate_observations(**{
            **_BASE,
            "overlap_pairs": [("CSPX", "SWDA", 65.0)],
        })
        ovr = [o for o in obs if o.page == "overlap"]
        assert any(o.severity == "high" for o in ovr)


class TestDiversifiedPortfolio:
    def test_no_high_observations(self):
        obs = generate_observations(**_BASE)
        assert not any(o.severity == "high" for o in obs)

    def test_filtered_by_page(self):
        obs = generate_observations(**_BASE)
        for o in obs:
            assert o.page in ("xray", "redundancy", "overlap", "sector", "factor")
