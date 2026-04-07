"""Tests for PDF report generation."""

import pytest

from src.dashboard.export.pdf_exporter import generate_report_pdf


@pytest.fixture
def sample_portfolio():
    return [
        {"ticker": "CSPX", "capital": 30000.0},
        {"ticker": "SWDA", "capital": 40000.0},
    ]


@pytest.fixture
def sample_xray_data():
    return {
        "n_holdings": 1329,
        "hhi": 0.0123,
        "effective_n": 81.3,
        "active_share_pct": 13.2,
        "top_10_pct": 28.5,
        "top_holdings": [
            {"name": "NVIDIA CORP", "ticker": "NVDA", "weight": 6.31,
             "sector": "Information Technology", "country": "United States"},
            {"name": "APPLE INC", "ticker": "AAPL", "weight": 5.56,
             "sector": "Information Technology", "country": "United States"},
            {"name": "MICROSOFT CORP", "ticker": "MSFT", "weight": 3.95,
             "sector": "Information Technology", "country": "United States"},
        ],
    }


@pytest.fixture
def sample_redundancy():
    return [
        {"etf_ticker": "CSPX", "redundancy_pct": 98.5, "ter_wasted": 20.6, "verdict": "red"},
        {"etf_ticker": "SWDA", "redundancy_pct": 25.3, "ter_wasted": 0.0, "verdict": "green"},
    ]


@pytest.fixture
def sample_overlap():
    return [[100.0, 53.2], [53.2, 100.0]]


@pytest.fixture
def sample_factor():
    return {
        "factor_scores": {
            "size": {"Large": 85.2, "Mid": 10.1, "Small": 3.5, "Unknown": 1.2},
            "value_growth": {"weighted_pe": 24.5, "weighted_pb": 4.2, "style": "Blend"},
            "quality": {"weighted_roe": 0.22, "weighted_debt_equity": 1.5},
            "dividend_yield": {"weighted_yield": 0.015},
        },
        "coverage_report": {
            "L1_pct": 95.0, "L2_pct": 80.0, "L3_pct": 10.0, "L4_pct": 5.0,
        },
    }


class TestPDFGeneration:
    def test_generates_valid_pdf(self, sample_portfolio, sample_xray_data, sample_redundancy, sample_overlap):
        pdf_bytes = generate_report_pdf(
            portfolio=sample_portfolio,
            benchmark_name="MSCI World (SWDA)",
            xray_data=sample_xray_data,
            redundancy_data=sample_redundancy,
            overlap_data=sample_overlap,
            overlap_labels=["CSPX", "SWDA"],
            recommendations=[],
        )
        assert isinstance(pdf_bytes, bytes)
        assert len(pdf_bytes) > 1000
        assert pdf_bytes[:4] == b"%PDF"

    def test_with_factor_data(self, sample_portfolio, sample_xray_data, sample_redundancy, sample_overlap, sample_factor):
        pdf_bytes = generate_report_pdf(
            portfolio=sample_portfolio,
            benchmark_name="MSCI World",
            xray_data=sample_xray_data,
            redundancy_data=sample_redundancy,
            overlap_data=sample_overlap,
            overlap_labels=["CSPX", "SWDA"],
            recommendations=[],
            factor_data=sample_factor,
        )
        assert pdf_bytes[:4] == b"%PDF"
        assert len(pdf_bytes) > 1000

    def test_without_recommendations(self, sample_portfolio, sample_xray_data, sample_redundancy, sample_overlap):
        pdf_bytes = generate_report_pdf(
            portfolio=sample_portfolio,
            benchmark_name=None,
            xray_data=sample_xray_data,
            redundancy_data=sample_redundancy,
            overlap_data=None,
            overlap_labels=None,
            recommendations=[],
        )
        assert pdf_bytes[:4] == b"%PDF"

    def test_with_recommendations(self, sample_portfolio, sample_xray_data, sample_redundancy, sample_overlap):
        from src.analytics.recommendations import Recommendation
        recs = [
            Recommendation(
                severity="high",
                title="CSPX duplica il portafoglio",
                explanation="Il 98% delle holdings è ridondante.",
                action="Considera di vendere CSPX.",
                saving_eur_annual=20.6,
                rule_id="R1",
            ),
        ]
        pdf_bytes = generate_report_pdf(
            portfolio=sample_portfolio,
            benchmark_name="MSCI World",
            xray_data=sample_xray_data,
            redundancy_data=sample_redundancy,
            overlap_data=sample_overlap,
            overlap_labels=["CSPX", "SWDA"],
            recommendations=recs,
        )
        assert pdf_bytes[:4] == b"%PDF"
        assert len(pdf_bytes) > 1000

    def test_many_holdings_no_crash(self, sample_redundancy, sample_overlap):
        big_portfolio = [{"ticker": f"ETF{i}", "capital": 1000.0} for i in range(20)]
        big_holdings = [
            {"name": f"HOLDING_{i}", "ticker": f"T{i}", "weight": 0.5,
             "sector": "Tech", "country": "US"}
            for i in range(500)
        ]
        pdf_bytes = generate_report_pdf(
            portfolio=big_portfolio,
            benchmark_name="MSCI World",
            xray_data={"n_holdings": 500, "hhi": 0.01, "effective_n": 100,
                       "active_share_pct": 30.0, "top_10_pct": 10.0,
                       "top_holdings": big_holdings},
            redundancy_data=sample_redundancy,
            overlap_data=sample_overlap,
            overlap_labels=["CSPX", "SWDA"],
            recommendations=[],
        )
        assert pdf_bytes[:4] == b"%PDF"
