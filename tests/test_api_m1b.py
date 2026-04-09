"""Tests for M1-b: POST /api/analyze integration + all endpoints."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from api.main import app


@pytest.fixture
def client():
    return TestClient(app)


def _make_holdings(n: int = 20, etf_ticker: str = "TEST") -> pd.DataFrame:
    """Create a fake holdings DataFrame matching the standard schema."""
    rows = []
    for i in range(n):
        rows.append({
            "etf_ticker": etf_ticker,
            "holding_name": f"Company {i}",
            "holding_isin": f"US000000{i:04d}",
            "holding_ticker": f"TICK{i}",
            "holding_sedol": "",
            "holding_cusip": "",
            "weight_pct": round(100.0 / n, 4),
            "market_value": 1000.0,
            "shares": 10,
            "sector": "Technology" if i % 3 == 0 else "Healthcare" if i % 3 == 1 else "Financials",
            "country": "United States" if i % 2 == 0 else "United Kingdom",
            "currency": "USD",
            "as_of_date": "2026-04-01",
        })
    return pd.DataFrame(rows)


def _mock_fetch_result(holdings: pd.DataFrame):
    """Create a mock FetchResult."""
    result = MagicMock()
    result.status = "success"
    result.holdings = holdings
    result.source = "mock"
    result.coverage_pct = 100.0
    result.message = "OK"
    return result


@patch("api.routes.analyze.get_orchestrator")
@patch("api.routes.analyze.get_session_factory_cached")
def test_analyze_single_etf(mock_session_factory, mock_get_orch, client):
    mock_orch = MagicMock()
    mock_orch.fetch.return_value = _mock_fetch_result(_make_holdings(20, "SWDA"))
    mock_get_orch.return_value = mock_orch

    # Mock session factory for enrichment
    mock_factory = MagicMock()
    mock_session = MagicMock()
    mock_factory.return_value = mock_session
    mock_session_factory.return_value = mock_factory

    # Patch the thread-local orchestrator creation too
    with patch("api.routes.analyze.FetchOrchestrator") as MockOrch, \
         patch("api.routes.analyze.HoldingsCacheManager"):
        MockOrch.return_value = mock_orch

        response = client.post("/api/analyze", json={
            "positions": [{"ticker": "SWDA", "amount_eur": 10000}],
            "benchmark": None,
        })

    assert response.status_code == 200
    data = response.json()
    assert "portfolio_id" in data
    assert data["kpis"]["unique_securities"] > 0


@patch("api.routes.analyze.get_orchestrator")
@patch("api.routes.analyze.get_session_factory_cached")
def test_analyze_multi_etf(mock_session_factory, mock_get_orch, client):
    mock_orch = MagicMock()

    def side_effect(ticker, **kwargs):
        # Return different holdings with some overlap
        if ticker == "SWDA":
            return _mock_fetch_result(_make_holdings(20, "SWDA"))
        elif ticker == "CSPX":
            return _mock_fetch_result(_make_holdings(15, "CSPX"))
        else:
            return _mock_fetch_result(_make_holdings(10, ticker))

    mock_orch.fetch.side_effect = side_effect
    mock_get_orch.return_value = mock_orch

    mock_factory = MagicMock()
    mock_session = MagicMock()
    mock_factory.return_value = mock_session
    mock_session_factory.return_value = mock_factory

    with patch("api.routes.analyze.FetchOrchestrator") as MockOrch, \
         patch("api.routes.analyze.HoldingsCacheManager"):
        MockOrch.return_value = mock_orch

        response = client.post("/api/analyze", json={
            "positions": [
                {"ticker": "SWDA", "amount_eur": 10000},
                {"ticker": "CSPX", "amount_eur": 5000},
                {"ticker": "EIMI", "amount_eur": 3000},
            ],
            "benchmark": None,
        })

    assert response.status_code == 200
    data = response.json()
    assert len(data["overlap"]["matrix"]) == 3
    assert len(data["redundancy"]) == 3
    assert len(data["holdings"]) > 0


@patch("api.routes.analyze.get_orchestrator")
@patch("api.routes.analyze.get_session_factory_cached")
def test_analyze_invalid_ticker(mock_session_factory, mock_get_orch, client):
    mock_orch = MagicMock()
    failed = MagicMock()
    failed.status = "failed"
    failed.holdings = None
    failed.source = "none"
    failed.message = "Not found"
    mock_orch.fetch.return_value = failed
    mock_get_orch.return_value = mock_orch

    mock_factory = MagicMock()
    mock_session = MagicMock()
    mock_factory.return_value = mock_session
    mock_session_factory.return_value = mock_factory

    with patch("api.routes.analyze.FetchOrchestrator") as MockOrch, \
         patch("api.routes.analyze.HoldingsCacheManager"):
        MockOrch.return_value = mock_orch

        response = client.post("/api/analyze", json={
            "positions": [{"ticker": "XXXXINVALID", "amount_eur": 1000}],
        })

    # Should return 422 since ALL ETFs failed
    assert response.status_code == 422


def test_analyze_empty_positions(client):
    response = client.post("/api/analyze", json={"positions": []})
    assert response.status_code == 422


@patch("api.dependencies.get_orchestrator")
def test_holdings_endpoint(mock_get_orch, client):
    mock_orch = MagicMock()
    mock_orch.fetch.return_value = _mock_fetch_result(_make_holdings(10, "SWDA"))
    mock_get_orch.return_value = mock_orch

    with patch("api.routes.holdings.get_orchestrator", return_value=mock_orch):
        response = client.get("/api/holdings/SWDA")

    assert response.status_code == 200
    data = response.json()
    assert "holdings" in data
    assert len(data["holdings"]) == 10


def test_search_endpoint(client):
    response = client.get("/api/search?q=swda")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)


def test_benchmarks_endpoint(client):
    response = client.get("/api/benchmarks")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 4
