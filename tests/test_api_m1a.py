"""Tests for M1-a: FastAPI scaffold, health endpoint, Pydantic models."""

import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.models.request import ETFPosition, PortfolioRequest


@pytest.fixture
def client():
    return TestClient(app)


def test_health_returns_200(client):
    response = client.get("/api/health")
    assert response.status_code == 200


def test_health_schema(client):
    response = client.get("/api/health")
    data = response.json()
    assert "status" in data
    assert "db" in data
    assert "cache_size" in data
    assert "fetcher_status" in data
    assert "version" in data
    assert isinstance(data["fetcher_status"], dict)
    assert data["version"] == "1.0.0"


def test_portfolio_request_valid():
    req = PortfolioRequest(
        positions=[
            ETFPosition(ticker="  swda  ", amount_eur=10000),
            ETFPosition(ticker="eimi", amount_eur=5000),
        ],
        benchmark="MSCI_WORLD",
    )
    assert req.positions[0].ticker == "SWDA"
    assert req.positions[1].ticker == "EIMI"
    assert len(req.positions) == 2


def test_portfolio_request_invalid_amount():
    with pytest.raises(Exception):
        PortfolioRequest(
            positions=[ETFPosition(ticker="SWDA", amount_eur=-100)]
        )


def test_portfolio_request_empty_positions():
    with pytest.raises(Exception):
        PortfolioRequest(positions=[])
