"""Tests for the FetchOrchestrator with mock fetchers."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingestion.base_fetcher import BaseFetcher, FetchResult, HOLDINGS_SCHEMA
from src.ingestion.orchestrator import FetchOrchestrator, ETFMetadata, resolve_metadata
from src.ingestion.registry import FetcherRegistry


# ---------------------------------------------------------------------------
# Mock fetchers
# ---------------------------------------------------------------------------

class MockSuccessFetcher(BaseFetcher):
    """Fetcher that succeeds for identifiers containing 'SUCCESS'."""

    def can_handle(self, identifier: str) -> float:
        if "SUCCESS" in identifier.upper():
            return 1.0
        return 0.0

    def fetch_holdings(self, identifier: str, as_of_date: date | None = None) -> pd.DataFrame:
        return pd.DataFrame({
            "etf_ticker": [identifier],
            "holding_name": ["APPLE INC"],
            "holding_isin": ["US0378331005"],
            "holding_ticker": ["AAPL"],
            "weight_pct": [6.5],
            "market_value": [46e9],
            "shares": [188_000_000],
            "sector": ["Technology"],
            "country": ["US"],
            "currency": ["USD"],
            "as_of_date": ["2026-03-28"],
            "holding_sedol": [None],
            "holding_cusip": [None],
        })


class MockFailFetcher(BaseFetcher):
    """Fetcher that always raises on fetch."""

    def can_handle(self, identifier: str) -> float:
        return 0.8

    def fetch_holdings(self, identifier: str, as_of_date: date | None = None) -> pd.DataFrame:
        raise ConnectionError(f"Network error fetching {identifier}")


class MockLowConfidenceFetcher(BaseFetcher):
    """Fetcher with low confidence that succeeds."""

    def can_handle(self, identifier: str) -> float:
        return 0.2

    def fetch_holdings(self, identifier: str, as_of_date: date | None = None) -> pd.DataFrame:
        return pd.DataFrame({
            "etf_ticker": [identifier],
            "holding_name": ["FALLBACK HOLDING"],
            "holding_isin": [None],
            "holding_ticker": ["FB"],
            "weight_pct": [100.0],
            "market_value": [1e6],
            "shares": [1000],
            "sector": ["Unknown"],
            "country": ["Unknown"],
            "currency": ["USD"],
            "as_of_date": ["2026-03-28"],
            "holding_sedol": [None],
            "holding_cusip": [None],
        })


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_registry():
    """Registry with mock fetchers — no auto-discovery."""
    registry = FetcherRegistry.__new__(FetcherRegistry)
    registry._fetchers = []
    return registry


@pytest.fixture
def success_fetcher():
    return MockSuccessFetcher()


@pytest.fixture
def fail_fetcher():
    return MockFailFetcher()


@pytest.fixture
def low_confidence_fetcher():
    return MockLowConfidenceFetcher()


# ---------------------------------------------------------------------------
# Tests: FetchOrchestrator.fetch()
# ---------------------------------------------------------------------------

class TestFetchOrchestratorSuccess:
    """Orchestrator finds and uses the right fetcher."""

    def test_direct_match(self, mock_registry, success_fetcher):
        mock_registry._fetchers = [success_fetcher]
        orch = FetchOrchestrator(registry=mock_registry)

        result = orch.fetch("SUCCESS-ETF")

        assert result.status == "success"
        assert result.holdings is not None
        assert len(result.holdings) == 1
        assert result.source == "MockSuccessFetcher"

    def test_schema_columns(self, mock_registry, success_fetcher):
        mock_registry._fetchers = [success_fetcher]
        orch = FetchOrchestrator(registry=mock_registry)

        result = orch.fetch("SUCCESS-ETF")

        assert list(result.holdings.columns) == HOLDINGS_SCHEMA

    def test_coverage_calculated(self, mock_registry, success_fetcher):
        mock_registry._fetchers = [success_fetcher]
        orch = FetchOrchestrator(registry=mock_registry)

        result = orch.fetch("SUCCESS-ETF")

        # 1 holding with ISIN → 100% coverage
        assert result.coverage_pct == 100.0


class TestFetchOrchestratorCascade:
    """Orchestrator tries fetchers in order and falls through on failure."""

    def test_skip_failed_try_next(self, mock_registry, fail_fetcher, low_confidence_fetcher):
        mock_registry._fetchers = [fail_fetcher, low_confidence_fetcher]
        orch = FetchOrchestrator(registry=mock_registry)

        result = orch.fetch("ANYTHING")

        # fail_fetcher has score 0.8 (tried first) but fails,
        # low_confidence_fetcher has score 0.2 and succeeds
        assert result.status == "success"
        assert result.source == "MockLowConfidenceFetcher"

    def test_all_fail_returns_failed(self, mock_registry, fail_fetcher):
        mock_registry._fetchers = [fail_fetcher]
        orch = FetchOrchestrator(registry=mock_registry)

        result = orch.fetch("ANYTHING")

        assert result.status == "failed"
        assert "non disponibil" in result.message.lower()

    def test_higher_confidence_tried_first(self, mock_registry, success_fetcher, low_confidence_fetcher):
        mock_registry._fetchers = [low_confidence_fetcher, success_fetcher]
        orch = FetchOrchestrator(registry=mock_registry)

        result = orch.fetch("SUCCESS-ETF")

        # success_fetcher has score 1.0, should be tried before low_confidence (0.2)
        assert result.source == "MockSuccessFetcher"


class TestFetchOrchestratorEdgeCases:
    """Edge cases and error handling."""

    def test_empty_identifier(self, mock_registry):
        orch = FetchOrchestrator(registry=mock_registry)

        result = orch.fetch("")

        assert result.status == "failed"
        assert "Empty identifier" in result.message

    def test_whitespace_identifier(self, mock_registry):
        orch = FetchOrchestrator(registry=mock_registry)

        result = orch.fetch("   ")

        assert result.status == "failed"
        assert "Empty identifier" in result.message

    def test_no_fetchers_registered(self, mock_registry):
        orch = FetchOrchestrator(registry=mock_registry)

        result = orch.fetch("CSPX")

        assert result.status == "failed"


class TestFetchOrchestratorIssuerRouting:
    """Orchestrator routes to correct fetcher based on issuer metadata."""

    @patch("src.ingestion.orchestrator.resolve_metadata")
    def test_issuer_routing(self, mock_resolve, mock_registry):
        # Pretend resolve_metadata returns iShares issuer
        mock_resolve.return_value = ETFMetadata(
            isin="IE00B5BMR087", issuer="iShares", name="iShares Core S&P 500"
        )
        # Use a MagicMock with the right class name (avoids polluting
        # BaseFetcher.__subclasses__() which breaks registry tests)
        fake = MagicMock()
        type(fake).__name__ = "ISharesFetcher"
        fake.can_handle.return_value = 1.0
        fake.try_fetch.return_value = FetchResult(
            status="success",
            holdings=pd.DataFrame({
                "etf_ticker": ["CSPX"], "holding_name": ["APPLE"],
                "holding_isin": ["US0378331005"], "holding_ticker": ["AAPL"],
                "weight_pct": [6.5], "market_value": [46e9], "shares": [188e6],
                "sector": ["Tech"], "country": ["US"], "currency": ["USD"],
                "as_of_date": ["2026-03-28"], "holding_sedol": [None],
                "holding_cusip": [None],
            }),
            message="ok", coverage_pct=100.0, source="ISharesFetcher",
        )
        mock_registry._fetchers = [fake]
        orch = FetchOrchestrator(registry=mock_registry)

        result = orch.fetch("CSPX")

        assert result.status == "success"
        mock_resolve.assert_called_once_with("CSPX")

    @patch("src.ingestion.orchestrator.resolve_metadata")
    def test_metadata_resolution_failure_continues(self, mock_resolve, mock_registry, success_fetcher):
        mock_resolve.return_value = None
        mock_registry._fetchers = [success_fetcher]
        orch = FetchOrchestrator(registry=mock_registry)

        result = orch.fetch("SUCCESS-ETF")

        # Should still succeed via brute force even without metadata
        assert result.status == "success"


class TestFetchOrchestratorJustETFFallback:
    """JustETF top-10 fallback for partial results."""

    @patch("src.ingestion.orchestrator.resolve_metadata")
    def test_justetf_fallback_partial(self, mock_resolve, mock_registry):
        mock_resolve.return_value = ETFMetadata(isin="IE00B5BMR087")

        # No fetchers registered, so brute force fails
        mock_registry._fetchers = []

        mock_overview = {
            "name": "Test ETF",
            "isin": "IE00B5BMR087",
            "ter": 0.07,
            "top_holdings": [
                {"name": "Apple", "isin": "US0378331005", "percentage": 6.5},
                {"name": "Microsoft", "isin": "US5949181045", "percentage": 4.9},
            ],
        }

        with patch.dict("sys.modules", {"justetf_scraping": MagicMock()}) as _:
            import sys
            mock_justetf = sys.modules["justetf_scraping"]
            mock_justetf.get_etf_overview.return_value = mock_overview

            orch = FetchOrchestrator(registry=mock_registry)
            result = orch.fetch("CSPX")

        assert result.status == "partial"
        assert result.source == "JustETFFetcher"
        assert result.holdings is not None
        assert len(result.holdings) == 2
        assert len(result.holdings) == 2


# ---------------------------------------------------------------------------
# Tests: FetchResult dataclass
# ---------------------------------------------------------------------------

class TestFetchResult:
    def test_defaults(self):
        r = FetchResult(status="failed")
        assert r.holdings is None
        assert r.message == ""
        assert r.coverage_pct == 0.0
        assert r.source == ""

    def test_success_result(self):
        df = pd.DataFrame({"a": [1]})
        r = FetchResult(
            status="success", holdings=df, message="ok",
            coverage_pct=95.0, source="TestFetcher",
        )
        assert r.status == "success"
        assert len(r.holdings) == 1


# ---------------------------------------------------------------------------
# Tests: BaseFetcher.try_fetch()
# ---------------------------------------------------------------------------

class TestTryFetch:
    def test_try_fetch_success(self, success_fetcher):
        result = success_fetcher.try_fetch("SUCCESS-ETF")

        assert result.status == "success"
        assert result.holdings is not None
        assert result.source == "MockSuccessFetcher"

    def test_try_fetch_catches_exception(self, fail_fetcher):
        result = fail_fetcher.try_fetch("ANYTHING")

        assert result.status == "failed"
        assert result.holdings is None
        assert "Network error" in result.message


# ---------------------------------------------------------------------------
# Tests: resolve_metadata
# ---------------------------------------------------------------------------

class TestResolveMetadata:
    @patch.dict("sys.modules", {"justetf_scraping": None})
    def test_missing_justetf_returns_none(self):
        result = resolve_metadata("CSPX")
        assert result is None

    def test_returns_metadata_dataclass(self):
        meta = ETFMetadata(
            isin="IE00B5BMR087", issuer="iShares", name="iShares Core S&P 500", ter=0.07,
        )
        assert meta.isin == "IE00B5BMR087"
        assert meta.issuer == "iShares"
        assert meta.ter == 0.07


# ---------------------------------------------------------------------------
# Tests: can_handle returns float
# ---------------------------------------------------------------------------

class TestCanHandleFloat:
    def test_success_fetcher_returns_float(self, success_fetcher):
        assert success_fetcher.can_handle("SUCCESS") == 1.0
        assert success_fetcher.can_handle("UNKNOWN") == 0.0

    def test_fail_fetcher_returns_float(self, fail_fetcher):
        assert isinstance(fail_fetcher.can_handle("X"), float)

    def test_low_confidence_returns_float(self, low_confidence_fetcher):
        assert low_confidence_fetcher.can_handle("X") == 0.2
