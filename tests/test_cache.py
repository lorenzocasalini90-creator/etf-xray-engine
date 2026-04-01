"""Tests for the holdings cache layer."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.ingestion.base_fetcher import FetchResult, HOLDINGS_SCHEMA
from src.ingestion.orchestrator import FetchOrchestrator
from src.ingestion.registry import FetcherRegistry
from src.storage.cache import HoldingsCacheManager, CACHE_TTL_DAYS
from src.storage.models import Base


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db_session_factory():
    """In-memory SQLite with all tables created."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)


@pytest.fixture
def cache(db_session_factory) -> HoldingsCacheManager:
    return HoldingsCacheManager(db_session_factory)


SAMPLE_DF = pd.DataFrame({
    "etf_ticker": ["CSPX", "CSPX", "CSPX"],
    "holding_name": ["APPLE INC", "MICROSOFT CORP", "NVIDIA CORP"],
    "holding_isin": ["US0378331005", "US5949181045", "US67066G1040"],
    "holding_ticker": ["AAPL", "MSFT", "NVDA"],
    "holding_sedol": [None, None, None],
    "holding_cusip": [None, None, None],
    "weight_pct": [6.5, 4.9, 7.3],
    "market_value": [46e9, 34e9, 51e9],
    "shares": [188e6, 95e6, 312e6],
    "sector": ["Tech", "Tech", "Tech"],
    "country": ["US", "US", "US"],
    "currency": ["USD", "USD", "USD"],
    "as_of_date": ["2026-03-28", "2026-03-28", "2026-03-28"],
})


# ---------------------------------------------------------------------------
# HoldingsCacheManager — basic operations
# ---------------------------------------------------------------------------


class TestCacheGetSet:
    def test_set_and_get(self, cache) -> None:
        cache.set("CSPX", SAMPLE_DF, source="ISharesFetcher", coverage_pct=95.0)

        result = cache.get("CSPX")

        assert result is not None
        assert result.status == "cached"
        assert result.holdings is not None
        assert len(result.holdings) == 3

    def test_get_returns_none_when_empty(self, cache) -> None:
        result = cache.get("NONEXISTENT")

        assert result is None

    def test_dataframe_roundtrip(self, cache) -> None:
        cache.set("CSPX", SAMPLE_DF, source="ISharesFetcher", coverage_pct=95.0)

        result = cache.get("CSPX")
        df = result.holdings

        assert list(df["holding_name"]) == ["APPLE INC", "MICROSOFT CORP", "NVIDIA CORP"]
        assert list(df["weight_pct"]) == [6.5, 4.9, 7.3]

    def test_coverage_pct_stored(self, cache) -> None:
        cache.set("CSPX", SAMPLE_DF, source="test", coverage_pct=42.5)

        result = cache.get("CSPX")

        assert result.coverage_pct == 42.5

    def test_source_stored(self, cache) -> None:
        cache.set("CSPX", SAMPLE_DF, source="XtrackersFetcher", coverage_pct=90.0)

        result = cache.get("CSPX")

        assert result.source == "XtrackersFetcher"

    def test_message_includes_date(self, cache) -> None:
        cache.set("CSPX", SAMPLE_DF, source="test", coverage_pct=90.0)

        result = cache.get("CSPX")

        assert "aggiornati al" in result.message.lower() or "aggiornati al" in result.message

    def test_case_insensitive_identifier(self, cache) -> None:
        cache.set("cspx", SAMPLE_DF, source="test", coverage_pct=90.0)

        result = cache.get("CSPX")

        assert result is not None

    def test_overwrite_existing(self, cache) -> None:
        cache.set("CSPX", SAMPLE_DF, source="old", coverage_pct=50.0)
        new_df = SAMPLE_DF.head(1).copy()
        cache.set("CSPX", new_df, source="new", coverage_pct=99.0)

        result = cache.get("CSPX")

        assert result.source == "new"
        assert result.coverage_pct == 99.0
        assert len(result.holdings) == 1


# ---------------------------------------------------------------------------
# TTL / freshness
# ---------------------------------------------------------------------------


class TestCacheFreshness:
    def test_is_fresh_true(self, cache) -> None:
        cache.set("CSPX", SAMPLE_DF, source="test", coverage_pct=90.0)

        assert cache.is_fresh("CSPX") is True

    def test_is_fresh_false_when_missing(self, cache) -> None:
        assert cache.is_fresh("NONEXISTENT") is False

    def test_get_returns_none_when_stale(self, cache, db_session_factory) -> None:
        cache.set("CSPX", SAMPLE_DF, source="test", coverage_pct=90.0)

        # Manually make it stale
        from src.storage.models import HoldingsCache
        from sqlalchemy import select
        with db_session_factory() as session:
            entry = session.execute(
                select(HoldingsCache).where(
                    HoldingsCache.etf_identifier == "CSPX"
                )
            ).scalar_one()
            entry.stale_after = datetime.now(timezone.utc) - timedelta(hours=1)
            session.commit()

        result = cache.get("CSPX")
        assert result is None
        assert cache.is_fresh("CSPX") is False


# ---------------------------------------------------------------------------
# Stale fallback
# ---------------------------------------------------------------------------


class TestStaleFallback:
    def test_get_stale_returns_data(self, cache, db_session_factory) -> None:
        cache.set("CSPX", SAMPLE_DF, source="test", coverage_pct=90.0)

        # Make stale
        from src.storage.models import HoldingsCache
        from sqlalchemy import select
        with db_session_factory() as session:
            entry = session.execute(
                select(HoldingsCache).where(
                    HoldingsCache.etf_identifier == "CSPX"
                )
            ).scalar_one()
            entry.stale_after = datetime.now(timezone.utc) - timedelta(days=1)
            session.commit()

        result = cache.get_stale("CSPX")

        assert result is not None
        assert result.status == "cached"
        assert "aggiornamento fallito" in result.message.lower() or "aggiornamento fallito" in result.message
        assert len(result.holdings) == 3

    def test_get_stale_returns_none_when_absent(self, cache) -> None:
        result = cache.get_stale("NONEXISTENT")

        assert result is None


# ---------------------------------------------------------------------------
# Clear
# ---------------------------------------------------------------------------


class TestCacheClear:
    def test_clear_specific(self, cache) -> None:
        cache.set("CSPX", SAMPLE_DF, source="a", coverage_pct=90.0)
        cache.set("IWDA", SAMPLE_DF, source="b", coverage_pct=80.0)

        deleted = cache.clear("CSPX")

        assert deleted == 1
        assert cache.get("CSPX") is None
        assert cache.get("IWDA") is not None

    def test_clear_all(self, cache) -> None:
        cache.set("CSPX", SAMPLE_DF, source="a", coverage_pct=90.0)
        cache.set("IWDA", SAMPLE_DF, source="b", coverage_pct=80.0)

        deleted = cache.clear()

        assert deleted == 2
        assert cache.get("CSPX") is None
        assert cache.get("IWDA") is None

    def test_clear_nonexistent(self, cache) -> None:
        deleted = cache.clear("NONEXISTENT")

        assert deleted == 0


# ---------------------------------------------------------------------------
# Orchestrator integration with cache
# ---------------------------------------------------------------------------


class TestOrchestratorCache:
    @pytest.fixture
    def mock_registry(self):
        registry = FetcherRegistry.__new__(FetcherRegistry)
        registry._fetchers = []
        return registry

    @patch("src.ingestion.orchestrator.resolve_metadata")
    def test_fresh_cache_returns_immediately(
        self, mock_resolve, mock_registry, cache
    ) -> None:
        mock_resolve.return_value = None
        cache.set("CSPX", SAMPLE_DF, source="ISharesFetcher", coverage_pct=95.0)

        orch = FetchOrchestrator(registry=mock_registry, cache=cache)
        result = orch.fetch("CSPX")

        assert result.status == "cached"
        assert result.holdings is not None
        assert len(result.holdings) == 3
        # Should NOT have called resolve_metadata (cache hit before metadata)
        mock_resolve.assert_not_called()

    @patch("src.ingestion.orchestrator.resolve_metadata")
    def test_force_refresh_bypasses_cache(
        self, mock_resolve, mock_registry, cache
    ) -> None:
        mock_resolve.return_value = None
        cache.set("CSPX", SAMPLE_DF, source="old", coverage_pct=90.0)

        # With no fetchers and force_refresh, should fail (no live source)
        orch = FetchOrchestrator(registry=mock_registry, cache=cache)
        result = orch.fetch("CSPX", force_refresh=True)

        assert result.status == "failed"

    @patch("src.ingestion.orchestrator.resolve_metadata")
    def test_successful_fetch_saves_to_cache(
        self, mock_resolve, mock_registry, cache
    ) -> None:
        mock_resolve.return_value = None

        # Add a mock fetcher that succeeds
        mock_fetcher = MagicMock()
        mock_fetcher.can_handle.return_value = 1.0
        mock_fetcher.try_fetch.return_value = FetchResult(
            status="success",
            holdings=SAMPLE_DF,
            message="ok",
            coverage_pct=95.0,
            source="MockFetcher",
        )
        mock_registry._fetchers = [mock_fetcher]

        orch = FetchOrchestrator(registry=mock_registry, cache=cache)
        result = orch.fetch("CSPX")

        assert result.status == "success"

        # Verify it was cached
        cached = cache.get("CSPX")
        assert cached is not None
        assert cached.source == "MockFetcher"

    @patch("src.ingestion.orchestrator.resolve_metadata")
    def test_stale_cache_fallback_on_fetch_failure(
        self, mock_resolve, mock_registry, cache, db_session_factory
    ) -> None:
        mock_resolve.return_value = None
        cache.set("CSPX", SAMPLE_DF, source="old", coverage_pct=90.0)

        # Make stale
        from src.storage.models import HoldingsCache
        from sqlalchemy import select
        with db_session_factory() as session:
            entry = session.execute(
                select(HoldingsCache).where(
                    HoldingsCache.etf_identifier == "CSPX"
                )
            ).scalar_one()
            entry.stale_after = datetime.now(timezone.utc) - timedelta(days=1)
            session.commit()

        # No fetchers → live fetch fails → should use stale cache
        orch = FetchOrchestrator(registry=mock_registry, cache=cache)
        result = orch.fetch("CSPX")

        assert result.status == "cached"
        assert "aggiornamento fallito" in result.message.lower() or "aggiornamento fallito" in result.message

    @patch("src.ingestion.orchestrator.resolve_metadata")
    def test_no_cache_no_fetchers_fails(
        self, mock_resolve, mock_registry, cache
    ) -> None:
        mock_resolve.return_value = None

        orch = FetchOrchestrator(registry=mock_registry, cache=cache)
        result = orch.fetch("UNKNOWN")

        assert result.status == "failed"

    def test_no_cache_manager_works(self, mock_registry) -> None:
        """Orchestrator works without cache (cache=None)."""
        orch = FetchOrchestrator(registry=mock_registry, cache=None)
        result = orch.fetch("CSPX")

        # Should fail normally (no fetchers), not crash
        assert result.status == "failed"
