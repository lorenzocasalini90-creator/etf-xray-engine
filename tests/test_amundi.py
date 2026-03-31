"""Tests for the Amundi fetcher."""

import pytest

from src.ingestion.amundi import AmundiFetcher


@pytest.fixture
def fetcher() -> AmundiFetcher:
    return AmundiFetcher()


# ---------------------------------------------------------------------------
# can_handle
# ---------------------------------------------------------------------------


class TestCanHandle:
    def test_recognises_amundi_tickers(self, fetcher: AmundiFetcher) -> None:
        for t in ("CW8", "MWRD", "PAEEM", "PANX", "PCEU", "AEEM", "ANEW"):
            assert fetcher.can_handle(t), f"Should handle {t}"

    def test_recognises_amundi_isins(self, fetcher: AmundiFetcher) -> None:
        assert fetcher.can_handle("LU1681043599")  # CW8
        assert fetcher.can_handle("LU2090063673")
        assert fetcher.can_handle("FR0010756098")

    def test_rejects_unknown(self, fetcher: AmundiFetcher) -> None:
        for t in ("CSPX", "VOO", "VWCE", "SPY", "QQQ"):
            assert not fetcher.can_handle(t), f"Should NOT handle {t}"

    def test_rejects_non_amundi_isins(self, fetcher: AmundiFetcher) -> None:
        assert not fetcher.can_handle("IE00B5BMR087")  # iShares
        assert not fetcher.can_handle("US9229087690")  # Vanguard

    def test_case_insensitive(self, fetcher: AmundiFetcher) -> None:
        assert fetcher.can_handle("cw8")
        assert fetcher.can_handle("  CW8  ")
        assert fetcher.can_handle("lu1681043599")


# ---------------------------------------------------------------------------
# fetch_holdings — NotImplementedError
# ---------------------------------------------------------------------------


class TestFetchHoldings:
    def test_raises_not_implemented(self, fetcher: AmundiFetcher) -> None:
        with pytest.raises(NotImplementedError, match="Amundi"):
            fetcher.fetch_holdings("CW8")

    def test_raises_not_implemented_with_isin(self, fetcher: AmundiFetcher) -> None:
        with pytest.raises(NotImplementedError, match="not yet implemented"):
            fetcher.fetch_holdings("LU1681043599")


# ---------------------------------------------------------------------------
# Registry integration
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_three_fetchers_discovered(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        names = sorted(type(f).__name__ for f in registry.fetchers)
        assert names == ["AmundiFetcher", "ISharesFetcher", "VanguardFetcher"]

    def test_routes_amundi(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        assert type(registry.get_fetcher("CW8")).__name__ == "AmundiFetcher"
        assert type(registry.get_fetcher("CSPX")).__name__ == "ISharesFetcher"
        assert type(registry.get_fetcher("VWCE")).__name__ == "VanguardFetcher"
