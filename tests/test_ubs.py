"""Tests for the UBS UCITS fetcher (stub)."""

import pytest

from src.ingestion.ubs import UBSFetcher, UBS_PRODUCTS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fetcher() -> UBSFetcher:
    return UBSFetcher()


# ---------------------------------------------------------------------------
# can_handle
# ---------------------------------------------------------------------------


class TestCanHandle:
    def test_known_tickers(self, fetcher: UBSFetcher) -> None:
        for t in ("WRDA", "SP5U", "EMUL", "ACWU", "WSRI"):
            assert fetcher.can_handle(t) == 0.95, f"Should handle {t}"

    def test_known_isins(self, fetcher: UBSFetcher) -> None:
        for isin in ("IE00B7KQ7B66", "IE00BD4TXS21", "LU0147308422", "IE00BYM11H29"):
            assert fetcher.can_handle(isin) == 0.95, f"Should handle {isin}"

    def test_ie_isin_unknown(self, fetcher: UBSFetcher) -> None:
        assert fetcher.can_handle("IE9999999999") == 0.4

    def test_lu_isin_unknown(self, fetcher: UBSFetcher) -> None:
        assert fetcher.can_handle("LU9999999999") == 0.4

    def test_unknown_ticker(self, fetcher: UBSFetcher) -> None:
        assert fetcher.can_handle("ZZZZZ") == 0.2

    def test_empty_string(self, fetcher: UBSFetcher) -> None:
        assert fetcher.can_handle("") == 0.0

    def test_case_insensitive(self, fetcher: UBSFetcher) -> None:
        assert fetcher.can_handle("wrda") == 0.95
        assert fetcher.can_handle("  WRDA  ") == 0.95


# ---------------------------------------------------------------------------
# Identifier resolution
# ---------------------------------------------------------------------------


class TestIdentifierResolution:
    def test_ticker_to_isin(self, fetcher: UBSFetcher) -> None:
        assert fetcher._resolve_isin("WRDA") == "IE00B7KQ7B66"

    def test_isin_passthrough(self, fetcher: UBSFetcher) -> None:
        assert fetcher._resolve_isin("IE00B7KQ7B66") == "IE00B7KQ7B66"

    def test_isin_to_ticker(self, fetcher: UBSFetcher) -> None:
        assert fetcher._resolve_ticker("IE00B7KQ7B66") == "WRDA"

    def test_unknown_passthrough(self, fetcher: UBSFetcher) -> None:
        assert fetcher._resolve_ticker("IE9999999999") == "IE9999999999"

    def test_case_insensitive_resolution(self, fetcher: UBSFetcher) -> None:
        assert fetcher._resolve_isin("wrda") == "IE00B7KQ7B66"


# ---------------------------------------------------------------------------
# fetch_holdings — stub (always fails)
# ---------------------------------------------------------------------------


class TestFetchHoldings:
    def test_raises_not_implemented(self, fetcher: UBSFetcher) -> None:
        with pytest.raises(NotImplementedError, match="UBS fetcher not yet implemented"):
            fetcher.fetch_holdings("WRDA")

    def test_raises_for_isin(self, fetcher: UBSFetcher) -> None:
        with pytest.raises(NotImplementedError):
            fetcher.fetch_holdings("IE00B7KQ7B66")


# ---------------------------------------------------------------------------
# try_fetch — never raises, returns failed
# ---------------------------------------------------------------------------


class TestTryFetch:
    def test_try_fetch_returns_failed(self, fetcher: UBSFetcher) -> None:
        result = fetcher.try_fetch("WRDA")

        assert result.status == "failed"
        assert result.holdings is None
        assert "UBSFetcher" in result.source
        assert "UBS" in result.message

    def test_try_fetch_never_raises(self, fetcher: UBSFetcher) -> None:
        """try_fetch should never raise, even though fetch_holdings does."""
        result = fetcher.try_fetch("IE00B7KQ7B66")
        assert result.status == "failed"


# ---------------------------------------------------------------------------
# Registry integration
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_ubs_discovered(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        names = {type(f).__name__ for f in registry.fetchers}
        assert "UBSFetcher" in names

    def test_routes_ubs_isin(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        fetcher = registry.get_fetcher("IE00B7KQ7B66")
        assert type(fetcher).__name__ == "UBSFetcher"

    def test_routes_ubs_ticker(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        fetcher = registry.get_fetcher("WRDA")
        assert type(fetcher).__name__ == "UBSFetcher"


# ---------------------------------------------------------------------------
# Product map integrity
# ---------------------------------------------------------------------------


class TestProductMap:
    def test_all_isins_12_chars(self) -> None:
        for ticker, isin in UBS_PRODUCTS.items():
            assert len(isin) == 12, f"{ticker} ISIN '{isin}' is not 12 chars"
            assert isin[:2].isalpha(), f"{ticker} ISIN '{isin}' doesn't start with country code"

    def test_no_duplicate_isins(self) -> None:
        isins = list(UBS_PRODUCTS.values())
        assert len(isins) == len(set(isins)), "Duplicate ISINs in UBS_PRODUCTS"
