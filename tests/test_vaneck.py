"""Tests for the VanEck UCITS fetcher."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from src.ingestion.base_fetcher import HOLDINGS_SCHEMA
from src.ingestion.vaneck import VanEckFetcher, VANECK_PRODUCTS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MOCK_HOLDINGS_PAGE_HTML = """
<html>
<body>
<div class="holdings-container">
  <ve-holdingsblock data-blockid="194732" data-pageid="233164"></ve-holdingsblock>
</div>
</body>
</html>
"""

MOCK_DATASET_RESPONSE = {
    "AsOfDate": "2026-04-06T00:00:00",
    "Holdings": [
        {
            "Label": "NVDA",
            "HoldingName": "Nvidia Corp",
            "ISIN": "US67066G1040",
            "CUSIP": "67066G104",
            "FIGI": "BBG000BBJQV0",
            "Weight": "19.36",
            "MV": "8,415,615,950",
            "Shares": "47,374,555",
            "Sector": "Information Technology",
            "Country": "United States",
            "CurrencyCode": "USD",
            "AsOfDate": "04/06/2026 00:00:00",
            "Ticker": "SMH",
        },
        {
            "Label": "TSM",
            "HoldingName": "Taiwan Semiconductor Manufacturing",
            "ISIN": "US8740391003",
            "CUSIP": "874039100",
            "FIGI": "BBG000BD8ZK0",
            "Weight": "11.52",
            "MV": "5,006,430,000",
            "Shares": "25,100,000",
            "Sector": "Information Technology",
            "Country": "Taiwan",
            "CurrencyCode": "USD",
            "AsOfDate": "04/06/2026 00:00:00",
            "Ticker": "SMH",
        },
        {
            "Label": "ASML",
            "HoldingName": "ASML Holding NV",
            "ISIN": "NL0010273215",
            "CUSIP": None,
            "FIGI": "BBG000C3LR12",
            "Weight": "7.84",
            "MV": "3,405,000,000",
            "Shares": "5,200,000",
            "Sector": "Information Technology",
            "Country": "Netherlands",
            "CurrencyCode": "EUR",
            "AsOfDate": "04/06/2026 00:00:00",
            "Ticker": "SMH",
        },
        {
            "Label": None,
            "HoldingName": "US Dollar Cash",
            "ISIN": None,
            "CUSIP": None,
            "FIGI": None,
            "Weight": "0.12",
            "MV": "52,000",
            "Shares": None,
            "Sector": "Cash",
            "Country": "United States",
            "CurrencyCode": "USD",
            "AsOfDate": "04/06/2026 00:00:00",
            "Ticker": "SMH",
        },
    ],
}


@pytest.fixture
def fetcher() -> VanEckFetcher:
    return VanEckFetcher()


def _mock_page_response() -> MagicMock:
    """Create a mock response returning holdings page HTML."""
    resp = MagicMock()
    resp.status_code = 200
    resp.text = MOCK_HOLDINGS_PAGE_HTML
    resp.raise_for_status = MagicMock()
    return resp


def _mock_dataset_response() -> MagicMock:
    """Create a mock response returning dataset JSON."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = MOCK_DATASET_RESPONSE
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# can_handle
# ---------------------------------------------------------------------------


class TestCanHandle:
    def test_known_tickers(self, fetcher: VanEckFetcher) -> None:
        for t in ("SMH", "VVSM", "DFNS", "NUCL", "GDX", "GDXJ", "MOAT"):
            assert fetcher.can_handle(t) == 0.95, f"Should handle {t}"

    def test_known_isins(self, fetcher: VanEckFetcher) -> None:
        for isin in ("IE00BMC38736", "IE000YYE6WK5", "NL0011683594"):
            assert fetcher.can_handle(isin) == 0.95, f"Should handle {isin}"

    def test_nl_isin_unknown(self, fetcher: VanEckFetcher) -> None:
        assert fetcher.can_handle("NL9999999999") == 0.7

    def test_ie_isin_unknown(self, fetcher: VanEckFetcher) -> None:
        assert fetcher.can_handle("IE9999999999") == 0.3

    def test_unknown_ticker(self, fetcher: VanEckFetcher) -> None:
        assert fetcher.can_handle("ZZZZZ") == 0.2

    def test_empty_string(self, fetcher: VanEckFetcher) -> None:
        assert fetcher.can_handle("") == 0.0

    def test_case_insensitive(self, fetcher: VanEckFetcher) -> None:
        assert fetcher.can_handle("smh") == 0.95
        assert fetcher.can_handle("  SMH  ") == 0.95


# ---------------------------------------------------------------------------
# Identifier resolution
# ---------------------------------------------------------------------------


class TestIdentifierResolution:
    def test_ticker_to_ticker(self, fetcher: VanEckFetcher) -> None:
        assert fetcher._resolve_ticker("SMH") == "SMH"

    def test_isin_to_ticker(self, fetcher: VanEckFetcher) -> None:
        assert fetcher._resolve_ticker("IE00BMC38736") == "SMH"

    def test_unknown_passthrough(self, fetcher: VanEckFetcher) -> None:
        assert fetcher._resolve_ticker("UNKNOWN") == "UNKNOWN"

    def test_ticker_to_slug(self, fetcher: VanEckFetcher) -> None:
        assert fetcher._resolve_slug("SMH") == "semiconductor-etf"
        assert fetcher._resolve_slug("DFNS") == "defense-etf"

    def test_isin_to_slug(self, fetcher: VanEckFetcher) -> None:
        assert fetcher._resolve_slug("IE00BMC38736") == "semiconductor-etf"

    def test_unknown_slug_raises(self, fetcher: VanEckFetcher) -> None:
        with pytest.raises(ValueError, match="No VanEck URL slug"):
            fetcher._resolve_slug("UNKNOWN")

    def test_case_insensitive_resolution(self, fetcher: VanEckFetcher) -> None:
        assert fetcher._resolve_ticker("smh") == "SMH"
        assert fetcher._resolve_slug("smh") == "semiconductor-etf"


# ---------------------------------------------------------------------------
# Block ID scraping
# ---------------------------------------------------------------------------


class TestScrapeBlockIds:
    @patch("src.ingestion.vaneck._retry_request")
    def test_extracts_ids(self, mock_req, fetcher: VanEckFetcher) -> None:
        mock_req.return_value = _mock_page_response()

        block_id, page_id = fetcher._scrape_block_ids("semiconductor-etf")

        assert block_id == "194732"
        assert page_id == "233164"

    @patch("src.ingestion.vaneck._retry_request")
    def test_missing_block_raises(self, mock_req, fetcher: VanEckFetcher) -> None:
        resp = MagicMock()
        resp.text = "<html><body>No holdings block here</body></html>"
        resp.raise_for_status = MagicMock()
        mock_req.return_value = resp

        with pytest.raises(ValueError, match="Could not find"):
            fetcher._scrape_block_ids("semiconductor-etf")


# ---------------------------------------------------------------------------
# fetch_holdings — full pipeline
# ---------------------------------------------------------------------------


class TestFetchHoldings:
    @patch("src.ingestion.vaneck._retry_request")
    def test_schema_matches(self, mock_req, fetcher: VanEckFetcher) -> None:
        mock_req.side_effect = [_mock_page_response(), _mock_dataset_response()]

        df = fetcher.fetch_holdings("SMH")

        assert list(df.columns) == HOLDINGS_SCHEMA

    @patch("src.ingestion.vaneck._retry_request")
    def test_parses_holdings(self, mock_req, fetcher: VanEckFetcher) -> None:
        mock_req.side_effect = [_mock_page_response(), _mock_dataset_response()]

        df = fetcher.fetch_holdings("SMH")

        # 3 equity rows (cash filtered out)
        assert len(df) == 3
        assert "Nvidia Corp" in df["holding_name"].values
        assert "Taiwan Semiconductor Manufacturing" in df["holding_name"].values
        assert "ASML Holding NV" in df["holding_name"].values

    @patch("src.ingestion.vaneck._retry_request")
    def test_cash_filtered(self, mock_req, fetcher: VanEckFetcher) -> None:
        mock_req.side_effect = [_mock_page_response(), _mock_dataset_response()]

        df = fetcher.fetch_holdings("SMH")

        assert "US Dollar Cash" not in df["holding_name"].values

    @patch("src.ingestion.vaneck._retry_request")
    def test_weight_parsed(self, mock_req, fetcher: VanEckFetcher) -> None:
        mock_req.side_effect = [_mock_page_response(), _mock_dataset_response()]

        df = fetcher.fetch_holdings("SMH")

        nvda = df.loc[df["holding_name"] == "Nvidia Corp"]
        assert abs(nvda["weight_pct"].iloc[0] - 19.36) < 0.01

    @patch("src.ingestion.vaneck._retry_request")
    def test_market_value_parsed(self, mock_req, fetcher: VanEckFetcher) -> None:
        mock_req.side_effect = [_mock_page_response(), _mock_dataset_response()]

        df = fetcher.fetch_holdings("SMH")

        nvda = df.loc[df["holding_name"] == "Nvidia Corp"]
        assert nvda["market_value"].iloc[0] == 8_415_615_950

    @patch("src.ingestion.vaneck._retry_request")
    def test_shares_parsed(self, mock_req, fetcher: VanEckFetcher) -> None:
        mock_req.side_effect = [_mock_page_response(), _mock_dataset_response()]

        df = fetcher.fetch_holdings("SMH")

        nvda = df.loc[df["holding_name"] == "Nvidia Corp"]
        assert nvda["shares"].iloc[0] == 47_374_555

    @patch("src.ingestion.vaneck._retry_request")
    def test_isin_present(self, mock_req, fetcher: VanEckFetcher) -> None:
        mock_req.side_effect = [_mock_page_response(), _mock_dataset_response()]

        df = fetcher.fetch_holdings("SMH")

        assert "US67066G1040" in df["holding_isin"].values

    @patch("src.ingestion.vaneck._retry_request")
    def test_holding_ticker_present(self, mock_req, fetcher: VanEckFetcher) -> None:
        mock_req.side_effect = [_mock_page_response(), _mock_dataset_response()]

        df = fetcher.fetch_holdings("SMH")

        assert "NVDA" in df["holding_ticker"].values

    @patch("src.ingestion.vaneck._retry_request")
    def test_cusip_present(self, mock_req, fetcher: VanEckFetcher) -> None:
        mock_req.side_effect = [_mock_page_response(), _mock_dataset_response()]

        df = fetcher.fetch_holdings("SMH")

        assert "67066G104" in df["holding_cusip"].values

    @patch("src.ingestion.vaneck._retry_request")
    def test_as_of_date_formatted(self, mock_req, fetcher: VanEckFetcher) -> None:
        mock_req.side_effect = [_mock_page_response(), _mock_dataset_response()]

        df = fetcher.fetch_holdings("SMH")

        assert df["as_of_date"].iloc[0] == "2026-04-06"

    @patch("src.ingestion.vaneck._retry_request")
    def test_etf_ticker_column(self, mock_req, fetcher: VanEckFetcher) -> None:
        mock_req.side_effect = [_mock_page_response(), _mock_dataset_response()]

        df = fetcher.fetch_holdings("SMH")

        assert (df["etf_ticker"] == "SMH").all()

    @patch("src.ingestion.vaneck._retry_request")
    def test_isin_lookup(self, mock_req, fetcher: VanEckFetcher) -> None:
        """Fetching by ISIN should resolve ticker for etf_ticker column."""
        mock_req.side_effect = [_mock_page_response(), _mock_dataset_response()]

        df = fetcher.fetch_holdings("IE00BMC38736")

        assert (df["etf_ticker"] == "SMH").all()

    @patch("src.ingestion.vaneck._retry_request")
    def test_sector_and_country(self, mock_req, fetcher: VanEckFetcher) -> None:
        mock_req.side_effect = [_mock_page_response(), _mock_dataset_response()]

        df = fetcher.fetch_holdings("SMH")

        nvda = df.loc[df["holding_name"] == "Nvidia Corp"]
        assert nvda["sector"].iloc[0] == "Information Technology"
        assert nvda["country"].iloc[0] == "United States"
        assert nvda["currency"].iloc[0] == "USD"


# ---------------------------------------------------------------------------
# try_fetch — never raises
# ---------------------------------------------------------------------------


class TestTryFetch:
    @patch("src.ingestion.vaneck._retry_request")
    def test_try_fetch_success(self, mock_req, fetcher: VanEckFetcher) -> None:
        mock_req.side_effect = [_mock_page_response(), _mock_dataset_response()]

        result = fetcher.try_fetch("SMH")

        assert result.status == "success"
        assert result.holdings is not None
        assert result.source == "VanEckFetcher"
        assert result.coverage_pct > 0

    @patch("src.ingestion.vaneck._retry_request")
    def test_try_fetch_network_failure(self, mock_req, fetcher: VanEckFetcher) -> None:
        mock_req.side_effect = requests.exceptions.ConnectionError("network down")

        result = fetcher.try_fetch("SMH")

        assert result.status == "failed"
        assert result.holdings is None
        assert "failed" in result.message.lower()

    @patch("src.ingestion.vaneck._retry_request")
    def test_try_fetch_empty_holdings(self, mock_req, fetcher: VanEckFetcher) -> None:
        """API returns empty holdings list."""
        empty_resp = MagicMock()
        empty_resp.json.return_value = {"AsOfDate": "2026-04-06", "Holdings": []}
        empty_resp.raise_for_status = MagicMock()
        mock_req.side_effect = [_mock_page_response(), empty_resp]

        result = fetcher.try_fetch("SMH")

        assert result.status == "failed"


# ---------------------------------------------------------------------------
# Weights sum
# ---------------------------------------------------------------------------


class TestWeightsSum:
    @patch("src.ingestion.vaneck._retry_request")
    def test_weights_are_positive(self, mock_req, fetcher: VanEckFetcher) -> None:
        mock_req.side_effect = [_mock_page_response(), _mock_dataset_response()]

        df = fetcher.fetch_holdings("SMH")

        assert (df["weight_pct"] > 0).all()
        # Sum of mock weights: 19.36 + 11.52 + 7.84 = 38.72
        assert abs(df["weight_pct"].sum() - 38.72) < 0.1


# ---------------------------------------------------------------------------
# Registry integration
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_vaneck_discovered(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        names = {type(f).__name__ for f in registry.fetchers}
        assert "VanEckFetcher" in names

    def test_routes_vaneck_ticker(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        fetcher = registry.get_fetcher("SMH")
        assert type(fetcher).__name__ == "VanEckFetcher"

    def test_routes_vaneck_isin(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        fetcher = registry.get_fetcher("IE00BMC38736")
        assert type(fetcher).__name__ == "VanEckFetcher"


# ---------------------------------------------------------------------------
# Normalisation edge cases
# ---------------------------------------------------------------------------


class TestNormalisationEdgeCases:
    def test_null_cusip_becomes_none(self, fetcher: VanEckFetcher) -> None:
        """CUSIP=None in API should become None, not string 'None'."""
        holdings = [{
            "Label": "ASML",
            "HoldingName": "ASML Holding NV",
            "ISIN": "NL0010273215",
            "CUSIP": None,
            "Weight": "7.84",
            "MV": "3,405,000,000",
            "Shares": "5,200,000",
            "Sector": "Information Technology",
            "Country": "Netherlands",
            "CurrencyCode": "EUR",
            "AsOfDate": "04/06/2026 00:00:00",
        }]
        df = fetcher._normalise(holdings, "SMH")
        assert df["holding_cusip"].iloc[0] is None

    def test_empty_weight_becomes_none(self, fetcher: VanEckFetcher) -> None:
        holdings = [{
            "HoldingName": "Test",
            "Weight": "",
            "MV": "",
            "Shares": "",
        }]
        df = fetcher._normalise(holdings, "TEST")
        assert df["weight_pct"].iloc[0] is None
