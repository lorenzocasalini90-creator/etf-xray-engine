"""Tests for the Xtrackers (DWS) fetcher."""

import io
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingestion.base_fetcher import HOLDINGS_SCHEMA
from src.ingestion.xtrackers import XtrackersFetcher, XTRACKERS_PRODUCTS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _build_mock_excel() -> bytes:
    """Build a mock Excel file mimicking DWS holdings format.

    Real DWS layout (0-indexed rows):
    - Row 0: fund name / metadata
    - Row 1: disclaimer text
    - Row 2: empty
    - Row 3: column headers (Name, ISIN, Country, ...)
    - Row 4+: data
    Sheet name is the as-of date.
    """
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.title = "2026-04-01"

    # Row 1: metadata
    ws.append(["Xtrackers MSCI World UCITS ETF 1C"])
    # Row 2: disclaimer
    ws.append(["Disclaimer: this data is for information only"])
    # Row 3: empty
    ws.append([])
    # Row 4: header
    ws.append([
        "Row", "Name", "ISIN", "Country", "Currency", "Exchange",
        "Type of Security", "Rating", "Primary Listing",
        "Industry Classification", "Weighting",
    ])
    # Row 5+: data
    ws.append([1, "APPLE INC", "US0378331005", "United States", "USD",
               "NASDAQ", "Equity", None, "NASDAQ", "Information Technology", 0.0514])
    ws.append([2, "MICROSOFT CORP", "US5949181045", "United States", "USD",
               "NASDAQ", "Equity", None, "NASDAQ", "Information Technology", 0.0412])
    ws.append([3, "NVIDIA CORP", "US67066G1040", "United States", "USD",
               "NASDAQ", "Equity", None, "NASDAQ", "Information Technology", 0.0380])
    ws.append([4, "US DOLLAR CASH", None, "United States", "USD",
               None, "Cash", None, None, None, 0.0015])

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


MOCK_EXCEL_BYTES = _build_mock_excel()


MOCK_JSON_RESPONSE = {
    "asOfDate": "2026-04-01",
    "body": [
        {
            "header": {"value": "US0378331005"},
            "column_0": {"value": "APPLE INC"},
            "column_1": {"value": "5.142%", "sortValue": 5.14},
            "column_2": {"value": "2,500,000,000", "sortValue": 2500000000},
            "column_3": {"value": "United States"},
            "column_4": {"value": "Information Technology"},
            "column_5": {"value": "Equity"},
        },
        {
            "header": {"value": "US5949181045"},
            "column_0": {"value": "MICROSOFT CORP"},
            "column_1": {"value": "4.120%", "sortValue": 4.12},
            "column_2": {"value": "2,000,000,000", "sortValue": 2000000000},
            "column_3": {"value": "United States"},
            "column_4": {"value": "Information Technology"},
            "column_5": {"value": "Equity"},
        },
        {
            "header": {"value": None},
            "column_0": {"value": "US DOLLAR CASH"},
            "column_1": {"value": "0.150%", "sortValue": 0.15},
            "column_2": {"value": "75,000,000", "sortValue": 75000000},
            "column_3": {"value": "United States"},
            "column_4": {"value": None},
            "column_5": {"value": "Cash"},
        },
    ],
}


@pytest.fixture
def fetcher() -> XtrackersFetcher:
    return XtrackersFetcher()


def _mock_excel_response() -> MagicMock:
    """Create a mock response that returns Excel bytes."""
    resp = MagicMock()
    resp.status_code = 200
    resp.content = MOCK_EXCEL_BYTES
    resp.raise_for_status = MagicMock()
    return resp


def _mock_json_response() -> MagicMock:
    """Create a mock response that returns JSON."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = MOCK_JSON_RESPONSE
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# can_handle
# ---------------------------------------------------------------------------


class TestCanHandle:
    def test_known_tickers(self, fetcher: XtrackersFetcher) -> None:
        for t in ("XDWD", "XMEU", "XMME", "XDAX"):
            assert fetcher.can_handle(t) == 0.95, f"Should handle {t}"

    def test_known_isins(self, fetcher: XtrackersFetcher) -> None:
        for isin in ("IE00BK1PV551", "LU0274209237"):
            assert fetcher.can_handle(isin) == 0.95, f"Should handle {isin}"

    def test_de_isin(self, fetcher: XtrackersFetcher) -> None:
        assert fetcher.can_handle("DE0005933931") == 0.8

    def test_lu_isin(self, fetcher: XtrackersFetcher) -> None:
        # Unknown LU ISIN (not in XTRACKERS_PRODUCTS)
        assert fetcher.can_handle("LU9999999999") == 0.5

    def test_ie_isin_unknown(self, fetcher: XtrackersFetcher) -> None:
        # Unknown IE ISIN (not in XTRACKERS_PRODUCTS)
        assert fetcher.can_handle("IE9999999999") == 0.4

    def test_unknown_ticker(self, fetcher: XtrackersFetcher) -> None:
        assert fetcher.can_handle("ZZZZZ") == 0.3

    def test_empty_string(self, fetcher: XtrackersFetcher) -> None:
        assert fetcher.can_handle("") == 0.0

    def test_case_insensitive(self, fetcher: XtrackersFetcher) -> None:
        assert fetcher.can_handle("xdwd") == 0.95
        assert fetcher.can_handle("  XDWD  ") == 0.95


# ---------------------------------------------------------------------------
# fetch_holdings — Excel path
# ---------------------------------------------------------------------------


class TestFetchExcel:
    @patch("src.ingestion.xtrackers._retry_request")
    def test_excel_schema_matches(self, mock_req, fetcher: XtrackersFetcher) -> None:
        mock_req.return_value = _mock_excel_response()

        df = fetcher.fetch_holdings("XDWD")

        assert list(df.columns) == HOLDINGS_SCHEMA

    @patch("src.ingestion.xtrackers._retry_request")
    def test_excel_parses_holdings(self, mock_req, fetcher: XtrackersFetcher) -> None:
        mock_req.return_value = _mock_excel_response()

        df = fetcher.fetch_holdings("XDWD")

        # 3 equity rows (cash filtered out)
        assert len(df) == 3
        assert "APPLE INC" in df["holding_name"].values
        assert "MICROSOFT CORP" in df["holding_name"].values
        assert "NVIDIA CORP" in df["holding_name"].values

    @patch("src.ingestion.xtrackers._retry_request")
    def test_excel_weight_converted_to_pct(self, mock_req, fetcher: XtrackersFetcher) -> None:
        mock_req.return_value = _mock_excel_response()

        df = fetcher.fetch_holdings("XDWD")

        # 0.0514 decimal → 5.14%
        apple_row = df.loc[df["holding_name"] == "APPLE INC"]
        assert abs(apple_row["weight_pct"].iloc[0] - 5.14) < 0.01

    @patch("src.ingestion.xtrackers._retry_request")
    def test_excel_isin_present(self, mock_req, fetcher: XtrackersFetcher) -> None:
        mock_req.return_value = _mock_excel_response()

        df = fetcher.fetch_holdings("XDWD")

        assert "US0378331005" in df["holding_isin"].values

    @patch("src.ingestion.xtrackers._retry_request")
    def test_excel_as_of_date_from_sheet(self, mock_req, fetcher: XtrackersFetcher) -> None:
        mock_req.return_value = _mock_excel_response()

        df = fetcher.fetch_holdings("XDWD")

        assert df["as_of_date"].iloc[0] == "2026-04-01"

    @patch("src.ingestion.xtrackers._retry_request")
    def test_excel_etf_ticker_column(self, mock_req, fetcher: XtrackersFetcher) -> None:
        mock_req.return_value = _mock_excel_response()

        df = fetcher.fetch_holdings("XDWD")

        assert (df["etf_ticker"] == "XDWD").all()

    @patch("src.ingestion.xtrackers._retry_request")
    def test_excel_isin_lookup(self, mock_req, fetcher: XtrackersFetcher) -> None:
        """Fetching by ISIN should resolve ticker for etf_ticker column."""
        mock_req.return_value = _mock_excel_response()

        df = fetcher.fetch_holdings("IE00BK1PV551")

        assert (df["etf_ticker"] == "XDWD").all()


# ---------------------------------------------------------------------------
# fetch_holdings — JSON API fallback
# ---------------------------------------------------------------------------


class TestFetchJsonFallback:
    @patch("src.ingestion.xtrackers._retry_request")
    def test_json_fallback_on_excel_failure(self, mock_req, fetcher: XtrackersFetcher) -> None:
        """If Excel fails, JSON API is used as fallback."""
        excel_exc = requests.exceptions.HTTPError("404 Not Found")
        mock_req.side_effect = [excel_exc, _mock_json_response()]

        df = fetcher.fetch_holdings("XDWD")

        # 2 equity rows (cash filtered out)
        assert len(df) == 2
        assert list(df.columns) == HOLDINGS_SCHEMA

    @patch("src.ingestion.xtrackers._retry_request")
    def test_json_parses_holdings(self, mock_req, fetcher: XtrackersFetcher) -> None:
        mock_req.side_effect = [
            requests.exceptions.HTTPError("fail"),
            _mock_json_response(),
        ]

        df = fetcher.fetch_holdings("XDWD")

        assert "APPLE INC" in df["holding_name"].values
        assert abs(df.loc[df["holding_name"] == "APPLE INC", "weight_pct"].iloc[0] - 5.14) < 0.01

    @patch("src.ingestion.xtrackers._retry_request")
    def test_json_market_value(self, mock_req, fetcher: XtrackersFetcher) -> None:
        mock_req.side_effect = [
            requests.exceptions.HTTPError("fail"),
            _mock_json_response(),
        ]

        df = fetcher.fetch_holdings("XDWD")

        apple_mv = df.loc[df["holding_name"] == "APPLE INC", "market_value"].iloc[0]
        assert apple_mv == 2_500_000_000


# ---------------------------------------------------------------------------
# try_fetch — never raises
# ---------------------------------------------------------------------------


class TestTryFetch:
    @patch("src.ingestion.xtrackers._retry_request")
    def test_try_fetch_success(self, mock_req, fetcher: XtrackersFetcher) -> None:
        mock_req.return_value = _mock_excel_response()

        result = fetcher.try_fetch("XDWD")

        assert result.status == "success"
        assert result.holdings is not None
        assert result.source == "XtrackersFetcher"
        assert result.coverage_pct > 0

    @patch("src.ingestion.xtrackers._retry_request")
    def test_try_fetch_total_failure(self, mock_req, fetcher: XtrackersFetcher) -> None:
        """If both Excel and JSON fail, try_fetch returns failed — no exception."""
        mock_req.side_effect = requests.exceptions.ConnectionError("network down")

        result = fetcher.try_fetch("XDWD")

        assert result.status == "failed"
        assert result.holdings is None
        assert "failed" in result.message.lower()


# ---------------------------------------------------------------------------
# Non-equity filtering
# ---------------------------------------------------------------------------


class TestFilterNonEquity:
    @patch("src.ingestion.xtrackers._retry_request")
    def test_cash_rows_filtered(self, mock_req, fetcher: XtrackersFetcher) -> None:
        mock_req.return_value = _mock_excel_response()

        df = fetcher.fetch_holdings("XDWD")

        # The mock has 4 rows: 3 equity + 1 cash. Cash should be filtered.
        assert len(df) == 3
        assert "US DOLLAR CASH" not in df["holding_name"].values


# ---------------------------------------------------------------------------
# Registry integration
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_xtrackers_discovered(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        names = {type(f).__name__ for f in registry.fetchers}
        assert "XtrackersFetcher" in names

    def test_routes_xtrackers_ticker(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        fetcher = registry.get_fetcher("XDWD")
        assert type(fetcher).__name__ == "XtrackersFetcher"

    def test_routes_xtrackers_isin(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        fetcher = registry.get_fetcher("IE00BK1PV551")
        assert type(fetcher).__name__ == "XtrackersFetcher"


# ---------------------------------------------------------------------------
# Identifier resolution
# ---------------------------------------------------------------------------


class TestIdentifierResolution:
    def test_ticker_to_isin(self, fetcher: XtrackersFetcher) -> None:
        assert fetcher._resolve_isin("XDWD") == "IE00BK1PV551"

    def test_isin_passthrough(self, fetcher: XtrackersFetcher) -> None:
        assert fetcher._resolve_isin("IE00BK1PV551") == "IE00BK1PV551"

    def test_isin_to_ticker(self, fetcher: XtrackersFetcher) -> None:
        assert fetcher._resolve_ticker("IE00BK1PV551") == "XDWD"

    def test_unknown_isin_passthrough(self, fetcher: XtrackersFetcher) -> None:
        assert fetcher._resolve_ticker("IE9999999999") == "IE9999999999"

    def test_case_insensitive_resolution(self, fetcher: XtrackersFetcher) -> None:
        assert fetcher._resolve_isin("xdwd") == "IE00BK1PV551"


# ---------------------------------------------------------------------------
# Weights sum
# ---------------------------------------------------------------------------


class TestWeightsSum:
    @patch("src.ingestion.xtrackers._retry_request")
    def test_weights_are_positive(self, mock_req, fetcher: XtrackersFetcher) -> None:
        mock_req.return_value = _mock_excel_response()

        df = fetcher.fetch_holdings("XDWD")

        assert (df["weight_pct"] > 0).all()
        # Sum of mock weights: 5.14 + 4.12 + 3.80 = 13.06
        assert abs(df["weight_pct"].sum() - 13.06) < 0.1


import requests  # noqa: E402 — needed for exception classes in test patches
