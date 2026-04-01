"""Tests for the Amundi fetcher."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingestion.amundi import AmundiFetcher, AMUNDI_PRODUCTS
from src.ingestion.base_fetcher import HOLDINGS_SCHEMA


# ---------------------------------------------------------------------------
# Mock API response
# ---------------------------------------------------------------------------

MOCK_API_RESPONSE = {
    "products": [
        {
            "productId": "LU1681043599",
            "characteristics": {
                "ISIN": "LU1681043599",
                "SHARE_MARKETING_NAME": "Amundi MSCI World Swap UCITS ETF EUR Acc",
                "TICKER": "CW8",
                "FUND_REPLICATION_METHODOLOGY": "Swap",
            },
            "composition": {
                "totalNumberOfInstruments": 4,
                "compositionData": [
                    {
                        "compositionCharacteristics": {
                            "date": "2026-03-30",
                            "quantity": 3606971.0,
                            "bbg": "SAP GY",
                            "name": "SAP SE / XETRA",
                            "weight": 0.0926,
                            "currency": "EUR",
                            "type": "EQUITY_ORDINARY",
                            "sector": "Information Technology",
                            "isin": "DE0007164600",
                            "countryOfRisk": "Germany",
                        },
                        "weight": 0.0926,
                    },
                    {
                        "compositionCharacteristics": {
                            "date": "2026-03-30",
                            "quantity": 435126.0,
                            "bbg": "ASML NA",
                            "name": "ASML HOLDING NV",
                            "weight": 0.0845,
                            "currency": "EUR",
                            "type": "EQUITY_ORDINARY",
                            "sector": "Information Technology",
                            "isin": "NL0010273215",
                            "countryOfRisk": "Netherlands",
                        },
                        "weight": 0.0845,
                    },
                    {
                        "compositionCharacteristics": {
                            "date": "2026-03-30",
                            "quantity": 2362875.0,
                            "bbg": "AIR FP",
                            "name": "AIRBUS SE",
                            "weight": 0.0657,
                            "currency": "EUR",
                            "type": "EQUITY_ORDINARY",
                            "sector": "Industrials",
                            "isin": "NL0000235190",
                            "countryOfRisk": "France",
                        },
                        "weight": 0.0657,
                    },
                    {
                        "compositionCharacteristics": {
                            "date": "2026-03-30",
                            "quantity": 50000.0,
                            "bbg": None,
                            "name": "EUR CASH",
                            "weight": 0.002,
                            "currency": "EUR",
                            "type": "CASH",
                            "sector": None,
                            "isin": None,
                            "countryOfRisk": None,
                        },
                        "weight": 0.002,
                    },
                ],
            },
        }
    ]
}

MOCK_EMPTY_RESPONSE = {"products": []}


@pytest.fixture
def fetcher() -> AmundiFetcher:
    return AmundiFetcher()


def _mock_post_response(json_data: dict, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# can_handle
# ---------------------------------------------------------------------------


class TestCanHandle:
    def test_known_tickers(self, fetcher: AmundiFetcher) -> None:
        for t in ("CW8", "MWRD", "PAEEM", "PANX", "PCEU", "AEEM", "ANEW"):
            assert fetcher.can_handle(t) == 0.95, f"Should handle {t}"

    def test_known_isins(self, fetcher: AmundiFetcher) -> None:
        assert fetcher.can_handle("LU1681043599") == 0.95  # CW8
        assert fetcher.can_handle("LU2090063673") == 0.95
        assert fetcher.can_handle("FR0010756098") == 0.95  # FR prefix

    def test_fr_isin_high_confidence(self, fetcher: AmundiFetcher) -> None:
        # Unknown FR ISIN (not in products, not matching known prefixes)
        assert fetcher.can_handle("FR9999999999") == 0.85

    def test_lu_isin_medium_confidence(self, fetcher: AmundiFetcher) -> None:
        # Unknown LU ISIN (not matching known prefixes)
        assert fetcher.can_handle("LU9999999999") == 0.6

    def test_rejects_unknown_isins(self, fetcher: AmundiFetcher) -> None:
        # IE and US ISINs should get low confidence
        ie_score = fetcher.can_handle("IE00B5BMR087")
        us_score = fetcher.can_handle("US9229087690")
        assert ie_score == 0.3
        assert us_score == 0.3

    def test_unknown_tickers(self, fetcher: AmundiFetcher) -> None:
        for t in ("CSPX", "VOO", "VWCE", "SPY", "QQQ"):
            assert fetcher.can_handle(t) == 0.3, f"{t} should get 0.3"

    def test_empty_string(self, fetcher: AmundiFetcher) -> None:
        assert fetcher.can_handle("") == 0.0

    def test_case_insensitive(self, fetcher: AmundiFetcher) -> None:
        assert fetcher.can_handle("cw8") == 0.95
        assert fetcher.can_handle("  CW8  ") == 0.95
        assert fetcher.can_handle("lu1681043599") == 0.95


# ---------------------------------------------------------------------------
# fetch_holdings — via mock API
# ---------------------------------------------------------------------------


class TestFetchHoldings:
    @patch.object(AmundiFetcher, "_fetch_api")
    def test_schema_matches(self, mock_api, fetcher: AmundiFetcher) -> None:
        mock_api.return_value = MOCK_API_RESPONSE["products"][0]

        df = fetcher.fetch_holdings("CW8")

        assert list(df.columns) == HOLDINGS_SCHEMA

    @patch.object(AmundiFetcher, "_fetch_api")
    def test_parses_holdings(self, mock_api, fetcher: AmundiFetcher) -> None:
        mock_api.return_value = MOCK_API_RESPONSE["products"][0]

        df = fetcher.fetch_holdings("CW8")

        # 3 equity rows (cash filtered out)
        assert len(df) == 3
        assert "SAP SE / XETRA" in df["holding_name"].values
        assert "ASML HOLDING NV" in df["holding_name"].values
        assert "AIRBUS SE" in df["holding_name"].values

    @patch.object(AmundiFetcher, "_fetch_api")
    def test_weight_converted_to_pct(self, mock_api, fetcher: AmundiFetcher) -> None:
        mock_api.return_value = MOCK_API_RESPONSE["products"][0]

        df = fetcher.fetch_holdings("CW8")

        sap_row = df.loc[df["holding_name"] == "SAP SE / XETRA"]
        # 0.0926 → 9.26%
        assert abs(sap_row["weight_pct"].iloc[0] - 9.26) < 0.01

    @patch.object(AmundiFetcher, "_fetch_api")
    def test_isin_present(self, mock_api, fetcher: AmundiFetcher) -> None:
        mock_api.return_value = MOCK_API_RESPONSE["products"][0]

        df = fetcher.fetch_holdings("CW8")

        assert "DE0007164600" in df["holding_isin"].values
        assert "NL0010273215" in df["holding_isin"].values

    @patch.object(AmundiFetcher, "_fetch_api")
    def test_bloomberg_ticker(self, mock_api, fetcher: AmundiFetcher) -> None:
        mock_api.return_value = MOCK_API_RESPONSE["products"][0]

        df = fetcher.fetch_holdings("CW8")

        assert "SAP GY" in df["holding_ticker"].values

    @patch.object(AmundiFetcher, "_fetch_api")
    def test_as_of_date(self, mock_api, fetcher: AmundiFetcher) -> None:
        mock_api.return_value = MOCK_API_RESPONSE["products"][0]

        df = fetcher.fetch_holdings("CW8")

        assert df["as_of_date"].iloc[0] == "2026-03-30"

    @patch.object(AmundiFetcher, "_fetch_api")
    def test_etf_ticker_column(self, mock_api, fetcher: AmundiFetcher) -> None:
        mock_api.return_value = MOCK_API_RESPONSE["products"][0]

        df = fetcher.fetch_holdings("CW8")

        assert (df["etf_ticker"] == "CW8").all()

    @patch.object(AmundiFetcher, "_fetch_api")
    def test_isin_resolves_ticker(self, mock_api, fetcher: AmundiFetcher) -> None:
        mock_api.return_value = MOCK_API_RESPONSE["products"][0]

        df = fetcher.fetch_holdings("LU1681043599")

        assert (df["etf_ticker"] == "CW8").all()

    @patch.object(AmundiFetcher, "_fetch_api")
    def test_sector_and_country(self, mock_api, fetcher: AmundiFetcher) -> None:
        mock_api.return_value = MOCK_API_RESPONSE["products"][0]

        df = fetcher.fetch_holdings("CW8")

        sap_row = df.loc[df["holding_name"] == "SAP SE / XETRA"]
        assert sap_row["sector"].iloc[0] == "Information Technology"
        assert sap_row["country"].iloc[0] == "Germany"


# ---------------------------------------------------------------------------
# Cash filtering
# ---------------------------------------------------------------------------


class TestFilterNonEquity:
    @patch.object(AmundiFetcher, "_fetch_api")
    def test_cash_rows_filtered(self, mock_api, fetcher: AmundiFetcher) -> None:
        mock_api.return_value = MOCK_API_RESPONSE["products"][0]

        df = fetcher.fetch_holdings("CW8")

        assert "EUR CASH" not in df["holding_name"].values
        assert len(df) == 3


# ---------------------------------------------------------------------------
# try_fetch — never raises
# ---------------------------------------------------------------------------


class TestTryFetch:
    @patch.object(AmundiFetcher, "_fetch_api")
    def test_try_fetch_success(self, mock_api, fetcher: AmundiFetcher) -> None:
        mock_api.return_value = MOCK_API_RESPONSE["products"][0]

        result = fetcher.try_fetch("CW8")

        assert result.status == "success"
        assert result.holdings is not None
        assert result.source == "AmundiFetcher"
        assert result.coverage_pct > 0

    @patch.object(AmundiFetcher, "_fetch_api")
    def test_try_fetch_api_failure(self, mock_api, fetcher: AmundiFetcher) -> None:
        mock_api.side_effect = ConnectionError("network down")

        result = fetcher.try_fetch("CW8")

        assert result.status == "failed"
        assert result.holdings is None


# ---------------------------------------------------------------------------
# Identifier resolution
# ---------------------------------------------------------------------------


class TestIdentifierResolution:
    def test_ticker_to_isin(self, fetcher: AmundiFetcher) -> None:
        assert fetcher._resolve_isin("CW8") == "LU1681043599"

    def test_isin_passthrough(self, fetcher: AmundiFetcher) -> None:
        assert fetcher._resolve_isin("LU1681043599") == "LU1681043599"

    def test_isin_to_ticker(self, fetcher: AmundiFetcher) -> None:
        assert fetcher._resolve_ticker("LU1681043599") == "CW8"

    def test_unknown_isin_passthrough(self, fetcher: AmundiFetcher) -> None:
        assert fetcher._resolve_ticker("LU9999999999") == "LU9999999999"

    def test_case_insensitive(self, fetcher: AmundiFetcher) -> None:
        assert fetcher._resolve_isin("cw8") == "LU1681043599"


# ---------------------------------------------------------------------------
# Weights sum
# ---------------------------------------------------------------------------


class TestWeightsSum:
    @patch.object(AmundiFetcher, "_fetch_api")
    def test_weights_are_positive(self, mock_api, fetcher: AmundiFetcher) -> None:
        mock_api.return_value = MOCK_API_RESPONSE["products"][0]

        df = fetcher.fetch_holdings("CW8")

        assert (df["weight_pct"] > 0).all()
        # Sum: 9.26 + 8.45 + 6.57 = 24.28
        assert abs(df["weight_pct"].sum() - 24.28) < 0.1


# ---------------------------------------------------------------------------
# Registry integration
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_amundi_discovered(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        names = {type(f).__name__ for f in registry.fetchers}
        assert "AmundiFetcher" in names

    def test_routes_amundi_ticker(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        fetcher = registry.get_fetcher("CW8")
        assert type(fetcher).__name__ == "AmundiFetcher"

    def test_routes_amundi_isin(self) -> None:
        from src.ingestion.registry import FetcherRegistry

        registry = FetcherRegistry()
        fetcher = registry.get_fetcher("LU1681043599")
        assert type(fetcher).__name__ == "AmundiFetcher"
