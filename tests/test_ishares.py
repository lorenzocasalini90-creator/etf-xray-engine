"""Tests for the iShares fetcher."""

import pandas as pd
import pytest

from src.ingestion.base_fetcher import HOLDINGS_SCHEMA
from src.ingestion.ishares import ISharesFetcher

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_UCITS_CSV = """\
Fund Holdings as of,"28/Mar/2026"

Ticker,Name,Sector,Asset Class,Market Value,Weight (%),Notional Value,Shares,Price,Location,Exchange,Market Currency,ISIN,SEDOL,CUSIP
AAPL,APPLE INC,Information Technology,Equity,"46,567,770,000",6.65,"46,567,770,000","188,816,321",246.63,United States,NASDAQ,USD,US0378331005,2046251,037833100
MSFT,MICROSOFT CORP,Information Technology,Equity,"34,281,620,000",4.90,"34,281,620,000","95,502,629",358.96,United States,NASDAQ,USD,US5949181045,2588173,594918104
NVDA,NVIDIA CORP,Information Technology,Equity,"51,620,030,000",7.37,"51,620,030,000","312,526,688",165.17,United States,NASDAQ,USD,US67066G1040,2379504,67066G104
AMZN,AMAZON COM INC,Consumer Discretionary,Equity,"25,300,000,000",3.61,"25,300,000,000","125,000,000",202.40,United States,NASDAQ,USD,US0231351067,2000019,023135106
GOOGL,ALPHABET INC CLASS A,Communication,Equity,"14,200,000,000",2.03,"14,200,000,000","85,000,000",167.06,United States,NASDAQ,USD,US02079K3059,BYVY8G0,02079K305
CASH_USD,US DOLLAR,Cash,Cash,"1,500,000",0.02,"1,500,000","1,500,000",1.00,United States,,USD,,,
FUT_SP500,S&P 500 EMINI FUT MAR26,Futures,Futures,"500,000",0.01,"500,000",10,"50,000.00",United States,,USD,,,
META,META PLATFORMS INC CLASS A,Communication,Equity,"17,000,000,000",2.43,"17,000,000,000","28,000,000",607.14,United States,NASDAQ,USD,US30303M1027,B7TL820,30303M102
"""

SAMPLE_SCRAPER_DF = pd.DataFrame(
    {
        "ticker": ["AAPL", "MSFT", "NVDA", "CASH_USD", "FUT_ES"],
        "name": ["APPLE INC", "MICROSOFT CORP", "NVIDIA CORP", "US DOLLAR", "S&P FUTURE"],
        "sector": [
            "Information Technology",
            "Information Technology",
            "Information Technology",
            "Cash",
            "Futures",
        ],
        "asset_class": ["Equity", "Equity", "Equity", "Cash", "Futures"],
        "market_value": [46e9, 34e9, 51e9, 1.5e6, 5e5],
        "weight": [6.65, 4.90, 7.37, 0.02, 0.01],
        "amount": [188e6, 95e6, 312e6, 1.5e6, 10],
        "location": ["United States"] * 5,
        "currency": ["USD"] * 5,
        "fund_ticker": ["IVV"] * 5,
        "as_of_date": [pd.Timestamp("2026-03-28")] * 5,
    }
)


@pytest.fixture
def fetcher() -> ISharesFetcher:
    return ISharesFetcher()


# ---------------------------------------------------------------------------
# can_handle
# ---------------------------------------------------------------------------


class TestCanHandle:
    def test_recognises_ucits_tickers(self, fetcher: ISharesFetcher) -> None:
        for t in ("CSPX", "SWDA", "IWDA", "EIMI"):
            assert fetcher.can_handle(t) == 1.0, f"Should handle {t} with full confidence"

    def test_recognises_us_tickers(self, fetcher: ISharesFetcher) -> None:
        assert fetcher.can_handle("IVV")

    def test_ie_isin_high_confidence(self, fetcher: ISharesFetcher) -> None:
        assert fetcher.can_handle("IE00B5BMR087") == 0.9

    def test_unknown_tickers_low_confidence(self, fetcher: ISharesFetcher) -> None:
        for t in ("VOO", "SPY", "QQQ", "VWCE", "XDWD"):
            assert fetcher.can_handle(t) == 0.5, f"Unknown {t} should get 0.5"

    def test_empty_identifier(self, fetcher: ISharesFetcher) -> None:
        assert fetcher.can_handle("") == 0.0

    def test_case_insensitive(self, fetcher: ISharesFetcher) -> None:
        assert fetcher.can_handle("cspx")
        assert fetcher.can_handle("  CSPX  ")


# ---------------------------------------------------------------------------
# fetch_holdings — schema
# ---------------------------------------------------------------------------


class TestFetchHoldingsSchema:
    def test_ucits_schema_matches(
        self, fetcher: ISharesFetcher, mocker
    ) -> None:
        mocker.patch.object(
            fetcher,
            "_fetch_ucits",
            return_value=fetcher._parse_ucits_csv(SAMPLE_UCITS_CSV, "CSPX"),
        )
        df = fetcher.fetch_holdings("CSPX")
        assert list(df.columns) == HOLDINGS_SCHEMA

    def test_scraper_schema_matches(
        self, fetcher: ISharesFetcher, mocker
    ) -> None:
        mocker.patch.object(
            fetcher,
            "_fetch_via_scraper",
            return_value=ISharesFetcher._normalise_scraper(
                SAMPLE_SCRAPER_DF, "IVV"
            ),
        )
        # Ensure IVV is routed to scraper path
        fetcher._scraper_tickers.add("IVV")
        df = fetcher.fetch_holdings("IVV")
        assert list(df.columns) == HOLDINGS_SCHEMA


# ---------------------------------------------------------------------------
# Non-equity filtering
# ---------------------------------------------------------------------------


class TestFilterNonEquity:
    def test_cash_and_futures_removed_ucits(
        self, fetcher: ISharesFetcher, mocker
    ) -> None:
        mocker.patch.object(
            fetcher,
            "_fetch_ucits",
            return_value=fetcher._parse_ucits_csv(SAMPLE_UCITS_CSV, "CSPX"),
        )
        df = fetcher.fetch_holdings("CSPX")
        names = df["holding_name"].tolist()
        assert "US DOLLAR" not in names
        assert "S&P 500 EMINI FUT MAR26" not in names
        assert "APPLE INC" in names

    def test_cash_and_futures_removed_scraper(
        self, fetcher: ISharesFetcher, mocker
    ) -> None:
        mocker.patch.object(
            fetcher,
            "_fetch_via_scraper",
            return_value=ISharesFetcher._normalise_scraper(
                SAMPLE_SCRAPER_DF, "IVV"
            ),
        )
        fetcher._scraper_tickers.add("IVV")
        df = fetcher.fetch_holdings("IVV")
        names = df["holding_name"].tolist()
        assert "US DOLLAR" not in names
        assert "S&P FUTURE" not in names
        assert len(df) == 3  # only AAPL, MSFT, NVDA


# ---------------------------------------------------------------------------
# Weights sanity
# ---------------------------------------------------------------------------


class TestWeightsSum:
    def test_weights_sum_realistic(
        self, fetcher: ISharesFetcher, mocker
    ) -> None:
        mocker.patch.object(
            fetcher,
            "_fetch_ucits",
            return_value=fetcher._parse_ucits_csv(SAMPLE_UCITS_CSV, "CSPX"),
        )
        df = fetcher.fetch_holdings("CSPX")
        total = df["weight_pct"].sum()
        # Fixture equities sum to ~26.99 — partial, but must be > 0
        assert total > 0
        assert total <= 100.0


# ---------------------------------------------------------------------------
# CSV parsing edge cases
# ---------------------------------------------------------------------------


class TestParsing:
    def test_as_of_date_extracted(self, fetcher: ISharesFetcher) -> None:
        df = fetcher._parse_ucits_csv(SAMPLE_UCITS_CSV, "CSPX")
        assert df["as_of_date"].iloc[0] == "2026-03-28"

    def test_numeric_columns_parsed(self, fetcher: ISharesFetcher) -> None:
        df = fetcher._parse_ucits_csv(SAMPLE_UCITS_CSV, "CSPX")
        for col in ("weight_pct", "market_value", "shares"):
            assert pd.api.types.is_numeric_dtype(df[col]), f"{col} should be numeric"
