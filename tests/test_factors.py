"""Tests for factor engine with mocked yfinance — no real API calls."""

from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.factors.factor_engine import FactorEngine
from src.factors.fundamentals import FundamentalsProvider
from src.factors.sector_proxies import (
    GICS_SECTOR_MEDIANS,
    get_sector_proxy,
    save_sector_proxies,
)
from src.storage.models import Base, FigiMapping, SecurityFundamental, SectorFactorProxy


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db_session():
    """In-memory SQLite session for testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session_ = sessionmaker(bind=engine)
    session = Session_()
    yield session
    session.close()


@pytest.fixture
def sample_figis(db_session):
    """Insert sample FIGI mappings and return a dict of ticker → figi_id."""
    tickers = {
        "AAPL": "BBG000B9XRY4",
        "MSFT": "BBG000BPH459",
        "GOOG": "BBG009S39JX6",
        "NESN": "BBG000BWFNX0",
        "TSM": "BBG000BD8ZK0",
    }
    id_map = {}
    for ticker, figi in tickers.items():
        mapping = FigiMapping(
            composite_figi=figi,
            ticker=ticker,
            name=f"{ticker} Corp",
        )
        db_session.add(mapping)
        db_session.flush()
        id_map[ticker] = mapping.id
    return id_map


@pytest.fixture
def portfolio_df():
    """Sample aggregated portfolio DataFrame."""
    return pd.DataFrame([
        {"composite_figi": "BBG000B9XRY4", "ticker": "AAPL", "name": "Apple",
         "sector": "Technology", "country": "US", "real_weight_pct": 30.0},
        {"composite_figi": "BBG000BPH459", "ticker": "MSFT", "name": "Microsoft",
         "sector": "Technology", "country": "US", "real_weight_pct": 25.0},
        {"composite_figi": "BBG009S39JX6", "ticker": "GOOG", "name": "Alphabet",
         "sector": "Communication Services", "country": "US", "real_weight_pct": 20.0},
        {"composite_figi": "BBG000BWFNX0", "ticker": "NESN", "name": "Nestle",
         "sector": "Consumer Staples", "country": "CH", "real_weight_pct": 15.0},
        {"composite_figi": "BBG000BD8ZK0", "ticker": "TSM", "name": "TSMC",
         "sector": "Technology", "country": "TW", "real_weight_pct": 10.0},
    ])


@pytest.fixture
def benchmark_df():
    """Sample benchmark DataFrame."""
    return pd.DataFrame([
        {"composite_figi": "BBG000B9XRY4", "ticker": "AAPL", "name": "Apple",
         "sector": "Technology", "country": "US", "real_weight_pct": 20.0},
        {"composite_figi": "BBG000BPH459", "ticker": "MSFT", "name": "Microsoft",
         "sector": "Technology", "country": "US", "real_weight_pct": 20.0},
        {"composite_figi": "BBG009S39JX6", "ticker": "GOOG", "name": "Alphabet",
         "sector": "Communication Services", "country": "US", "real_weight_pct": 20.0},
        {"composite_figi": "BBG000BWFNX0", "ticker": "NESN", "name": "Nestle",
         "sector": "Consumer Staples", "country": "CH", "real_weight_pct": 20.0},
        {"composite_figi": "BBG000BD8ZK0", "ticker": "TSM", "name": "TSMC",
         "sector": "Technology", "country": "TW", "real_weight_pct": 20.0},
    ])


def _mock_yf_info(ticker: str) -> dict:
    """Return mock yfinance .info for known tickers."""
    data = {
        "AAPL": {
            "regularMarketPrice": 170.0,
            "trailingPE": 28.5,
            "priceToBook": 45.0,
            "returnOnEquity": 0.30,
            "debtToEquity": 180.0,   # yfinance returns %
            "dividendYield": 0.005,
            "marketCap": 2.7e12,
        },
        "MSFT": {
            "regularMarketPrice": 380.0,
            "trailingPE": 35.0,
            "priceToBook": 12.0,
            "returnOnEquity": 0.35,
            "debtToEquity": 42.0,
            "dividendYield": 0.008,
            "marketCap": 2.8e12,
        },
        "GOOG": {
            "regularMarketPrice": 140.0,
            "trailingPE": 24.0,
            "priceToBook": 6.5,
            "returnOnEquity": 0.25,
            "debtToEquity": 10.0,
            "dividendYield": None,
            "marketCap": 1.7e12,
        },
        "NESN": {
            "regularMarketPrice": 100.0,
            "trailingPE": 20.0,
            "priceToBook": 5.0,
            "returnOnEquity": 0.40,
            "debtToEquity": 120.0,
            "dividendYield": 0.03,
            "marketCap": 250e9,
        },
        "TSM": {
            "regularMarketPrice": 150.0,
            "trailingPE": 22.0,
            "priceToBook": 7.0,
            "returnOnEquity": 0.28,
            "debtToEquity": 30.0,
            "dividendYield": 0.015,
            "marketCap": 700e9,
        },
    }
    return data.get(ticker, {})


# ---------------------------------------------------------------------------
# Sector Proxies tests
# ---------------------------------------------------------------------------

class TestSectorProxies:

    def test_get_known_sector(self):
        """Known sector should return medians."""
        proxy = get_sector_proxy("Technology")
        assert proxy is not None
        assert proxy["median_pe"] == 28.0
        assert proxy["style"] == "Growth"

    def test_get_unknown_sector(self):
        """Unknown sector should return None."""
        assert get_sector_proxy("Crypto") is None

    def test_all_sectors_have_required_keys(self):
        """Every sector should have median_pe, median_pb, median_roe, style."""
        for sector, data in GICS_SECTOR_MEDIANS.items():
            assert "median_pe" in data, f"{sector} missing median_pe"
            assert "median_pb" in data, f"{sector} missing median_pb"
            assert "median_roe" in data, f"{sector} missing median_roe"
            assert "style" in data, f"{sector} missing style"

    def test_save_to_db(self, db_session):
        """Saving proxies should create entries in sector_factor_proxies."""
        save_sector_proxies(db_session, as_of=date(2026, 3, 31))
        count = db_session.query(SectorFactorProxy).count()
        # 11 sectors × 3 factors = 33
        assert count == 33

    def test_save_idempotent(self, db_session):
        """Saving twice should not create duplicates."""
        save_sector_proxies(db_session, as_of=date(2026, 3, 31))
        save_sector_proxies(db_session, as_of=date(2026, 3, 31))
        count = db_session.query(SectorFactorProxy).count()
        assert count == 33


# ---------------------------------------------------------------------------
# FundamentalsProvider tests
# ---------------------------------------------------------------------------

class TestFundamentalsProvider:

    @patch("src.factors.fundamentals.yf")
    def test_fetch_from_yfinance(self, mock_yf_module, db_session, sample_figis):
        """Should fetch and cache fundamentals from yfinance."""
        mock_ticker = MagicMock()
        mock_ticker.info = _mock_yf_info("AAPL")
        mock_yf_module.Ticker.return_value = mock_ticker

        provider = FundamentalsProvider(db_session)
        result = provider.fetch("AAPL", sample_figis["AAPL"])

        assert result is not None
        assert result["pe_ratio"] == 28.5
        assert result["market_cap"] == 2.7e12
        # debt_to_equity should be divided by 100
        assert abs(result["debt_to_equity"] - 1.80) < 0.01

    @patch("src.factors.fundamentals.yf")
    def test_cache_hit(self, mock_yf_module, db_session, sample_figis):
        """Second fetch should use cache, not call yfinance again."""
        mock_ticker = MagicMock()
        mock_ticker.info = _mock_yf_info("AAPL")
        mock_yf_module.Ticker.return_value = mock_ticker

        provider = FundamentalsProvider(db_session)
        provider.fetch("AAPL", sample_figis["AAPL"])
        # Reset mock
        mock_yf_module.Ticker.reset_mock()

        result = provider.fetch("AAPL", sample_figis["AAPL"])
        assert result is not None
        mock_yf_module.Ticker.assert_not_called()

    @patch("src.factors.fundamentals.yf")
    def test_stale_cache(self, mock_yf_module, db_session, sample_figis):
        """Stale cache (> 7 days) should trigger a fresh yfinance call."""
        figi_id = sample_figis["MSFT"]
        old = SecurityFundamental(
            figi_id=figi_id,
            pe_ratio=30.0, pb_ratio=10.0,
            roe=0.30, debt_to_equity=0.5,
            dividend_yield=0.01, market_cap=2e12,
            data_source="L2",
            as_of_date=date(2026, 3, 1),
            updated_at=datetime.utcnow() - timedelta(days=10),
        )
        db_session.add(old)
        db_session.flush()

        mock_ticker = MagicMock()
        mock_ticker.info = _mock_yf_info("MSFT")
        mock_yf_module.Ticker.return_value = mock_ticker

        provider = FundamentalsProvider(db_session)
        result = provider.fetch("MSFT", figi_id)

        assert result is not None
        # Should have refreshed
        mock_yf_module.Ticker.assert_called_once_with("MSFT")
        assert result["pe_ratio"] == 35.0

    @patch("src.factors.fundamentals.yf")
    def test_yfinance_failure_returns_none(self, mock_yf_module, db_session, sample_figis):
        """If yfinance raises an exception, return None gracefully."""
        mock_yf_module.Ticker.side_effect = Exception("API down")

        provider = FundamentalsProvider(db_session, max_retries=1)
        result = provider.fetch("AAPL", sample_figis["AAPL"])
        assert result is None

    @patch("src.factors.fundamentals.yf")
    def test_batch_fetch(self, mock_yf_module, db_session, sample_figis):
        """Batch fetch should process multiple tickers."""
        def make_ticker(ticker_str):
            mock = MagicMock()
            mock.info = _mock_yf_info(ticker_str)
            return mock

        mock_yf_module.Ticker.side_effect = lambda t: make_ticker(t)

        provider = FundamentalsProvider(db_session)
        batch = [
            {"ticker": "AAPL", "figi_id": sample_figis["AAPL"]},
            {"ticker": "MSFT", "figi_id": sample_figis["MSFT"]},
        ]
        with patch("src.factors.fundamentals.time.sleep"):
            results = provider.fetch_batch(batch, sleep_between=0)

        assert "AAPL" in results
        assert "MSFT" in results
        assert results["AAPL"]["pe_ratio"] == 28.5
        assert results["MSFT"]["pe_ratio"] == 35.0


# ---------------------------------------------------------------------------
# FactorEngine tests
# ---------------------------------------------------------------------------

class TestFactorEngine:

    @patch("src.factors.fundamentals.yf")
    def test_full_analysis_with_mock(
        self, mock_yf_module, db_session, sample_figis, portfolio_df,
    ):
        """Full factor analysis with mocked yfinance data."""
        def make_ticker(ticker_str):
            mock = MagicMock()
            mock.info = _mock_yf_info(ticker_str)
            return mock

        mock_yf_module.Ticker.side_effect = lambda t: make_ticker(t)

        engine = FactorEngine(db_session, top_n_yfinance=50)
        with patch("src.factors.fundamentals.time.sleep"):
            result = engine.analyze(portfolio_df)

        assert "factor_scores" in result
        assert "coverage_report" in result
        assert "factor_drivers" in result

        scores = result["factor_scores"]
        assert scores["value_growth"]["weighted_pe"] is not None
        assert scores["quality"]["weighted_roe"] is not None
        assert scores["size"]["Large"] > 0

    @patch("src.factors.fundamentals.yf")
    def test_fallback_to_l3(self, mock_yf_module, db_session, sample_figis):
        """If yfinance fails, should fall back to sector proxy (L3)."""
        mock_yf_module.Ticker.side_effect = Exception("API down")

        df = pd.DataFrame([
            {"composite_figi": "BBG000B9XRY4", "ticker": "AAPL", "name": "Apple",
             "sector": "Technology", "country": "US", "real_weight_pct": 60.0},
            {"composite_figi": "BBG000BPH459", "ticker": "MSFT", "name": "Microsoft",
             "sector": "Technology", "country": "US", "real_weight_pct": 40.0},
        ])

        engine = FactorEngine(db_session, top_n_yfinance=50)
        engine.fundamentals.max_retries = 1
        with patch("src.factors.fundamentals.time.sleep"):
            result = engine.analyze(df)

        coverage = result["coverage_report"]
        # Both should be L3 (sector proxy) since yfinance failed
        assert coverage["L3_proxy_count"] == 2
        assert coverage["L2_fundamentals_count"] == 0

        # Should still have factor scores from sector proxy
        scores = result["factor_scores"]
        assert scores["value_growth"]["weighted_pe"] == 28.0  # Technology median

    def test_l4_unclassified(self, db_session):
        """Holdings with no ticker and unknown sector should be L4."""
        df = pd.DataFrame([
            {"composite_figi": "FIGI_UNKNOWN", "ticker": "", "name": "Mystery Corp",
             "sector": "", "country": "XX", "real_weight_pct": 100.0},
        ])

        engine = FactorEngine(db_session, top_n_yfinance=0)
        result = engine.analyze(df)

        coverage = result["coverage_report"]
        assert coverage["L4_unclassified_count"] == 1
        assert coverage["L4_pct"] == 100.0

    @patch("src.factors.fundamentals.yf")
    def test_weighted_average_calculation(
        self, mock_yf_module, db_session, sample_figis,
    ):
        """Verify weighted P/E calculation with known values."""
        # 2 holdings: AAPL (60% weight, PE=28.5) and MSFT (40% weight, PE=35.0)
        # Expected weighted PE = 0.6*28.5 + 0.4*35.0 = 17.1 + 14.0 = 31.1
        def make_ticker(ticker_str):
            mock = MagicMock()
            mock.info = _mock_yf_info(ticker_str)
            return mock

        mock_yf_module.Ticker.side_effect = lambda t: make_ticker(t)

        df = pd.DataFrame([
            {"composite_figi": "BBG000B9XRY4", "ticker": "AAPL", "name": "Apple",
             "sector": "Technology", "country": "US", "real_weight_pct": 60.0},
            {"composite_figi": "BBG000BPH459", "ticker": "MSFT", "name": "Microsoft",
             "sector": "Technology", "country": "US", "real_weight_pct": 40.0},
        ])

        engine = FactorEngine(db_session, top_n_yfinance=50)
        with patch("src.factors.fundamentals.time.sleep"):
            result = engine.analyze(df)

        pe = result["factor_scores"]["value_growth"]["weighted_pe"]
        expected_pe = (60.0 * 28.5 + 40.0 * 35.0) / 100.0
        assert abs(pe - expected_pe) < 0.1

    @patch("src.factors.fundamentals.yf")
    def test_benchmark_comparison(
        self, mock_yf_module, db_session, sample_figis, portfolio_df, benchmark_df,
    ):
        """Benchmark comparison should produce delta values."""
        def make_ticker(ticker_str):
            mock = MagicMock()
            mock.info = _mock_yf_info(ticker_str)
            return mock

        mock_yf_module.Ticker.side_effect = lambda t: make_ticker(t)

        engine = FactorEngine(db_session, top_n_yfinance=50)
        with patch("src.factors.fundamentals.time.sleep"):
            result = engine.analyze(portfolio_df, benchmark_df)

        assert result["benchmark_comparison"] is not None
        delta = result["benchmark_comparison"]
        assert "value_growth" in delta
        assert "quality" in delta
        assert "size" in delta

    @patch("src.factors.fundamentals.yf")
    def test_coverage_percentages_sum(
        self, mock_yf_module, db_session, sample_figis, portfolio_df,
    ):
        """Coverage percentages should sum to ~100%."""
        def make_ticker(ticker_str):
            mock = MagicMock()
            mock.info = _mock_yf_info(ticker_str)
            return mock

        mock_yf_module.Ticker.side_effect = lambda t: make_ticker(t)

        engine = FactorEngine(db_session, top_n_yfinance=50)
        with patch("src.factors.fundamentals.time.sleep"):
            result = engine.analyze(portfolio_df)

        cov = result["coverage_report"]
        total_pct = cov["L2_pct"] + cov["L3_pct"] + cov["L4_pct"]
        assert abs(total_pct - 100.0) < 1.0
