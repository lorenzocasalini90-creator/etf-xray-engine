"""Tests for analytics modules with synthetic data.

Uses 3 fake ETFs with known holdings to verify all calculations.
"""

import pandas as pd
import pytest

from src.analytics.active_share import active_share
from src.analytics.aggregator import aggregate_portfolio, country_exposure, sector_exposure
from src.analytics.overlap import overlap_matrix, portfolio_hhi, shared_holdings
from src.analytics.redundancy import redundancy_scores


# ---------------------------------------------------------------------------
# Fixtures: 3 fake ETFs with known, overlapping holdings
# ---------------------------------------------------------------------------

def _make_holdings(rows: list[dict]) -> pd.DataFrame:
    """Helper to create a holdings DataFrame from a list of dicts."""
    return pd.DataFrame(rows)


@pytest.fixture
def etf_alpha() -> pd.DataFrame:
    """ETF Alpha: US Tech heavy, 4 holdings."""
    return _make_holdings([
        {"composite_figi": "FIGI_AAPL", "holding_name": "Apple", "holding_ticker": "AAPL",
         "sector": "Technology", "country": "US", "weight_pct": 40.0},
        {"composite_figi": "FIGI_MSFT", "holding_name": "Microsoft", "holding_ticker": "MSFT",
         "sector": "Technology", "country": "US", "weight_pct": 30.0},
        {"composite_figi": "FIGI_GOOG", "holding_name": "Alphabet", "holding_ticker": "GOOG",
         "sector": "Technology", "country": "US", "weight_pct": 20.0},
        {"composite_figi": "FIGI_AMZN", "holding_name": "Amazon", "holding_ticker": "AMZN",
         "sector": "Consumer", "country": "US", "weight_pct": 10.0},
    ])


@pytest.fixture
def etf_beta() -> pd.DataFrame:
    """ETF Beta: Overlaps with Alpha on AAPL/MSFT, adds European stocks."""
    return _make_holdings([
        {"composite_figi": "FIGI_AAPL", "holding_name": "Apple", "holding_ticker": "AAPL",
         "sector": "Technology", "country": "US", "weight_pct": 25.0},
        {"composite_figi": "FIGI_MSFT", "holding_name": "Microsoft", "holding_ticker": "MSFT",
         "sector": "Technology", "country": "US", "weight_pct": 25.0},
        {"composite_figi": "FIGI_NESN", "holding_name": "Nestle", "holding_ticker": "NESN",
         "sector": "Consumer Staples", "country": "CH", "weight_pct": 30.0},
        {"composite_figi": "FIGI_ASML", "holding_name": "ASML", "holding_ticker": "ASML",
         "sector": "Technology", "country": "NL", "weight_pct": 20.0},
    ])


@pytest.fixture
def etf_gamma() -> pd.DataFrame:
    """ETF Gamma: Completely different holdings (EM focus)."""
    return _make_holdings([
        {"composite_figi": "FIGI_TSM", "holding_name": "TSMC", "holding_ticker": "TSM",
         "sector": "Technology", "country": "TW", "weight_pct": 50.0},
        {"composite_figi": "FIGI_BABA", "holding_name": "Alibaba", "holding_ticker": "BABA",
         "sector": "Consumer", "country": "CN", "weight_pct": 30.0},
        {"composite_figi": "FIGI_INFY", "holding_name": "Infosys", "holding_ticker": "INFY",
         "sector": "Technology", "country": "IN", "weight_pct": 20.0},
    ])


@pytest.fixture
def portfolio_positions() -> list[dict]:
    """Portfolio: Alpha 50K, Beta 30K, Gamma 20K."""
    return [
        {"ticker": "ALPHA", "capital": 50_000},
        {"ticker": "BETA", "capital": 30_000},
        {"ticker": "GAMMA", "capital": 20_000},
    ]


@pytest.fixture
def holdings_db(etf_alpha, etf_beta, etf_gamma) -> dict[str, pd.DataFrame]:
    return {"ALPHA": etf_alpha, "BETA": etf_beta, "GAMMA": etf_gamma}


# ---------------------------------------------------------------------------
# Aggregator tests
# ---------------------------------------------------------------------------

class TestAggregator:

    def test_aggregate_weights_sum(self, portfolio_positions, holdings_db):
        """Total real_weight_pct should sum to ~100%."""
        agg = aggregate_portfolio(portfolio_positions, holdings_db)
        total = agg["real_weight_pct"].sum()
        assert abs(total - 100.0) < 0.01, f"Weights sum to {total}, expected ~100"

    def test_aggregate_apple_weight(self, portfolio_positions, holdings_db):
        """Apple is in Alpha (40%, 50% capital) and Beta (25%, 30% capital).
        Expected: 0.5*40 + 0.3*25 = 20 + 7.5 = 27.5%."""
        agg = aggregate_portfolio(portfolio_positions, holdings_db)
        apple = agg[agg["composite_figi"] == "FIGI_AAPL"]
        assert len(apple) == 1
        assert abs(apple.iloc[0]["real_weight_pct"] - 27.5) < 0.01

    def test_aggregate_n_etf_sources(self, portfolio_positions, holdings_db):
        """Apple should appear in 2 ETFs, TSMC in 1."""
        agg = aggregate_portfolio(portfolio_positions, holdings_db)
        apple = agg[agg["composite_figi"] == "FIGI_AAPL"].iloc[0]
        assert apple["n_etf_sources"] == 2
        tsmc = agg[agg["composite_figi"] == "FIGI_TSM"].iloc[0]
        assert tsmc["n_etf_sources"] == 1

    def test_sector_exposure(self, portfolio_positions, holdings_db):
        """Sector exposure should include Technology and Consumer."""
        agg = aggregate_portfolio(portfolio_positions, holdings_db)
        sectors = sector_exposure(agg)
        assert "Technology" in sectors["sector"].values
        assert sectors["weight_pct"].sum() == pytest.approx(100.0, abs=0.01)

    def test_country_exposure(self, portfolio_positions, holdings_db):
        """Country exposure should include US, CH, TW, etc."""
        agg = aggregate_portfolio(portfolio_positions, holdings_db)
        countries = country_exposure(agg)
        assert "US" in countries["country"].values
        assert countries["weight_pct"].sum() == pytest.approx(100.0, abs=0.01)

    def test_empty_portfolio(self):
        """Empty portfolio should return empty DataFrame."""
        agg = aggregate_portfolio([], {})
        assert agg.empty


# ---------------------------------------------------------------------------
# Overlap tests
# ---------------------------------------------------------------------------

class TestOverlap:

    def test_overlap_matrix_diagonal(self, etf_alpha, etf_beta, etf_gamma):
        """Diagonal should be 100%."""
        holdings = {"ALPHA": etf_alpha, "BETA": etf_beta, "GAMMA": etf_gamma}
        matrix = overlap_matrix(holdings)
        for ticker in ["ALPHA", "BETA", "GAMMA"]:
            assert matrix.loc[ticker, ticker] == 100.0

    def test_overlap_alpha_beta(self, etf_alpha, etf_beta, etf_gamma):
        """Alpha and Beta share AAPL+MSFT. Weighted Jaccard should be > 0."""
        holdings = {"ALPHA": etf_alpha, "BETA": etf_beta, "GAMMA": etf_gamma}
        matrix = overlap_matrix(holdings)
        assert matrix.loc["ALPHA", "BETA"] > 0
        assert matrix.loc["ALPHA", "BETA"] == matrix.loc["BETA", "ALPHA"]

    def test_overlap_alpha_gamma_zero(self, etf_alpha, etf_beta, etf_gamma):
        """Alpha and Gamma share no holdings → overlap = 0."""
        holdings = {"ALPHA": etf_alpha, "BETA": etf_beta, "GAMMA": etf_gamma}
        matrix = overlap_matrix(holdings)
        assert matrix.loc["ALPHA", "GAMMA"] == 0.0

    def test_overlap_matrix_symmetric(self, etf_alpha, etf_beta, etf_gamma):
        """Matrix should be symmetric."""
        holdings = {"ALPHA": etf_alpha, "BETA": etf_beta, "GAMMA": etf_gamma}
        matrix = overlap_matrix(holdings)
        pd.testing.assert_frame_equal(matrix, matrix.T)

    def test_portfolio_hhi(self, portfolio_positions, holdings_db):
        """HHI should be between 0 and 1 for a diversified portfolio."""
        agg = aggregate_portfolio(portfolio_positions, holdings_db)
        result = portfolio_hhi(agg)
        assert 0 < result["hhi"] < 1
        assert result["effective_n"] > 1
        assert result["top_5_pct"] > 0

    def test_shared_holdings_alpha_beta(self, etf_alpha, etf_beta):
        """Alpha/Beta share AAPL and MSFT."""
        shared = shared_holdings(etf_alpha, etf_beta)
        assert len(shared) == 2
        keys = set(shared["composite_figi"])
        assert keys == {"AAPL", "MSFT"}  # matched by holding_ticker

    def test_shared_holdings_alpha_gamma(self, etf_alpha, etf_gamma):
        """Alpha/Gamma share nothing."""
        shared = shared_holdings(etf_alpha, etf_gamma)
        assert len(shared) == 0


# ---------------------------------------------------------------------------
# Redundancy tests
# ---------------------------------------------------------------------------

class TestRedundancy:

    def test_redundancy_gamma_unique(self, portfolio_positions, holdings_db):
        """Gamma has no overlap with Alpha/Beta → 0% redundancy."""
        result = redundancy_scores(portfolio_positions, holdings_db)
        gamma = result[result["etf_ticker"] == "GAMMA"].iloc[0]
        assert gamma["redundancy_pct"] == 0.0
        assert gamma["unique_pct"] == 100.0
        assert gamma["verdict"] == "green"

    def test_redundancy_alpha_nonzero(self, portfolio_positions, holdings_db):
        """Alpha shares AAPL+MSFT with Beta → nonzero redundancy."""
        result = redundancy_scores(portfolio_positions, holdings_db)
        alpha = result[result["etf_ticker"] == "ALPHA"].iloc[0]
        # AAPL (40%) + MSFT (30%) = 70% of Alpha is redundant
        assert abs(alpha["redundancy_pct"] - 70.0) < 0.01
        assert alpha["verdict"] == "red"

    def test_redundancy_beta_nonzero(self, portfolio_positions, holdings_db):
        """Beta shares AAPL+MSFT with Alpha → nonzero redundancy."""
        result = redundancy_scores(portfolio_positions, holdings_db)
        beta = result[result["etf_ticker"] == "BETA"].iloc[0]
        # AAPL (25%) + MSFT (25%) = 50% of Beta is redundant
        assert abs(beta["redundancy_pct"] - 50.0) < 0.01
        assert beta["verdict"] == "yellow"

    def test_ter_wasted(self, portfolio_positions, holdings_db):
        """TER wasted should be > 0 for redundant ETFs."""
        ter = {"ALPHA": 0.10, "BETA": 0.20, "GAMMA": 0.15}
        result = redundancy_scores(portfolio_positions, holdings_db, ter_override=ter)
        alpha = result[result["etf_ticker"] == "ALPHA"].iloc[0]
        # 70% redundancy * 0.10% TER * 50000 EUR = 35 EUR
        assert abs(alpha["ter_wasted"] - 35.0) < 0.01


# ---------------------------------------------------------------------------
# Active Share tests
# ---------------------------------------------------------------------------

class TestActiveShare:

    def test_active_share_identical(self, etf_alpha):
        """Portfolio identical to benchmark → Active Share = 0."""
        port = etf_alpha.rename(columns={"weight_pct": "real_weight_pct"})
        result = active_share(port, etf_alpha)
        assert result["active_share_pct"] == 0.0
        assert result["missed_exposures"].empty

    def test_active_share_completely_different(self, etf_alpha, etf_gamma):
        """Completely different holdings → Active Share = 100%."""
        # Rename columns for aggregated format
        port = etf_alpha.rename(columns={"weight_pct": "real_weight_pct"})
        result = active_share(port, etf_gamma)
        assert result["active_share_pct"] == 100.0

    def test_active_share_partial_overlap(self, etf_alpha, etf_beta):
        """Partial overlap → Active Share between 0 and 100."""
        port = etf_alpha.rename(columns={"weight_pct": "real_weight_pct"})
        result = active_share(port, etf_beta)
        assert 0 < result["active_share_pct"] < 100

    def test_top_active_bets(self, etf_alpha, etf_gamma):
        """All portfolio holdings should be active bets vs completely different benchmark."""
        port = etf_alpha.rename(columns={"weight_pct": "real_weight_pct"})
        result = active_share(port, etf_gamma)
        assert len(result["top_active_bets"]) == 4  # All 4 Alpha holdings

    def test_missed_exposures(self, etf_alpha, etf_gamma):
        """Benchmark holdings absent from portfolio should be in missed_exposures."""
        port = etf_alpha.rename(columns={"weight_pct": "real_weight_pct"})
        result = active_share(port, etf_gamma)
        missed = result["missed_exposures"]
        assert len(missed) == 3  # All 3 Gamma holdings are missed
