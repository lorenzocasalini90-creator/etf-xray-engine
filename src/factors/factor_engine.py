"""Factor Engine: 5-dimension factor scoring with fallback hierarchy.

Computes Size, Value/Growth, Quality, Momentum, Dividend Yield for a
portfolio, with L1→L2→L3→L4 cascade and coverage disclosure.
"""

import logging
import time
from dataclasses import dataclass, field

import pandas as pd
import yfinance as yf
from sqlalchemy.orm import Session

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from collections.abc import Callable

from src.factors.fundamentals import FundamentalsProvider
from src.factors.sector_proxies import GICS_SECTOR_MEDIANS, get_sector_proxy

logger = logging.getLogger(__name__)

# Market cap buckets (USD)
LARGE_CAP_THRESHOLD = 10e9   # > $10B
MID_CAP_THRESHOLD = 2e9      # $2B–$10B
# Below $2B → Small

# Value/Growth classification by P/E
VG_DEEP_VALUE = 15
VG_VALUE = 20
VG_BLEND = 25


@dataclass
class CoverageReport:
    """Tracks how each holding was classified."""

    l1_sector: int = 0
    l2_fundamentals: int = 0
    l3_proxy: int = 0
    l4_unclassified: int = 0
    total: int = 0
    l1_weight: float = 0.0
    l2_weight: float = 0.0
    l3_weight: float = 0.0
    l4_weight: float = 0.0
    total_weight: float = 0.0

    def as_dict(self) -> dict:
        """Return coverage as a dict with percentages."""
        tw = self.total_weight if self.total_weight > 0 else 1.0
        return {
            "total_holdings": self.total,
            "L1_sector_count": self.l1_sector,
            "L2_fundamentals_count": self.l2_fundamentals,
            "L3_proxy_count": self.l3_proxy,
            "L4_unclassified_count": self.l4_unclassified,
            "L1_pct": round(self.l1_weight / tw * 100, 1),
            "L2_pct": round(self.l2_weight / tw * 100, 1),
            "L3_pct": round(self.l3_weight / tw * 100, 1),
            "L4_pct": round(self.l4_weight / tw * 100, 1),
        }


class FactorEngine:
    """Orchestrate factor scoring with fallback hierarchy.

    Cascade:
        L1: GICS sector from holdings data
        L2: yfinance fundamentals (cached in DB)
        L3: Sector proxy medians
        L4: Unclassified

    Args:
        session: SQLAlchemy session.
        top_n_yfinance: Max tickers to fetch from yfinance (rate limit).
    """

    def __init__(self, session: Session, top_n_yfinance: int = 50) -> None:
        self.session = session
        self.top_n_yfinance = top_n_yfinance
        self.fundamentals = FundamentalsProvider(session)
        self.coverage = CoverageReport()

    def _classify_size(self, market_cap: float | None) -> str:
        """Classify market cap into Large/Mid/Small."""
        if market_cap is None:
            return "Unknown"
        if market_cap >= LARGE_CAP_THRESHOLD:
            return "Large"
        if market_cap >= MID_CAP_THRESHOLD:
            return "Mid"
        return "Small"

    def _classify_value_growth(self, pe: float | None) -> str:
        """Classify P/E into value/growth style."""
        if pe is None:
            return "Unknown"
        if pe < VG_DEEP_VALUE:
            return "Deep Value"
        if pe < VG_VALUE:
            return "Value"
        if pe < VG_BLEND:
            return "Blend"
        return "Growth"

    def _resolve_fundamentals(
        self,
        df: pd.DataFrame,
    ) -> tuple[dict[str, dict], CoverageReport]:
        """Resolve fundamentals for each holding using the cascade.

        Returns:
            Tuple of (ticker → fundamentals dict, CoverageReport).
        """
        coverage = CoverageReport()
        coverage.total = len(df)
        df = df.copy()
        df["real_weight_pct"] = pd.to_numeric(df["real_weight_pct"], errors="coerce").fillna(0.0)
        coverage.total_weight = df["real_weight_pct"].sum()

        results: dict[str, dict] = {}

        # Determine which tickers to try with yfinance (top N by weight)
        sorted_df = df.sort_values("real_weight_pct", ascending=False)
        # Resolve the ticker column name: prefer "ticker", fallback to "holding_ticker"
        _ticker_col = "ticker"
        if _ticker_col not in sorted_df.columns:
            if "holding_ticker" in sorted_df.columns:
                _ticker_col = "holding_ticker"
            else:
                # No ticker column available — skip yfinance fetch entirely
                _ticker_col = None
        top_tickers: set[str] = set()
        if _ticker_col is not None:
            top_tickers = set(
                sorted_df.head(self.top_n_yfinance)[_ticker_col]
                .dropna()
                .unique()
            )

        # Batch fetch from yfinance for top tickers
        yf_batch = []
        yf_no_cache: list[str] = []
        figi_id_map: dict[str, int] = {}
        seen_tickers: set[str] = set()
        for _, row in sorted_df.iterrows():
            ticker = row.get(_ticker_col, "") if _ticker_col else ""
            figi = row.get("composite_figi", "")
            if not ticker or pd.isna(ticker) or ticker not in top_tickers:
                continue
            if ticker in seen_tickers:
                continue
            seen_tickers.add(ticker)
            figi_id = self._get_or_create_figi_id(figi, ticker)
            if figi_id:
                figi_id_map[ticker] = figi_id
                yf_batch.append({"ticker": ticker, "figi_id": figi_id})
            else:
                yf_no_cache.append(ticker)

        # Fetch fundamentals from yfinance (with DB caching where possible)
        yf_results: dict[str, dict] = {}
        if yf_batch:
            yf_results = self.fundamentals.fetch_batch(yf_batch)
        # For tickers without figi_id, fetch directly (no DB cache)
        for ticker in yf_no_cache:
            data = self.fundamentals._fetch_from_yfinance(ticker)
            if data is not None:
                yf_results[ticker] = data
            if ticker != yf_no_cache[-1]:
                time.sleep(0.5)

        # Now classify each holding
        for _, row in df.iterrows():
            ticker = row.get(_ticker_col, "") if _ticker_col else ""
            sector = row.get("sector", "")
            weight = row.get("real_weight_pct", 0)
            level = "L4"
            fundamentals: dict = {}

            # L2: yfinance fundamentals
            if ticker and ticker in yf_results:
                fundamentals = yf_results[ticker]
                level = "L2"
                coverage.l2_fundamentals += 1
                coverage.l2_weight += weight
            # L3: sector proxy
            elif sector and not pd.isna(sector):
                proxy = get_sector_proxy(sector)
                if proxy:
                    fundamentals = {
                        "pe_ratio": proxy["median_pe"],
                        "pb_ratio": proxy["median_pb"],
                        "roe": proxy["median_roe"],
                        "debt_to_equity": None,
                        "dividend_yield": None,
                        "market_cap": None,
                    }
                    level = "L3"
                    coverage.l3_proxy += 1
                    coverage.l3_weight += weight
                else:
                    coverage.l4_unclassified += 1
                    coverage.l4_weight += weight
            else:
                coverage.l4_unclassified += 1
                coverage.l4_weight += weight

            # L1 is implicit: we always use the sector from holdings
            if sector and not pd.isna(sector) and level != "L4":
                coverage.l1_sector += 1
                coverage.l1_weight += weight

            key = ticker if ticker else row.get("composite_figi", str(_))
            results[key] = {
                "fundamentals": fundamentals,
                "level": level,
                "sector": sector,
                "weight": weight,
                "ticker": ticker,
            }

        self.coverage = coverage
        return results, coverage

    def _get_or_create_figi_id(self, composite_figi: str, ticker: str) -> int | None:
        """Look up figi_mapping.id for a composite_figi or ticker."""
        from src.storage.models import FigiMapping

        if composite_figi and not pd.isna(composite_figi):
            row = (
                self.session.query(FigiMapping)
                .filter(FigiMapping.composite_figi == composite_figi)
                .first()
            )
            if row:
                return row.id

        if ticker and not pd.isna(ticker):
            row = (
                self.session.query(FigiMapping)
                .filter(FigiMapping.ticker == ticker)
                .first()
            )
            if row:
                return row.id

        return None

    def _compute_weighted_factors(
        self,
        resolved: dict[str, dict],
    ) -> dict:
        """Compute portfolio-level factor scores as weighted averages."""
        total_weight = sum(r["weight"] for r in resolved.values())
        if total_weight == 0:
            return self._empty_factors()

        # Accumulators
        pe_sum = pb_sum = roe_sum = de_sum = dy_sum = 0.0
        pe_w = pb_w = roe_w = de_w = dy_w = 0.0
        size_buckets = {"Large": 0.0, "Mid": 0.0, "Small": 0.0, "Unknown": 0.0}

        for r in resolved.values():
            f = r.get("fundamentals", {})
            w = r["weight"]
            if not f:
                size_buckets["Unknown"] += w
                continue

            pe = f.get("pe_ratio")
            pb = f.get("pb_ratio")
            roe = f.get("roe")
            de = f.get("debt_to_equity")
            dy = f.get("dividend_yield")
            mc = f.get("market_cap")

            if pe is not None:
                pe_sum += pe * w
                pe_w += w
            if pb is not None:
                pb_sum += pb * w
                pb_w += w
            if roe is not None:
                roe_sum += roe * w
                roe_w += w
            if de is not None:
                de_sum += de * w
                de_w += w
            if dy is not None:
                dy_sum += dy * w
                dy_w += w

            size_buckets[self._classify_size(mc)] += w

        weighted_pe = pe_sum / pe_w if pe_w > 0 else None
        weighted_pb = pb_sum / pb_w if pb_w > 0 else None
        weighted_roe = roe_sum / roe_w if roe_w > 0 else None
        weighted_de = de_sum / de_w if de_w > 0 else None
        weighted_dy = dy_sum / dy_w if dy_w > 0 else None

        # Normalize size buckets to %
        for k in size_buckets:
            size_buckets[k] = round(size_buckets[k] / total_weight * 100, 1)

        return {
            "size": size_buckets,
            "value_growth": {
                "weighted_pe": round(weighted_pe, 2) if weighted_pe else None,
                "weighted_pb": round(weighted_pb, 2) if weighted_pb else None,
                "style": self._classify_value_growth(weighted_pe),
            },
            "quality": {
                "weighted_roe": round(weighted_roe, 4) if weighted_roe else None,
                "weighted_debt_equity": round(weighted_de, 2) if weighted_de else None,
            },
            "dividend_yield": {
                "weighted_yield": round(weighted_dy, 4) if weighted_dy else None,
            },
            # Momentum is computed separately via _compute_momentum()
            # and merged in analyze(). Placeholder here for consistency.
            "momentum": {"weighted_return": None, "score": None},
        }

    @staticmethod
    def _fetch_ticker_return(ticker: str) -> float | None:
        """Fetch 12-month return for a single ticker (thread-safe)."""
        try:
            hist = yf.Ticker(ticker).history(period="1y", timeout=2)
            if hist is None or hist.empty or len(hist) < 2:
                return None
            price_old = hist["Close"].iloc[0]
            price_new = hist["Close"].iloc[-1]
            if price_old > 0:
                return (price_new - price_old) / price_old * 100
        except Exception:
            return None
        return None

    def _compute_momentum(
        self,
        resolved: dict[str, dict],
        top_n: int = 10,
    ) -> dict:
        """Compute weighted-average 12-month return (momentum score).

        Fetches 1-year price history in parallel for the top-N holdings
        by weight, computes each holding's 12M return, then a weighted
        average.

        The raw return is normalised to a 0–100 scale:
            -50% return → 0, 0% → 50, +50% → 100.

        Returns:
            Dict with ``weighted_return`` (raw %) and ``score`` (0–100).
            Both are ``None`` if no price data could be fetched.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Sort by weight, keep top_n with valid tickers
        items = sorted(resolved.values(), key=lambda r: r["weight"], reverse=True)
        ticker_weight: list[tuple[str, float]] = []
        for r in items:
            t = r.get("ticker", "")
            if t:
                ticker_weight.append((t, r["weight"]))
            if len(ticker_weight) >= top_n:
                break

        if not ticker_weight:
            return {"weighted_return": None, "score": None}

        # Parallel fetch — max 10s total wall-clock
        ret_sum = 0.0
        ret_weight = 0.0

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {
                pool.submit(self._fetch_ticker_return, t): (t, w)
                for t, w in ticker_weight
            }
            for future in as_completed(futures, timeout=10):
                ticker, w = futures[future]
                try:
                    ret_12m = future.result(timeout=0)
                except Exception:
                    continue
                if ret_12m is not None:
                    ret_sum += ret_12m * w
                    ret_weight += w

        if ret_weight == 0:
            return {"weighted_return": None, "score": None}

        weighted_return = ret_sum / ret_weight
        # Normalise: -50% → 0, 0% → 50, +50% → 100  (clamp 0–100)
        score = max(0.0, min(100.0, weighted_return + 50.0))

        return {
            "weighted_return": round(weighted_return, 2),
            "score": round(score, 1),
        }

    @staticmethod
    def _compute_momentum_proxy(resolved: dict[str, dict]) -> dict:
        """Sector-based momentum proxy (L3 fallback).

        Uses historical sector momentum tendencies to estimate portfolio
        momentum when yfinance price data is unavailable.

        Scores per sector (long-run relative momentum tendency, 0–100):
            >50 = sectors that historically exhibit positive momentum
            <50 = sectors that historically lag
        """
        SECTOR_MOMENTUM: dict[str, float] = {
            "Technology": 62,
            "Consumer Discretionary": 58,
            "Communication Services": 55,
            "Healthcare": 53,
            "Industrials": 52,
            "Financials": 50,
            "Consumer Staples": 47,
            "Materials": 46,
            "Real Estate": 45,
            "Energy": 43,
            "Utilities": 40,
        }
        DEFAULT_SCORE = 50.0

        score_sum = 0.0
        weight_sum = 0.0

        for r in resolved.values():
            sector = r.get("sector", "")
            w = r.get("weight", 0)
            if not sector or not w:
                continue
            s = SECTOR_MOMENTUM.get(sector, DEFAULT_SCORE)
            score_sum += s * w
            weight_sum += w

        if weight_sum == 0:
            return {"weighted_return": None, "score": 50.0, "source": "proxy"}

        score = score_sum / weight_sum
        return {
            "weighted_return": None,
            "score": round(score, 1),
            "source": "proxy",
        }

    def _empty_factors(self) -> dict:
        return {
            "size": {"Large": 0, "Mid": 0, "Small": 0, "Unknown": 100},
            "value_growth": {"weighted_pe": None, "weighted_pb": None, "style": "Unknown"},
            "quality": {"weighted_roe": None, "weighted_debt_equity": None},
            "dividend_yield": {"weighted_yield": None},
            "momentum": {"weighted_return": None, "score": None},
        }

    def _find_factor_drivers(
        self,
        resolved: dict[str, dict],
        top_n: int = 5,
    ) -> dict:
        """Find top holdings driving each factor tilt."""
        items = sorted(
            resolved.values(),
            key=lambda r: r["weight"],
            reverse=True,
        )

        drivers: dict[str, list] = {
            "value_growth": [],
            "quality": [],
            "size": [],
        }

        for r in items[:top_n]:
            f = r.get("fundamentals", {})
            entry = {
                "ticker": r["ticker"],
                "weight": round(r["weight"], 2),
                "sector": r["sector"],
            }

            pe = f.get("pe_ratio")
            roe = f.get("roe")
            mc = f.get("market_cap")

            drivers["value_growth"].append({
                **entry,
                "pe_ratio": pe,
                "style": self._classify_value_growth(pe),
            })
            drivers["quality"].append({
                **entry,
                "roe": roe,
                "debt_equity": f.get("debt_to_equity"),
            })
            drivers["size"].append({
                **entry,
                "market_cap": mc,
                "bucket": self._classify_size(mc),
            })

        return drivers

    def analyze(
        self,
        portfolio_df: pd.DataFrame,
        benchmark_df: pd.DataFrame | None = None,
        progress_callback: "Callable[[float, str], None] | None" = None,
    ) -> dict:
        """Run full factor analysis on a portfolio.

        Args:
            portfolio_df: Output of aggregate_portfolio() with columns:
                composite_figi, ticker, sector, real_weight_pct.
            benchmark_df: Optional benchmark aggregated DataFrame (same schema).

        Returns:
            Dict with keys:
                - factor_scores: 5 factor dimensions
                - benchmark_comparison: delta vs benchmark (if provided)
                - coverage_report: L1/L2/L3/L4 breakdown
                - factor_drivers: top holdings per factor
        """
        def _progress(pct: float, msg: str) -> None:
            if progress_callback:
                progress_callback(pct, msg)

        _progress(0.05, "Carico dati fondamentali (P/E, P/B, ROE)…")
        resolved, coverage = self._resolve_fundamentals(portfolio_df)

        _progress(0.45, "Classifico titoli per Size e Value/Growth…")
        factor_scores = self._compute_weighted_factors(resolved)

        _progress(0.50, "Calcolo Momentum (rendimenti 12M)…")
        from concurrent.futures import ThreadPoolExecutor as _TPE, TimeoutError as _TE
        try:
            with _TPE(max_workers=1) as _ex:
                _fut = _ex.submit(self._compute_momentum, resolved)
                momentum = _fut.result(timeout=8)
        except _TE:
            logger.warning("Momentum timeout (>8s) — falling back to sector proxy")
            momentum = {"weighted_return": None, "score": None}
        except Exception as exc:
            logger.warning("Momentum computation failed: %s — falling back to sector proxy", exc)
            momentum = {"weighted_return": None, "score": None}

        # L3 fallback: sector-based proxy when yfinance returns no score
        if momentum.get("score") is None:
            momentum = self._compute_momentum_proxy(resolved)
            logger.info("Momentum using sector proxy: score=%.1f", momentum.get("score", 0))
        factor_scores["momentum"] = momentum

        _progress(0.55, "Identifico factor drivers…")
        factor_drivers = self._find_factor_drivers(resolved)

        result = {
            "factor_scores": factor_scores,
            "coverage_report": coverage.as_dict(),
            "factor_drivers": factor_drivers,
            "benchmark_comparison": None,
        }

        if benchmark_df is not None and not benchmark_df.empty:
            _progress(0.65, "Analizzo benchmark…")
            # Benchmark DataFrames use 'weight_pct'; normalize to 'real_weight_pct'
            if "real_weight_pct" not in benchmark_df.columns and "weight_pct" in benchmark_df.columns:
                benchmark_df = benchmark_df.copy()
                benchmark_df["real_weight_pct"] = pd.to_numeric(
                    benchmark_df["weight_pct"], errors="coerce"
                ).fillna(0.0)
            bench_resolved, _ = self._resolve_fundamentals(benchmark_df)

            _progress(0.85, "Calcolo delta vs benchmark…")
            bench_factors = self._compute_weighted_factors(bench_resolved)
            result["benchmark_comparison"] = self._compute_delta(
                factor_scores, bench_factors,
            )

        _progress(1.0, "Completato")
        return result

    def _compute_delta(self, portfolio: dict, benchmark: dict) -> dict:
        """Compute factor score deltas: portfolio - benchmark."""
        delta: dict = {}

        # Value/Growth
        p_pe = portfolio["value_growth"].get("weighted_pe")
        b_pe = benchmark["value_growth"].get("weighted_pe")
        p_pb = portfolio["value_growth"].get("weighted_pb")
        b_pb = benchmark["value_growth"].get("weighted_pb")
        delta["value_growth"] = {
            "pe_delta": round(p_pe - b_pe, 2) if p_pe and b_pe else None,
            "pb_delta": round(p_pb - b_pb, 2) if p_pb and b_pb else None,
            "portfolio_style": portfolio["value_growth"]["style"],
            "benchmark_style": benchmark["value_growth"]["style"],
        }

        # Quality
        p_roe = portfolio["quality"].get("weighted_roe")
        b_roe = benchmark["quality"].get("weighted_roe")
        p_de = portfolio["quality"].get("weighted_debt_equity")
        b_de = benchmark["quality"].get("weighted_debt_equity")
        delta["quality"] = {
            "roe_delta": round(p_roe - b_roe, 4) if p_roe and b_roe else None,
            "debt_equity_delta": round(p_de - b_de, 2) if p_de and b_de else None,
        }

        # Dividend Yield
        p_dy = portfolio["dividend_yield"].get("weighted_yield")
        b_dy = benchmark["dividend_yield"].get("weighted_yield")
        delta["dividend_yield"] = {
            "yield_delta": round(p_dy - b_dy, 4) if p_dy and b_dy else None,
        }

        # Size
        delta["size"] = {}
        for bucket in ("Large", "Mid", "Small", "Unknown"):
            p_val = portfolio["size"].get(bucket, 0)
            b_val = benchmark["size"].get(bucket, 0)
            delta["size"][f"{bucket}_delta"] = round(p_val - b_val, 1)

        return delta
