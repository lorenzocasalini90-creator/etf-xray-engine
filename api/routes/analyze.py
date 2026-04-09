"""Portfolio analysis endpoint — full pipeline integration."""

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import pandas as pd
from fastapi import APIRouter, HTTPException

from api.dependencies import get_orchestrator, get_session_factory_cached
from api.models.request import PortfolioRequest
from api.models.response import (
    ActiveBet,
    ActiveBets,
    AnalysisResult,
    ExposureItem,
    FactorCoverage,
    FactorDimension,
    FactorResult,
    FetchMetadata,
    HoldingRow,
    Insight,
    KPIs,
    OverlapPair,
    OverlapResult,
    RedundancyItem,
)
from src.analytics.aggregator import aggregate_portfolio, country_exposure, sector_exposure
from src.analytics.overlap import overlap_matrix, portfolio_hhi
from src.analytics.redundancy import redundancy_breakdown, redundancy_scores
from src.ingestion.orchestrator import FetchOrchestrator
from src.storage.cache import HoldingsCacheManager

logger = logging.getLogger(__name__)

router = APIRouter()


def _clean_str(val) -> str:
    """Sanitize a string to clean UTF-8, removing garbled chars."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val)
    # Re-encode to drop invalid sequences
    return s.encode("utf-8", errors="replace").decode("utf-8").strip()


def _fetch_all(
    positions: list[dict],
    orchestrator: FetchOrchestrator,
) -> tuple[dict[str, pd.DataFrame], list[str], list[str]]:
    """Fetch holdings for all ETFs in parallel.

    Returns:
        (holdings_db, sources, warnings)
    """
    holdings_db: dict[str, pd.DataFrame] = {}
    sources: list[str] = []
    warnings: list[str] = []

    def _fetch_one(ticker: str) -> tuple[str, object]:
        # Each thread gets its own orchestrator for thread safety
        factory = get_session_factory_cached()
        cache = HoldingsCacheManager(factory)
        orch = FetchOrchestrator(cache=cache)
        return ticker, orch.fetch(ticker)

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_fetch_one, p["ticker"]): p["ticker"] for p in positions}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                _, result = future.result()
                if result.status in ("success", "cached", "partial"):
                    if result.holdings is not None and not result.holdings.empty:
                        holdings_db[ticker] = result.holdings
                        sources.append(f"{ticker}:{result.source}")
                    else:
                        warnings.append(f"{ticker}: empty holdings from {result.source}")
                else:
                    warnings.append(f"{ticker}: {result.message}")
            except Exception as exc:
                logger.error("Fetch failed for %s: %s", ticker, exc)
                warnings.append(f"{ticker}: fetch error — {exc}")

    return holdings_db, sources, warnings


def _build_overlap(
    holdings_db: dict[str, pd.DataFrame],
) -> OverlapResult:
    """Compute overlap matrix and pairs."""
    tickers = list(holdings_db.keys())
    if len(tickers) < 2:
        return OverlapResult(
            matrix=[[100.0]] if tickers else [],
            tickers=tickers,
            pairs=[],
        )

    mat = overlap_matrix(holdings_db)
    matrix_list = mat.values.tolist()

    pairs = []
    labels = mat.columns.tolist()
    for i in range(len(labels)):
        for j in range(i + 1, len(labels)):
            pairs.append(OverlapPair(
                etf_a=labels[i],
                etf_b=labels[j],
                jaccard=round(mat.iloc[i, j], 2),
                common_holdings_count=0,  # not tracked by overlap_matrix
            ))

    return OverlapResult(matrix=matrix_list, tickers=labels, pairs=pairs)


def _build_redundancy(
    positions: list[dict],
    holdings_db: dict[str, pd.DataFrame],
) -> list[RedundancyItem]:
    """Compute redundancy for each ETF."""
    red_df = redundancy_scores(positions, holdings_db)
    items = []
    for _, row in red_df.iterrows():
        ticker = row["etf_ticker"]
        breakdown = redundancy_breakdown(ticker, holdings_db)
        covered_by = [{k: v} for k, v in breakdown.items()]
        items.append(RedundancyItem(
            etf_ticker=ticker,
            redundancy_pct=round(row["redundancy_pct"], 2),
            ter_waste_eur=round(row["ter_wasted"], 2),
            covered_by=covered_by,
        ))
    return items


def _build_active_bets(
    aggregated: pd.DataFrame,
    benchmark_name: str | None,
) -> tuple[ActiveBets, float | None]:
    """Compute active share and build ActiveBets response."""
    if not benchmark_name:
        return ActiveBets(overweight=[], underweight=[]), None

    try:
        from src.analytics.active_share import active_share
        from src.analytics.benchmark import BenchmarkManager

        bmgr = BenchmarkManager()
        bench_df = bmgr.get_benchmark_holdings(benchmark_name)
        result = active_share(aggregated, bench_df)

        overweight = []
        if not result["top_active_bets"].empty:
            for _, row in result["top_active_bets"].head(20).iterrows():
                overweight.append(ActiveBet(
                    ticker=row.get("composite_figi", ""),
                    name=row.get("name", ""),
                    portfolio_pct=round(row.get("portfolio_weight", 0) * 1, 4),
                    benchmark_pct=round(row.get("benchmark_weight", 0) * 1, 4),
                    delta_pct=round(row.get("overweight", 0) * 1, 4),
                ))

        underweight = []
        if not result["missed_exposures"].empty:
            for _, row in result["missed_exposures"].head(20).iterrows():
                underweight.append(ActiveBet(
                    ticker=row.get("composite_figi", ""),
                    name=row.get("name", ""),
                    portfolio_pct=0.0,
                    benchmark_pct=round(row.get("benchmark_weight", 0) * 1, 4),
                    delta_pct=round(-row.get("benchmark_weight", 0) * 1, 4),
                ))

        return (
            ActiveBets(overweight=overweight, underweight=underweight),
            result["active_share_pct"],
        )
    except Exception as exc:
        logger.warning("Active share failed: %s", exc)
        return ActiveBets(overweight=[], underweight=[]), None


def _build_exposure(
    aggregated: pd.DataFrame,
    fn,
    label_col: str,
) -> list[ExposureItem]:
    """Build sector or country exposure list."""
    df = fn(aggregated)
    items = []
    for _, row in df.iterrows():
        items.append(ExposureItem(
            label=row[label_col],
            portfolio_pct=round(row["weight_pct"], 2),
        ))
    return items


def _build_factors(
    aggregated: pd.DataFrame,
    benchmark_name: str | None,
) -> FactorResult:
    """Run factor engine and build FactorResult."""
    try:
        from src.factors.factor_engine import FactorEngine

        factory = get_session_factory_cached()
        session = factory()
        try:
            engine = FactorEngine(session, top_n_yfinance=30)

            bench_df = None
            if benchmark_name:
                try:
                    from src.analytics.benchmark import BenchmarkManager
                    bmgr = BenchmarkManager()
                    bench_df = bmgr.get_benchmark_holdings(benchmark_name)
                except Exception:
                    pass

            result = engine.analyze(aggregated, benchmark_df=bench_df)
            scores = result["factor_scores"]
            coverage = result["coverage_report"]
            comparison = result.get("benchmark_comparison") or {}

            dimensions = []

            # Value/Growth
            vg = scores.get("value_growth", {})
            bench_vg = comparison.get("value_growth", {})
            pe = vg.get("weighted_pe") or 0
            bench_pe = (pe - (bench_vg.get("pe_delta") or 0)) if bench_vg else 0
            dimensions.append(FactorDimension(
                name="Value/Growth",
                portfolio_score=pe,
                benchmark_score=bench_pe,
                tilt=vg.get("style", "Unknown"),
                sigma=bench_vg.get("pe_delta", 0) or 0,
            ))

            # Quality
            q = scores.get("quality", {})
            bench_q = comparison.get("quality", {})
            roe = (q.get("weighted_roe") or 0) * 100
            bench_roe = roe - ((bench_q.get("roe_delta") or 0) * 100)
            dimensions.append(FactorDimension(
                name="Quality",
                portfolio_score=round(roe, 2),
                benchmark_score=round(bench_roe, 2),
                tilt="High" if roe > 15 else "Low" if roe < 10 else "Neutral",
                sigma=round((bench_q.get("roe_delta") or 0) * 100, 2),
            ))

            # Size
            size = scores.get("size", {})
            large = size.get("Large", 0)
            dimensions.append(FactorDimension(
                name="Size",
                portfolio_score=large,
                benchmark_score=large - (comparison.get("size", {}).get("Large_delta", 0) or 0),
                tilt="Large Cap" if large > 70 else "Mid Cap" if large > 40 else "Small Cap",
                sigma=comparison.get("size", {}).get("Large_delta", 0) or 0,
            ))

            # Dividend Yield
            dy = scores.get("dividend_yield", {})
            bench_dy = comparison.get("dividend_yield", {})
            dyield = (dy.get("weighted_yield") or 0) * 100
            bench_dyield = dyield - ((bench_dy.get("yield_delta") or 0) * 100)
            dimensions.append(FactorDimension(
                name="Dividend Yield",
                portfolio_score=round(dyield, 2),
                benchmark_score=round(bench_dyield, 2),
                tilt="High Yield" if dyield > 3 else "Low Yield" if dyield < 1.5 else "Neutral",
                sigma=round((bench_dy.get("yield_delta") or 0) * 100, 2),
            ))

            # Determine reliability
            l2_pct = coverage.get("L2_pct", 0)
            if l2_pct >= 60:
                reliability = "high"
            elif l2_pct >= 30:
                reliability = "medium"
            else:
                reliability = "low"

            return FactorResult(
                dimensions=dimensions,
                coverage=FactorCoverage(
                    l1_pct=coverage.get("L1_pct", 0),
                    l2_pct=coverage.get("L2_pct", 0),
                    l3_pct=coverage.get("L3_pct", 0),
                    l4_pct=coverage.get("L4_pct", 0),
                ),
                reliability=reliability,
            )
        finally:
            session.close()
    except Exception as exc:
        logger.warning("Factor analysis failed: %s", exc)
        return FactorResult(
            dimensions=[],
            coverage=FactorCoverage(l1_pct=0, l2_pct=0, l3_pct=0, l4_pct=0),
            reliability="low",
        )


def _build_insights(
    aggregated: pd.DataFrame,
    hhi_stats: dict,
    active_share_pct: float | None,
    redundancy_items: list[RedundancyItem],
    overlap_result: OverlapResult,
    benchmark_name: str | None,
    fetch_warnings: list[str],
) -> list[Insight]:
    """Generate portfolio insights."""
    insights: list[Insight] = []

    # Add fetch warnings as insights
    for w in fetch_warnings:
        insights.append(Insight(
            severity="warning",
            title="Dati parziali",
            body=w,
        ))

    try:
        from src.analytics.observations import generate_observations

        # Build inputs for generate_observations
        red_scores = {r.etf_ticker: r.redundancy_pct / 100 for r in redundancy_items}
        ter_wasted = {r.etf_ticker: r.ter_waste_eur for r in redundancy_items}

        overlap_pairs = [
            (p.etf_a, p.etf_b, p.jaccard) for p in overlap_result.pairs
        ]

        top1 = aggregated.nlargest(1, "real_weight_pct").iloc[0] if not aggregated.empty else None

        # US exposure
        us_weight = 0.0
        country_df = country_exposure(aggregated)
        if not country_df.empty:
            us_row = country_df[country_df["country"].str.contains("United States", case=False, na=False)]
            us_weight = us_row["weight_pct"].sum() if not us_row.empty else 0.0

        bench_labels = {
            "MSCI_WORLD": "MSCI World", "SP500": "S&P 500",
            "MSCI_EM": "MSCI EM", "FTSE_ALL_WORLD": "FTSE All-World",
        }
        bench_display = bench_labels.get(benchmark_name or "", "mercato")

        observations = generate_observations(
            hhi=hhi_stats["hhi"],
            effective_n=hhi_stats["effective_n"],
            active_share=active_share_pct,
            top10_weight=hhi_stats["top_10_pct"],
            top1_name=top1["name"] if top1 is not None else "",
            top1_weight=(top1["real_weight_pct"] / 100) if top1 is not None else 0,
            redundancy_scores=red_scores,
            ter_wasted_eur=ter_wasted,
            overlap_pairs=overlap_pairs,
            us_weight=us_weight,
            benchmark_name=bench_display,
        )

        severity_map = {"high": "critical", "medium": "warning", "info": "positive"}
        for obs in observations:
            insights.append(Insight(
                severity=severity_map.get(obs.severity, "warning"),
                title=obs.page.replace("_", " ").title(),
                body=obs.text,
            ))
    except Exception as exc:
        logger.warning("Insight generation failed: %s", exc)

    return insights


@router.post("/analyze", response_model=AnalysisResult)
async def analyze_portfolio(request: PortfolioRequest):
    """Run full portfolio X-Ray analysis."""
    portfolio_id = str(uuid.uuid4())
    orchestrator = get_orchestrator()

    # Convert Pydantic positions to dicts matching src/ conventions
    positions = [{"ticker": p.ticker, "capital": p.amount_eur} for p in request.positions]

    # 1. Fetch all holdings
    holdings_db, sources, fetch_warnings = _fetch_all(positions, orchestrator)

    if not holdings_db:
        raise HTTPException(
            status_code=422,
            detail="Could not fetch holdings for any ETF. " + "; ".join(fetch_warnings),
        )

    # 2. Aggregate portfolio
    try:
        aggregated = aggregate_portfolio(positions, holdings_db)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Aggregation failed: {exc}")

    if aggregated.empty:
        raise HTTPException(status_code=422, detail="Aggregation produced no results")

    # 3. Enrich missing sector/country
    try:
        from src.analytics.enrichment import enrich_missing_data

        factory = get_session_factory_cached()
        session = factory()
        aggregated = enrich_missing_data(aggregated, db_session=session)
        session.close()
    except Exception as exc:
        logger.warning("Enrichment failed: %s", exc)

    # 4. Concentration metrics
    hhi_stats = portfolio_hhi(aggregated)

    # 5. Overlap
    try:
        overlap_result = _build_overlap(holdings_db)
    except Exception as exc:
        logger.warning("Overlap failed: %s", exc)
        overlap_result = OverlapResult(matrix=[], tickers=[], pairs=[])

    # 6. Redundancy
    try:
        redundancy_items = _build_redundancy(positions, holdings_db)
    except Exception as exc:
        logger.warning("Redundancy failed: %s", exc)
        redundancy_items = []

    # 7. Active bets
    active_bets, active_share_pct = _build_active_bets(aggregated, request.benchmark)

    # 8. Sector/Country exposure
    sector_items = _build_exposure(aggregated, sector_exposure, "sector")
    country_items = _build_exposure(aggregated, country_exposure, "country")

    # 9. Factors
    factor_result = _build_factors(aggregated, request.benchmark)

    # 10. Insights
    insights = _build_insights(
        aggregated, hhi_stats, active_share_pct,
        redundancy_items, overlap_result, request.benchmark, fetch_warnings,
    )

    # 11. Build holdings list
    total_capital = sum(p["capital"] for p in positions)
    holdings_list = []
    for rank, (_, row) in enumerate(aggregated.head(100).iterrows(), 1):
        holdings_list.append(HoldingRow(
            rank=rank,
            name=_clean_str(row.get("name", "")),
            ticker=_clean_str(row.get("ticker", "")),
            isin=None,
            weight_pct=round(row.get("real_weight_pct", 0), 4),
            value_eur=round(row.get("real_weight_pct", 0) / 100 * total_capital, 2),
            n_etfs=int(row.get("n_etf_sources", 1)),
            sector=_clean_str(row.get("sector")) or None,
            country=_clean_str(row.get("country")) or None,
        ))

    # 12. Fetch metadata
    any_cached = any("cache" in s.lower() for s in sources)
    as_of = date.today().isoformat()

    # Coverage: sum of weights in aggregated vs 100%
    total_weight = aggregated["real_weight_pct"].sum() if not aggregated.empty else 0
    coverage_pct = min(round(total_weight, 1), 100.0)

    return AnalysisResult(
        portfolio_id=portfolio_id,
        kpis=KPIs(
            unique_securities=len(aggregated),
            hhi=round(hhi_stats["hhi"], 4),
            effective_n=round(hhi_stats["effective_n"], 1),
            active_share=round(active_share_pct, 2) if active_share_pct is not None else 0.0,
            top10_concentration=round(hhi_stats["top_10_pct"], 2),
        ),
        holdings=holdings_list,
        active_bets=active_bets,
        overlap=overlap_result,
        redundancy=redundancy_items,
        sector_exposure=sector_items,
        country_exposure=country_items,
        factors=factor_result,
        insights=insights,
        fetch_metadata=FetchMetadata(
            sources=sources,
            cached=any_cached,
            as_of_date=as_of,
            coverage_pct=coverage_pct,
        ),
    )
