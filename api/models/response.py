"""Pydantic models for API responses."""

from typing import Dict, List, Optional

from pydantic import BaseModel


class KPIs(BaseModel):
    unique_securities: int
    hhi: float
    effective_n: float
    active_share: float
    top10_concentration: float


class HoldingRow(BaseModel):
    rank: int
    name: str
    ticker: str
    isin: Optional[str] = None
    weight_pct: float
    value_eur: float
    n_etfs: int
    sector: Optional[str] = None
    country: Optional[str] = None


class RedundancyItem(BaseModel):
    etf_ticker: str
    redundancy_pct: float
    ter_waste_eur: float
    covered_by: List[Dict[str, float]]


class OverlapPair(BaseModel):
    etf_a: str
    etf_b: str
    jaccard: float
    common_holdings_count: int


class OverlapResult(BaseModel):
    matrix: List[List[float]]
    tickers: List[str]
    pairs: List[OverlapPair]


class ActiveBet(BaseModel):
    ticker: str
    name: str
    portfolio_pct: float
    benchmark_pct: float
    delta_pct: float


class ActiveBets(BaseModel):
    overweight: List[ActiveBet]
    underweight: List[ActiveBet]


class ExposureItem(BaseModel):
    label: str
    portfolio_pct: float
    benchmark_pct: Optional[float] = None
    delta_pct: Optional[float] = None


class FactorDimension(BaseModel):
    name: str
    portfolio_score: float
    benchmark_score: float
    tilt: str
    sigma: float


class FactorCoverage(BaseModel):
    l1_pct: float
    l2_pct: float
    l3_pct: float
    l4_pct: float


class FactorResult(BaseModel):
    dimensions: List[FactorDimension]
    coverage: FactorCoverage
    reliability: str


class Insight(BaseModel):
    severity: str
    title: str
    body: str
    cta: Optional[str] = None


class FetchMetadata(BaseModel):
    sources: List[str]
    cached: bool
    as_of_date: str
    coverage_pct: float


class AnalysisResult(BaseModel):
    portfolio_id: str
    kpis: KPIs
    holdings: List[HoldingRow]
    active_bets: ActiveBets
    overlap: OverlapResult
    redundancy: List[RedundancyItem]
    sector_exposure: List[ExposureItem]
    country_exposure: List[ExposureItem]
    factors: FactorResult
    insights: List[Insight]
    fetch_metadata: FetchMetadata
