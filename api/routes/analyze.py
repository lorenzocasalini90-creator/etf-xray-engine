"""Portfolio analysis endpoint (stub)."""

from fastapi import APIRouter

from api.models.request import PortfolioRequest
from api.models.response import AnalysisResult

router = APIRouter()


@router.post("/analyze", response_model=AnalysisResult)
async def analyze_portfolio(request: PortfolioRequest):
    """Run full portfolio X-Ray analysis."""
    raise NotImplementedError("Coming in M1-b")
