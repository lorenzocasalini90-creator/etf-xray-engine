"""Holdings lookup endpoint (stub)."""

from fastapi import APIRouter

router = APIRouter()


@router.get("/holdings/{ticker}")
async def get_holdings(ticker: str):
    """Fetch holdings for a single ETF."""
    raise NotImplementedError("Coming in M1-b")
