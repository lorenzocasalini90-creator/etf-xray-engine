"""ETF search endpoint (stub)."""

from fastapi import APIRouter

router = APIRouter()


@router.get("/etf/search")
async def search_etfs(q: str = ""):
    """Search ETFs by ticker or name."""
    raise NotImplementedError("Coming in M1-b")
