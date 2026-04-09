"""Holdings lookup endpoint."""

import logging
from datetime import date

import pandas as pd
from fastapi import APIRouter, HTTPException

from api.dependencies import get_orchestrator

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/holdings/{ticker}")
async def get_holdings(ticker: str):
    """Fetch holdings for a single ETF."""
    ticker = ticker.strip().upper()
    orchestrator = get_orchestrator()

    result = orchestrator.fetch(ticker)

    if result.status == "failed" or result.holdings is None or result.holdings.empty:
        raise HTTPException(status_code=404, detail=f"No holdings found for {ticker}")

    df = result.holdings
    holdings_list = []
    for _, row in df.iterrows():
        holdings_list.append({
            "name": row.get("holding_name", ""),
            "isin": row.get("holding_isin", ""),
            "ticker": row.get("holding_ticker", ""),
            "weight_pct": round(float(row.get("weight_pct", 0) or 0), 4),
            "sector": row.get("sector", ""),
            "country": row.get("country", ""),
        })

    return {
        "ticker": ticker,
        "source": result.source,
        "status": result.status,
        "coverage_pct": result.coverage_pct,
        "as_of_date": date.today().isoformat(),
        "num_holdings": len(holdings_list),
        "holdings": holdings_list,
    }
