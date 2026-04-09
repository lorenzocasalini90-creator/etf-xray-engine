"""ETF search endpoint — reads CSV directly (no Streamlit dependency)."""

import logging
from pathlib import Path

import pandas as pd
from fastapi import APIRouter

logger = logging.getLogger(__name__)

router = APIRouter()

_CSV_PATH = Path(__file__).resolve().parent.parent.parent / "src" / "dashboard" / "data" / "etf_directory.csv"
_directory_cache: pd.DataFrame | None = None


def _load_directory() -> pd.DataFrame:
    """Load ETF directory CSV (cached in module)."""
    global _directory_cache
    if _directory_cache is None:
        if _CSV_PATH.exists():
            _directory_cache = pd.read_csv(_CSV_PATH, dtype=str).fillna("")
        else:
            logger.warning("ETF directory CSV not found at %s", _CSV_PATH)
            _directory_cache = pd.DataFrame(columns=["isin", "ticker", "name", "provider", "ter_pct"])
    return _directory_cache


@router.get("/search")
async def search_etfs(q: str = "", limit: int = 10):
    """Search ETFs by ticker, ISIN, or name."""
    if len(q.strip()) < 2:
        return []

    query = q.strip().upper()
    df = _load_directory()

    if df.empty:
        return []

    exact_ticker = df[df["ticker"].str.upper() == query]
    exact_isin = df[df["isin"].str.upper() == query]
    partial_ticker = df[df["ticker"].str.upper().str.startswith(query, na=False)]

    words = query.split()
    name_upper = df["name"].str.upper()
    mask = name_upper.str.contains(words[0], na=False)
    for word in words[1:]:
        mask = mask & name_upper.str.contains(word, na=False)
    partial_name = df[mask]

    combined = pd.concat([exact_ticker, exact_isin, partial_ticker, partial_name])
    combined = combined.drop_duplicates(subset="isin").head(limit)

    return combined.to_dict("records")
