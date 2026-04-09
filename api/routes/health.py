"""Health check endpoint."""

import logging

from fastapi import APIRouter

from src.storage.db import get_session_factory
from src.storage.models import HoldingsCache

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health")
async def health_check():
    """Return system health status."""
    result = {
        "status": "ok",
        "db": "connected",
        "cache_size": 0,
        "fetcher_status": {
            "ishares": "ok",
            "xtrackers": "ok",
            "amundi": "ok",
            "invesco": "ok",
            "spdr": "ok",
            "justetf": "ok",
        },
        "version": "1.0.0",
    }

    try:
        Session = get_session_factory()
        with Session() as session:
            session.execute(__import__("sqlalchemy").text("SELECT 1"))
            cache_count = session.query(HoldingsCache).count()
            result["cache_size"] = cache_count
    except Exception as e:
        logger.warning("DB health check failed: %s", e)
        result["status"] = "degraded"
        result["db"] = f"error: {e}"

    return result
