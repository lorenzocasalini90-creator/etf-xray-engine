"""Sector proxy medians: GICS hardcoded fallback (Level 3).

Provides sector-level median fundamentals when individual security data
is unavailable from yfinance.
"""

import logging
from datetime import date

from sqlalchemy.orm import Session

from src.storage.models import SectorFactorProxy

logger = logging.getLogger(__name__)

# Hardcoded GICS sector medians — fallback Level 3
GICS_SECTOR_MEDIANS: dict[str, dict] = {
    "Technology": {
        "median_pe": 28.0,
        "median_pb": 7.5,
        "median_roe": 0.25,
        "style": "Growth",
    },
    "Healthcare": {
        "median_pe": 22.0,
        "median_pb": 4.0,
        "median_roe": 0.18,
        "style": "Blend",
    },
    "Financials": {
        "median_pe": 12.0,
        "median_pb": 1.3,
        "median_roe": 0.12,
        "style": "Value",
    },
    "Consumer Discretionary": {
        "median_pe": 25.0,
        "median_pb": 5.0,
        "median_roe": 0.20,
        "style": "Growth",
    },
    "Industrials": {
        "median_pe": 20.0,
        "median_pb": 3.5,
        "median_roe": 0.16,
        "style": "Blend",
    },
    "Consumer Staples": {
        "median_pe": 22.0,
        "median_pb": 4.5,
        "median_roe": 0.22,
        "style": "Blend",
    },
    "Energy": {
        "median_pe": 11.0,
        "median_pb": 1.8,
        "median_roe": 0.15,
        "style": "Value",
    },
    "Utilities": {
        "median_pe": 16.0,
        "median_pb": 1.9,
        "median_roe": 0.10,
        "style": "Value",
    },
    "Materials": {
        "median_pe": 15.0,
        "median_pb": 2.2,
        "median_roe": 0.13,
        "style": "Value",
    },
    "Real Estate": {
        "median_pe": 35.0,
        "median_pb": 2.5,
        "median_roe": 0.08,
        "style": "Blend",
    },
    "Communication Services": {
        "median_pe": 18.0,
        "median_pb": 3.0,
        "median_roe": 0.15,
        "style": "Blend",
    },
}


def get_sector_proxy(gics_sector: str) -> dict | None:
    """Return median fundamentals for a GICS sector.

    Args:
        gics_sector: GICS sector name (e.g. 'Technology').

    Returns:
        Dict with median_pe, median_pb, median_roe, style — or None
        if the sector is not in the lookup table.
    """
    return GICS_SECTOR_MEDIANS.get(gics_sector)


def save_sector_proxies(session: Session, as_of: date | None = None) -> None:
    """Persist all sector proxy medians to the sector_factor_proxies table.

    Args:
        session: SQLAlchemy session.
        as_of: Date for the snapshot. Defaults to today.
    """
    snapshot_date = as_of or date.today()

    for sector, medians in GICS_SECTOR_MEDIANS.items():
        for factor_name in ("median_pe", "median_pb", "median_roe"):
            existing = (
                session.query(SectorFactorProxy)
                .filter(
                    SectorFactorProxy.sector == sector,
                    SectorFactorProxy.factor_name == factor_name,
                    SectorFactorProxy.as_of_date == snapshot_date,
                )
                .first()
            )
            if existing:
                existing.factor_value = medians[factor_name]
                existing.source = "GICS_hardcoded_L3"
            else:
                session.add(SectorFactorProxy(
                    sector=sector,
                    factor_name=factor_name,
                    factor_value=medians[factor_name],
                    source="GICS_hardcoded_L3",
                    as_of_date=snapshot_date,
                ))
    session.flush()
    logger.info("Saved %d sector proxy entries", len(GICS_SECTOR_MEDIANS) * 3)
