"""Pydantic models for API requests."""

from typing import List, Optional

from pydantic import BaseModel, field_validator


class ETFPosition(BaseModel):
    """Single ETF position in a portfolio."""

    ticker: str
    amount_eur: float

    @field_validator("ticker")
    @classmethod
    def normalize_ticker(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("amount_eur")
    @classmethod
    def amount_must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("amount_eur must be > 0")
        return v


class PortfolioRequest(BaseModel):
    """Portfolio analysis request."""

    positions: List[ETFPosition]
    benchmark: Optional[str] = "MSCI_WORLD"

    @field_validator("positions")
    @classmethod
    def validate_positions(cls, v: List[ETFPosition]) -> List[ETFPosition]:
        if len(v) < 1:
            raise ValueError("At least 1 position required")
        if len(v) > 15:
            raise ValueError("Maximum 15 positions allowed")
        return v
