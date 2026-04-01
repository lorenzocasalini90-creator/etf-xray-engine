"""SQLAlchemy ORM models for the ETF X-Ray Engine database."""

from datetime import date, datetime

from sqlalchemy import (
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class EtfMetadata(Base):
    """Anagrafica ETF."""

    __tablename__ = "etf_metadata"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(20), unique=True, nullable=False)
    isin: Mapped[str | None] = mapped_column(String(12))
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    issuer: Mapped[str | None] = mapped_column(String(100))
    expense_ratio: Mapped[float | None] = mapped_column(Float)
    currency: Mapped[str | None] = mapped_column(String(3))
    domicile: Mapped[str | None] = mapped_column(String(50))
    inception_date: Mapped[date | None] = mapped_column(Date)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    holdings: Mapped[list["Holding"]] = relationship(back_populates="etf")


class Holding(Base):
    """Posizioni per ETF + data."""

    __tablename__ = "holdings"
    __table_args__ = (
        UniqueConstraint("etf_id", "figi_id", "as_of_date", name="uq_holding"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    etf_id: Mapped[int] = mapped_column(ForeignKey("etf_metadata.id"), nullable=False)
    figi_id: Mapped[int | None] = mapped_column(ForeignKey("figi_mapping.id"))
    holding_name: Mapped[str] = mapped_column(String(255), nullable=False)
    weight_pct: Mapped[float | None] = mapped_column(Float)
    market_value: Mapped[float | None] = mapped_column(Float)
    shares: Mapped[float | None] = mapped_column(Float)
    sector: Mapped[str | None] = mapped_column(String(100))
    country: Mapped[str | None] = mapped_column(String(100))
    currency: Mapped[str | None] = mapped_column(String(3))
    as_of_date: Mapped[date] = mapped_column(Date, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    etf: Mapped["EtfMetadata"] = relationship(back_populates="holdings")
    figi: Mapped["FigiMapping | None"] = relationship()


class FigiMapping(Base):
    """Mapping identificativi → composite FIGI."""

    __tablename__ = "figi_mapping"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    composite_figi: Mapped[str] = mapped_column(String(12), unique=True, nullable=False)
    isin: Mapped[str | None] = mapped_column(String(12), index=True)
    ticker: Mapped[str | None] = mapped_column(String(20), index=True)
    sedol: Mapped[str | None] = mapped_column(String(7), index=True)
    cusip: Mapped[str | None] = mapped_column(String(9), index=True)
    name: Mapped[str | None] = mapped_column(String(255))
    exchange: Mapped[str | None] = mapped_column(String(10))
    market_sector: Mapped[str | None] = mapped_column(String(20))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class SecurityFundamental(Base):
    """Dati fondamentali per security."""

    __tablename__ = "security_fundamentals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    figi_id: Mapped[int] = mapped_column(ForeignKey("figi_mapping.id"), nullable=False)
    pe_ratio: Mapped[float | None] = mapped_column(Float)
    pb_ratio: Mapped[float | None] = mapped_column(Float)
    dividend_yield: Mapped[float | None] = mapped_column(Float)
    market_cap: Mapped[float | None] = mapped_column(Float)
    roe: Mapped[float | None] = mapped_column(Float)
    debt_to_equity: Mapped[float | None] = mapped_column(Float)
    revenue_growth: Mapped[float | None] = mapped_column(Float)
    data_source: Mapped[str | None] = mapped_column(String(10))
    as_of_date: Mapped[date] = mapped_column(Date, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    figi: Mapped["FigiMapping"] = relationship()


class SectorFactorProxy(Base):
    """Proxy settore/fattore per securities senza fondamentali diretti."""

    __tablename__ = "sector_factor_proxies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sector: Mapped[str] = mapped_column(String(100), nullable=False)
    factor_name: Mapped[str] = mapped_column(String(50), nullable=False)
    factor_value: Mapped[float] = mapped_column(Float, nullable=False)
    source: Mapped[str | None] = mapped_column(String(100))
    as_of_date: Mapped[date] = mapped_column(Date, nullable=False)

    __table_args__ = (
        UniqueConstraint("sector", "factor_name", "as_of_date", name="uq_sector_factor"),
    )


class Benchmark(Base):
    """Benchmark di riferimento."""

    __tablename__ = "benchmarks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    ticker: Mapped[str | None] = mapped_column(String(20))
    description: Mapped[str | None] = mapped_column(Text)
    provider: Mapped[str | None] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Portfolio(Base):
    """Portafogli utente."""

    __tablename__ = "portfolios"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    positions: Mapped[list["PortfolioPosition"]] = relationship(back_populates="portfolio")


class HoldingsCache(Base):
    """Cache per holdings ETF scaricate dai fetcher."""

    __tablename__ = "holdings_cache"
    __table_args__ = (
        UniqueConstraint("etf_identifier", "source", name="uq_holdings_cache"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    etf_identifier: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    holdings_json: Mapped[str] = mapped_column(Text, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    stale_after: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    coverage_pct: Mapped[float] = mapped_column(Float, default=0.0)
    num_holdings: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(20), default="success")


class PortfolioPosition(Base):
    """Posizioni nei portafogli utente."""

    __tablename__ = "portfolio_positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    portfolio_id: Mapped[int] = mapped_column(
        ForeignKey("portfolios.id"), nullable=False
    )
    etf_id: Mapped[int] = mapped_column(
        ForeignKey("etf_metadata.id"), nullable=False
    )
    weight_pct: Mapped[float] = mapped_column(Float, nullable=False)
    shares: Mapped[float | None] = mapped_column(Float)
    added_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    portfolio: Mapped["Portfolio"] = relationship(back_populates="positions")
    etf: Mapped["EtfMetadata"] = relationship()
