"""Database session management.

Supports SQLite (default) and PostgreSQL via the ``DATABASE_URL`` env var.
"""

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.storage.models import Base

DEFAULT_DATABASE_URL = "sqlite:///etf_xray.db"


def get_database_url() -> str:
    """Return database URL from env or default to local SQLite."""
    return os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)


def get_engine(url: str | None = None):
    """Create a SQLAlchemy engine.

    Args:
        url: Database URL. If ``None``, reads from env / default.
    """
    db_url = url or get_database_url()
    connect_args = {}
    if db_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    return create_engine(db_url, connect_args=connect_args)


def get_session_factory(url: str | None = None) -> sessionmaker[Session]:
    """Return a configured ``sessionmaker`` bound to an engine.

    Args:
        url: Optional database URL override.
    """
    engine = get_engine(url)
    return sessionmaker(bind=engine)


def init_db(url: str | None = None) -> None:
    """Create all tables defined in the ORM models.

    Args:
        url: Optional database URL override.
    """
    engine = get_engine(url)
    Base.metadata.create_all(engine)
