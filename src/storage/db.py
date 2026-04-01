"""Database session management.

Supports SQLite (default) and PostgreSQL via the ``DATABASE_URL`` env var.
"""

import logging
import os

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from src.storage.models import Base

logger = logging.getLogger(__name__)

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


def _run_migrations(engine) -> None:
    """Apply lightweight schema migrations for existing databases.

    Checks for missing columns that were added after the initial schema
    and adds them via ALTER TABLE. Safe to run repeatedly — only acts
    when columns are actually missing.

    Args:
        engine: SQLAlchemy engine.
    """
    insp = inspect(engine)

    if "security_fundamentals" in insp.get_table_names():
        columns = {col["name"] for col in insp.get_columns("security_fundamentals")}
        if "data_source" not in columns:
            logger.info("Migrating: adding data_source column to security_fundamentals")
            with engine.begin() as conn:
                conn.execute(
                    text("ALTER TABLE security_fundamentals ADD COLUMN data_source VARCHAR(10)")
                )


def init_db(url: str | None = None) -> None:
    """Create all tables defined in the ORM models.

    Also runs lightweight migrations for existing databases that may
    be missing columns added after the initial schema.

    Args:
        url: Optional database URL override.
    """
    engine = get_engine(url)
    _run_migrations(engine)
    Base.metadata.create_all(engine)
