# tests/conftest.py
# =============================================================================
# TRP1 LEDGER — Test Infrastructure
# =============================================================================
# Uses testcontainers 4.x for real PostgreSQL testing.
# No database mocking — the assessor requires real PostgreSQL.
# =============================================================================
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import psycopg
import pytest
import structlog
from psycopg_pool import AsyncConnectionPool

from src.event_store import EventStore


# ---------------------------------------------------------------------------
# Windows event-loop fix: psycopg async requires SelectorEventLoop
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def event_loop_policy():
    if sys.platform == "win32":
        return asyncio.WindowsSelectorEventLoopPolicy()
    return asyncio.DefaultEventLoopPolicy()


# ---------------------------------------------------------------------------
# Configure structlog for test output (readable, not JSON)
# ---------------------------------------------------------------------------
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(0),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=False,
)


# ---------------------------------------------------------------------------
# PostgreSQL Test Container (testcontainers 4.x)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def postgres_container():
    """
    Spin up a real PostgreSQL 16 container for the test session.
    Uses testcontainers 4.x API.
    """
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16") as pg:
        yield pg


@pytest.fixture(scope="session")
def db_url(postgres_container):
    """
    Get the connection URL and run schema.sql migration against the container.
    Uses psycopg3 directly to execute the schema.
    """
    url = postgres_container.get_connection_url()

    # Convert SQLAlchemy-style URL to psycopg-compatible URL
    # testcontainers returns: postgresql+psycopg2://...
    # psycopg3 needs: postgresql://...
    psycopg_url = url.replace("postgresql+psycopg2://", "postgresql://")

    # Read and execute schema.sql
    schema_path = Path(__file__).parent.parent / "src" / "schema.sql"
    schema_sql = schema_path.read_text()

    with psycopg.connect(psycopg_url) as conn:
        conn.execute(schema_sql)
        conn.commit()

    yield psycopg_url


# ---------------------------------------------------------------------------
# EventStore Fixture
# ---------------------------------------------------------------------------


@pytest.fixture
async def event_store(db_url):
    """
    Provide an EventStore instance with a fresh connection pool.
    Truncates all tables between tests for isolation.
    """
    async with AsyncConnectionPool(db_url) as pool:
        store = EventStore(pool)
        yield store

        # Truncate tables between tests for isolation
        async with pool.connection() as conn:
            await conn.execute(
                "TRUNCATE events, event_streams, projection_checkpoints, outbox CASCADE"
            )
            await conn.commit()
