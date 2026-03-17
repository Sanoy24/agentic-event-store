# TRP1 LEDGER — Agentic Event Store & Enterprise Audit Infrastructure

> Building the immutable memory and governance backbone for multi-agent AI systems at production scale.

## Overview

The Ledger is a production-quality event sourcing infrastructure for the Apex Financial Services multi-agent loan processing platform. It provides:

- **Append-only event store** with full PostgreSQL-backed ACID guarantees
- **Optimistic concurrency control** preventing conflicting agent decisions
- **Domain aggregates** with strict state machine enforcement
- **Gas Town pattern** ensuring every AI agent declares its context before making decisions
- **Causal tracing** via correlation_id/causation_id metadata on every event

## Prerequisites

- **Python 3.12+**
- **[uv](https://docs.astral.sh/uv/)** (package manager)
- **PostgreSQL 14+** or **Docker** (for running PostgreSQL)

## Installation

```bash
# Install all dependencies including test extras
uv sync --extra test
```

## Database Setup

### Option A — Docker (Recommended)

```bash
docker run --name trp1-pg \
  -e POSTGRES_DB=trp1_ledger \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  -d postgres:16

# Run schema migration
psql postgresql://postgres:postgres@localhost/trp1_ledger < src/schema.sql
```

### Option B — Local PostgreSQL

```bash
createdb trp1_ledger
psql trp1_ledger < src/schema.sql
```

## Environment Variables

```bash
export DATABASE_URL=postgresql://postgres:postgres@localhost/trp1_ledger
```

## Run the Test Suite

Tests use **testcontainers** to spin up a real PostgreSQL instance automatically — no manual database setup required for testing.

```bash
# Run all tests
uv run pytest tests/ -v

# Run the double-decision concurrency test specifically
uv run pytest tests/test_concurrency.py -v -s
```

### Expected Output — Double-Decision Test

```
tests/test_concurrency.py::test_double_decision_concurrency PASSED

# Log output shows:
# [info] events_appended    stream_id=loan-{uuid} new_version=1 ...
# [info] events_appended    stream_id=loan-{uuid} new_version=2 ...
# [info] events_appended    stream_id=loan-{uuid} new_version=3 ...
# [info] events_appended    stream_id=loan-{uuid} new_version=4 ...
# [warning] optimistic_concurrency_conflict  stream_id=loan-{uuid} actual_version=4
# One agent succeeds (version 3→4), one receives OptimisticConcurrencyError
```

## Project Structure

```
trp1-ledger/
├── pyproject.toml                  # Project config and dependencies
├── .python-version                 # Python 3.12
├── README.md                       # This file
├── DOMAIN_NOTES.md                 # Phase 0 written deliverable
├── src/
│   ├── schema.sql                  # PostgreSQL schema with design rationale
│   ├── event_store.py              # EventStore async class
│   ├── models/
│   │   └── events.py               # All domain events, exceptions, base classes
│   ├── aggregates/
│   │   ├── loan_application.py     # LoanApplication aggregate with state machine
│   │   └── agent_session.py        # AgentSession aggregate with Gas Town
│   └── commands/
│       └── handlers.py             # Command handlers (load → validate → determine → append)
└── tests/
    ├── conftest.py                 # Test infra (testcontainers, fixtures)
    └── test_concurrency.py         # Double-decision concurrency test (MANDATORY)
```

## Technology Stack

| Component  | Package                                  | Version           |
| ---------- | ---------------------------------------- | ----------------- |
| Runtime    | Python                                   | ≥3.12             |
| Database   | psycopg[binary,pool]                     | ≥3.2              |
| Validation | pydantic                                 | ≥2.7              |
| Logging    | structlog                                | ≥24.0             |
| Async      | anyio                                    | ≥4.0              |
| Testing    | pytest + pytest-asyncio + testcontainers | ≥8.0, ≥0.24, ≥4.0 |
