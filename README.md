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

## Running the Application

### 1. Start the Projections Daemon

The Async Projection Daemon continuously polls the event store (or uses `LISTEN/NOTIFY`) to rebuild read models in near real-time.

```bash
uv run python -m src.projections.daemon
```

### 2. Start the MCP Server

The MCP (Model Context Protocol) Server exposes the event commands as `Tools` and projections as `Resources` to connected AI Agents or clients.

```bash
# Using FastMCP
uv run python -m src.mcp.server
```
*Note: The server uses stdio for communication by default, integrating seamlessly with Claude Desktop or cursor.*

## Advanced Features (Phase 4-6)

### Temporal What-If Analysis (Bonus 1)
To run counterfactual scenarios (e.g., branching an event stream in-memory without modifying the real ledger to see projected outcomes):
```python
from src.what_if.projector import run_what_if, WhatIfScenario
# Pass your scenario and event store connection
result = await run_what_if(store, scenario)
```

### Regulatory Examination Packaging (Bonus 2)
To generate a cryptographically sealed JSON package proving an AI agent's decision chain:
```python
from datetime import datetime, timezone
from src.regulatory.package import generate_regulatory_package
# This pulls the full context from Gas Town and verifies the audit chain hashes
pkg = await generate_regulatory_package(
    store,
    "application_id",
    datetime.now(timezone.utc),
)
```

## Complete Project Structure

```
trp1-ledger/
├── pyproject.toml                  # Project config and dependencies
├── .python-version                 # Python 3.12
├── README.md                       # This file
├── docs/                           # Challenge documentation and notes
├── src/
│   ├── event_store.py              # EventStore async class
│   ├── schema.sql                  # PostgreSQL schema with design rationale
│   ├── models/
│   │   └── events.py               # Domain events, exceptions, base classes
│   ├── aggregates/
│   │   ├── loan_application.py     # LoanApplication aggregate with state machine
│   │   ├── agent_session.py        # AgentSession aggregate with Gas Town
│   │   ├── audit_ledger.py         # Cryptographic audit aggregate
│   │   └── compliance_record.py    # Compliance tracking aggregate
│   ├── commands/
│   │   └── handlers.py             # Command handlers (load → validate → append)
│   ├── integrity/
│   │   ├── audit_chain.py          # Cryptographic hashing & tamper detection
│   │   └── gas_town.py             # Agent Memory Context restoration
│   ├── mcp/
│   │   ├── server.py               # MCP Server entry point
│   │   ├── tools.py                # MCP commands (submit, analyze, decide)
│   │   └── resources.py            # MCP temporal projections
│   ├── projections/
│   │   ├── daemon.py               # Async projection polling daemon
│   │   └── ...                     # Read models (AgentPerformance, ApplicationSummary)
│   ├── regulatory/
│   │   └── package.py              # Self-contained Regulatory JSON packager
│   ├── upcasting/
│   │   ├── registry.py             # Schema immutability & UpcasterRegistry
│   │   └── upcasters.py            # v1→v2 migrations ran at read-time
│   └── what_if/
│       └── projector.py            # Counterfactual in-memory what-if projections
└── tests/
    ├── conftest.py                 # Test infra (testcontainers, fixtures)
    ├── test_concurrency.py         # Double-decision concurrency tests
    ├── test_gas_town.py            # Agent context re-population
    ├── test_mcp_lifecycle.py       # Full lifecycle integration tests
    ├── test_projections.py         # Async projection builder tests
    └── test_upcasting.py           # Read-time upcasting tests
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
