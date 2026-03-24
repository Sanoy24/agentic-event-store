# The Ledger

Agentic Event Store & Enterprise Audit Infrastructure

Production-grade event sourcing system for multi-agent AI — built for auditability, reproducibility, and governance.

## Overview

The Ledger is an append-only event store that acts as the single source of truth for all AI agent decisions.

It enables:

- Full audit trails of every action
- Reproducible AI decisions via event replay
- Temporal queries (state at any point in time)
- Regulatory compliance with immutable logs
- Crash-safe agent memory (Gas Town pattern)

## Architecture

### Core Concepts

- Event Sourcing → events are the database
- CQRS → commands write, projections read
- Optimistic Concurrency → no conflicting writes
- Upcasting → schema evolution without mutation

## Agent Model

Agents never mutate state directly.

They must:

- Start a session (AgentContextLoaded)
- Perform actions via MCP tools
- Write decisions as events
- Recover state via event replay

## System Components

### Event Store

- PostgreSQL-backed
- Append-only streams
- Version-controlled writes (expected_version)

### Aggregates

- LoanApplication → lifecycle & business rules
- AgentSession → agent state & context
- ComplianceRecord → regulatory checks
- AuditLedger → integrity + traceability

### Projections (Read Models)

- ApplicationSummary
- AgentPerformanceLedger
- ComplianceAuditView (supports time-travel queries)

## Guarantees

- Immutability → events never change
- Consistency → enforced via concurrency control
- Traceability → full causal chains (correlation + causation IDs)
- Tamper Detection → cryptographic hash chain

## Key Features

### Optimistic Concurrency

```python
append(stream_id, events, expected_version)
```

- One writer wins
- Others retry after reload

### Temporal Queries

```bash
GET /applications/{id}/compliance?as_of=timestamp
```

Reconstructs exact historical state.

## Crash Recovery (Gas Town)

Agents rebuild context from events:

- No memory loss
- No duplicate work
- Safe continuation

### Upcasting (Schema Evolution)

- Old events upgraded at read-time
- Stored data remains unchanged
- No destructive migrations

## What This Proves

This system demonstrates:

- Production-ready event sourcing design
- Real-world multi-agent coordination
- Enterprise-grade audit & compliance infrastructure
- Strong handling of concurrency, failure, and evolution

## Definition of Done

A complete system can:

- Reconstruct any application’s full decision history
- Handle concurrent agent writes safely
- Recover agents after crashes
- Answer “what happened and why” instantly
- Pass regulatory audit requirements

## Why It Matters

Most AI systems fail in production not because of models —
but because they lack trust, traceability, and governance.

The Ledger solves that.
