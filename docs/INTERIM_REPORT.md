# TRP1 Ledger - Interim Submission Report

**Candidate Submission - Interim Deadline: Sunday March 22, 03:00 UTC**

---

## 1. DOMAIN_NOTES.md

*See [DOMAIN_NOTES.md](./DOMAIN_NOTES.md) for the complete Phase 0 written deliverable, covering all 6 required questions with specificity and Practitioner Manual references.*

---

## 2. Architecture Diagrams

### 2.1 Event Store Schema

```mermaid
erDiagram
    events {
        UUID event_id PK "gen_random_uuid()"
        TEXT stream_id "Human-readable: loan-uuid"
        BIGINT stream_position "Position within stream"
        BIGINT global_position "GENERATED ALWAYS AS IDENTITY"
        TEXT event_type "PascalCase past tense"
        SMALLINT event_version "Schema version for upcasting"
        JSONB payload "Domain event data"
        JSONB metadata "correlation_id, causation_id"
        TIMESTAMPTZ recorded_at "clock_timestamp()"
    }

    event_streams {
        TEXT stream_id PK
        TEXT aggregate_type "loan, agent, compliance, audit"
        BIGINT current_version "O(1) OCC check"
        TIMESTAMPTZ created_at
        TIMESTAMPTZ archived_at "NULL = active"
        JSONB metadata
    }

    projection_checkpoints {
        TEXT projection_name PK
        BIGINT last_position "Last processed global_position"
        TIMESTAMPTZ updated_at "Lag proxy"
    }

    outbox {
        UUID id PK
        UUID event_id FK
        TEXT destination "Target system"
        JSONB payload "Denormalized"
        TIMESTAMPTZ created_at
        TIMESTAMPTZ published_at "NULL = unpublished"
        SMALLINT attempts "Retry counter"
    }

    events ||--o{ outbox : "event_id FK"
    event_streams ||--o{ events : "stream_id (logical)"
```

### 2.2 Aggregate Boundaries & Stream Ownership

```mermaid
graph TB
    subgraph "Aggregate Boundaries"
        subgraph LA["LoanApplication Aggregate"]
            LA_STREAM["Stream: loan-{uuid}"]
            LA_EVENTS["Events:<br/>ApplicationSubmitted<br/>CreditAnalysisRequested<br/>DecisionGenerated<br/>HumanReviewCompleted<br/>ApplicationApproved<br/>ApplicationDeclined<br/>ApplicationUnderReview"]
        end

        subgraph AS["AgentSession Aggregate"]
            AS_STREAM["Stream: agent-{id}-{session}"]
            AS_EVENTS["Events:<br/>AgentContextLoaded<br/>CreditAnalysisCompleted<br/>FraudScreeningCompleted<br/>AgentDecisionSuperseded"]
        end

        subgraph CR["ComplianceRecord Aggregate"]
            CR_STREAM["Stream: compliance-{uuid}"]
            CR_EVENTS["Events:<br/>ComplianceCheckRequested<br/>ComplianceRulePassed<br/>ComplianceRuleFailed<br/>ComplianceClearanceIssued"]
        end

        subgraph AL["AuditLedger Aggregate"]
            AL_STREAM["Stream: audit-{type}-{id}"]
            AL_EVENTS["Events:<br/>AuditIntegrityCheckRun<br/>AuditTamperDetected"]
        end
    end

    LA_STREAM --- LA_EVENTS
    AS_STREAM --- AS_EVENTS
    CR_STREAM --- CR_EVENTS
    AL_STREAM --- AL_EVENTS
```

### 2.3 Command Flow - The 4-Step Handler Pattern

```mermaid
sequenceDiagram
    participant Client as AI Agent / API
    participant Handler as Command Handler
    participant Agg as Aggregate
    participant Store as EventStore
    participant DB as PostgreSQL

    Client->>Handler: SubmitApplicationCommand
    Note over Handler: Step 1 - Reconstruct State
    Handler->>Store: load_application_events(application_id)
    Store->>DB: SELECT related events across loan/agent/compliance streams
    DB-->>Store: rows[]
    Store-->>Handler: StoredEvent[]
    Handler->>Agg: LoanApplicationAggregate.load(...)
    Note over Agg: Replay events -> rebuild state

    Note over Handler: Step 2 - Validate Business Rules
    Handler->>Agg: assert_valid_transition()
    Agg-->>Handler: OK or DomainError

    Note over Handler: Step 3 - Determine New Events
    Handler->>Handler: Create domain event(s)

    Note over Handler: Step 4 - Append Atomically
    Handler->>Store: append(stream_id, events, expected_version)
    Store->>DB: BEGIN TRANSACTION
    Store->>DB: SELECT current_version FOR UPDATE
    Store->>DB: INSERT INTO events (...)
    Store->>DB: UPDATE event_streams SET current_version
    Store->>DB: INSERT INTO outbox (...)
    Store->>DB: COMMIT
    DB-->>Store: new_version
    Store-->>Handler: new_version
    Handler-->>Client: Success(new_version)
```

### 2.4 LoanApplication State Machine

```mermaid
stateDiagram-v2
    [*] --> SUBMITTED : ApplicationSubmitted
    SUBMITTED --> AWAITING_ANALYSIS : CreditAnalysisRequested
    AWAITING_ANALYSIS --> ANALYSIS_COMPLETE : CreditAnalysisCompleted
    ANALYSIS_COMPLETE --> COMPLIANCE_REVIEW : ComplianceCheckRequested
    COMPLIANCE_REVIEW --> PENDING_DECISION : ComplianceClearanceIssued
    PENDING_DECISION --> APPROVED_PENDING_HUMAN : DecisionGenerated (APPROVE)
    PENDING_DECISION --> DECLINED_PENDING_HUMAN : DecisionGenerated (DECLINE/REFER)
    APPROVED_PENDING_HUMAN --> FINAL_APPROVED : HumanReviewCompleted / ApplicationApproved
    DECLINED_PENDING_HUMAN --> FINAL_DECLINED : HumanReviewCompleted / ApplicationDeclined
    FINAL_APPROVED --> [*]
    FINAL_DECLINED --> [*]
```

### 2.5 Double-Decision Concurrency - OCC Mechanism

```mermaid
sequenceDiagram
    participant A as Agent A
    participant B as Agent B
    participant ES as EventStore
    participant DB as PostgreSQL

    Note over A,B: Both read stream at version 3

    par Concurrent Append
        A->>ES: append(expected_version=3)
        B->>ES: append(expected_version=3)
    end

    ES->>DB: SELECT current_version FOR UPDATE (Agent A)
    Note over DB: Row lock acquired by A
    DB-->>ES: version = 3
    ES->>DB: INSERT event at position 4
    ES->>DB: UPDATE version = 4
    ES->>DB: COMMIT (lock released)

    ES->>DB: SELECT current_version FOR UPDATE (Agent B)
    Note over DB: Lock released, B proceeds
    DB-->>ES: version = 4 (expected 3)
    ES->>DB: ROLLBACK

    ES-->>A: Success (new_version = 4)
    ES-->>B: OptimisticConcurrencyError(expected=3, actual=4)

    Note over B: Must reload stream,<br/>check if analysis is still relevant,<br/>then retry or abort
```

---

## 3. Progress Summary

### What Is Working (Phase 1 + Phase 2)

**Phase 1 - Event Store Core Complete**

| Component | Status | Details |
|-----------|--------|---------|
| PostgreSQL schema | Complete | All 4 tables implemented: `events`, `event_streams`, `projection_checkpoints`, `outbox` |
| `EventStore.append()` | Complete | Atomic write with OCC, outbox in same transaction, metadata envelope |
| `EventStore.load_stream()` | Complete | Position-bounded stream reads |
| `EventStore.load_application_events()` | Complete | Cross-stream replay for application-scoped validation across loan, agent-session, and compliance streams |
| `EventStore.load_all()` | Complete | Async generator with batching and optional event-type filtering |
| `EventStore.stream_version()` | Complete | O(1) lookup via `event_streams` primary key |
| `EventStore.archive_stream()` | Complete | Soft archive with `archived_at`, rejects future appends |
| `EventStore.get_stream_metadata()` | Complete | Full `StreamMetadata` return |
| Double-decision concurrency test | Complete | 2 concurrent tasks, exactly 1 succeeds, 1 gets OCC error, total = 4 events |

**Phase 2 - Domain Logic Complete**

| Component | Status | Details |
|-----------|--------|---------|
| `LoanApplicationAggregate` | Complete | 9-state machine, application-level replay via `load()`, loan-stream version preserved for OCC |
| `AgentSessionAggregate` | Complete | Gas Town enforcement and model-version tracking |
| `ComplianceRecordAggregate` | Complete | Separate compliance stream replay, required/passed check tracking, clearance state |
| Business Rule 1 | Complete | Valid state transitions enforced via `VALID_TRANSITIONS` |
| Business Rule 2 | Complete | `assert_context_loaded()` prevents decisions before `AgentContextLoaded` |
| Business Rule 3 | Complete | Single credit-analysis completion enforced per application with validation plus application-scoped advisory lock |
| Business Rule 4 | Complete | `confidence_score < 0.6` forces `REFER` |
| Business Rule 5 | Complete | Approval checks `ComplianceRecord` stream and requires `ComplianceClearanceIssued` |
| Business Rule 6 | Complete | `DecisionGenerated` rejects contributing sessions that never processed the application |
| Command handlers | Complete | `handle_submit_application`, `handle_start_agent_session`, `handle_credit_analysis_completed`, `handle_generate_decision`, `handle_application_approved` |
| Event catalogue | Complete for interim scope | Event models implemented from catalogue plus identified missing events |

### Test Results

All 9 tests pass:

```text
tests/test_concurrency.py::test_double_decision_concurrency PASSED                                           [ 11%]
tests/test_concurrency.py::test_new_stream_creation PASSED                                                   [ 22%]
tests/test_concurrency.py::test_stream_version_nonexistent PASSED                                            [ 33%]
tests/test_concurrency.py::test_load_stream_empty PASSED                                                     [ 44%]
tests/test_concurrency.py::test_metadata_contains_correlation_id PASSED                                      [ 55%]
tests/test_concurrency.py::test_archive_stream PASSED                                                        [ 66%]
tests/test_handlers.py::test_credit_analysis_written_to_agent_session_stream PASSED                          [ 77%]
tests/test_handlers.py::test_generate_decision_rejects_invalid_contributing_sessions PASSED                  [ 88%]
tests/test_handlers.py::test_application_approval_checks_compliance_record_stream PASSED                     [100%]

======================== 9 passed in 4.40s ========================
```

Tests use `testcontainers` to spin up a PostgreSQL 16 instance in Docker. The passing run above was verified locally on Windows using `uv run pytest tests/ -v`.

---

## 4. Concurrency Test Results

The **Double-Decision Concurrency Test** (the mandatory Phase 1 test from the challenge brief) passes with all 3 required assertions:

- **(a)** Total events in stream after both tasks = **4** (not 5)
- **(b)** Winning task's event has `stream_position = 4`
- **(c)** Losing task raises `OptimisticConcurrencyError` and is not silently swallowed

Additional assertions verified:

- Error contains `stream_id`, `expected_version = 3`, `actual_version = 4`
- Error includes `suggested_action = "reload_stream_and_retry"`
- Stream positions after test are `[1, 2, 3, 4]` with no gaps or duplicates

Additional domain-regression tests now verify:

- `CreditAnalysisCompleted` is appended to the correct `AgentSession` stream while still updating application state through replay
- `DecisionGenerated` rejects invalid contributing session IDs that never processed the application
- `ApplicationApproved` validates against the `ComplianceRecord` stream rather than assuming compliance facts are in the loan stream

---

## 5. Known Gaps & Plan for Final Submission

### Phase 3 - Projections & Async Daemon (Not Started)

| Deliverable | Plan |
|-------------|------|
| `src/projections/daemon.py` | `ProjectionDaemon` with fault-tolerant batch processing, per-projection checkpoints, `get_lag()` |
| `src/projections/application_summary.py` | One row per application, upsert-based, SLO < 500ms lag |
| `src/projections/agent_performance.py` | Metrics per agent model version (rate, duration, confidence) |
| `src/projections/compliance_audit.py` | Temporal query support with `get_compliance_at(app_id, timestamp)` |
| `tests/test_projections.py` | Projection and lag tests under concurrent load |

### Phase 4 - Upcasting, Integrity & Gas Town (Not Started)

| Deliverable | Plan |
|-------------|------|
| `src/upcasting/registry.py` | `UpcasterRegistry` with automatic version-chain application on load |
| `src/upcasting/upcasters.py` | `CreditAnalysisCompleted` and `DecisionGenerated` version migrations |
| `src/integrity/audit_chain.py` | SHA-256 hash chain, tamper detection |
| `src/integrity/gas_town.py` | `reconstruct_agent_context()` with token budget |
| `tests/test_upcasting.py` | Immutability and version-chain tests |
| `tests/test_gas_town.py` | Simulated crash-recovery test |

### Phase 5 - MCP Server (Not Started)

| Deliverable | Plan |
|-------------|------|
| `src/mcp/server.py` | MCP server entry point |
| `src/mcp/tools.py` | 8 command-side tools with structured errors |
| `src/mcp/resources.py` | 6 query-side resources reading from projections |
| `tests/test_mcp_lifecycle.py` | End-to-end lifecycle through MCP only |

### Phase 6 - Bonus (If Time Permits)

| Deliverable | Plan |
|-------------|------|
| `src/what_if/projector.py` | Counterfactual projection with causal dependency filtering |
| `src/regulatory/package.py` | Self-contained JSON examination package |

### DESIGN.md (Required for Final)

Will contain the 6 required sections: aggregate boundary justification, projection strategy, concurrency analysis, upcasting inference decisions, EventStoreDB comparison, and "what you would do differently."

### Priority Order for Final Submission

1. **Phase 3** - Projections
2. **Phase 4** - Upcasting + Integrity
3. **Phase 5** - MCP Server
4. **DESIGN.md**
5. **Phase 6** - Bonus, only if Phases 3-5 are solid
