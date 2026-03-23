# DESIGN.md — TRP1 Ledger Architecture & Tradeoff Analysis

## 1. Aggregate Boundaries

| Aggregate | Stream Format | Invariant Owned |
|-----------|---------------|-----------------|
| LoanApplication | `loan-{application_id}` | Lifecycle transitions, human decision outcome, contributing-session validity |
| AgentSession | `agent-{agent_id}-{session_id}` | Gas Town precondition: no agent decision before context declaration |
| ComplianceRecord | `compliance-{application_id}` | Required-rule completion, per-rule pass/fail state, clearance issuance |
| AuditLedger | `audit-{entity_type}-{entity_id}` | Integrity-check chain continuity and tamper detection |

The main rejected alternative was folding `ComplianceRecord` into `LoanApplication`. That would force every compliance rule result to contend on the loan stream and turn parallel checks into artificial optimistic-concurrency failures. Keeping compliance separate means credit, fraud, and compliance work can progress independently while the loan aggregate still observes the resulting facts through cross-stream replay.

## 2. Projection Strategy

All projections run asynchronously behind `ProjectionDaemon`. The write path stays fast and projections can be rebuilt by replaying the immutable log.

| Projection | Read Purpose | SLO |
|-----------|--------------|-----|
| ApplicationSummary | Operator dashboard and application lookup | lag under 500ms |
| AgentPerformanceLedger | Agent/model analytics | no hard real-time requirement |
| ComplianceAuditView | Regulatory examination and time-travel | lag under 2000ms |

### Compliance Snapshot Strategy

`ComplianceAuditView` uses three tables:

- `compliance_audit_view`: current full state
- `compliance_audit_events`: immutable per-event audit log
- `compliance_audit_snapshots`: periodic state snapshots keyed by source event

`get_compliance_at(application_id, timestamp)` first loads the newest snapshot at or before `timestamp`, then replays only the later `compliance_audit_events`. Snapshots are written every three compliance events and on terminal-style events such as `ComplianceClearanceIssued` and `ComplianceRuleFailed`. This keeps temporal reads bounded without sacrificing exact replay semantics.

### Zero-Downtime Rebuild

`ComplianceAuditProjection.rebuild_from_scratch()` rebuilds into shadow tables, swaps them atomically, then drops the old tables. Live reads keep using the existing tables until the replacement is ready, so rebuild completes without read downtime.

## 3. Concurrency, Retry Budget, and Projection Fault Tolerance

### Event Store OCC

The write path uses two layers:

1. `SELECT ... FOR UPDATE` on `event_streams`
2. `UNIQUE(stream_id, stream_position)` on `events`

The loser receives `OptimisticConcurrencyError` with `suggested_action = "reload_stream_and_retry"`.

### Expected OCC Rate

Under 100 concurrent applications with four agents each, most writes hit different streams, so inter-stream contention is effectively zero. Real conflicts should cluster around same-application orchestration races and remain low, typically below two conflicts per loan lifecycle.

### Caller Retry Budget

- Max retries: 3
- Backoff: 50ms, 100ms, 200ms
- After budget exhaustion: return the typed concurrency error to the caller

### Projection Retry Budget

Projection failures are tracked in `projection_failures`. Each `(projection_name, global_position)` pair carries a retry counter. The daemon retries a failing event up to `max_retries_per_event` and then marks it skipped, advances the checkpoint, and continues processing later events. This prevents one poisoned event from stalling the entire daemon.

## 4. Upcasting and Historical Inference

### CreditAnalysisCompleted v1→v2

Added fields:

- `model_version`: inferred from `recorded_at`
- `confidence_score`: preserved if present, otherwise `None`
- `model_deployment_id`: `"unknown-pre-v2"`
- `regulatory_basis`: inferred from the regulatory timeline at `recorded_at`

`None` is preferred over fabrication for genuinely unknown historical values. Fabricating confidence would pollute downstream performance analytics and regulatory evidence.

### DecisionGenerated v1→v2

`model_versions` is reconstructed by loading each contributing agent-session stream and reading its `AgentContextLoaded` event. This is intentionally done at read time through the upcaster path, not by mutating historical rows. The tradeoff is cost: one legacy decision load can trigger N additional stream loads. That is acceptable for rare historical replay but would be too expensive for a hot analytics path, so the performance implication is explicit.

### Immutability Guarantee

Upcasters always return new `StoredEvent` instances. The raw JSONB payload stored in `events` is never modified. The mandatory database-level immutability test verifies this.

## 5. EventStoreDB Comparison

This PostgreSQL implementation maps closely to core EventStoreDB ideas:

- `stream_id` in `event_streams` and `events` maps to EventStoreDB stream IDs
- `load_all()` maps to subscribing to the EventStoreDB `$all` stream
- `ProjectionDaemon` maps to persistent subscriptions / projection consumers
- `projection_checkpoints` plays the role EventStoreDB gives you more natively through subscription state management
- `outbox` is the additional delivery layer we need because PostgreSQL is not a purpose-built event broker

What EventStoreDB would give us more directly:

- append/read APIs designed around streams instead of generic SQL tables
- stronger subscription primitives for fan-out consumers
- less custom work around checkpointing, replay coordination, and stream metadata conventions
- fewer chances to accidentally bypass event-store invariants with ad hoc SQL

What PostgreSQL gives us in exchange:

- total control over schema and indexing
- one operational data store for events, checkpoints, projections, and outbox
- flexible SQL for rebuilds, shadow-table swaps, and ad hoc regulatory queries

The tradeoff is clear: PostgreSQL can absolutely support this system, but it makes us build and maintain more of the event-store ergonomics ourselves.

## 6. What I Would Do Differently

The single architectural decision I would revisit first is keeping the projection daemon and command write path on the same connection-pool class with the same storage engine responsibilities.

With another full day, I would separate the architecture more decisively into:

1. a latency-sensitive append path with its own constrained pool and metrics
2. a projection / replay subsystem with isolated resources and clearer dead-letter handling

That would reduce the chance that rebuilds, replay-heavy diagnostics, or bursty projection work interfere with the core guarantee of low-latency atomic appends. It is the decision with the biggest impact on production resilience, and it is the one I would change before adding any new feature.
