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

## 5. MCP Contract Design

The MCP layer is intentionally CQRS-shaped:

- tools are commands
- resources are queries

Required tool names are exposed directly: `submit_application`, `start_agent_session`, `record_credit_analysis`, `record_fraud_screening`, `record_compliance_check`, `generate_decision`, `record_human_review`, and `run_integrity_check`.

Two workflow gaps in the raw domain handlers are bridged in the MCP layer:

1. `record_credit_analysis` bootstraps `CreditAnalysisRequested` if the application is freshly submitted, so the lifecycle is driveable through MCP without a hidden preparatory command.
2. `record_compliance_check` can initialize the compliance record on first use when given `regulation_set_version` and `checks_required`, removing the need for a separate MCP-only setup step.

Resources expose:

- `ledger://applications/{id}`
- `ledger://applications/{id}/compliance` with optional `?as_of={timestamp}`
- `ledger://applications/{id}/audit-trail`
- `ledger://agents/{id}/performance`
- `ledger://agents/{id}/sessions/{session_id}`
- `ledger://ledger/health`

`audit-trail` is the justified exception that reads directly from event streams. It loads the full application-linked event history, not just the loan stream, so the regulator can see credit, fraud, compliance, and human review facts in one ordered trace.

## 6. What I Would Change Next

1. Add an explicit dead-letter replay workflow for skipped projection events instead of only tracking them in `projection_failures`.
2. Split read and write pools so heavy projection replay cannot compete with latency-sensitive appends.
3. Introduce schema-registry style compatibility checks around event version changes to catch bad writes before they reach storage.
4. Add stronger load tests for the combined ApplicationSummary and ComplianceAudit projections under sustained, not burst-only, concurrency.
