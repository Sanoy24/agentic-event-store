-- =============================================================================
-- TRP1 LEDGER — Event Store Schema
-- =============================================================================
-- Source: Challenge Doc Phase 1 pages 7-8 + Practitioner Manual Part IV pages 20-22
--
-- This schema implements the event store foundation for the Apex Financial
-- Services multi-agent loan processing platform. Every column below maps to
-- a specific design rationale from the Practitioner Manual.
-- =============================================================================

-- =============================================================================
-- TABLE: events
-- =============================================================================
-- The append-only event log. This table is NEVER updated or deleted from.
-- All state is derived by replaying events from this table.
-- =============================================================================
CREATE TABLE events (
    event_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- UUID not integer: integers leak event volume, hard to shard.
    -- UUIDs enable cross-service correlation without a central authority.
    -- (Manual p.20)

    stream_id       TEXT NOT NULL,
    -- Human-readable: "loan-{uuid}", "agent-{id}-{session}"
    -- Text not UUID: must be debuggable in operational queries.
    -- Operators must be able to grep logs for stream IDs without a lookup table.
    -- (Manual p.20)

    stream_position BIGINT NOT NULL,
    -- Position within this stream. BIGINT not INT: active streams
    -- running for years can exceed INT range (2^31 ≈ 2.1 billion).
    -- Financial audit streams may accumulate millions of events per entity.
    -- (Manual p.20)

    global_position BIGINT GENERATED ALWAYS AS IDENTITY,
    -- GENERATED ALWAYS: database owns global ordering.
    -- Application must NEVER set this value. This is the global sequence
    -- used by projection daemons to process events in causal order.
    -- (Manual p.20)

    event_type      TEXT NOT NULL,
    -- PascalCase, past tense. Routing key for upcasters/projections.
    -- Examples: "ApplicationSubmitted", "CreditAnalysisCompleted"
    -- (Manual Section 4.3 p.24)

    event_version   SMALLINT NOT NULL DEFAULT 1,
    -- Schema version for upcasting. SMALLINT sufficient (<v10 typical).
    -- Enables version-chain upcasting: v1→v2→v3 at read time.
    -- (Manual p.20 — Schema Immortality Awareness)

    payload         JSONB NOT NULL,
    -- JSONB not JSON: enables efficient GIN indexes, supports fast containment
    -- queries (@>), and validates JSON structure at write time.
    -- NEVER store unencrypted PII here — use a separate encrypted store.
    -- (Manual p.20)

    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Separate from payload: keeps infrastructure concerns
    -- (correlation_id, causation_id, agent_id, schema_version)
    -- out of domain event schemas. This separation enables infrastructure
    -- evolution without domain event version bumps.
    -- (Manual p.20 — Causal Tracing Reflex)

    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    -- clock_timestamp() not NOW(): NOW() returns transaction start time —
    -- all events in one transaction get identical timestamps, which is
    -- misleading for audit and temporal queries.
    -- clock_timestamp() returns wall-clock time at point of insertion,
    -- giving each event its own distinct recording time.
    -- (Manual p.20)

    CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
    -- This UNIQUE constraint IS the optimistic concurrency mechanism.
    -- Two concurrent inserts with same (stream_id, stream_position) cannot
    -- both succeed. One gets UniqueViolation → OptimisticConcurrencyError.
    -- No application-level locking required — the database enforces it.
    -- (Manual p.20)
);

-- Index: load_stream() scan — the most frequent query in the system.
-- Composite index on (stream_id, stream_position) enables efficient
-- range scans for aggregate event replay.
CREATE INDEX idx_events_stream_id ON events (stream_id, stream_position);

-- Index: projection daemon scan — processes events in global order.
-- The daemon queries: WHERE global_position > $1 ORDER BY global_position ASC
CREATE INDEX idx_events_global_pos ON events (global_position);

-- Index: type filter queries — enables event-type-specific projections.
-- Example: SELECT * FROM events WHERE event_type = 'CreditAnalysisCompleted'
CREATE INDEX idx_events_type ON events (event_type);

-- Index: time-range audit queries — regulatory examination date ranges.
-- B-tree for precise range queries on recorded_at.
CREATE INDEX idx_events_recorded ON events (recorded_at);

-- BRIN index: much smaller than B-tree for append-only time-ordered data.
-- Append-only tables have naturally correlated physical and logical order,
-- making BRIN extremely efficient for time-range scans.
-- Include both: B-tree for precise range queries, BRIN for space efficiency
-- on large-scale regulatory scans.
CREATE INDEX idx_events_recorded_brin ON events USING BRIN (recorded_at);


-- =============================================================================
-- TABLE: event_streams
-- =============================================================================
-- Stream metadata and version tracking. Enables O(1) optimistic concurrency
-- checks without scanning the events table.
-- =============================================================================
CREATE TABLE event_streams (
    stream_id       TEXT PRIMARY KEY,
    -- References streams in the events table.
    -- Format: "loan-{uuid}", "agent-{id}-{session}", "compliance-{uuid}"

    aggregate_type  TEXT NOT NULL,
    -- Type component of stream_id: "loan", "agent", "compliance", "audit"
    -- Enables: SELECT * FROM event_streams WHERE aggregate_type = 'loan'
    -- for administrative queries and batch operations.

    current_version BIGINT NOT NULL DEFAULT 0,
    -- Enables O(1) OCC check: SELECT current_version FOR UPDATE
    -- Without this: SELECT MAX(stream_position) FROM events WHERE stream_id = $1
    -- is O(n) and gets slower as stream grows.
    -- With this: primary key lookup on event_streams — constant time.
    -- (Manual p.21)

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Stream creation timestamp for lifecycle tracking.

    archived_at     TIMESTAMPTZ,
    -- NULL = active. Set on archive_stream().
    -- Archived streams reject new appends.
    -- This enables regulatory retention: keep events, prevent new writes.

    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb
    -- Stream-level metadata: owner, classification, retention policy.
    -- Separate from event metadata — this describes the stream itself.
);

-- Index: list active streams by aggregate type.
-- Partial index could be used for WHERE archived_at IS NULL but the
-- composite index supports both active and archived queries.
CREATE INDEX idx_streams_type_archived ON event_streams (aggregate_type, archived_at);


-- =============================================================================
-- TABLE: projection_checkpoints
-- =============================================================================
-- Tracks the last processed global_position for each projection.
-- On daemon restart: query events WHERE global_position > last_position
-- Not from 0. Without this, restart = full replay = O(total_events).
-- (Manual p.21)
-- =============================================================================
CREATE TABLE projection_checkpoints (
    projection_name TEXT PRIMARY KEY,
    -- Unique identifier for each projection: "ApplicationSummary",
    -- "AgentPerformanceLedger", "ComplianceAuditView"

    last_position   BIGINT NOT NULL DEFAULT 0,
    -- Last successfully processed global_position.
    -- CRITICAL: checkpoint must be updated in the SAME transaction as the
    -- projection table update. Two failure modes if separated:
    --   1. Checkpoint updated but projection write fails → events skipped
    --   2. Projection updated but checkpoint fails → events reprocessed
    -- Both violate exactly-once processing guarantee.
    -- (Manual p.21)

    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    -- Lag proxy: NOW() - updated_at gives approximate lag when no events
    -- have been processed recently. Enables monitoring dashboards to detect
    -- stalled projections without querying the events table.
);

CREATE TABLE projection_failures (
    projection_name TEXT NOT NULL,
    global_position BIGINT NOT NULL,
    stream_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    first_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    skipped_at TIMESTAMPTZ,
    PRIMARY KEY (projection_name, global_position)
);

CREATE INDEX idx_projection_failures_active
    ON projection_failures (projection_name, last_failed_at)
    WHERE skipped_at IS NULL;


-- =============================================================================
-- TABLE: outbox
-- =============================================================================
-- Transactional outbox pattern: write events to both the event store AND the
-- outbox in the same database transaction. A separate poller publishes from
-- the outbox reliably. This guarantees at-least-once delivery to external
-- systems (Kafka, Redis Streams, etc.) without distributed transactions.
-- =============================================================================
CREATE TABLE outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Unique identifier for this outbox entry.

    event_id        UUID NOT NULL REFERENCES events(event_id),
    -- References the event in the events table.
    -- Foreign key ensures referential integrity.

    destination     TEXT NOT NULL,
    -- Target system: "kafka:loan-events", "redis:agent-actions"
    -- Enables routing to different downstream systems.

    payload         JSONB NOT NULL,
    -- Denormalized event payload for the poller.
    -- Avoids JOIN with events table during polling.

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- When the outbox entry was created.

    published_at    TIMESTAMPTZ,
    -- NULL = not yet published. Poller: WHERE published_at IS NULL
    -- Set to NOW() when successfully published.

    attempts        SMALLINT NOT NULL DEFAULT 0
    -- Retry counter. Dead-letter after max_retries.
    -- Enables exponential backoff strategy.
);

-- Partial index: only unpublished rows. Keeps poll query fast as table grows.
-- As events are published, they exit this index — maintaining constant
-- poll performance regardless of total outbox size.
CREATE INDEX idx_outbox_unpublished ON outbox (published_at, created_at)
    WHERE published_at IS NULL;


-- =============================================================================
-- SCHEMA IMPROVEMENTS IDENTIFIED
-- =============================================================================
-- The following improvements are not in the original spec but would improve
-- production validity in future scenarios:
--
-- 1. PARTITIONING: The events table should be range-partitioned by recorded_at
--    for hot/cold storage separation. Old partitions can be moved to cheaper
--    storage while keeping recent events on fast SSDs. This is critical for
--    regulatory retention requirements (7+ years for financial services).
--
-- 2. DEAD LETTER TABLE: A separate dead_letter_outbox table for messages that
--    exceed max_retries, with error details and manual retry capability.
--
-- 3. SNAPSHOTS TABLE: For aggregates with long event streams (1000+ events),
--    periodic snapshots reduce load time from O(n) to O(1) + O(events_since_snapshot).
CREATE TABLE snapshots (
    stream_id TEXT NOT NULL,
    version BIGINT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream_id, version)
);
--
-- 4. EVENT DEDUPLICATION: An idempotency_key column on events would prevent
--    duplicate event writes from retrying command handlers. This is especially
--    important in distributed systems where network failures can cause
--    ambiguous write outcomes.
--
-- 5. STREAM CATEGORY INDEX: A GIN index on stream_id using trigram matching
--    would enable efficient prefix queries for administrative dashboards:
--    SELECT * FROM events WHERE stream_id LIKE 'loan-%'
--
-- 6. PAYLOAD VALIDATION: A CHECK constraint using jsonb_typeof() could enforce
--    that payload is always a JSON object (not array, string, or null).
--    CHECK (jsonb_typeof(payload) = 'object')
-- =============================================================================

-- =============================================================================
-- PROJECTION TABLES
-- =============================================================================

CREATE TABLE application_summary (
    application_id TEXT PRIMARY KEY,
    state TEXT,
    applicant_id TEXT,
    applicant_name TEXT,
    requested_amount_usd NUMERIC,
    risk_tier TEXT,
    confidence_score NUMERIC,
    fraud_score NUMERIC,
    compliance_status TEXT,
    decision TEXT,
    human_reviewer_id TEXT,
    override BOOLEAN,
    override_reason TEXT,
    final_decision TEXT,
    approved_amount_usd NUMERIC,
    interest_rate NUMERIC,
    conditions JSONB,
    final_decision_at TIMESTAMPTZ,
    last_event_type TEXT,
    last_event_at TIMESTAMPTZ,
    agent_sessions JSONB DEFAULT '[]'::jsonb,
    decline_reasons JSONB DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE agent_performance_ledger (
    agent_id TEXT,
    model_version TEXT,
    analyses_completed INT DEFAULT 0,
    decisions_generated INT DEFAULT 0,
    approve_count INT DEFAULT 0,
    decline_count INT DEFAULT 0,
    refer_count INT DEFAULT 0,
    human_override_count INT DEFAULT 0,
    superseded_count INT DEFAULT 0,
    total_confidence NUMERIC DEFAULT 0,
    total_duration_ms INT DEFAULT 0,
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    PRIMARY KEY (agent_id, model_version)
);

CREATE TABLE compliance_audit_view (
    application_id TEXT PRIMARY KEY,
    regulation_set_version TEXT,
    checks_required JSONB DEFAULT '[]'::jsonb,
    checks_passed JSONB DEFAULT '[]'::jsonb,
    checks_failed JSONB DEFAULT '[]'::jsonb,
    rule_results JSONB DEFAULT '{}'::jsonb,
    compliance_status TEXT,
    clearance_issued BOOLEAN DEFAULT FALSE,
    clearance_timestamp TIMESTAMPTZ,
    clearance_issued_by TEXT,
    events_processed INT DEFAULT 0,
    last_event_id UUID,
    last_global_position BIGINT DEFAULT 0,
    last_event_type TEXT,
    last_event_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE compliance_audit_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    global_position BIGINT NOT NULL UNIQUE,
    application_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    regulation_set_version TEXT,
    rule_id TEXT,
    rule_version TEXT,
    verdict TEXT,
    detail JSONB DEFAULT '{}'::jsonb,
    event_timestamp TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_compliance_audit_events_lookup
    ON compliance_audit_events (application_id, event_timestamp, global_position);

CREATE TABLE compliance_audit_snapshots (
    snapshot_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id TEXT NOT NULL,
    snapshot_version INT NOT NULL,
    source_event_id UUID NOT NULL UNIQUE,
    source_global_position BIGINT NOT NULL UNIQUE,
    snapshot_taken_at TIMESTAMPTZ NOT NULL,
    state JSONB NOT NULL
);

CREATE INDEX idx_compliance_audit_snapshots_lookup
    ON compliance_audit_snapshots (
        application_id,
        snapshot_version,
        snapshot_taken_at DESC,
        source_global_position DESC
    );
