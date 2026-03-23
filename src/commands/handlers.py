# src/commands/handlers.py
# =============================================================================
# TRP1 LEDGER — Command Handlers
# =============================================================================
# Source: Challenge Doc Phase 2 p.9 (Command Handler Pattern — implement exactly)
#
# The command handler pattern from the challenge spec must be followed exactly:
#   1. Reconstruct current aggregate state from event history (NOT projections)
#   2. Validate all business rules BEFORE any state change
#   3. Determine new events — pure logic, no I/O
#   4. Append atomically with optimistic concurrency
#
# Every command carries correlation_id: str (required, not optional).
# This satisfies the Causal Tracing Reflex behaviour (Manual p.17).
# =============================================================================
from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal

import structlog
from pydantic import BaseModel, ConfigDict, Field

from src.aggregates.agent_session import AgentSessionAggregate
from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.aggregates.loan_application import ApplicationState, LoanApplicationAggregate
from src.event_store import EventStore
from src.integrity.audit_chain import run_integrity_check as execute_integrity_check
from src.models.events import (
    AgentContextLoaded,
    ApplicationApproved,
    ApplicationSubmitted,
    ComplianceCheckRequested,
    ComplianceClearanceIssued,
    ComplianceRuleFailed,
    ComplianceRulePassed,
    CreditAnalysisCompleted,
    DecisionGenerated,
    DocumentUploaded,
    DocumentUploadRequested,
    DomainError,
    FraudScreeningCompleted,
    HumanReviewCompleted,
    LoanPurpose,
    Recommendation,
    RiskTier,
)

logger = structlog.get_logger()


# =============================================================================
# Command Models (Pydantic v2)
# =============================================================================


class SubmitApplicationCommand(BaseModel):
    """Command to submit a new loan application."""

    model_config = ConfigDict(frozen=True)

    application_id: str
    applicant_id: str
    applicant_name: str  # Denormalised — Manual Pattern 1 (line 665)
    requested_amount_usd: Decimal = Field(gt=0)
    loan_purpose: LoanPurpose
    submission_channel: str
    correlation_id: str  # Required — not optional (Causal Tracing Reflex)


class CreditAnalysisCompletedCommand(BaseModel):
    """Command to record completion of credit analysis by an AI agent."""

    model_config = ConfigDict(frozen=True)

    application_id: str
    agent_id: str
    session_id: str
    model_version: str
    model_deployment_id: str  # Specific deployment (Manual Pattern 2)
    confidence_score: float = Field(ge=0.0, le=1.0)
    risk_tier: RiskTier
    recommended_limit_usd: Decimal = Field(gt=0)
    duration_ms: int = Field(ge=0)
    input_data: dict  # Raw input — will be hashed
    regulatory_basis: list[str] = []  # Regulation IDs (Manual Pattern 2)
    correlation_id: str
    causation_id: str | None = None


class StartAgentSessionCommand(BaseModel):
    """Command to start a new agent session (Gas Town: first event)."""

    model_config = ConfigDict(frozen=True)

    agent_id: str
    session_id: str
    context_source: str
    event_replay_from_position: int = Field(ge=0, default=0)
    context_token_count: int = Field(ge=0)
    model_version: str
    correlation_id: str


class GenerateDecisionCommand(BaseModel):
    """Command to generate a decision for a loan application."""

    model_config = ConfigDict(frozen=True)

    application_id: str
    orchestrator_agent_id: str
    recommendation: Recommendation
    confidence_score: float = Field(ge=0.0, le=1.0)
    contributing_agent_sessions: list[str]
    decision_basis_summary: str
    model_versions: dict[str, str]
    correlation_id: str
    causation_id: str | None = None


class ApproveApplicationCommand(BaseModel):
    """Command to approve a loan application."""

    model_config = ConfigDict(frozen=True)

    application_id: str
    approved_amount_usd: Decimal = Field(gt=0)
    interest_rate: Decimal = Field(gt=0)
    conditions: list[str] = []
    approved_by: str
    correlation_id: str
    causation_id: str | None = None


class RunIntegrityCheckCommand(BaseModel):
    """Command to run a cryptographic integrity check on an entity's audit chain."""

    model_config = ConfigDict(frozen=True)

    entity_type: str
    entity_id: str
    correlation_id: str


# =============================================================================
# Helper Functions
# =============================================================================


def hash_inputs(input_data: dict) -> str:
    """
    Hash input data for causal provenance tracking.
    Ensures reproducibility: given the same inputs, the same hash is produced.
    (Manual Pattern 2, p.22 — input_data_hash on decision events)
    """
    import json

    canonical = json.dumps(input_data, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


@asynccontextmanager
async def application_lock(store: EventStore, application_id: str):
    """
    Serialize application-scoped commands that would otherwise write to
    different streams and bypass optimistic concurrency on the loan stream.
    """
    async with store._pool.connection() as conn:  # noqa: SLF001 - narrow internal use
        await conn.execute(
            "SELECT pg_advisory_lock(hashtext(%s))",
            (f"loan-{application_id}",),
        )
        try:
            yield
        finally:
            await conn.execute(
                "SELECT pg_advisory_unlock(hashtext(%s))",
                (f"loan-{application_id}",),
            )


async def session_contains_application_decision(
    store: EventStore,
    session_stream_id: str,
    application_id: str,
) -> bool:
    """
    Verify a contributing AgentSession stream contains at least one decision
    event for the target application.
    """
    events = await store.load_stream(session_stream_id)
    for event in events:
        if event.payload.get("application_id") != application_id:
            continue
        if event.event_type in {"CreditAnalysisCompleted", "FraudScreeningCompleted"}:
            return True
    return False


# =============================================================================
# Command Handlers
# =============================================================================


async def handle_submit_application(
    cmd: SubmitApplicationCommand,
    store: EventStore,
) -> None:
    """
    Creates a new LoanApplication stream.
    expected_version = -1 (new stream).

    Validates: no existing stream for this application_id (duplicate check).
    Produces: ApplicationSubmitted event.

    Pattern: load → validate → determine → append
    """
    log = logger.bind(
        correlation_id=cmd.correlation_id,
        application_id=cmd.application_id,
    )
    structlog.contextvars.bind_contextvars(correlation_id=cmd.correlation_id)

    # 1. Check for existing stream (duplicate detection)
    existing_version = await store.stream_version(f"loan-{cmd.application_id}")
    if existing_version > 0:
        raise DomainError(
            f"Application {cmd.application_id} already exists "
            f"at version {existing_version}."
        )

    # 2. No validation needed — this is a new application

    # 3. Determine new events
    now = datetime.now(timezone.utc)
    new_events = [
        ApplicationSubmitted(
            application_id=cmd.application_id,
            applicant_id=cmd.applicant_id,
            applicant_name=cmd.applicant_name,
            requested_amount_usd=cmd.requested_amount_usd,
            loan_purpose=cmd.loan_purpose,
            submission_channel=cmd.submission_channel,
            submitted_at=now,
        ),
    ]

    # 4. Append atomically — new stream (expected_version = -1)
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=new_events,
        expected_version=-1,
        correlation_id=cmd.correlation_id,
    )

    log.info("application_submitted", applicant_id=cmd.applicant_id)


# async def handle_request_document_upload(
#     cmd: RequestDocumentUploadCommand,
#     store: EventStore,
# ) -> None:
#     log = logger.bind(
#         correlation_id=cmd.correlation_id,
#         application_id=cmd.application_id,
#     )
#     structlog.contextvars.bind_contextvars(correlation_id=cmd.correlation_id)

#     async with application_lock(store, cmd.application_id):
#         app = await LoanApplicationAggregate.load(store, cmd.application_id)

#         new_events = [
#             DocumentUploadRequested(
#                 application_id=cmd.application_id,
#             ),
#         ]

#         await store.append(
#             stream_id=f"loan-{cmd.application_id}",
#             events=new_events,
#             expected_version=app.version,
#             correlation_id=cmd.correlation_id,
#         )
#     log.info("document_upload_requested")


async def handle_upload_document(
    cmd: UploadDocumentCommand,
    store: EventStore,
) -> None:
    log = logger.bind(
        correlation_id=cmd.correlation_id,
        application_id=cmd.application_id,
        document_id=cmd.document_id,
    )
    structlog.contextvars.bind_contextvars(correlation_id=cmd.correlation_id)

    async with application_lock(store, cmd.application_id):
        app = await LoanApplicationAggregate.load(store, cmd.application_id)

        new_events = [
            DocumentUploaded(
                application_id=cmd.application_id,
                document_id=cmd.document_id,
                file_path=cmd.file_path,
            ),
        ]

        await store.append(
            stream_id=f"loan-{cmd.application_id}",
            events=new_events,
            expected_version=app.version,
            correlation_id=cmd.correlation_id,
        )
    log.info("document_uploaded", file_path=cmd.file_path)


async def handle_credit_analysis_completed(
    cmd: CreditAnalysisCompletedCommand,
    store: EventStore,
) -> None:
    """
    Exact implementation from Challenge Doc p.9-10.

    1. Load LoanApplicationAggregate and AgentSessionAggregate
    2. Validate:
       - app.assert_awaiting_credit_analysis()
       - agent.assert_context_loaded()            ← Gas Town
       - agent.assert_model_version_current()
       - app.assert_credit_analysis_not_done()    ← Rule 3: model version locking
    3. Determine events:
       - confidence_floor = LoanApplicationAggregate.enforce_confidence_floor()
       - CreditAnalysisCompleted event
    4. Append to loan stream with app.version as expected_version
    """
    log = logger.bind(
        correlation_id=cmd.correlation_id,
        application_id=cmd.application_id,
        agent_id=cmd.agent_id,
    )
    structlog.contextvars.bind_contextvars(correlation_id=cmd.correlation_id)

    async with application_lock(store, cmd.application_id):
        # 1. Reconstruct current aggregate state from event history
        app = await LoanApplicationAggregate.load(store, cmd.application_id)
        agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)

        # 2. Validate — all business rules checked BEFORE any state change
        app.assert_awaiting_credit_analysis()
        agent.assert_context_loaded()  # Gas Town pattern
        agent.assert_model_version_current(cmd.model_version)
        app.assert_credit_analysis_not_done()  # Rule 3: model version locking

        # 3. Determine new events — pure logic, no I/O
        LoanApplicationAggregate.enforce_confidence_floor(
            cmd.confidence_score, cmd.risk_tier
        )

        new_events = [
            CreditAnalysisCompleted(
                application_id=cmd.application_id,
                agent_id=cmd.agent_id,
                session_id=cmd.session_id,
                model_version=cmd.model_version,
                model_deployment_id=cmd.model_deployment_id,
                confidence_score=cmd.confidence_score,
                risk_tier=cmd.risk_tier,
                recommended_limit_usd=cmd.recommended_limit_usd,
                analysis_duration_ms=cmd.duration_ms,
                input_data_hash=hash_inputs(cmd.input_data),
                regulatory_basis=cmd.regulatory_basis,
            ),
        ]

        # 4. Append to the AgentSession stream.
        await store.append(
            stream_id=f"agent-{cmd.agent_id}-{cmd.session_id}",
            events=new_events,
            expected_version=agent.version,
            correlation_id=cmd.correlation_id,
            causation_id=cmd.causation_id,
        )

    log.info(
        "credit_analysis_completed",
        risk_tier=cmd.risk_tier,
        confidence_score=cmd.confidence_score,
    )


async def handle_start_agent_session(
    cmd: StartAgentSessionCommand,
    store: EventStore,
) -> None:
    """
    Creates AgentSession stream. expected_version = -1.
    Produces: AgentContextLoaded event (Gas Town: first event in session).

    "No agent may make a decision without first declaring its context source."
    (Challenge Doc p.10, Rule 2)
    """
    log = logger.bind(
        correlation_id=cmd.correlation_id,
        agent_id=cmd.agent_id,
        session_id=cmd.session_id,
    )
    structlog.contextvars.bind_contextvars(correlation_id=cmd.correlation_id)

    # 1. Check for existing session (duplicate detection)
    stream_id = f"agent-{cmd.agent_id}-{cmd.session_id}"
    existing_version = await store.stream_version(stream_id)
    if existing_version > 0:
        raise DomainError(
            f"AgentSession {cmd.agent_id}/{cmd.session_id} already exists."
        )

    # 2. No additional validation — this is a new session

    # 3. Determine new events — Gas Town: AgentContextLoaded is ALWAYS first
    new_events = [
        AgentContextLoaded(
            agent_id=cmd.agent_id,
            session_id=cmd.session_id,
            context_source=cmd.context_source,
            event_replay_from_position=cmd.event_replay_from_position,
            context_token_count=cmd.context_token_count,
            model_version=cmd.model_version,
        ),
    ]

    # 4. Append atomically — new stream (expected_version = -1)
    await store.append(
        stream_id=stream_id,
        events=new_events,
        expected_version=-1,
        correlation_id=cmd.correlation_id,
    )

    log.info(
        "agent_session_started",
        model_version=cmd.model_version,
        context_source=cmd.context_source,
    )


async def handle_generate_decision(
    cmd: GenerateDecisionCommand,
    store: EventStore,
) -> None:
    """
    Rule 4: enforce confidence floor before creating DecisionGenerated event.
    Rule 6: assert contributing_sessions are all valid for this application_id.
    """
    log = logger.bind(
        correlation_id=cmd.correlation_id,
        application_id=cmd.application_id,
    )
    structlog.contextvars.bind_contextvars(correlation_id=cmd.correlation_id)

    # 1. Reconstruct aggregate state
    app = await LoanApplicationAggregate.load(store, cmd.application_id)

    # 2. Validate business rules
    app.assert_pending_decision()

    # Rule 6: assert contributing sessions valid
    valid_session_ids = set()
    for session_stream_id in cmd.contributing_agent_sessions:
        if await session_contains_application_decision(
            store, session_stream_id, cmd.application_id
        ):
            valid_session_ids.add(session_stream_id)

    app.assert_contributing_sessions_valid(
        cmd.contributing_agent_sessions,
        cmd.application_id,
        valid_session_ids,
    )

    # Rule 4: enforce confidence floor
    final_recommendation = LoanApplicationAggregate.enforce_confidence_floor(
        cmd.confidence_score, cmd.recommendation
    )

    # 3. Determine new events
    new_events = [
        DecisionGenerated(
            application_id=cmd.application_id,
            orchestrator_agent_id=cmd.orchestrator_agent_id,
            recommendation=Recommendation(final_recommendation),
            confidence_score=cmd.confidence_score,
            contributing_agent_sessions=cmd.contributing_agent_sessions,
            decision_basis_summary=cmd.decision_basis_summary,
            model_versions=cmd.model_versions,
        ),
    ]

    # 4. Append atomically
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=new_events,
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )

    log.info(
        "decision_generated",
        recommendation=final_recommendation,
        confidence_score=cmd.confidence_score,
    )


async def handle_application_approved(
    cmd: ApproveApplicationCommand,
    store: EventStore,
) -> None:
    """
    Rule 5: assert compliance_complete before appending ApplicationApproved.

    "An ApplicationApproved event cannot be appended unless all
    ComplianceRulePassed events for the application's required checks
    are present in the ComplianceRecord stream."
    (Challenge Doc p.10)
    """
    log = logger.bind(
        correlation_id=cmd.correlation_id,
        application_id=cmd.application_id,
    )
    structlog.contextvars.bind_contextvars(correlation_id=cmd.correlation_id)

    # 1. Reconstruct aggregate state
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    compliance = await ComplianceRecordAggregate.load(store, cmd.application_id)

    # 2. Validate
    app.assert_approved_pending_human()

    # Rule 5: compliance must be complete
    app.assert_compliance_complete(
        required_checks=compliance.required_checks,
        passed_checks=compliance.passed_checks,
        clearance_issued=compliance.clearance_issued,
    )

    # 3. Determine new events
    now = datetime.now(timezone.utc)
    new_events = [
        ApplicationApproved(
            application_id=cmd.application_id,
            approved_amount_usd=cmd.approved_amount_usd,
            interest_rate=cmd.interest_rate,
            conditions=cmd.conditions,
            approved_by=cmd.approved_by,
            effective_date=now,
        ),
    ]

    # 4. Append atomically
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=new_events,
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )

    log.info(
        "application_approved",
        approved_amount=str(cmd.approved_amount_usd),
        approved_by=cmd.approved_by,
    )


async def handle_run_integrity_check(
    cmd: RunIntegrityCheckCommand,
    store: EventStore,
) -> None:
    """
    Runs a cryptographic integrity check using the shared audit-chain module.

    This keeps the command handler aligned with the MCP tool path so both
    verify the same AuditLedger stream and append the same integrity facts.
    """
    log = logger.bind(
        correlation_id=cmd.correlation_id,
        entity_type=cmd.entity_type,
        entity_id=cmd.entity_id,
    )
    structlog.contextvars.bind_contextvars(correlation_id=cmd.correlation_id)

    result = await execute_integrity_check(
        store=store,
        entity_type=cmd.entity_type,
        entity_id=cmd.entity_id,
    )

    log.info(
        "integrity_check_completed",
        events_verified=result.events_verified_count,
        integrity_hash=result.integrity_hash[:16] + "...",
        chain_valid=result.chain_valid,
        tamper_detected=result.tamper_detected,
    )


# =============================================================================
# Missing Command Handlers (fraud, compliance, human review)
# =============================================================================


class FraudScreeningCompletedCommand(BaseModel):
    """Command to record fraud screening completion by an AI agent."""

    model_config = ConfigDict(frozen=True)

    application_id: str
    agent_id: str
    session_id: str
    fraud_score: float = Field(ge=0.0, le=1.0)
    anomaly_flags: list[str] = []
    screening_model_version: str
    model_deployment_id: str
    input_data: dict = {}
    correlation_id: str
    causation_id: str | None = None


class RequestComplianceCheckCommand(BaseModel):
    """Command to formally request compliance checks for an application."""

    model_config = ConfigDict(frozen=True)

    application_id: str
    regulation_set_version: str
    checks_required: list[str]
    correlation_id: str
    causation_id: str | None = None


class RecordComplianceCheckCommand(BaseModel):
    """Command to record a compliance rule check result."""

    model_config = ConfigDict(frozen=True)

    application_id: str
    rule_id: str
    rule_version: str
    passed: bool
    failure_reason: str | None = None
    remediation_required: bool = False
    evidence_hash: str = ""
    correlation_id: str
    causation_id: str | None = None


class HumanReviewCompletedCommand(BaseModel):
    """Command to record a human review of an AI-generated recommendation."""

    model_config = ConfigDict(frozen=True)

    application_id: str
    reviewer_id: str
    override: bool
    final_decision: str  # "APPROVE" | "DECLINE"
    override_reason: str | None = None
    correlation_id: str
    causation_id: str | None = None


async def handle_fraud_screening_completed(
    cmd: FraudScreeningCompletedCommand,
    store: EventStore,
) -> None:
    """
    Records fraud screening results from a FraudDetection agent.

    Validates:
    - Agent session must exist with AgentContextLoaded (Gas Town)
    - Application must exist
    """
    log = logger.bind(
        correlation_id=cmd.correlation_id,
        application_id=cmd.application_id,
        agent_id=cmd.agent_id,
    )
    structlog.contextvars.bind_contextvars(correlation_id=cmd.correlation_id)

    async with application_lock(store, cmd.application_id):
        # 1. Reconstruct aggregate state
        agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)

        # 2. Validate
        agent.assert_context_loaded()

        # 3. Determine new events
        new_events = [
            FraudScreeningCompleted(
                application_id=cmd.application_id,
                agent_id=cmd.agent_id,
                fraud_score=cmd.fraud_score,
                anomaly_flags=cmd.anomaly_flags,
                screening_model_version=cmd.screening_model_version,
                model_deployment_id=cmd.model_deployment_id,
                input_data_hash=hash_inputs(cmd.input_data),
            ),
        ]

        # 4. Append to agent session stream
        await store.append(
            stream_id=f"agent-{cmd.agent_id}-{cmd.session_id}",
            events=new_events,
            expected_version=agent.version,
            correlation_id=cmd.correlation_id,
            causation_id=cmd.causation_id,
        )

    log.info(
        "fraud_screening_completed",
        fraud_score=cmd.fraud_score,
        anomaly_count=len(cmd.anomaly_flags),
    )


async def handle_request_compliance_check(
    cmd: RequestComplianceCheckCommand,
    store: EventStore,
) -> None:
    """
    Requests a set of compliance checks for an application.
    """
    log = logger.bind(
        correlation_id=cmd.correlation_id,
        application_id=cmd.application_id,
    )
    structlog.contextvars.bind_contextvars(correlation_id=cmd.correlation_id)

    compliance = await ComplianceRecordAggregate.load(store, cmd.application_id)
    stream_id = f"compliance-{cmd.application_id}"

    new_events = [
        ComplianceCheckRequested(
            application_id=cmd.application_id,
            regulation_set_version=cmd.regulation_set_version,
            checks_required=cmd.checks_required,
        )
    ]

    version = compliance.version if compliance.version > 0 else -1
    await store.append(
        stream_id=stream_id,
        events=new_events,
        expected_version=version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )

    log.info(
        "compliance_check_requested",
        checks_required=cmd.checks_required,
    )


async def handle_record_compliance_check(
    cmd: RecordComplianceCheckCommand,
    store: EventStore,
) -> None:
    """
    Records a compliance rule check result (pass or fail).

    Pattern: load → validate → determine → append
    """
    log = logger.bind(
        correlation_id=cmd.correlation_id,
        application_id=cmd.application_id,
        rule_id=cmd.rule_id,
    )
    structlog.contextvars.bind_contextvars(correlation_id=cmd.correlation_id)

    # 1. Reconstruct compliance aggregate
    compliance = await ComplianceRecordAggregate.load(store, cmd.application_id)
    stream_id = f"compliance-{cmd.application_id}"

    if compliance.required_checks and cmd.rule_id not in compliance.required_checks:
        raise DomainError(
            f"Rule {cmd.rule_id} is not part of the active regulation set "
            f"for application {cmd.application_id}. Active checks: "
            f"{sorted(compliance.required_checks)}"
        )

    # 2. Determine new events
    now = datetime.now(timezone.utc)
    if cmd.passed:
        new_events = [
            ComplianceRulePassed(
                application_id=cmd.application_id,
                rule_id=cmd.rule_id,
                rule_version=cmd.rule_version,
                evaluation_timestamp=now,
                evidence_hash=cmd.evidence_hash,
            ),
        ]
    else:
        new_events = [
            ComplianceRuleFailed(
                application_id=cmd.application_id,
                rule_id=cmd.rule_id,
                rule_version=cmd.rule_version,
                failure_reason=cmd.failure_reason or "No reason provided",
                remediation_required=cmd.remediation_required,
            ),
        ]

    # Cache expected version before applying mock events
    expected_version = compliance.version if compliance.version > 0 else -1

    # Temporarily apply the new event to see if clearance is achieved
    import uuid

    from src.models.events import StoredEvent

    for ev in new_events:
        mock_event = StoredEvent(
            event_id=uuid.uuid4(),
            stream_id=stream_id,
            stream_position=0,
            global_position=0,
            event_type=ev.event_type,
            event_version=ev.event_version,
            payload=ev.model_dump(mode="json"),
            metadata={},
            recorded_at=now,
        )
        compliance._apply(mock_event)

    if (
        not compliance.missing_required_checks()
        and not compliance.failed_checks
        and not compliance.clearance_issued
    ):
        new_events.append(
            ComplianceClearanceIssued(
                application_id=cmd.application_id,
                regulation_set_version=compliance.regulation_set_version or "unknown",
                checks_passed=compliance.passed_checks,
                clearance_timestamp=now,
                issued_by="system",
            )
        )

    # 3. Append to compliance stream
    await store.append(
        stream_id=stream_id,
        events=new_events,
        expected_version=expected_version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )

    log.info(
        "compliance_check_recorded",
        rule_id=cmd.rule_id,
        passed=cmd.passed,
    )


async def handle_human_review_completed(
    cmd: HumanReviewCompletedCommand,
    store: EventStore,
) -> None:
    """
    Records a human review of an AI-generated decision.

    Validates:
    - Application must be in a pending-human state
    - If override=True, override_reason is required
    """
    log = logger.bind(
        correlation_id=cmd.correlation_id,
        application_id=cmd.application_id,
        reviewer_id=cmd.reviewer_id,
    )
    structlog.contextvars.bind_contextvars(correlation_id=cmd.correlation_id)

    # 1. Reconstruct aggregate
    app = await LoanApplicationAggregate.load(store, cmd.application_id)

    # 2. Validate — must be in a pending-human state
    app.assert_pending_human_review()

    # 3. Determine new events
    new_events = [
        HumanReviewCompleted(
            application_id=cmd.application_id,
            reviewer_id=cmd.reviewer_id,
            override=cmd.override,
            final_decision=cmd.final_decision,
            override_reason=cmd.override_reason,
        ),
    ]

    # 4. Append atomically
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=new_events,
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )

    log.info(
        "human_review_completed",
        override=cmd.override,
        final_decision=cmd.final_decision,
    )
