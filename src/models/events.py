# src/models/events.py
# =============================================================================
# TRP1 LEDGER — Event Models & Domain Exceptions
# =============================================================================
# Source: Challenge Doc pages 6-7 (Event Catalogue) + Manual Part IV Section 4.2
#
# This module defines:
#   PART A — Base classes (BaseEvent, StoredEvent, StreamMetadata)
#   PART B — Custom exceptions (OCC, Domain, InvalidStateTransition)
#   PART C — All domain events from the Event Catalogue
#   PART D — Identified missing events (4 additional)
#
# All events follow Manual Section 4.3 naming rules:
#   - Past tense, domain language — no CRUD verbs
#   - PascalCase, no abbreviations
#   - Noun + past participle (ApplicationSubmitted, not SubmittedApplication)
# =============================================================================
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, ClassVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

# ---------------------------------------------------------------------------
# PEP 695 Type Aliases (Python 3.12+)
# ---------------------------------------------------------------------------
type StreamId = str
type EventType = str
type CorrelationId = str
type CausationId = str
type AgentId = str
type SessionId = str
type ApplicationId = str
type ModelVersion = str


# =============================================================================
# PART A — Base Classes
# =============================================================================


class BaseEvent(BaseModel):
    """
    Base class for all domain events.

    Design decisions:
    - frozen=True: events are immutable facts — they must never be modified.
    - populate_by_name=True: enables construction from both alias and field name.
    - event_type and event_version are ClassVar: they are class-level constants,
      NOT instance fields. ClassVar fields are excluded from Pydantic serialisation
      automatically. This enables answering "What is the current event_version of
      your most-changed event type?" without instantiation.
      (Manual p.17 — Schema Immortality Awareness self-diagnosis)
    """

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    event_type: ClassVar[str]
    event_version: ClassVar[int]


class StoredEvent(BaseModel):
    """
    An event as it exists in the database — includes store-assigned fields.

    This is the read-side representation. The event_version here is an instance
    field because the stored version may differ from the current class version
    (before upcasting is applied).
    """

    model_config = ConfigDict(frozen=True)

    event_id: UUID
    stream_id: str
    stream_position: int
    global_position: int
    event_type: str
    event_version: int  # Instance field — the stored version
    payload: dict[str, Any]
    metadata: dict[str, Any]
    recorded_at: datetime

    def with_payload(self, new_payload: dict, version: int) -> StoredEvent:
        """
        Returns new StoredEvent with updated payload and version.
        Used by UpcasterRegistry — never mutates the original.

        This method is critical for the upcasting pipeline:
        raw stored event → upcast v1→v2 → upcast v2→v3 → final event
        The raw stored payload in the database is NEVER modified.
        """
        return self.model_copy(
            update={"payload": new_payload, "event_version": version}
        )


class StreamMetadata(BaseModel):
    """Metadata about an event stream, read from event_streams table."""

    model_config = ConfigDict(frozen=True)

    stream_id: str
    aggregate_type: str
    current_version: int
    created_at: datetime
    archived_at: datetime | None
    metadata: dict[str, Any]


# =============================================================================
# PART B — Custom Exceptions
# =============================================================================


class OptimisticConcurrencyError(Exception):
    """
    Raised when append expected_version != actual stream version.

    This is the cornerstone of event store concurrency control.
    When two agents simultaneously try to append to the same stream,
    the database's UNIQUE constraint on (stream_id, stream_position)
    ensures exactly one succeeds. The loser receives this error and
    must reload the stream and retry.

    The suggested_action field enables LLM-based agents to autonomously
    recover from concurrency conflicts.
    """

    def __init__(self, stream_id: str, expected: int, actual: int):
        self.stream_id = stream_id
        self.expected_version = expected
        self.actual_version = actual
        self.suggested_action = "reload_stream_and_retry"
        super().__init__(
            f"Stream '{stream_id}': expected version {expected}, "
            f"actual {actual}. {self.suggested_action}"
        )


class DomainError(Exception):
    """
    Raised when a business invariant is violated in aggregate logic.

    Business rules are enforced in the aggregate, not in the API layer.
    A rule that is only checked in a request handler is not a business
    rule — it is a UI validation.
    (Challenge Doc Phase 2 p.9)
    """

    pass


class InvalidStateTransitionError(DomainError):
    """
    Raised when an invalid state machine transition is attempted.

    The LoanApplication aggregate has a strict state machine with defined
    valid transitions. Any out-of-order transition raises this error.
    (Challenge Doc p.10, Business Rule 1)
    """

    def __init__(self, from_state: str, to_state: str, valid_next: list[str]):
        self.from_state = from_state
        self.to_state = to_state
        self.valid_next_states = valid_next
        super().__init__(
            f"Invalid transition: {from_state} → {to_state}. "
            f"Valid next states: {valid_next}"
        )


class StreamArchivedError(DomainError):
    """Raised when attempting to append to an archived stream."""

    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        super().__init__(f"Stream '{stream_id}' is archived and rejects new appends.")


# =============================================================================
# PART C — Domain Events from Challenge Doc Event Catalogue (pages 6-7)
# =============================================================================
# Every event listed in the catalogue is implemented below.
# Decision events include: agent_id, model_version, input_data_hash
# following Manual Pattern 2 (p.22) — full causal provenance on every decision.
# =============================================================================

# --- Enums for constrained fields ---


class LoanPurpose(StrEnum):
    """Loan purpose categories for Apex Financial Services."""

    WORKING_CAPITAL = "WORKING_CAPITAL"
    EQUIPMENT = "EQUIPMENT"
    REAL_ESTATE = "REAL_ESTATE"
    EXPANSION = "EXPANSION"
    REFINANCING = "REFINANCING"
    OTHER = "OTHER"


class RiskTier(StrEnum):
    """Risk classification tiers from credit analysis."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class Recommendation(StrEnum):
    """Decision recommendation values."""

    APPROVE = "APPROVE"
    DECLINE = "DECLINE"
    REFER = "REFER"


# --- DocumentPackage Events ---


class PackageCreated(BaseEvent):
    """(DocumentPackage aggregate, v1)"""

    event_type: ClassVar[str] = "PackageCreated"
    event_version: ClassVar[int] = 1

    application_id: str


class DocumentAdded(BaseEvent):
    """(DocumentPackage aggregate, v1)"""

    event_type: ClassVar[str] = "DocumentAdded"
    event_version: ClassVar[int] = 1

    application_id: str
    document_id: str
    file_name: str
    document_type: str


class DocumentFormatValidated(BaseEvent):
    """(DocumentPackage aggregate, v1)"""

    event_type: ClassVar[str] = "DocumentFormatValidated"
    event_version: ClassVar[int] = 1

    application_id: str
    document_id: str
    is_valid: bool
    format_notes: str | None = None


class ExtractionStarted(BaseEvent):
    """(DocumentPackage aggregate, v1)"""

    event_type: ClassVar[str] = "ExtractionStarted"
    event_version: ClassVar[int] = 1

    application_id: str
    document_id: str
    extraction_model: str


class ExtractionCompleted(BaseEvent):
    """(DocumentPackage aggregate, v1)"""

    event_type: ClassVar[str] = "ExtractionCompleted"
    event_version: ClassVar[int] = 1

    application_id: str
    document_id: str
    facts: dict
    field_confidence: dict[str, float]
    extraction_notes: list[str]


class QualityAssessmentCompleted(BaseEvent):
    """(DocumentPackage aggregate, v1)"""

    event_type: ClassVar[str] = "QualityAssessmentCompleted"
    event_version: ClassVar[int] = 1

    application_id: str
    overall_confidence: float
    is_coherent: bool
    anomalies: list[str]
    critical_missing_fields: list[str]
    reextraction_recommended: bool
    auditor_notes: str


class PackageReadyForAnalysis(BaseEvent):
    """(DocumentPackage aggregate, v1)"""

    event_type: ClassVar[str] = "PackageReadyForAnalysis"
    event_version: ClassVar[int] = 1

    application_id: str


# --- LoanApplication Events ---


class ApplicationSubmitted(BaseEvent):
    """
    First event in a LoanApplication stream. Records the fact that a
    commercial loan application was submitted to the platform.
    (LoanApplication aggregate, v1)
    """

    event_type: ClassVar[str] = "ApplicationSubmitted"
    event_version: ClassVar[int] = 1

    application_id: str
    applicant_id: str
    applicant_name: (
        str  # Denormalised from applicant record (Manual Pattern 1, line 665)
    )
    requested_amount_usd: Decimal = Field(gt=0)
    loan_purpose: LoanPurpose
    submission_channel: str  # "web" | "mobile" | "agent" | "branch"
    submitted_at: datetime


class DocumentUploadRequested(BaseEvent):
    """(LoanApplication aggregate, v1)"""

    event_type: ClassVar[str] = "DocumentUploadRequested"
    event_version: ClassVar[int] = 1

    application_id: str


class DocumentUploaded(BaseEvent):
    """(LoanApplication aggregate, v1)"""

    event_type: ClassVar[str] = "DocumentUploaded"
    event_version: ClassVar[int] = 1

    application_id: str
    document_id: str
    file_path: str


class HumanReviewRequested(BaseEvent):
    """(LoanApplication aggregate, v1)"""

    event_type: ClassVar[str] = "HumanReviewRequested"
    event_version: ClassVar[int] = 1

    application_id: str


class CreditAnalysisRequested(BaseEvent):
    """
    Records that a credit analysis has been requested for a loan application.
    Triggers assignment of a CreditAnalysis agent.
    (LoanApplication aggregate, v1)
    """

    event_type: ClassVar[str] = "CreditAnalysisRequested"
    event_version: ClassVar[int] = 1

    application_id: str
    assigned_agent_id: str
    requested_at: datetime
    priority: str


class CreditAnalysisCompleted(BaseEvent):
    """
    Records the completion of credit analysis by an AI agent.
    Already at v2 per catalogue — includes model_version and input_data_hash
    for full causal provenance. (Manual Pattern 2, p.22)
    (AgentSession aggregate, v2)
    """

    event_type: ClassVar[str] = "CreditAnalysisCompleted"
    event_version: ClassVar[int] = 2

    application_id: str
    agent_id: str
    session_id: str
    model_version: str  # "credit-model-v2.4.1"
    model_deployment_id: str  # Specific deployment, not just version (Manual Pattern 2)
    confidence_score: float = Field(ge=0.0, le=1.0)
    risk_tier: RiskTier
    recommended_limit_usd: Decimal = Field(gt=0)
    analysis_duration_ms: int = Field(ge=0)
    input_data_hash: str  # SHA-256 of all input data — not the data itself
    regulatory_basis: list[
        str
    ] = []  # Regulation IDs this analysis satisfies (Manual Pattern 2)


class DecisionGenerated(BaseEvent):
    """
    Records the decision generated by the DecisionOrchestrator agent.
    Already at v2 per catalogue — includes model_versions dict.
    (LoanApplication aggregate, v2)

    Business Rule 4: confidence_score < 0.6 forces recommendation = 'REFER'.
    This is enforced in the aggregate, not here.
    """

    event_type: ClassVar[str] = "DecisionGenerated"
    event_version: ClassVar[int] = 2

    application_id: str
    orchestrator_agent_id: str
    recommendation: Recommendation
    confidence_score: float = Field(ge=0.0, le=1.0)
    contributing_agent_sessions: list[str]
    decision_basis_summary: str
    model_versions: dict[str, str]


class HumanReviewCompleted(BaseEvent):
    """
    Records that a human loan officer has reviewed the AI-generated recommendation.
    If override=True, the human overrides the AI recommendation and override_reason
    is required.
    (LoanApplication aggregate, v1)
    """

    event_type: ClassVar[str] = "HumanReviewCompleted"
    event_version: ClassVar[int] = 1

    application_id: str
    reviewer_id: str
    override: bool
    final_decision: str
    override_reason: str | None = None

    @model_validator(mode="after")
    def validate_override_reason(self) -> HumanReviewCompleted:
        """If override is True, override_reason is required."""
        if self.override and not self.override_reason:
            raise ValueError("override_reason is required when override is True")
        return self


class ApplicationApproved(BaseEvent):
    """
    Records that a loan application has been approved.
    Cannot be appended unless all compliance checks have passed.
    (LoanApplication aggregate, v1)
    """

    event_type: ClassVar[str] = "ApplicationApproved"
    event_version: ClassVar[int] = 1

    application_id: str
    approved_amount_usd: Decimal = Field(gt=0)
    interest_rate: Decimal = Field(gt=0)
    conditions: list[str]
    approved_by: str  # human_id or "auto"
    effective_date: datetime


class ApplicationDeclined(BaseEvent):
    """
    Records that a loan application has been declined.
    (LoanApplication aggregate, v1)
    """

    event_type: ClassVar[str] = "ApplicationDeclined"
    event_version: ClassVar[int] = 1

    application_id: str
    decline_reasons: list[str]
    declined_by: str
    adverse_action_notice_required: bool


# --- AgentSession Events ---


class AgentSessionStarted(BaseEvent):
    """Gas Town anchor. (AgentSession aggregate, v1)"""

    event_type: ClassVar[str] = "AgentSessionStarted"
    event_version: ClassVar[int] = 1

    agent_id: str
    session_id: str
    agent_type: str
    model_version: str
    context_source: str
    context_token_count: int = Field(ge=0)


class AgentContextLoaded(BaseEvent):
    """
    First event in every AgentSession stream (Gas Town pattern).
    Records that an agent has declared its context source before making
    any decisions. This is the declaration of memory source.

    "The key insight: the event store IS the agent's memory."
    (Manual p.13)

    Business Rule 2: An AgentSession MUST have AgentContextLoaded as its
    first event before any decision event can be appended.
    (AgentSession aggregate, v1)
    """

    event_type: ClassVar[str] = "AgentContextLoaded"
    event_version: ClassVar[int] = 1

    agent_id: str
    session_id: str
    context_source: str
    event_replay_from_position: int = Field(ge=0)
    context_token_count: int = Field(ge=0)
    model_version: str


class AgentInputValidated(BaseEvent):
    """(AgentSession aggregate, v1)"""

    event_type: ClassVar[str] = "AgentInputValidated"
    event_version: ClassVar[int] = 1

    agent_id: str
    session_id: str
    inputs_validated: list[str]
    validation_duration_ms: int = Field(ge=0)


class AgentInputValidationFailed(BaseEvent):
    """(AgentSession aggregate, v1)"""

    event_type: ClassVar[str] = "AgentInputValidationFailed"
    event_version: ClassVar[int] = 1

    agent_id: str
    session_id: str
    missing_inputs: list[str]
    validation_errors: list[str]


class AgentNodeExecuted(BaseEvent):
    """(AgentSession aggregate, v1)"""

    event_type: ClassVar[str] = "AgentNodeExecuted"
    event_version: ClassVar[int] = 1

    agent_id: str
    session_id: str
    node_name: str
    node_sequence: int
    input_keys: list[str]
    output_keys: list[str]
    llm_called: bool
    llm_tokens_input: int | None = None
    llm_tokens_output: int | None = None
    llm_cost_usd: float | None = None
    duration_ms: int = Field(ge=0)


class AgentToolCalled(BaseEvent):
    """(AgentSession aggregate, v1)"""

    event_type: ClassVar[str] = "AgentToolCalled"
    event_version: ClassVar[int] = 1

    agent_id: str
    session_id: str
    tool_name: str
    tool_input_summary: str
    tool_output_summary: str
    tool_duration_ms: int = Field(ge=0)


class AgentOutputWritten(BaseEvent):
    """(AgentSession aggregate, v1)"""

    event_type: ClassVar[str] = "AgentOutputWritten"
    event_version: ClassVar[int] = 1

    agent_id: str
    session_id: str
    events_written: list[dict]
    output_summary: str


class AgentSessionCompleted(BaseEvent):
    """(AgentSession aggregate, v1)"""

    event_type: ClassVar[str] = "AgentSessionCompleted"
    event_version: ClassVar[int] = 1

    agent_id: str
    session_id: str
    total_nodes_executed: int
    total_llm_calls: int
    total_tokens_used: int
    total_cost_usd: float
    next_agent_triggered: str | None = None


class AgentSessionFailed(BaseEvent):
    """(AgentSession aggregate, v1)"""

    event_type: ClassVar[str] = "AgentSessionFailed"
    event_version: ClassVar[int] = 1

    agent_id: str
    session_id: str
    error_type: str
    error_message: str
    last_successful_node: str | None = None
    recoverable: bool


class AgentSessionRecovered(BaseEvent):
    """(AgentSession aggregate, v1)"""

    event_type: ClassVar[str] = "AgentSessionRecovered"
    event_version: ClassVar[int] = 1

    agent_id: str
    session_id: str
    recovered_from_session_id: str
    recovery_point: str


class FraudScreeningCompleted(BaseEvent):
    """
    Records the completion of fraud screening by a FraudDetection agent.
    (AgentSession aggregate, v1)
    """

    event_type: ClassVar[str] = "FraudScreeningCompleted"
    event_version: ClassVar[int] = 1

    application_id: str
    agent_id: str
    fraud_score: float = Field(ge=0.0, le=1.0)
    anomaly_flags: list[str]
    screening_model_version: str
    model_deployment_id: str  # Specific deployment (Manual Pattern 2)
    input_data_hash: str


# --- CreditRecord Events ---


class CreditRecordOpened(BaseEvent):
    """(CreditRecord aggregate, v1)"""

    event_type: ClassVar[str] = "CreditRecordOpened"
    event_version: ClassVar[int] = 1

    application_id: str
    agent_id: str


class HistoricalProfileConsumed(BaseEvent):
    """(CreditRecord aggregate, v1)"""

    event_type: ClassVar[str] = "HistoricalProfileConsumed"
    event_version: ClassVar[int] = 1

    application_id: str
    profile_data: dict


class ExtractedFactsConsumed(BaseEvent):
    """(CreditRecord aggregate, v1)"""

    event_type: ClassVar[str] = "ExtractedFactsConsumed"
    event_version: ClassVar[int] = 1

    application_id: str
    facts_data: dict


class CreditAnalysisDeferred(BaseEvent):
    """(CreditRecord aggregate, v1)"""

    event_type: ClassVar[str] = "CreditAnalysisDeferred"
    event_version: ClassVar[int] = 1

    application_id: str
    agent_id: str
    reason: str


# --- FraudScreening Events ---


class FraudScreeningInitiated(BaseEvent):
    """(FraudScreening aggregate, v1)"""

    event_type: ClassVar[str] = "FraudScreeningInitiated"
    event_version: ClassVar[int] = 1

    application_id: str
    agent_id: str


class FraudAnomalyDetected(BaseEvent):
    """(FraudScreening aggregate, v1)"""

    event_type: ClassVar[str] = "FraudAnomalyDetected"
    event_version: ClassVar[int] = 1

    application_id: str
    agent_id: str
    anomaly_type: str
    description: str
    severity: float
    evidence: str

# --- ComplianceRecord Events ---


class ComplianceCheckRequested(BaseEvent):
    """
    Records that compliance checks have been requested for an application.
    (ComplianceRecord aggregate, v1)
    """

    event_type: ClassVar[str] = "ComplianceCheckRequested"
    event_version: ClassVar[int] = 1

    application_id: str
    regulation_set_version: str
    checks_required: list[str]


class ComplianceCheckInitiated(BaseEvent):
    """(ComplianceRecord aggregate, v1)"""

    event_type: ClassVar[str] = "ComplianceCheckInitiated"
    event_version: ClassVar[int] = 1

    application_id: str
    agent_id: str


class ComplianceRuleNoted(BaseEvent):
    """(ComplianceRecord aggregate, v1)"""

    event_type: ClassVar[str] = "ComplianceRuleNoted"
    event_version: ClassVar[int] = 1

    application_id: str
    rule_id: str
    note_type: str
    note: str


class ComplianceCheckCompleted(BaseEvent):
    """(ComplianceRecord aggregate, v1)"""

    event_type: ClassVar[str] = "ComplianceCheckCompleted"
    event_version: ClassVar[int] = 1

    application_id: str
    overall_verdict: str
    has_hard_block: bool


class ComplianceRulePassed(BaseEvent):
    """(ComplianceRecord aggregate, v1)"""

    event_type: ClassVar[str] = "ComplianceCheckInitiated"
    event_version: ClassVar[int] = 1

    application_id: str
    agent_id: str


class ComplianceRuleNoted(BaseEvent):
    """(ComplianceRecord aggregate, v1)"""

    event_type: ClassVar[str] = "ComplianceRuleNoted"
    event_version: ClassVar[int] = 1

    application_id: str
    rule_id: str
    note_type: str
    note: str


class ComplianceCheckCompleted(BaseEvent):
    """(ComplianceRecord aggregate, v1)"""

    event_type: ClassVar[str] = "ComplianceCheckCompleted"
    event_version: ClassVar[int] = 1

    application_id: str
    overall_verdict: str
    has_hard_block: bool


class ComplianceRulePassed(BaseEvent):
    """
    Records that a specific compliance rule has been evaluated and passed.
    (ComplianceRecord aggregate, v1)
    """

    event_type: ClassVar[str] = "ComplianceRulePassed"
    event_version: ClassVar[int] = 1

    application_id: str
    rule_id: str
    rule_version: str
    evaluation_timestamp: datetime
    evidence_hash: str


class ComplianceRuleFailed(BaseEvent):
    """
    Records that a specific compliance rule has been evaluated and failed.
    (ComplianceRecord aggregate, v1)
    """

    event_type: ClassVar[str] = "ComplianceRuleFailed"
    event_version: ClassVar[int] = 1

    application_id: str
    rule_id: str
    rule_version: str
    failure_reason: str
    remediation_required: bool


# --- AuditLedger Events ---


class AuditIntegrityCheckRun(BaseEvent):
    """
    Records the result of a cryptographic integrity check on the audit chain.
    Each check hashes all preceding events plus the previous integrity hash,
    forming a blockchain-style chain. Any post-hoc modification breaks the chain.
    (AuditLedger aggregate, v1)
    """

    event_type: ClassVar[str] = "AuditIntegrityCheckRun"
    event_version: ClassVar[int] = 1

    entity_id: str
    check_timestamp: datetime
    events_verified_count: int = Field(ge=0)
    integrity_hash: str
    previous_hash: str  # Chain link to previous check


class AuditEventLinked(BaseEvent):
    """
    Records a business event in the cross-stream AuditLedger trail.

    This is the AuditLedger fact that ties loan, compliance, and agent-session
    events together for a single business entity without mutating the source
    streams themselves.
    """

    event_type: ClassVar[str] = "AuditEventLinked"
    event_version: ClassVar[int] = 1

    entity_id: str
    source_event_id: str
    source_stream_id: str
    source_stream_position: int
    source_global_position: int
    source_event_type: str
    source_event_version: int
    source_recorded_at: datetime
    correlation_id: str | None = None
    causation_id: str | None = None
    payload_snapshot: dict[str, Any]
    metadata_snapshot: dict[str, Any]


# =============================================================================
# PART D — Identified Missing Events
# =============================================================================
# The catalogue is "intentionally incomplete" (Challenge Doc p.6).
# These 4 events fill gaps identified during domain analysis.
# Justification for each is provided in DOMAIN_NOTES.md.
# =============================================================================


class ApplicationUnderReview(BaseEvent):
    """
    MISSING EVENT 1 — LoanApplication aggregate, v1

    Records the transition to human review status after a DecisionGenerated
    event with recommendation = 'REFER' or when confidence_score is below
    the regulatory floor. Without this event, the state machine has no
    explicit transition from PENDING_DECISION to the review states
    (APPROVED_PENDING_HUMAN / DECLINED_PENDING_HUMAN).

    This event captures WHO initiated the review, WHY it was triggered,
    and what the pending recommendation is — enabling audit queries like
    "Show me all applications that went to human review and why."

    Justified in DOMAIN_NOTES.md Section: Missing Events.
    """

    event_type: ClassVar[str] = "ApplicationUnderReview"
    event_version: ClassVar[int] = 1

    application_id: str
    review_reason: str
    triggered_by: str  # agent_id or system rule
    pending_recommendation: Recommendation
    assigned_reviewer_id: str | None = None


class AgentDecisionSuperseded(BaseEvent):
    """
    MISSING EVENT 2 — AgentSession aggregate, v1

    Records that a previous agent decision has been superseded, typically
    by a HumanReviewOverride. Without this event, Business Rule 3
    (model version locking) has no mechanism to "unlock" the analysis
    for resubmission. This event records:
    - WHICH original decision was superseded
    - WHY it was superseded (human override, model update, etc.)
    - WHAT the original values were (for audit trail)

    Justified in DOMAIN_NOTES.md Section: Missing Events.
    """

    event_type: ClassVar[str] = "AgentDecisionSuperseded"
    event_version: ClassVar[int] = 1

    application_id: str
    agent_id: str
    session_id: str
    original_decision_event_id: str
    superseded_reason: str
    superseded_by: str  # human_id or agent_id


class ComplianceClearanceIssued(BaseEvent):
    """
    MISSING EVENT 3 — ComplianceRecord aggregate, v1

    Records the formal issuance of compliance clearance after all required
    checks have passed. The catalogue has ComplianceRulePassed/Failed for
    individual checks, but no event that records the aggregate compliance
    outcome. Without this, determining "is this application compliance-clear?"
    requires replaying all rule events and computing the result — violating
    the principle that facts should be recorded, not computed.

    This event is the trigger for transitioning the LoanApplication from
    COMPLIANCE_REVIEW to PENDING_DECISION.

    Justified in DOMAIN_NOTES.md Section: Missing Events.
    """

    event_type: ClassVar[str] = "ComplianceClearanceIssued"
    event_version: ClassVar[int] = 1

    application_id: str
    regulation_set_version: str
    checks_passed: list[str]
    clearance_timestamp: datetime
    issued_by: str  # compliance agent_id


class AuditTamperDetected(BaseEvent):
    """
    MISSING EVENT 4 — AuditLedger aggregate, v1

    Records the detection of tampering in the audit chain. The catalogue
    has AuditIntegrityCheckRun which records successful checks, but no
    event for when a check FAILS (detects tampering). Without this event,
    tamper detection is a silent failure — it can only be inferred from
    the absence of a successful check. In a regulatory system, tamper
    detection MUST be an explicit, recorded, alertable fact.

    This event triggers immediate alerting and investigation workflows.

    Justified in DOMAIN_NOTES.md Section: Missing Events.
    """

    event_type: ClassVar[str] = "AuditTamperDetected"
    event_version: ClassVar[int] = 1

    entity_id: str
    detection_timestamp: datetime
    expected_hash: str
    actual_hash: str
    affected_event_range: str  # "global_position 100-150"
    severity: str  # "CRITICAL" — always
    alert_sent: bool = False
