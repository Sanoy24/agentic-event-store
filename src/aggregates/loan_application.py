# src/aggregates/loan_application.py
# =============================================================================
# TRP1 LEDGER — LoanApplication Aggregate
# =============================================================================
# Source: Challenge Doc Phase 2 pages 9-11 + Manual Part II Cluster A p.8
#
# The LoanApplication aggregate is the central consistency boundary for a
# commercial loan application's lifecycle. It enforces the state machine,
# business rules, and invariants defined in the Challenge Doc.
#
# Key design decisions:
# - State reconstructed by replaying events (CQRS — never read from projections)
# - match/case for state machine transitions (Python 3.12+)
# - All business rules enforced here, not in the API layer
# =============================================================================
from __future__ import annotations

from decimal import Decimal
from enum import StrEnum

from src.event_store import EventStore
from src.models.events import (
    DomainError,
    InvalidStateTransitionError,
    Recommendation,
    StoredEvent,
)


# =============================================================================
# PART A — State Machine
# =============================================================================


class ApplicationState(StrEnum):
    """
    Loan application lifecycle states.
    Uses StrEnum (Python 3.12+) for human-readable serialisation.
    """

    SUBMITTED = "SUBMITTED"
    AWAITING_ANALYSIS = "AWAITING_ANALYSIS"
    ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"
    COMPLIANCE_REVIEW = "COMPLIANCE_REVIEW"
    PENDING_DECISION = "PENDING_DECISION"
    APPROVED_PENDING_HUMAN = "APPROVED_PENDING_HUMAN"
    DECLINED_PENDING_HUMAN = "DECLINED_PENDING_HUMAN"
    FINAL_APPROVED = "FINAL_APPROVED"
    FINAL_DECLINED = "FINAL_DECLINED"


VALID_TRANSITIONS: dict[ApplicationState, set[ApplicationState]] = {
    ApplicationState.SUBMITTED: {ApplicationState.AWAITING_ANALYSIS},
    ApplicationState.AWAITING_ANALYSIS: {ApplicationState.ANALYSIS_COMPLETE},
    ApplicationState.ANALYSIS_COMPLETE: {ApplicationState.COMPLIANCE_REVIEW},
    ApplicationState.COMPLIANCE_REVIEW: {ApplicationState.PENDING_DECISION},
    ApplicationState.PENDING_DECISION: {
        ApplicationState.APPROVED_PENDING_HUMAN,
        ApplicationState.DECLINED_PENDING_HUMAN,
    },
    ApplicationState.APPROVED_PENDING_HUMAN: {ApplicationState.FINAL_APPROVED},
    ApplicationState.DECLINED_PENDING_HUMAN: {ApplicationState.FINAL_DECLINED},
    ApplicationState.FINAL_APPROVED: set(),  # Terminal state
    ApplicationState.FINAL_DECLINED: set(),  # Terminal state
}


# =============================================================================
# PART B — Aggregate Class
# =============================================================================


class LoanApplicationAggregate:
    """
    Consistency boundary for a commercial loan application.

    State is reconstructed exclusively by replaying events from the event store.
    This is the ONLY way to get aggregate state — never read from a projection.
    (Manual p.5: "Command handlers must reconstruct aggregate state by replaying
    events." Reading from a projection in a command handler violates CQRS.)
    """

    def __init__(self, application_id: str):
        self.application_id = application_id
        self.version: int = 0
        self.state: ApplicationState | None = None
        self.applicant_id: str | None = None
        self.applicant_name: str | None = None  # Denormalised (Manual Pattern 1)
        self.requested_amount: Decimal | None = None
        self.approved_amount: Decimal | None = None
        self.risk_tier: str | None = None
        self.fraud_score: float | None = None
        self.compliance_required_checks: list[str] = []
        self.compliance_passed_checks: list[str] = []
        self.confidence_score: float | None = None
        self.recommendation: str | None = None
        self.contributing_sessions: list[str] = []
        self.credit_analysis_done: bool = False
        self.human_review_override: bool = False

    @classmethod
    async def load(
        cls,
        store: EventStore,
        application_id: str,
    ) -> LoanApplicationAggregate:
        """
        Replay event stream to reconstruct current aggregate state.

        This is the ONLY way to get aggregate state — never read from a projection.
        (Manual p.5: "Command handlers must reconstruct aggregate state by replaying
        events." Reading from a projection in a command handler violates CQRS.)
        """
        events = await store.load_application_events(application_id)
        agg = cls(application_id=application_id)
        for event in events:
            agg._apply(event)
        return agg

    # =========================================================================
    # Event Application (State Reconstruction)
    # =========================================================================

    def _apply(self, event: StoredEvent) -> None:
        """Dispatch to typed handler. Update version after every event."""
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        if event.stream_id == f"loan-{self.application_id}":
            self.version = event.stream_position

    def _on_ApplicationSubmitted(self, event: StoredEvent) -> None:
        """Initial state: application submitted."""
        self.state = ApplicationState.SUBMITTED
        self.applicant_id = event.payload["applicant_id"]
        self.applicant_name = event.payload.get("applicant_name")
        self.requested_amount = Decimal(str(event.payload["requested_amount_usd"]))

    def _on_CreditAnalysisRequested(self, event: StoredEvent) -> None:
        """Transition to awaiting analysis."""
        self._transition_to(ApplicationState.AWAITING_ANALYSIS)

    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        """Analysis received — store results and transition."""
        self._transition_to(ApplicationState.ANALYSIS_COMPLETE)
        self.risk_tier = event.payload.get("risk_tier")
        self.confidence_score = event.payload.get("confidence_score")
        self.credit_analysis_done = True

    def _on_FraudScreeningCompleted(self, event: StoredEvent) -> None:
        """Fraud screening result received."""
        self.fraud_score = event.payload.get("fraud_score")

    def _on_ComplianceCheckRequested(self, event: StoredEvent) -> None:
        """Compliance checks initiated — track required checks."""
        self._transition_to(ApplicationState.COMPLIANCE_REVIEW)
        self.compliance_required_checks = event.payload.get("checks_required", [])

    def _on_ComplianceRulePassed(self, event: StoredEvent) -> None:
        """A compliance rule has passed."""
        rule_id = event.payload.get("rule_id")
        if rule_id and rule_id not in self.compliance_passed_checks:
            self.compliance_passed_checks.append(rule_id)

    def _on_ComplianceClearanceIssued(self, event: StoredEvent) -> None:
        """All compliance checks passed — transition to pending decision."""
        self._transition_to(ApplicationState.PENDING_DECISION)

    def _on_DecisionGenerated(self, event: StoredEvent) -> None:
        """Decision by orchestrator agent — transition to pending human review."""
        recommendation = event.payload.get("recommendation", "")
        self.recommendation = recommendation
        self.confidence_score = event.payload.get("confidence_score")
        self.contributing_sessions = event.payload.get(
            "contributing_agent_sessions", []
        )

        match recommendation:
            case "APPROVE":
                self._transition_to(ApplicationState.APPROVED_PENDING_HUMAN)
            case "DECLINE":
                self._transition_to(ApplicationState.DECLINED_PENDING_HUMAN)
            case "REFER":
                # REFER goes to DECLINED_PENDING_HUMAN for human review
                self._transition_to(ApplicationState.DECLINED_PENDING_HUMAN)
            case _:
                raise DomainError(f"Unknown recommendation: {recommendation}")

    def _on_HumanReviewCompleted(self, event: StoredEvent) -> None:
        """Human reviewer has made final decision."""
        if event.payload.get("override", False):
            self.human_review_override = True
        final_decision = event.payload.get("final_decision")
        if final_decision == "APPROVE":
            self.state = ApplicationState.FINAL_APPROVED
        elif final_decision == "DECLINE":
            self.state = ApplicationState.FINAL_DECLINED

    def _on_ApplicationApproved(self, event: StoredEvent) -> None:
        """Application approved — terminal state."""
        self._transition_to(ApplicationState.FINAL_APPROVED)
        self.approved_amount = Decimal(str(event.payload["approved_amount_usd"]))

    def _on_ApplicationDeclined(self, event: StoredEvent) -> None:
        """Application declined — terminal state."""
        self._transition_to(ApplicationState.FINAL_DECLINED)

    def _on_ApplicationUnderReview(self, event: StoredEvent) -> None:
        """Application placed under human review."""
        # This event is informational within a pending state — no state transition
        pass

    # =========================================================================
    # PART C — Business Rule Assertions
    # =========================================================================

    def _transition_to(self, new_state: ApplicationState) -> None:
        """
        Enforce state machine. Raises InvalidStateTransitionError if invalid.
        Uses match/case for clear dispatch (Python 3.12+).
        (Challenge Doc p.10, Business Rule 1)
        """
        if self.state is None:
            raise DomainError("Cannot transition: aggregate has no state yet")
        if new_state not in VALID_TRANSITIONS[self.state]:
            raise InvalidStateTransitionError(
                from_state=self.state,
                to_state=new_state,
                valid_next=sorted(str(s) for s in VALID_TRANSITIONS[self.state]),
            )
        self.state = new_state

    def assert_awaiting_credit_analysis(self) -> None:
        """
        Rule 1: Must be in AWAITING_ANALYSIS state to accept credit analysis.
        """
        if self.state != ApplicationState.AWAITING_ANALYSIS:
            raise DomainError(
                f"Application {self.application_id} is in state {self.state}, "
                f"expected {ApplicationState.AWAITING_ANALYSIS} for credit analysis."
            )

    def assert_credit_analysis_not_done(self) -> None:
        """
        Rule 3: Model version locking — no second CreditAnalysisCompleted
        unless superseded by HumanReviewOverride.

        "Once a CreditAnalysisCompleted event is appended for an application,
        no further CreditAnalysisCompleted events may be appended for the same
        application unless the first was superseded by a HumanReviewOverride."
        (Challenge Doc p.10)
        """
        if self.credit_analysis_done and not self.human_review_override:
            raise DomainError(
                f"Application {self.application_id}: CreditAnalysisCompleted already "
                f"recorded. Cannot submit another without HumanReviewOverride."
            )

    def assert_pending_decision(self) -> None:
        """Decision generation requires compliance clearance first."""
        if self.state != ApplicationState.PENDING_DECISION:
            raise DomainError(
                f"Application {self.application_id} is in state {self.state}, "
                f"expected {ApplicationState.PENDING_DECISION} for decision generation."
            )

    def assert_approved_pending_human(self) -> None:
        """Formal approval requires an approval recommendation awaiting review."""
        if self.state != ApplicationState.APPROVED_PENDING_HUMAN:
            raise DomainError(
                f"Application {self.application_id} is in state {self.state}, "
                f"expected {ApplicationState.APPROVED_PENDING_HUMAN} for approval."
            )

    def assert_pending_human_review(self) -> None:
        """Human review is only valid after a machine recommendation exists."""
        pending_states = {
            ApplicationState.APPROVED_PENDING_HUMAN,
            ApplicationState.DECLINED_PENDING_HUMAN,
        }
        if self.state not in pending_states:
            raise DomainError(
                f"Application {self.application_id} is in state {self.state}, "
                f"expected one of {pending_states} for human review."
            )

    def assert_compliance_complete(
        self,
        required_checks: list[str],
        passed_checks: list[str],
        clearance_issued: bool,
    ) -> None:
        """
        Rule 5: All required compliance checks must be passed before approval.
        Checks: set(compliance_required_checks) ⊆ set(compliance_passed_checks)

        "An ApplicationApproved event cannot be appended unless all
        ComplianceRulePassed events for the application's required checks
        are present in the ComplianceRecord stream."
        (Challenge Doc p.10)
        """
        required = set(required_checks)
        passed = set(passed_checks)
        missing = required - passed
        if not clearance_issued:
            raise DomainError(
                f"Application {self.application_id}: compliance clearance "
                "has not been issued."
            )
        if missing:
            raise DomainError(
                f"Application {self.application_id}: compliance checks incomplete. "
                f"Missing: {sorted(missing)}"
            )

    def assert_contributing_sessions_valid(
        self,
        session_ids: list[str],
        application_id: str,
        valid_session_ids: set[str],
    ) -> None:
        """
        Rule 6: All contributing_agent_sessions must reference sessions that
        contain a decision event for this application_id.

        "An orchestrator that references sessions that never processed this
        application must be rejected."
        (Challenge Doc p.10)
        """
        if not session_ids:
            raise DomainError(
                f"Application {application_id}: DecisionGenerated must reference "
                f"at least one contributing agent session."
            )
        invalid = sorted(
            session_id
            for session_id in session_ids
            if session_id not in valid_session_ids
        )
        if invalid:
            raise DomainError(
                f"Application {application_id}: contributing sessions do not "
                f"contain decisions for this application: {invalid}"
            )

    @staticmethod
    def enforce_confidence_floor(
        confidence_score: float,
        recommendation: str,
    ) -> str:
        """
        Rule 4: confidence_score < 0.6 forces recommendation = 'REFER'.
        This is a regulatory requirement enforced in aggregate, not API layer.
        (Challenge Doc p.10: "enforced in the aggregate")

        Returns the (possibly overridden) recommendation.
        """
        if confidence_score < 0.6:
            return Recommendation.REFER
        return recommendation
