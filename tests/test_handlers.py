from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.aggregates.loan_application import ApplicationState, LoanApplicationAggregate
from src.commands.handlers import (
    ApproveApplicationCommand,
    CreditAnalysisCompletedCommand,
    GenerateDecisionCommand,
    StartAgentSessionCommand,
    handle_application_approved,
    handle_credit_analysis_completed,
    handle_generate_decision,
    handle_start_agent_session,
)
from src.event_store import EventStore
from src.models.events import (
    ApplicationSubmitted,
    ComplianceCheckRequested,
    ComplianceClearanceIssued,
    ComplianceRulePassed,
    CreditAnalysisRequested,
    DecisionGenerated,
    DomainError,
    LoanPurpose,
    Recommendation,
    RiskTier,
)


async def seed_application_awaiting_analysis(
    store: EventStore,
    application_id: str,
) -> None:
    await store.append(
        stream_id=f"loan-{application_id}",
        events=[
            ApplicationSubmitted(
                application_id=application_id,
                applicant_id="applicant-001",
                applicant_name="Apex Capital Ltd",
                requested_amount_usd=Decimal("100000.00"),
                loan_purpose=LoanPurpose.EXPANSION,
                submission_channel="api",
                submitted_at=datetime.now(timezone.utc),
            ),
        ],
        expected_version=-1,
        correlation_id=f"corr-submit-{application_id}",
    )
    await store.append(
        stream_id=f"loan-{application_id}",
        events=[
            CreditAnalysisRequested(
                application_id=application_id,
                assigned_agent_id="agent-credit-001",
                requested_at=datetime.now(timezone.utc),
                priority="HIGH",
            ),
        ],
        expected_version=1,
        correlation_id=f"corr-request-{application_id}",
    )


async def seed_compliance_stream(
    store: EventStore,
    application_id: str,
    *,
    passed_checks: list[str],
    issue_clearance: bool,
) -> None:
    await store.append(
        stream_id=f"compliance-{application_id}",
        events=[
            ComplianceCheckRequested(
                application_id=application_id,
                regulation_set_version="2026-Q1",
                checks_required=["KYC"],
            ),
        ],
        expected_version=-1,
        correlation_id=f"corr-comp-req-{application_id}",
    )

    version = 1
    for rule_id in passed_checks:
        version = await store.append(
            stream_id=f"compliance-{application_id}",
            events=[
                ComplianceRulePassed(
                    application_id=application_id,
                    rule_id=rule_id,
                    rule_version="v1",
                    evaluation_timestamp=datetime.now(timezone.utc),
                    evidence_hash=f"hash-{rule_id}",
                ),
            ],
            expected_version=version,
            correlation_id=f"corr-comp-pass-{application_id}-{rule_id}",
        )

    if issue_clearance:
        await store.append(
            stream_id=f"compliance-{application_id}",
            events=[
                ComplianceClearanceIssued(
                    application_id=application_id,
                    regulation_set_version="2026-Q1",
                    checks_passed=passed_checks,
                    clearance_timestamp=datetime.now(timezone.utc),
                    issued_by="compliance-agent-01",
                ),
            ],
            expected_version=version,
            correlation_id=f"corr-comp-clear-{application_id}",
        )


@pytest.mark.asyncio
async def test_credit_analysis_written_to_agent_session_stream(
    event_store: EventStore,
) -> None:
    application_id = str(uuid.uuid4())
    await seed_application_awaiting_analysis(event_store, application_id)

    await handle_start_agent_session(
        StartAgentSessionCommand(
            agent_id="credit-agent-01",
            session_id="session-01",
            context_source="event-replay",
            context_token_count=512,
            model_version="credit-model-v2.2",
            correlation_id="corr-session-start",
        ),
        event_store,
    )

    await handle_credit_analysis_completed(
        CreditAnalysisCompletedCommand(
            application_id=application_id,
            agent_id="credit-agent-01",
            session_id="session-01",
            model_version="credit-model-v2.2",
            model_deployment_id="deploy-01",
            confidence_score=0.91,
            risk_tier=RiskTier.LOW,
            recommended_limit_usd=Decimal("85000.00"),
            duration_ms=850,
            input_data={"cashflow": "healthy"},
            correlation_id="corr-analysis",
        ),
        event_store,
    )

    loan_events = await event_store.load_stream(f"loan-{application_id}")
    assert [event.event_type for event in loan_events] == [
        "ApplicationSubmitted",
        "CreditAnalysisRequested",
    ]

    agent_events = await event_store.load_stream("agent-credit-agent-01-session-01")
    assert [event.event_type for event in agent_events] == [
        "AgentContextLoaded",
        "CreditAnalysisCompleted",
    ]

    app = await LoanApplicationAggregate.load(event_store, application_id)
    assert app.state == ApplicationState.ANALYSIS_COMPLETE


@pytest.mark.asyncio
async def test_generate_decision_rejects_invalid_contributing_sessions(
    event_store: EventStore,
) -> None:
    application_id = str(uuid.uuid4())
    await seed_application_awaiting_analysis(event_store, application_id)

    await handle_start_agent_session(
        StartAgentSessionCommand(
            agent_id="credit-agent-02",
            session_id="session-02",
            context_source="event-replay",
            context_token_count=256,
            model_version="credit-model-v2.2",
            correlation_id="corr-session-start-2",
        ),
        event_store,
    )

    await handle_credit_analysis_completed(
        CreditAnalysisCompletedCommand(
            application_id=application_id,
            agent_id="credit-agent-02",
            session_id="session-02",
            model_version="credit-model-v2.2",
            model_deployment_id="deploy-02",
            confidence_score=0.88,
            risk_tier=RiskTier.MEDIUM,
            recommended_limit_usd=Decimal("60000.00"),
            duration_ms=900,
            input_data={"cashflow": "stable"},
            correlation_id="corr-analysis-2",
        ),
        event_store,
    )

    await seed_compliance_stream(
        event_store,
        application_id,
        passed_checks=["KYC"],
        issue_clearance=True,
    )

    with pytest.raises(DomainError, match="contributing sessions"):
        await handle_generate_decision(
            GenerateDecisionCommand(
                application_id=application_id,
                orchestrator_agent_id="orchestrator-01",
                recommendation=Recommendation.APPROVE,
                confidence_score=0.82,
                contributing_agent_sessions=["agent-missing-session"],
                decision_basis_summary="All checks passed",
                model_versions={"orchestrator": "orch-v1"},
                correlation_id="corr-decision",
            ),
            event_store,
        )


@pytest.mark.asyncio
async def test_application_approval_checks_compliance_record_stream(
    event_store: EventStore,
) -> None:
    application_id = str(uuid.uuid4())
    await seed_application_awaiting_analysis(event_store, application_id)

    await handle_start_agent_session(
        StartAgentSessionCommand(
            agent_id="credit-agent-03",
            session_id="session-03",
            context_source="event-replay",
            context_token_count=384,
            model_version="credit-model-v2.2",
            correlation_id="corr-session-start-3",
        ),
        event_store,
    )

    await handle_credit_analysis_completed(
        CreditAnalysisCompletedCommand(
            application_id=application_id,
            agent_id="credit-agent-03",
            session_id="session-03",
            model_version="credit-model-v2.2",
            model_deployment_id="deploy-03",
            confidence_score=0.87,
            risk_tier=RiskTier.MEDIUM,
            recommended_limit_usd=Decimal("70000.00"),
            duration_ms=910,
            input_data={"cashflow": "acceptable"},
            correlation_id="corr-analysis-3",
        ),
        event_store,
    )

    await seed_compliance_stream(
        event_store,
        application_id,
        passed_checks=[],
        issue_clearance=True,
    )

    await event_store.append(
        stream_id=f"loan-{application_id}",
        events=[
            DecisionGenerated(
                application_id=application_id,
                orchestrator_agent_id="orchestrator-02",
                recommendation=Recommendation.APPROVE,
                confidence_score=0.9,
                contributing_agent_sessions=["agent-credit-agent-03-session-03"],
                decision_basis_summary="Approved pending human review",
                model_versions={"orchestrator": "orch-v1"},
            ),
        ],
        expected_version=2,
        correlation_id="corr-direct-decision",
    )

    with pytest.raises(DomainError, match="compliance"):
        await handle_application_approved(
            ApproveApplicationCommand(
                application_id=application_id,
                approved_amount_usd=Decimal("65000.00"),
                interest_rate=Decimal("6.5"),
                approved_by="loan-officer-01",
                correlation_id="corr-approve",
            ),
            event_store,
        )
