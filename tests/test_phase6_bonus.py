from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from src.event_store import EventStore
from src.models.events import (
    AgentContextLoaded,
    ApplicationSubmitted,
    ComplianceCheckRequested,
    ComplianceClearanceIssued,
    ComplianceRulePassed,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    DecisionGenerated,
    FraudScreeningCompleted,
    HumanReviewCompleted,
    LoanPurpose,
    Recommendation,
    RiskTier,
)
from src.regulatory.package import generate_regulatory_package
from src.what_if.projector import WhatIfScenario, run_what_if


async def _append_and_get_latest(
    store: EventStore,
    *,
    stream_id: str,
    events: list,
    expected_version: int,
    correlation_id: str,
):
    await store.append(
        stream_id=stream_id,
        events=events,
        expected_version=expected_version,
        correlation_id=correlation_id,
    )
    return (await store.load_stream(stream_id))[-1]


async def _seed_full_lifecycle(store: EventStore) -> dict[str, str | datetime]:
    application_id = f"app-{uuid4().hex[:8]}"
    credit_agent_id = "credit-agent-01"
    credit_session_id = "session-alpha-01"
    fraud_agent_id = "fraud-agent-02"
    fraud_session_id = "session-beta-02"

    await _append_and_get_latest(
        store,
        stream_id=f"loan-{application_id}",
        events=[
            ApplicationSubmitted(
                application_id=application_id,
                applicant_id="borrower-001",
                applicant_name="Northwind Manufacturing",
                requested_amount_usd=Decimal("250000"),
                loan_purpose=LoanPurpose.WORKING_CAPITAL,
                submission_channel="agent",
                submitted_at=datetime.now(timezone.utc),
            )
        ],
        expected_version=-1,
        correlation_id=f"submit-{application_id}",
    )
    await asyncio.sleep(0.02)

    await _append_and_get_latest(
        store,
        stream_id=f"loan-{application_id}",
        events=[
            CreditAnalysisRequested(
                application_id=application_id,
                assigned_agent_id=credit_agent_id,
                requested_at=datetime.now(timezone.utc),
                priority="HIGH",
            )
        ],
        expected_version=1,
        correlation_id=f"credit-request-{application_id}",
    )
    await asyncio.sleep(0.02)

    await _append_and_get_latest(
        store,
        stream_id=f"agent-{credit_agent_id}-{credit_session_id}",
        events=[
            AgentContextLoaded(
                agent_id=credit_agent_id,
                session_id=credit_session_id,
                context_source="event_store",
                event_replay_from_position=0,
                context_token_count=4096,
                model_version="credit-model-v3.1",
            )
        ],
        expected_version=-1,
        correlation_id=f"credit-session-{application_id}",
    )
    await asyncio.sleep(0.02)

    credit_completed = await _append_and_get_latest(
        store,
        stream_id=f"agent-{credit_agent_id}-{credit_session_id}",
        events=[
            CreditAnalysisCompleted(
                application_id=application_id,
                agent_id=credit_agent_id,
                session_id=credit_session_id,
                model_version="credit-model-v3.1",
                model_deployment_id="deploy-credit-v3.1",
                confidence_score=0.82,
                risk_tier=RiskTier.MEDIUM,
                recommended_limit_usd=Decimal("200000"),
                analysis_duration_ms=850,
                input_data_hash="hash-credit-medium",
            )
        ],
        expected_version=1,
        correlation_id=f"credit-complete-{application_id}",
    )
    await asyncio.sleep(0.02)

    await _append_and_get_latest(
        store,
        stream_id=f"agent-{fraud_agent_id}-{fraud_session_id}",
        events=[
            AgentContextLoaded(
                agent_id=fraud_agent_id,
                session_id=fraud_session_id,
                context_source="event_store",
                event_replay_from_position=0,
                context_token_count=2048,
                model_version="fraud-model-v2.4",
            )
        ],
        expected_version=-1,
        correlation_id=f"fraud-session-{application_id}",
    )
    await asyncio.sleep(0.02)

    await _append_and_get_latest(
        store,
        stream_id=f"agent-{fraud_agent_id}-{fraud_session_id}",
        events=[
            FraudScreeningCompleted(
                application_id=application_id,
                agent_id=fraud_agent_id,
                fraud_score=0.08,
                anomaly_flags=[],
                screening_model_version="fraud-model-v2.4",
                model_deployment_id="deploy-fraud-v2.4",
                input_data_hash="hash-fraud-low",
            )
        ],
        expected_version=1,
        correlation_id=f"fraud-complete-{application_id}",
    )
    await asyncio.sleep(0.02)

    await _append_and_get_latest(
        store,
        stream_id=f"compliance-{application_id}",
        events=[
            ComplianceCheckRequested(
                application_id=application_id,
                regulation_set_version="Basel-III-2026-Q1",
                checks_required=["KYC", "AML"],
            )
        ],
        expected_version=-1,
        correlation_id=f"compliance-request-{application_id}",
    )
    await asyncio.sleep(0.02)

    await _append_and_get_latest(
        store,
        stream_id=f"compliance-{application_id}",
        events=[
            ComplianceRulePassed(
                application_id=application_id,
                rule_id="KYC",
                rule_version="kyc-v1.2",
                evaluation_timestamp=datetime.now(timezone.utc),
                evidence_hash="hash-kyc",
            )
        ],
        expected_version=1,
        correlation_id=f"kyc-pass-{application_id}",
    )
    await asyncio.sleep(0.02)

    await _append_and_get_latest(
        store,
        stream_id=f"compliance-{application_id}",
        events=[
            ComplianceRulePassed(
                application_id=application_id,
                rule_id="AML",
                rule_version="aml-v3.0",
                evaluation_timestamp=datetime.now(timezone.utc),
                evidence_hash="hash-aml",
            )
        ],
        expected_version=2,
        correlation_id=f"aml-pass-{application_id}",
    )
    await asyncio.sleep(0.02)

    await _append_and_get_latest(
        store,
        stream_id=f"compliance-{application_id}",
        events=[
            ComplianceClearanceIssued(
                application_id=application_id,
                regulation_set_version="Basel-III-2026-Q1",
                checks_passed=["KYC", "AML"],
                clearance_timestamp=datetime.now(timezone.utc),
                issued_by="compliance-agent-01",
            )
        ],
        expected_version=3,
        correlation_id=f"clearance-{application_id}",
    )
    await asyncio.sleep(0.02)

    decision_event = await _append_and_get_latest(
        store,
        stream_id=f"loan-{application_id}",
        events=[
            DecisionGenerated(
                application_id=application_id,
                orchestrator_agent_id="orchestrator-01",
                recommendation=Recommendation.APPROVE,
                confidence_score=0.78,
                contributing_agent_sessions=[
                    f"agent-{credit_agent_id}-{credit_session_id}",
                    f"agent-{fraud_agent_id}-{fraud_session_id}",
                ],
                decision_basis_summary="Credit, fraud, and compliance checks passed.",
                model_versions={
                    "orchestrator": "decision-model-v1",
                    "credit": "credit-model-v3.1",
                    "fraud": "fraud-model-v2.4",
                },
            )
        ],
        expected_version=2,
        correlation_id=f"decision-{application_id}",
        # causation chain to the credit result so the what-if projector can prune it
    )
    await asyncio.sleep(0.02)

    await store.append(
        stream_id=f"loan-{application_id}",
        events=[
            HumanReviewCompleted(
                application_id=application_id,
                reviewer_id="loan-officer-01",
                override=False,
                final_decision="APPROVE",
            )
        ],
        expected_version=3,
        correlation_id=f"review-{application_id}",
        causation_id=str(decision_event.event_id),
    )

    return {
        "application_id": application_id,
        "credit_agent_id": credit_agent_id,
        "credit_session_id": credit_session_id,
        "fraud_agent_id": fraud_agent_id,
        "fraud_session_id": fraud_session_id,
        "credit_stream_id": f"agent-{credit_agent_id}-{credit_session_id}",
        "decision_recorded_at": decision_event.recorded_at,
    }


@pytest.mark.asyncio
async def test_generate_regulatory_package_returns_projection_states_as_of_examination_date(
    event_store: EventStore,
) -> None:
    seeded = await _seed_full_lifecycle(event_store)
    application_id = seeded["application_id"]
    audit_before = await event_store.load_stream(f"audit-loan-{application_id}")

    package = await generate_regulatory_package(
        event_store,
        application_id,
        seeded["decision_recorded_at"],
    )

    assert package.application_id == application_id
    assert (
        package.projection_states["application_summary"]["state"]
        == "APPROVED_PENDING_HUMAN"
    )
    assert package.projection_states["application_summary"]["final_decision"] is None
    assert (
        package.projection_states["application_summary"]["applicant_id"]
        == "borrower-001"
    )
    assert (
        package.projection_states["compliance_audit_view"]["compliance_status"]
        == "CLEARED"
    )
    assert package.projection_states["compliance_audit_view"]["events_processed"] == 4

    credit_session = next(
        session
        for session in package.agent_sessions
        if session["stream_id"] == seeded["credit_stream_id"]
    )
    assert credit_session["agent_id"] == seeded["credit_agent_id"]
    assert credit_session["session_id"] == seeded["credit_session_id"]

    orchestrator_row = next(
        row
        for row in package.projection_states["agent_performance_ledger"]
        if row["agent_id"] == "orchestrator-01"
    )
    assert orchestrator_row["decisions_generated"] == 1
    assert package.package_hash

    audit_after = await event_store.load_stream(f"audit-loan-{application_id}")
    assert len(audit_after) == len(audit_before)


@pytest.mark.asyncio
async def test_run_what_if_high_risk_invalidates_downstream_decision_path(
    event_store: EventStore,
) -> None:
    seeded = await _seed_full_lifecycle(event_store)
    application_id = seeded["application_id"]

    result = await run_what_if(
        event_store,
        WhatIfScenario(
            name="high-risk-credit-counterfactual",
            description="Replace the medium-risk credit result with a high-risk result.",
            stream_id=seeded["credit_stream_id"],
            inject_at_position=1,
            hypothetical_events=[
                CreditAnalysisCompleted(
                    application_id=application_id,
                    agent_id=seeded["credit_agent_id"],
                    session_id=seeded["credit_session_id"],
                    model_version="credit-model-v3.1",
                    model_deployment_id="deploy-credit-v3.1",
                    confidence_score=0.82,
                    risk_tier=RiskTier.HIGH,
                    recommended_limit_usd=Decimal("120000"),
                    analysis_duration_ms=850,
                    input_data_hash="hash-credit-high",
                )
            ],
            remove_event_types=["CreditAnalysisCompleted"],
        ),
    )

    original_summary = result.state_differences["original_application_summary"]
    counterfactual_summary = result.state_differences[
        "counterfactual_application_summary"
    ]

    assert original_summary["state"] == "FINAL_APPROVED"
    assert original_summary["risk_tier"] == "MEDIUM"
    assert counterfactual_summary["risk_tier"] == "HIGH"
    assert counterfactual_summary["state"] == "PENDING_DECISION"
    assert result.state_differences["decision_recomputation_required"] is True
    assert "DecisionGenerated" in result.state_differences["removed_events"]
    assert "HumanReviewCompleted" in result.state_differences["removed_events"]
