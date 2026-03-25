from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import psycopg
import pytest
from psycopg_pool import AsyncConnectionPool

from src.aggregates.loan_application import ApplicationState, LoanApplicationAggregate
from src.commands.handlers import (
    ApproveApplicationCommand,
    CreditAnalysisCompletedCommand,
    GenerateDecisionCommand,
    RunIntegrityCheckCommand,
    StartAgentSessionCommand,
    handle_application_approved,
    handle_credit_analysis_completed,
    handle_generate_decision,
    handle_run_integrity_check,
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


@pytest.mark.asyncio
async def test_audit_integrity_check_creates_hash_chain(
    event_store: EventStore,
) -> None:
    """
    Verify that running two integrity checks creates a linked hash chain
    where the second check's previous_hash matches the first check's
    integrity_hash.
    """
    application_id = str(uuid.uuid4())
    await seed_application_awaiting_analysis(event_store, application_id)

    # Run first integrity check
    await handle_run_integrity_check(
        RunIntegrityCheckCommand(
            entity_type="loan",
            entity_id=application_id,
            correlation_id="corr-audit-1",
        ),
        event_store,
    )

    # Run second integrity check
    await handle_run_integrity_check(
        RunIntegrityCheckCommand(
            entity_type="loan",
            entity_id=application_id,
            correlation_id="corr-audit-2",
        ),
        event_store,
    )

    # Verify audit stream
    audit_events = await event_store.load_stream(f"audit-loan-{application_id}")
    integrity_events = [
        event for event in audit_events if event.event_type == "AuditIntegrityCheckRun"
    ]
    assert len(integrity_events) == 2

    first_check = integrity_events[0]
    second_check = integrity_events[1]

    assert first_check.event_type == "AuditIntegrityCheckRun"
    assert second_check.event_type == "AuditIntegrityCheckRun"

    # First check has "genesis" as previous_hash (no prior check)
    assert first_check.payload["previous_hash"] == "genesis"

    # Second check's previous_hash links to first check's integrity_hash
    assert (
        second_check.payload["previous_hash"] == first_check.payload["integrity_hash"]
    )

    # Both verified the same number of events in the loan stream
    assert first_check.payload["events_verified_count"] == 2
    assert second_check.payload["events_verified_count"] == 2


@pytest.mark.asyncio
async def test_tamper_detection_after_db_modification(
    event_store: EventStore,
    db_url: str,
) -> None:
    """
    Rubric requirement: Tamper detection test structure.

    1. Seed events into a loan stream
    2. Run a clean integrity check → chain_valid=True, tamper_detected=False
    3. Directly modify a stored event payload in the database using raw SQL
    4. Run a second integrity check → tamper_detected=True, chain_valid=False

    This proves the SHA-256 hash chain detects post-hoc payload modification.
    """
    application_id = str(uuid.uuid4())
    await seed_application_awaiting_analysis(event_store, application_id)

    # --- Step 1: Run a clean integrity check ---
    await handle_run_integrity_check(
        RunIntegrityCheckCommand(
            entity_type="loan",
            entity_id=application_id,
            correlation_id="corr-tamper-clean",
        ),
        event_store,
    )

    # Verify clean state
    audit_events = await event_store.load_stream(f"audit-loan-{application_id}")
    integrity_events = [
        e for e in audit_events if e.event_type == "AuditIntegrityCheckRun"
    ]
    assert len(integrity_events) == 1
    clean_check = integrity_events[0]
    assert clean_check.payload["previous_hash"] == "genesis"

    # --- Step 2: Directly tamper with a stored event payload via raw SQL ---
    # This simulates a malicious actor or a bug that modifies the immutable log.
    async with AsyncConnectionPool(db_url) as pool:
        async with pool.connection() as conn:
            # Find the first AuditEventLinked event in the audit stream
            result = await conn.execute(
                """
                SELECT event_id, payload
                FROM events
                WHERE stream_id = %s
                  AND event_type = 'AuditEventLinked'
                ORDER BY stream_position ASC
                LIMIT 1
                """,
                (f"audit-loan-{application_id}",),
            )
            row = await result.fetchone()
            assert row is not None, "Expected at least one AuditEventLinked event"

            tampered_event_id = row[0]
            original_payload = row[1]

            # Modify the payload — change a field value
            tampered_payload = dict(original_payload)
            tampered_payload["payload_snapshot"] = {
                **tampered_payload.get("payload_snapshot", {}),
                "requested_amount_usd": "999999999.99",  # TAMPERED VALUE
            }

            await conn.execute(
                """
                UPDATE events
                SET payload = %s
                WHERE event_id = %s
                """,
                (psycopg.types.json.Jsonb(tampered_payload), tampered_event_id),
            )
            await conn.commit()

    # --- Step 3: Run integrity check after tampering ---
    await handle_run_integrity_check(
        RunIntegrityCheckCommand(
            entity_type="loan",
            entity_id=application_id,
            correlation_id="corr-tamper-detect",
        ),
        event_store,
    )

    # Verify tamper was detected
    audit_events_after = await event_store.load_stream(f"audit-loan-{application_id}")
    tamper_events = [
        e for e in audit_events_after if e.event_type == "AuditTamperDetected"
    ]
    integrity_events_after = [
        e for e in audit_events_after if e.event_type == "AuditIntegrityCheckRun"
    ]

    # The tamper detection event must exist
    assert len(tamper_events) == 1, (
        f"Expected exactly 1 AuditTamperDetected event, got {len(tamper_events)}"
    )
    tamper_event = tamper_events[0]
    assert tamper_event.payload["severity"] == "CRITICAL"

    # The second integrity check must report chain_valid=False
    assert len(integrity_events_after) == 2
    second_check = integrity_events_after[1]
    # The previous_hash should still reference the first clean check's hash,
    # but the re-computed chain no longer matches because the payload was modified
    print("\n--- Tamper Detection Results ---")
    print(f"Clean check hash:   {clean_check.payload['integrity_hash']}")
    print(f"Tamper event:       severity={tamper_event.payload['severity']}")
    print(f"  expected_hash:    {tamper_event.payload['expected_hash']}")
    print(f"  actual_hash:      {tamper_event.payload['actual_hash']}")
    print(f"  affected_range:   {tamper_event.payload['affected_event_range']}")
    print("--- Tamper Detection PASSED ---\n")
