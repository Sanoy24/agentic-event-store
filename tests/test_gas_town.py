from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest

from src.integrity.gas_town import SessionHealthStatus, reconstruct_agent_context
from src.models.events import (
    AgentContextLoaded,
    AgentInputValidated,
    AgentOutputWritten,
    AgentToolCalled,
    CreditAnalysisCompleted,
    RiskTier,
)


@pytest.mark.asyncio
async def test_empty_session_returns_empty_context(event_store) -> None:
    context = await reconstruct_agent_context(
        store=event_store,
        agent_id="agent-nonexistent",
        session_id="sess-nonexistent",
    )

    assert context.session_health_status == SessionHealthStatus.EMPTY
    assert context.total_events == 0
    assert context.last_event_position == 0
    assert context.pending_work == []


@pytest.mark.asyncio
async def test_context_loaded_only_is_healthy(event_store) -> None:
    agent_id = f"agent-{uuid4().hex[:8]}"
    session_id = f"sess-{uuid4().hex[:8]}"
    stream_id = f"agent-{agent_id}-{session_id}"

    await event_store.append(
        stream_id=stream_id,
        events=[
            AgentContextLoaded(
                agent_id=agent_id,
                session_id=session_id,
                context_source="event_store",
                event_replay_from_position=0,
                context_token_count=5000,
                model_version="credit-model-v2.4.1",
            ),
        ],
        expected_version=-1,
        correlation_id="test-gas-town",
    )

    context = await reconstruct_agent_context(
        store=event_store,
        agent_id=agent_id,
        session_id=session_id,
    )

    assert context.session_health_status == SessionHealthStatus.HEALTHY
    assert context.total_events == 1
    assert context.model_version == "credit-model-v2.4.1"
    assert context.context_source == "event_store"
    assert context.completed_applications == []


@pytest.mark.asyncio
async def test_crash_recovery_with_five_events(event_store) -> None:
    """
    Required Gas Town crash-recovery scenario: reconstruct after five events.
    """
    agent_id = f"agent-{uuid4().hex[:8]}"
    session_id = f"sess-{uuid4().hex[:8]}"
    stream_id = f"agent-{agent_id}-{session_id}"

    await event_store.append(
        stream_id=stream_id,
        events=[
            AgentContextLoaded(
                agent_id=agent_id,
                session_id=session_id,
                context_source="event_store",
                event_replay_from_position=0,
                context_token_count=5000,
                model_version="credit-model-v2.4.1",
            ),
            AgentInputValidated(
                agent_id=agent_id,
                session_id=session_id,
                inputs_validated=["application_id", "financials"],
                validation_duration_ms=42,
            ),
            AgentToolCalled(
                agent_id=agent_id,
                session_id=session_id,
                tool_name="load_financials",
                tool_input_summary="application_id=loan-crash-001",
                tool_output_summary="financial profile loaded",
                tool_duration_ms=17,
            ),
            AgentOutputWritten(
                agent_id=agent_id,
                session_id=session_id,
                events_written=[{"kind": "draft-analysis"}],
                output_summary="draft analysis prepared",
            ),
            CreditAnalysisCompleted(
                application_id="loan-crash-001",
                agent_id=agent_id,
                session_id=session_id,
                model_version="credit-model-v2.4.1",
                model_deployment_id="deploy-001",
                confidence_score=0.92,
                risk_tier=RiskTier.LOW,
                recommended_limit_usd=Decimal("500000"),
                analysis_duration_ms=1500,
                input_data_hash="hash-crash-001",
            ),
        ],
        expected_version=-1,
        correlation_id="test-crash",
    )

    context = await reconstruct_agent_context(
        store=event_store,
        agent_id=agent_id,
        session_id=session_id,
    )

    assert context.session_health_status == SessionHealthStatus.HEALTHY
    assert context.total_events == 5
    assert context.model_version == "credit-model-v2.4.1"
    assert "loan-crash-001" in context.completed_applications
    assert context.last_event_position == 5


@pytest.mark.asyncio
async def test_partial_last_event_needs_reconciliation(event_store) -> None:
    agent_id = f"agent-{uuid4().hex[:8]}"
    session_id = f"sess-{uuid4().hex[:8]}"
    stream_id = f"agent-{agent_id}-{session_id}"

    await event_store.append(
        stream_id=stream_id,
        events=[
            AgentContextLoaded(
                agent_id=agent_id,
                session_id=session_id,
                context_source="event_store",
                event_replay_from_position=0,
                context_token_count=5000,
                model_version="credit-model-v2.4.1",
            ),
            AgentToolCalled(
                agent_id=agent_id,
                session_id=session_id,
                tool_name="screen-document",
                tool_input_summary="application_id=loan-pending-001",
                tool_output_summary="awaiting downstream write",
                tool_duration_ms=25,
            ),
        ],
        expected_version=-1,
        correlation_id="test-pending",
    )

    context = await reconstruct_agent_context(
        store=event_store,
        agent_id=agent_id,
        session_id=session_id,
    )

    assert context.session_health_status == SessionHealthStatus.NEEDS_RECONCILIATION
    assert len(context.pending_work) == 1
    assert context.pending_work[0].event_type == "AgentToolCalled"


@pytest.mark.asyncio
async def test_no_context_loaded_returns_no_context(event_store) -> None:
    agent_id = f"agent-{uuid4().hex[:8]}"
    session_id = f"sess-{uuid4().hex[:8]}"
    stream_id = f"agent-{agent_id}-{session_id}"

    await event_store.append(
        stream_id=stream_id,
        events=[
            CreditAnalysisCompleted(
                application_id="loan-violation-001",
                agent_id=agent_id,
                session_id=session_id,
                model_version="unknown",
                model_deployment_id="unknown",
                confidence_score=0.5,
                risk_tier=RiskTier.MEDIUM,
                recommended_limit_usd=Decimal("100000"),
                analysis_duration_ms=500,
                input_data_hash="hash-violation",
            ),
        ],
        expected_version=-1,
        correlation_id="test-violation",
    )

    context = await reconstruct_agent_context(
        store=event_store,
        agent_id=agent_id,
        session_id=session_id,
    )

    assert context.session_health_status == SessionHealthStatus.NO_CONTEXT
