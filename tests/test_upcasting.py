from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from src.models.events import AgentContextLoaded, StoredEvent
from src.upcasting.registry import UpcasterRegistry
from src.upcasting.upcasters import registry, upcast_credit_v1_to_v2


def test_registry_register_and_upcast() -> None:
    reg = UpcasterRegistry()

    @reg.register("TestEvent", from_version=1)
    def v1_to_v2(payload: dict) -> dict:
        return {**payload, "new_field": "added_in_v2"}

    event = StoredEvent(
        event_id=uuid4(),
        stream_id="test-stream",
        stream_position=1,
        global_position=1,
        event_type="TestEvent",
        event_version=1,
        payload={"old_field": "value"},
        metadata={},
        recorded_at=datetime.now(timezone.utc),
    )

    upcasted = reg.upcast(event)
    assert upcasted.event_version == 2
    assert upcasted.payload["new_field"] == "added_in_v2"
    assert event.event_version == 1
    assert "new_field" not in event.payload


def test_registry_multi_step_chain() -> None:
    reg = UpcasterRegistry()

    @reg.register("ChainEvent", from_version=1)
    def v1_to_v2(payload: dict) -> dict:
        return {**payload, "v2_field": True}

    @reg.register("ChainEvent", from_version=2)
    def v2_to_v3(payload: dict) -> dict:
        return {**payload, "v3_field": "final"}

    event = StoredEvent(
        event_id=uuid4(),
        stream_id="test",
        stream_position=1,
        global_position=1,
        event_type="ChainEvent",
        event_version=1,
        payload={"original": "data"},
        metadata={},
        recorded_at=datetime.now(timezone.utc),
    )

    result = reg.upcast(event)
    assert result.event_version == 3
    assert result.payload["v2_field"] is True
    assert result.payload["v3_field"] == "final"


def test_credit_analysis_v1_to_v2_uses_recorded_at() -> None:
    payload = {"application_id": "loan-001"}
    event = StoredEvent(
        event_id=uuid4(),
        stream_id="agent-credit-session",
        stream_position=1,
        global_position=1,
        event_type="CreditAnalysisCompleted",
        event_version=1,
        payload=payload,
        metadata={},
        recorded_at=datetime(2025, 3, 15, 10, 0, tzinfo=timezone.utc),
    )

    upcasted = registry.upcast(event)
    assert upcasted.payload["model_version"] == "credit-model-v2.0"
    assert upcasted.payload["confidence_score"] is None
    assert upcasted.payload["model_deployment_id"] == "unknown-pre-v2"


@pytest.mark.asyncio
async def test_decision_upcaster_reconstructs_model_versions(event_store) -> None:
    event_store._upcasters = registry

    credit_agent_id = f"credit-agent-{uuid4().hex[:6]}"
    credit_session_id = f"sess-{uuid4().hex[:6]}"
    fraud_agent_id = f"fraud-agent-{uuid4().hex[:6]}"
    fraud_session_id = f"sess-{uuid4().hex[:6]}"
    credit_stream = f"agent-{credit_agent_id}-{credit_session_id}"
    fraud_stream = f"agent-{fraud_agent_id}-{fraud_session_id}"

    await event_store.append(
        credit_stream,
        [
            AgentContextLoaded(
                agent_id=credit_agent_id,
                session_id=credit_session_id,
                context_source="event_store",
                event_replay_from_position=0,
                context_token_count=400,
                model_version="credit-model-v2.9",
            )
        ],
        expected_version=-1,
        correlation_id="credit-session",
    )
    await event_store.append(
        fraud_stream,
        [
            AgentContextLoaded(
                agent_id=fraud_agent_id,
                session_id=fraud_session_id,
                context_source="event_store",
                event_replay_from_position=0,
                context_token_count=300,
                model_version="fraud-model-v1.8",
            )
        ],
        expected_version=-1,
        correlation_id="fraud-session",
    )

    legacy_event = StoredEvent(
        event_id=uuid4(),
        stream_id=f"loan-{uuid4().hex[:8]}",
        stream_position=1,
        global_position=99,
        event_type="DecisionGenerated",
        event_version=1,
        payload={
            "application_id": "loan-123",
            "orchestrator_agent_id": "orch-1",
            "recommendation": "APPROVE",
            "confidence_score": 0.82,
            "contributing_agent_sessions": [credit_stream, fraud_stream],
            "decision_basis_summary": "reconstructed",
        },
        metadata={},
        recorded_at=datetime.now(timezone.utc),
    )

    upcasted = await registry.upcast_event(
        legacy_event,
        context={"store": event_store},
    )
    assert upcasted.event_version == 2
    assert upcasted.payload["model_versions"][credit_stream] == "credit-model-v2.9"
    assert upcasted.payload["model_versions"][fraud_stream] == "fraud-model-v1.8"


@pytest.mark.asyncio
async def test_upcasting_immutability_via_database(event_store) -> None:
    event_store._upcasters = registry

    agent_id = f"agent-{uuid4().hex[:8]}"
    session_id = f"sess-{uuid4().hex[:8]}"
    stream_id = f"agent-{agent_id}-{session_id}"

    v1_payload = {
        "application_id": "loan-immutability-test",
        "agent_id": agent_id,
        "session_id": session_id,
        "risk_tier": "LOW",
        "recommended_limit_usd": "500000",
        "analysis_duration_ms": 1200,
        "input_data_hash": "abc123hash",
    }

    async with event_store._pool.connection() as conn:  # noqa: SLF001
        await conn.execute(
            """
            INSERT INTO event_streams (stream_id, aggregate_type, current_version)
            VALUES (%s, %s, %s)
            ON CONFLICT (stream_id) DO UPDATE SET current_version = %s
            """,
            (stream_id, "agent", 1, 1),
        )
        await conn.execute(
            """
            INSERT INTO events (
                stream_id, stream_position, event_type, event_version, payload, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                stream_id,
                1,
                "CreditAnalysisCompleted",
                1,
                json.dumps(v1_payload),
                json.dumps({"correlation_id": "test-immutability"}),
            ),
        )
        await conn.commit()

    async with event_store._pool.connection() as conn:  # noqa: SLF001
        result = await conn.execute(
            """
            SELECT event_version, payload
            FROM events
            WHERE stream_id = %s AND stream_position = %s
            """,
            (stream_id, 1),
        )
        raw_version, raw_payload = await result.fetchone()
        assert raw_version == 1
        assert "model_version" not in raw_payload
        assert "model_deployment_id" not in raw_payload

    loaded_event = (await event_store.load_stream(stream_id))[0]
    assert loaded_event.event_version == 2
    assert "model_version" in loaded_event.payload
    assert "model_deployment_id" in loaded_event.payload
    assert "regulatory_basis" in loaded_event.payload

    async with event_store._pool.connection() as conn:  # noqa: SLF001
        result = await conn.execute(
            """
            SELECT event_version, payload
            FROM events
            WHERE stream_id = %s AND stream_position = %s
            """,
            (stream_id, 1),
        )
        raw_version_after, raw_payload_after = await result.fetchone()
        assert raw_version_after == 1
        assert "model_version" not in raw_payload_after
        assert "model_deployment_id" not in raw_payload_after
        assert raw_payload_after == raw_payload


def test_credit_analysis_v1_to_v2_direct_helper() -> None:
    payload = {
        "application_id": "loan-001",
        "_recorded_at": "2024-06-15T10:00:00+00:00",
    }
    v2_payload = upcast_credit_v1_to_v2(payload)
    assert v2_payload["model_version"] == "credit-model-v1.x-legacy"
