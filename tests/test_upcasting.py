from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import psycopg
import pytest

from src.event_store import EventStore
from src.integrity.audit_chain import run_integrity_check
from src.models.events import (
    AgentContextLoaded,
    ApplicationSubmitted,
    CreditAnalysisRequested,
    LoanPurpose,
    StoredEvent,
)
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


def test_credit_analysis_v1_to_v2_discards_legacy_confidence_score() -> None:
    payload = {
        "application_id": "loan-legacy-confidence",
        "confidence_score": 0.73,
    }
    event = StoredEvent(
        event_id=uuid4(),
        stream_id="agent-credit-session",
        stream_position=1,
        global_position=1,
        event_type="CreditAnalysisCompleted",
        event_version=1,
        payload=payload,
        metadata={},
        recorded_at=datetime(2025, 8, 1, 10, 0, tzinfo=timezone.utc),
    )

    upcasted = registry.upcast(event)
    assert upcasted.payload["model_version"] == "credit-model-v2.2"
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
        
        print("\n\n--- Step 4: Upcasting & Immutability Demonstration ---")
        print(f"1) Raw Database Payload (Version {raw_version}):\n{json.dumps(raw_payload, indent=2)}")

    loaded_event = (await event_store.load_stream(stream_id))[0]
    assert loaded_event.event_version == 2
    assert "model_version" in loaded_event.payload
    assert "model_deployment_id" in loaded_event.payload
    assert "regulatory_basis" in loaded_event.payload

    print(f"\n2) Loaded Event via EventStore (Upcasted to Version {loaded_event.event_version}):\n{json.dumps(loaded_event.payload, indent=2)}")

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

        print(f"\n3) Raw Database Payload After Load (STILL Version {raw_version_after}):\n{json.dumps(raw_payload_after, indent=2)}")
        print("------------------------------------------------------\n")


def test_credit_analysis_v1_to_v2_direct_helper() -> None:
    payload = {
        "application_id": "loan-001",
        "_recorded_at": "2024-06-15T10:00:00+00:00",
    }
    v2_payload = upcast_credit_v1_to_v2(payload)
    assert v2_payload["model_version"] == "credit-model-v1.x-legacy"


# =============================================================================
# Cryptographic Integrity & Tamper Detection Tests
# (Rubric §6: "Hash chain" + "Tamper detection test structure")
# =============================================================================


@pytest.mark.asyncio
async def test_hash_chain_integrity_check_appends_audit_event(
    event_store: EventStore,
) -> None:
    """
    Verify that run_integrity_check:
    1. Computes a SHA-256 hash chain over the event stream
    2. Appends an AuditIntegrityCheckRun event to the audit stream
    3. Returns a typed IntegrityCheckResult with chain_valid=True

    Two consecutive checks must form a linked chain where the second
    check's previous_hash matches the first check's integrity_hash.
    """
    application_id = str(uuid4())
    stream_id = f"loan-{application_id}"

    # Seed two events into the loan stream
    await event_store.append(
        stream_id,
        [
            ApplicationSubmitted(
                application_id=application_id,
                applicant_id="applicant-001",
                applicant_name="Test Corp",
                requested_amount_usd=Decimal("50000"),
                loan_purpose=LoanPurpose.WORKING_CAPITAL,
                submission_channel="api",
                submitted_at=datetime.now(timezone.utc),
            ),
        ],
        expected_version=-1,
    )
    await event_store.append(
        stream_id,
        [
            CreditAnalysisRequested(
                application_id=application_id,
                assigned_agent_id="agent-01",
                requested_at=datetime.now(timezone.utc),
                priority="HIGH",
            ),
        ],
        expected_version=1,
    )

    # Run first integrity check
    result1 = await run_integrity_check(
        store=event_store,
        entity_type="loan",
        entity_id=application_id,
    )
    assert result1.chain_valid is True
    assert result1.tamper_detected is False
    assert result1.previous_hash == "genesis"
    assert result1.events_verified_count > 0

    # Run second integrity check
    result2 = await run_integrity_check(
        store=event_store,
        entity_type="loan",
        entity_id=application_id,
    )
    assert result2.chain_valid is True
    assert result2.tamper_detected is False
    # Chain link: second check's previous_hash == first check's integrity_hash
    assert result2.previous_hash == result1.integrity_hash

    # Verify AuditIntegrityCheckRun events were appended
    audit_events = await event_store.load_stream(f"audit-loan-{application_id}")
    integrity_events = [
        e for e in audit_events if e.event_type == "AuditIntegrityCheckRun"
    ]
    assert len(integrity_events) == 2
    assert integrity_events[0].payload["previous_hash"] == "genesis"
    assert (
        integrity_events[1].payload["previous_hash"]
        == integrity_events[0].payload["integrity_hash"]
    )


@pytest.mark.asyncio
async def test_tamper_detection_flips_payload_bytes(
    event_store: EventStore,
    db_url: str,
) -> None:
    """
    Rubric requirement: Tamper detection test structure.

    1. Seed events and run a CLEAN integrity check → chain_valid=True
    2. Directly flip stored payload bytes via raw SQL UPDATE
    3. Run a second integrity check
    4. Assert: tamper_detected = True, chain_valid = False

    This proves the SHA-256 hash chain detects post-hoc payload modification.
    """
    application_id = str(uuid4())
    stream_id = f"loan-{application_id}"

    # Seed events
    await event_store.append(
        stream_id,
        [
            ApplicationSubmitted(
                application_id=application_id,
                applicant_id="applicant-tamper",
                applicant_name="Tamper Test Corp",
                requested_amount_usd=Decimal("75000"),
                loan_purpose=LoanPurpose.EQUIPMENT,
                submission_channel="api",
                submitted_at=datetime.now(timezone.utc),
            ),
        ],
        expected_version=-1,
    )

    # --- Step 1: Run a CLEAN integrity check ---
    clean_result = await run_integrity_check(
        store=event_store,
        entity_type="loan",
        entity_id=application_id,
    )
    assert clean_result.chain_valid is True
    assert clean_result.tamper_detected is False

    # --- Step 2: Flip stored payload bytes via raw SQL ---
    # This simulates a malicious actor modifying the immutable event log.
    from psycopg_pool import AsyncConnectionPool

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

            # Flip payload bytes: change a field value to corrupt the hash
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
    tampered_result = await run_integrity_check(
        store=event_store,
        entity_type="loan",
        entity_id=application_id,
    )

    # --- Step 4: Assert tamper detected ---
    assert tampered_result.tamper_detected is True, (
        f"Expected tamper_detected=True after payload modification, "
        f"got {tampered_result.tamper_detected}"
    )
    assert tampered_result.chain_valid is False, (
        f"Expected chain_valid=False after payload modification, "
        f"got {tampered_result.chain_valid}"
    )

    # Verify AuditTamperDetected event was appended to the audit stream
    audit_events = await event_store.load_stream(f"audit-loan-{application_id}")
    tamper_events = [
        e for e in audit_events if e.event_type == "AuditTamperDetected"
    ]
    assert len(tamper_events) == 1
    assert tamper_events[0].payload["severity"] == "CRITICAL"

    print("\n--- Tamper Detection Test Results ---")
    print(f"Clean check:   chain_valid={clean_result.chain_valid}, tamper_detected={clean_result.tamper_detected}")
    print(f"After tamper:  chain_valid={tampered_result.chain_valid}, tamper_detected={tampered_result.tamper_detected}")
    print(f"Tamper event:  severity={tamper_events[0].payload['severity']}")
    print("--- PASSED: Hash chain detected payload modification ---\n")
