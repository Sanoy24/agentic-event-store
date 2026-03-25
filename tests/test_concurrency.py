# tests/test_concurrency.py
# =============================================================================
# TRP1 LEDGER — Double-Decision Concurrency Test
# =============================================================================
# Source: Challenge Doc Phase 1 p.8 — "The Double-Decision Test" — MANDATORY
#
# This is the most critical test in the interim submission. The assessor will
# run it.
#
# Scenario:
#   Two AI agents simultaneously attempt to append CreditAnalysisCompleted
#   to the same loan application stream. Both read the stream at version 3
#   and pass expected_version=3 to their append call.
#
# Assertions (Challenge Doc p.8):
#   (a) Total events in stream after both tasks = 4 (not 5)
#   (b) Winning task's event has stream_position = 4
#   (c) Losing task raises OptimisticConcurrencyError — NOT silently swallowed
#
# Business meaning (Challenge Doc p.8):
#   "Two fraud-detection agents simultaneously flagging the same application.
#   Without OCC, both flags are applied and no one knows which score is
#   authoritative. With OCC, one agent wins; the other must reload and
#   determine if its analysis is still relevant."
#
# Uses anyio.create_task_group() — NOT asyncio.gather() (anti-pattern)
# =============================================================================
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

import anyio
import pytest

from src.event_store import EventStore
from src.models.events import (
    ApplicationSubmitted,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    LoanPurpose,
    OptimisticConcurrencyError,
    RiskTier,
)


@pytest.fixture
def application_id() -> str:
    """Generate a unique application ID for each test."""
    return str(uuid.uuid4())


async def _setup_stream_at_version_3(
    event_store: EventStore,
    application_id: str,
) -> None:
    """
    SETUP: Create a loan stream at version 3.
    Append ApplicationSubmitted + CreditAnalysisRequested + a placeholder
    event to get the stream to version 3.
    """
    # Event 1: ApplicationSubmitted (stream_position = 1)
    await event_store.append(
        stream_id=f"loan-{application_id}",
        events=[
            ApplicationSubmitted(
                application_id=application_id,
                applicant_id="applicant-001",
                applicant_name="Apex Commercial Holdings",
                requested_amount_usd=Decimal("500000.00"),
                loan_purpose=LoanPurpose.WORKING_CAPITAL,
                submission_channel="api",
                submitted_at=datetime.now(timezone.utc),
            ),
        ],
        expected_version=-1,
        correlation_id="corr-setup-1",
    )

    # Event 2: CreditAnalysisRequested (stream_position = 2)
    await event_store.append(
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
        correlation_id="corr-setup-2",
    )

    # Event 3: Another CreditAnalysisRequested to reach version 3
    # (In a real system this might be a different event type,
    # but for the concurrency test we just need version 3)
    await event_store.append(
        stream_id=f"loan-{application_id}",
        events=[
            CreditAnalysisRequested(
                application_id=application_id,
                assigned_agent_id="agent-credit-002",
                requested_at=datetime.now(timezone.utc),
                priority="MEDIUM",
            ),
        ],
        expected_version=2,
        correlation_id="corr-setup-3",
    )


def _make_credit_analysis_event(
    application_id: str,
    agent_name: str,
) -> CreditAnalysisCompleted:
    """Create a CreditAnalysisCompleted event for a given agent."""
    return CreditAnalysisCompleted(
        application_id=application_id,
        agent_id=f"agent-{agent_name}",
        session_id=f"session-{agent_name}",
        model_version="credit-model-v2.2",
        model_deployment_id=f"deploy-{agent_name}-001",
        confidence_score=0.85,
        risk_tier=RiskTier.MEDIUM,
        recommended_limit_usd=Decimal("450000.00"),
        analysis_duration_ms=1500,
        input_data_hash=f"sha256-{agent_name}-input",
    )


async def test_double_decision_concurrency(
    event_store: EventStore,
    application_id: str,
) -> None:
    """
    Double-Decision Concurrency Test.

    Two AI agents simultaneously attempt to append CreditAnalysisCompleted
    to the same loan application stream. Both read the stream at version 3
    and pass expected_version=3 to their append call.

    Uses anyio.create_task_group() for concurrent task spawning.
    """
    # SETUP: Create a loan stream at version 3
    await _setup_stream_at_version_3(event_store, application_id)

    # Verify setup
    version = await event_store.stream_version(f"loan-{application_id}")
    assert version == 3, f"Setup failed: expected version 3, got {version}"

    # BARRIER: Use anyio.Event to make both tasks start simultaneously
    start_signal = anyio.Event()

    agent_a_result: dict = {}
    agent_b_result: dict = {}

    async def agent_task(task_name: str, result_holder: dict) -> None:
        """
        Agent task that waits for the start signal, then attempts to append
        a CreditAnalysisCompleted event at expected_version=3.
        """
        await start_signal.wait()  # Both tasks wait before appending
        try:
            new_version = await event_store.append(
                stream_id=f"loan-{application_id}",
                events=[_make_credit_analysis_event(application_id, task_name)],
                expected_version=3,
                correlation_id=f"corr-{task_name}",
            )
            result_holder["success"] = True
            result_holder["new_version"] = new_version
        except OptimisticConcurrencyError as e:
            result_holder["error"] = e
            result_holder["success"] = False

    async with anyio.create_task_group() as tg:
        tg.start_soon(agent_task, "agent-a", agent_a_result)
        tg.start_soon(agent_task, "agent-b", agent_b_result)
        # Release both tasks simultaneously
        start_signal.set()

    # =========================================================================
    # ASSERTIONS
    # =========================================================================

    results = [agent_a_result, agent_b_result]
    successes = [r for r in results if r.get("success")]
    failures = [r for r in results if not r.get("success")]

    # Exactly one agent must succeed
    assert len(successes) == 1, (
        f"Exactly one agent must succeed, but {len(successes)} succeeded. "
        f"Results: {results}"
    )

    # Exactly one agent must fail
    assert len(failures) == 1, (
        f"Exactly one agent must fail, but {len(failures)} failed. Results: {results}"
    )

    # (a) Total events in stream = 4 (not 5)
    final_version = await event_store.stream_version(f"loan-{application_id}")

    print("\n\n--- Step 2: Concurrency Under Pressure ---")
    print(f"Agent A success: {agent_a_result.get('success')}")
    print(f"Agent B success: {agent_b_result.get('success')}")
    print(f"Final stream version: {final_version} (Expected: 4 - exactly one analysis succeeded)")

    assert final_version == 4, (
        f"Total events should be 4, got {final_version}. "
        "OCC failed: both agents' events were accepted."
    )

    # (b) Winner's event at stream_position = 4
    assert successes[0]["new_version"] == 4, (
        f"Winner's new version should be 4, got {successes[0]['new_version']}"
    )

    # (c) Loser received OptimisticConcurrencyError — not silently swallowed
    loser_error = failures[0]["error"]

    print(f"Loser Error: OptimisticConcurrencyError - expected {loser_error.expected_version}, actual {loser_error.actual_version}")
    print("Split-brain state avoided without locking!")
    print("------------------------------------------\n")

    assert isinstance(loser_error, OptimisticConcurrencyError), (
        f"Loser should receive OptimisticConcurrencyError, got {type(loser_error)}"
    )
    assert loser_error.stream_id == f"loan-{application_id}", (
        f"Error stream_id mismatch: {loser_error.stream_id}"
    )
    assert loser_error.expected_version == 3, (
        f"Error expected_version should be 3, got {loser_error.expected_version}"
    )
    assert loser_error.actual_version == 4, (
        f"Error actual_version should be 4, got {loser_error.actual_version}"
    )

    # ADDITIONAL: load stream and verify event integrity
    events = await event_store.load_stream(f"loan-{application_id}")
    assert len(events) == 4, (
        f"Stream should contain exactly 4 events, got {len(events)}"
    )

    # Verify no gaps, no duplicates in stream positions
    positions = [e.stream_position for e in events]
    assert positions == [1, 2, 3, 4], (
        f"Stream positions should be [1, 2, 3, 4], got {positions}"
    )


async def test_double_decision_concurrency_with_retry(
    event_store: EventStore,
    application_id: str,
) -> None:
    """
    Double-Decision Concurrency Test WITH RETRY.

    Same race condition as above, but the losing agent follows the
    suggested_action='reload_stream_and_retry' pattern to recover.
    Both agents' events end up in the stream.
    """
    await _setup_stream_at_version_3(event_store, application_id)

    start_signal = anyio.Event()
    agent_a_result: dict = {}
    agent_b_result: dict = {}

    async def agent_task_with_retry(
        task_name: str, result_holder: dict
    ) -> None:
        await start_signal.wait()
        expected_version = 3
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                new_version = await event_store.append(
                    stream_id=f"loan-{application_id}",
                    events=[
                        _make_credit_analysis_event(application_id, task_name)
                    ],
                    expected_version=expected_version,
                    correlation_id=f"corr-{task_name}-attempt-{attempt}",
                )
                result_holder["success"] = True
                result_holder["new_version"] = new_version
                result_holder["attempts"] = attempt
                return
            except OptimisticConcurrencyError as e:
                result_holder["retried"] = True
                print(
                    f"  [{task_name}] Attempt {attempt}: "
                    f"OptimisticConcurrencyError "
                    f"(expected={e.expected_version}, "
                    f"actual={e.actual_version}). "
                    f"Suggested action: {e.suggested_action}"
                )
                # Reload stream to get current version
                expected_version = await event_store.stream_version(
                    f"loan-{application_id}"
                )
                print(
                    f"  [{task_name}] Reloaded stream version: "
                    f"{expected_version}. Retrying..."
                )

        result_holder["success"] = False
        result_holder["error"] = "Max retries exceeded"

    async with anyio.create_task_group() as tg:
        tg.start_soon(agent_task_with_retry, "agent-a", agent_a_result)
        tg.start_soon(agent_task_with_retry, "agent-b", agent_b_result)
        start_signal.set()

    # --- Assertions ---
    print("\n\n--- Step 2: Concurrency Under Pressure (with Retry) ---")
    print(
        f"Agent A: success={agent_a_result.get('success')}, "
        f"attempts={agent_a_result.get('attempts')}, "
        f"retried={agent_a_result.get('retried', False)}"
    )
    print(
        f"Agent B: success={agent_b_result.get('success')}, "
        f"attempts={agent_b_result.get('attempts')}, "
        f"retried={agent_b_result.get('retried', False)}"
    )

    assert agent_a_result["success"] is True
    assert agent_b_result["success"] is True

    final_version = await event_store.stream_version(
        f"loan-{application_id}"
    )
    print(
        f"Final stream version: {final_version} "
        "(both agents succeeded after retry)"
    )
    assert final_version == 5

    events = await event_store.load_stream(f"loan-{application_id}")
    positions = [e.stream_position for e in events]
    assert positions == [1, 2, 3, 4, 5]

    retried_count = sum(
        1 for r in [agent_a_result, agent_b_result] if r.get("retried")
    )
    print(f"Agents that retried: {retried_count}")
    print(
        "Split-brain avoided AND loser recovered via "
        "reload_stream_and_retry!"
    )
    print("------------------------------------------------------\n")

    assert retried_count == 1


async def test_new_stream_creation(event_store: EventStore) -> None:
    """
    Verify that a new stream can be created with expected_version = -1.
    """
    app_id = str(uuid.uuid4())
    version = await event_store.append(
        stream_id=f"loan-{app_id}",
        events=[
            ApplicationSubmitted(
                application_id=app_id,
                applicant_id="applicant-new",
                applicant_name="New Applicant LLC",
                requested_amount_usd=Decimal("100000.00"),
                loan_purpose=LoanPurpose.EQUIPMENT,
                submission_channel="web",
                submitted_at=datetime.now(timezone.utc),
            ),
        ],
        expected_version=-1,
        correlation_id="corr-new-stream",
    )
    assert version == 1


async def test_stream_version_nonexistent(event_store: EventStore) -> None:
    """
    Verify that stream_version returns 0 for a non-existent stream.
    O(1) lookup — primary key on event_streams.
    """
    version = await event_store.stream_version("loan-does-not-exist")
    assert version == 0


async def test_load_stream_empty(event_store: EventStore) -> None:
    """
    Verify that loading a non-existent stream returns an empty list.
    """
    events = await event_store.load_stream("loan-does-not-exist")
    assert events == []


async def test_metadata_contains_correlation_id(event_store: EventStore) -> None:
    """
    Verify that stored event metadata includes correlation_id and causation_id.
    (Causal Tracing Reflex — Manual p.17)
    """
    app_id = str(uuid.uuid4())
    await event_store.append(
        stream_id=f"loan-{app_id}",
        events=[
            ApplicationSubmitted(
                application_id=app_id,
                applicant_id="applicant-meta",
                applicant_name="Metadata Test Corp",
                requested_amount_usd=Decimal("200000.00"),
                loan_purpose=LoanPurpose.EXPANSION,
                submission_channel="api",
                submitted_at=datetime.now(timezone.utc),
            ),
        ],
        expected_version=-1,
        correlation_id="corr-meta-test",
        causation_id="cause-meta-test",
    )

    events = await event_store.load_stream(f"loan-{app_id}")
    assert len(events) == 1
    metadata = events[0].metadata
    assert metadata["correlation_id"] == "corr-meta-test"
    assert metadata["causation_id"] == "cause-meta-test"
    assert metadata["recorded_by"] == "event_store"
    assert "schema_version" in metadata


async def test_archive_stream(event_store: EventStore) -> None:
    """
    Verify that archived streams reject new appends.
    """
    app_id = str(uuid.uuid4())
    stream_id = f"loan-{app_id}"

    # Create stream
    await event_store.append(
        stream_id=stream_id,
        events=[
            ApplicationSubmitted(
                application_id=app_id,
                applicant_id="applicant-archive",
                applicant_name="Archive Test Ventures",
                requested_amount_usd=Decimal("300000.00"),
                loan_purpose=LoanPurpose.REFINANCING,
                submission_channel="api",
                submitted_at=datetime.now(timezone.utc),
            ),
        ],
        expected_version=-1,
        correlation_id="corr-archive",
    )

    # Archive stream
    await event_store.archive_stream(stream_id)

    # Verify metadata shows archived
    meta = await event_store.get_stream_metadata(stream_id)
    assert meta.archived_at is not None

    # Attempt to append should fail
    from src.models.events import StreamArchivedError

    with pytest.raises(StreamArchivedError):
        await event_store.append(
            stream_id=stream_id,
            events=[
                CreditAnalysisRequested(
                    application_id=app_id,
                    assigned_agent_id="agent-after-archive",
                    requested_at=datetime.now(timezone.utc),
                    priority="LOW",
                ),
            ],
            expected_version=1,
            correlation_id="corr-archive-fail",
        )
