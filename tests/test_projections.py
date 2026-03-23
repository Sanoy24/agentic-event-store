from __future__ import annotations

import asyncio
from decimal import Decimal
from time import monotonic
from uuid import uuid4

import pytest
from psycopg_pool import AsyncConnectionPool

from src.commands.handlers import (
    RequestComplianceCheckCommand,
    SubmitApplicationCommand,
    handle_request_compliance_check,
    handle_submit_application,
)
from src.event_store import EventStore
from src.models.events import (
    ComplianceCheckRequested,
    ComplianceClearanceIssued,
    ComplianceRulePassed,
    LoanPurpose,
    StoredEvent,
)
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.compliance_audit import (
    ComplianceAuditProjection,
    get_compliance_at,
    get_current_compliance,
    get_projection_lag,
)
from src.projections.daemon import Projection, ProjectionDaemon


class FailingProjection(Projection):
    @property
    def name(self) -> str:
        return "AlwaysFails"

    def interested_in(self, event_type: str) -> bool:
        return event_type == "ApplicationSubmitted"

    async def handle(self, event: StoredEvent, conn) -> None:
        raise RuntimeError("boom")


@pytest.fixture
async def projection_env(event_store: EventStore, db_url: str):
    async with AsyncConnectionPool(db_url) as pool:
        daemon = ProjectionDaemon(
            store=event_store,
            pool=pool,
            projections=[ComplianceAuditProjection()],
            batch_size=50,
        )
        yield {
            "store": event_store,
            "pool": pool,
            "daemon": daemon,
        }


async def _drain_daemon(daemon: ProjectionDaemon) -> None:
    while True:
        processed = await daemon.run_once()
        if processed == 0:
            break


async def _seed_compliance_history(store: EventStore, application_id: str) -> None:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)

    await store.append(
        stream_id=f"compliance-{application_id}",
        events=[
            ComplianceCheckRequested(
                application_id=application_id,
                regulation_set_version="Basel-III-2026-Q1",
                checks_required=["KYC", "AML"],
            ),
        ],
        expected_version=-1,
        correlation_id=f"corr-{application_id}-requested",
    )
    await asyncio.sleep(0.02)
    await store.append(
        stream_id=f"compliance-{application_id}",
        events=[
            ComplianceRulePassed(
                application_id=application_id,
                rule_id="KYC",
                rule_version="kyc-v1.2",
                evaluation_timestamp=now,
                evidence_hash="hash-kyc",
            ),
        ],
        expected_version=1,
        correlation_id=f"corr-{application_id}-kyc",
    )
    await asyncio.sleep(0.02)
    await store.append(
        stream_id=f"compliance-{application_id}",
        events=[
            ComplianceRulePassed(
                application_id=application_id,
                rule_id="AML",
                rule_version="aml-v3.0",
                evaluation_timestamp=now,
                evidence_hash="hash-aml",
            ),
        ],
        expected_version=2,
        correlation_id=f"corr-{application_id}-aml",
    )
    await asyncio.sleep(0.02)
    await store.append(
        stream_id=f"compliance-{application_id}",
        events=[
            ComplianceClearanceIssued(
                application_id=application_id,
                regulation_set_version="Basel-III-2026-Q1",
                checks_passed=["KYC", "AML"],
                clearance_timestamp=now,
                issued_by="compliance-agent-01",
            ),
        ],
        expected_version=3,
        correlation_id=f"corr-{application_id}-clearance",
    )


@pytest.mark.asyncio
async def test_compliance_audit_current_and_temporal_queries(projection_env) -> None:
    store = projection_env["store"]
    pool = projection_env["pool"]
    daemon = projection_env["daemon"]
    application_id = uuid4().hex[:8]

    await _seed_compliance_history(store, application_id)
    await _drain_daemon(daemon)

    async with pool.connection() as conn:
        current = await get_current_compliance(conn, application_id)
        assert current["compliance_status"] == "CLEARED"
        assert current["checks_required"] == ["KYC", "AML"]
        assert current["checks_passed"] == ["AML", "KYC"]

        rules = {rule["rule_id"]: rule for rule in current["rules"]}
        assert rules["KYC"]["rule_version"] == "kyc-v1.2"
        assert rules["AML"]["rule_version"] == "aml-v3.0"

        result = await conn.execute(
            """
            SELECT event_timestamp
            FROM compliance_audit_events
            WHERE application_id = %s
            ORDER BY global_position ASC
            """,
            (application_id,),
        )
        timestamps = [row[0] for row in await result.fetchall()]

        historical = await get_compliance_at(conn, application_id, timestamps[1])
        assert historical["compliance_status"] == "IN_PROGRESS"
        assert historical["checks_passed"] == ["KYC"]
        assert historical["clearance_issued"] is False


@pytest.mark.asyncio
async def test_compliance_projection_creates_snapshots(projection_env) -> None:
    store = projection_env["store"]
    pool = projection_env["pool"]
    daemon = projection_env["daemon"]
    application_id = uuid4().hex[:8]

    await _seed_compliance_history(store, application_id)
    await _drain_daemon(daemon)

    async with pool.connection() as conn:
        result = await conn.execute(
            """
            SELECT COUNT(*), MAX(snapshot_version)
            FROM compliance_audit_snapshots
            WHERE application_id = %s
            """,
            (application_id,),
        )
        row = await result.fetchone()
        assert row is not None
        assert row[0] >= 2
        assert row[1] == 1


@pytest.mark.asyncio
async def test_compliance_projection_lag_metric(projection_env) -> None:
    from datetime import datetime, timezone

    store = projection_env["store"]
    pool = projection_env["pool"]
    daemon = projection_env["daemon"]
    application_id = uuid4().hex[:8]

    await store.append(
        stream_id=f"compliance-{application_id}",
        events=[
            ComplianceCheckRequested(
                application_id=application_id,
                regulation_set_version="Basel-III-2026-Q1",
                checks_required=["KYC"],
            ),
        ],
        expected_version=-1,
        correlation_id=f"corr-{application_id}-requested",
    )
    await _drain_daemon(daemon)

    async with pool.connection() as conn:
        assert await get_projection_lag(conn) == 0

    await asyncio.sleep(0.03)
    await store.append(
        stream_id=f"compliance-{application_id}",
        events=[
            ComplianceRulePassed(
                application_id=application_id,
                rule_id="KYC",
                rule_version="kyc-v1.2",
                evaluation_timestamp=datetime.now(timezone.utc),
                evidence_hash="hash-kyc",
            ),
        ],
        expected_version=1,
        correlation_id=f"corr-{application_id}-kyc",
    )

    async with pool.connection() as conn:
        lag_before = await get_projection_lag(conn)
        assert lag_before is not None
        assert lag_before > 0

    await _drain_daemon(daemon)

    async with pool.connection() as conn:
        assert await get_projection_lag(conn) == 0


@pytest.mark.asyncio
async def test_compliance_rebuild_from_scratch_preserves_history(
    projection_env,
) -> None:
    store = projection_env["store"]
    pool = projection_env["pool"]
    daemon = projection_env["daemon"]
    application_id = uuid4().hex[:8]

    await _seed_compliance_history(store, application_id)
    await _drain_daemon(daemon)

    async with pool.connection() as conn:
        result = await conn.execute(
            """
            SELECT COUNT(*)
            FROM compliance_audit_events
            WHERE application_id = %s
            """,
            (application_id,),
        )
        before_count = (await result.fetchone())[0]
        await conn.execute(
            """
            UPDATE compliance_audit_view
            SET compliance_status = 'CORRUPTED'
            WHERE application_id = %s
            """,
            (application_id,),
        )
        await conn.commit()

    rebuilt = await daemon.rebuild_projection("ComplianceAuditView")
    assert rebuilt == 4

    async with pool.connection() as conn:
        current = await get_current_compliance(conn, application_id)
        assert current["compliance_status"] == "CLEARED"

        result = await conn.execute(
            """
            SELECT COUNT(*)
            FROM compliance_audit_events
            WHERE application_id = %s
            """,
            (application_id,),
        )
        after_count = (await result.fetchone())[0]
        assert after_count == before_count


@pytest.mark.asyncio
async def test_projection_daemon_skips_after_configured_retries(
    event_store: EventStore,
    db_url: str,
) -> None:
    async with AsyncConnectionPool(db_url) as pool:
        daemon = ProjectionDaemon(
            store=event_store,
            pool=pool,
            projections=[FailingProjection()],
            max_retries_per_event=2,
        )

        await handle_submit_application(
            SubmitApplicationCommand(
                application_id="skip-me",
                applicant_id="applicant-skip",
                applicant_name="Skip Me Ltd",
                requested_amount_usd=Decimal("1000"),
                loan_purpose=LoanPurpose.WORKING_CAPITAL,
                submission_channel="api",
                correlation_id="skip-correlation",
            ),
            event_store,
        )

        assert await daemon.run_once() == 1
        assert await daemon.get_lag("AlwaysFails") == 1

        assert await daemon.run_once() == 1
        assert await daemon.get_lag("AlwaysFails") == 0

        async with pool.connection() as conn:
            result = await conn.execute(
                """
                SELECT retry_count, skipped_at
                FROM projection_failures
                WHERE projection_name = %s AND global_position = 1
                """,
                ("AlwaysFails",),
            )
            row = await result.fetchone()
            assert row is not None
            assert row[0] == 2
            assert row[1] is not None


@pytest.mark.asyncio
async def test_projection_lag_slos_under_50_concurrent_command_handlers(
    event_store: EventStore,
    db_url: str,
) -> None:
    async with AsyncConnectionPool(db_url) as pool:
        daemon = ProjectionDaemon(
            store=event_store,
            pool=pool,
            projections=[
                ApplicationSummaryProjection(),
                ComplianceAuditProjection(),
            ],
            batch_size=200,
            poll_interval_ms=10,
        )

        async def workflow(index: int) -> None:
            application_id = f"loan-{index:03d}"
            await handle_submit_application(
                SubmitApplicationCommand(
                    application_id=application_id,
                    applicant_id=f"applicant-{index:03d}",
                    applicant_name=f"Applicant {index:03d}",
                    requested_amount_usd=Decimal("250000"),
                    loan_purpose=LoanPurpose.WORKING_CAPITAL,
                    submission_channel="api",
                    correlation_id=f"submit-{index:03d}",
                ),
                event_store,
            )
            await handle_request_compliance_check(
                RequestComplianceCheckCommand(
                    application_id=application_id,
                    regulation_set_version="Basel-III-2026-Q1",
                    checks_required=["KYC", "AML"],
                    correlation_id=f"compliance-{index:03d}",
                ),
                event_store,
            )

        await asyncio.gather(*(workflow(i) for i in range(50)))

        lags = await daemon.get_all_lags()
        assert lags["ApplicationSummary"] > 0
        assert lags["ComplianceAuditView"] > 0

        start = monotonic()
        await _drain_daemon(daemon)
        elapsed_ms = (monotonic() - start) * 1000

        final_lags = await daemon.get_all_lags()
        assert final_lags["ApplicationSummary"] == 0
        assert final_lags["ComplianceAuditView"] == 0
        assert elapsed_ms < 500
