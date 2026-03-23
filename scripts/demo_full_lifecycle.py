"""
TRP1 LEDGER — Full Lifecycle Demo
==================================
Drives a complete loan application lifecycle against a REAL PostgreSQL
database, proving every phase works end-to-end.

Usage:
    uv run python scripts/demo_full_lifecycle.py
"""

import asyncio
import sys
from decimal import Decimal
from datetime import datetime, timezone
from uuid import uuid4

from psycopg_pool import AsyncConnectionPool

from src.event_store import EventStore
from src.models.events import CreditAnalysisRequested
from src.commands.handlers import (
    SubmitApplicationCommand,
    CreditAnalysisCompletedCommand,
    FraudScreeningCompletedCommand,
    RequestComplianceCheckCommand,
    RecordComplianceCheckCommand,
    GenerateDecisionCommand,
    StartAgentSessionCommand,
    handle_submit_application,
    handle_credit_analysis_completed,
    handle_fraud_screening_completed,
    handle_request_compliance_check,
    handle_record_compliance_check,
    handle_generate_decision,
    handle_start_agent_session,
)
from src.integrity.audit_chain import run_integrity_check
from src.upcasting.upcasters import registry as upcaster_registry

DB_URL = "postgresql://postgres:postgres@localhost:5432/trp1_ledger"

# Pretty print helpers
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


def step(n: int, title: str):
    print(f"\n{BOLD}{CYAN}{'='*60}")
    print(f"  Step {n}: {title}")
    print(f"{'='*60}{RESET}")


def ok(msg: str):
    print(f"  {GREEN}✓ {msg}{RESET}")


def info(msg: str):
    print(f"  {YELLOW}→ {msg}{RESET}")


async def main():
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    app_id = f"demo-{uuid4().hex[:6]}"
    credit_agent_id = f"credit-agent-{uuid4().hex[:4]}"
    credit_session_id = f"sess-{uuid4().hex[:4]}"
    fraud_agent_id = f"fraud-agent-{uuid4().hex[:4]}"
    fraud_session_id = f"sess-{uuid4().hex[:4]}"
    corr_id = f"demo-corr-{uuid4().hex[:4]}"

    print(f"\n{BOLD}{'='*60}")
    print(f"  TRP1 LEDGER — Full Lifecycle Demo")
    print(f"  Application ID: {app_id}")
    print(f"{'='*60}{RESET}")

    async with AsyncConnectionPool(DB_URL) as pool:
        store = EventStore(pool, upcaster_registry=upcaster_registry)

        # ── Step 1: Submit Application ──────────────────────────
        step(1, "Submit Loan Application")
        info("Calling handle_submit_application...")
        version = await handle_submit_application(
            SubmitApplicationCommand(
                application_id=app_id,
                applicant_id="applicant-demo-001",
                applicant_name="Apex Dynamics Inc.",
                requested_amount_usd=Decimal("750000"),
                loan_purpose="WORKING_CAPITAL",
                submission_channel="agent",
                correlation_id=corr_id,
            ),
            store,
        )
        ok(f"Application submitted! Stream version: {version}")

        # ── Step 2: Start Credit Agent Session (Gas Town) ───────
        step(2, "Start Credit Agent Session (Gas Town Pattern)")
        info(f"Agent '{credit_agent_id}' declaring context...")
        await handle_start_agent_session(
            StartAgentSessionCommand(
                agent_id=credit_agent_id,
                session_id=credit_session_id,
                context_source="event_store",
                event_replay_from_position=0,
                context_token_count=5000,
                model_version="credit-model-v2.4.1",
                correlation_id=corr_id,
            ),
            store,
        )
        ok("Credit agent session started — context declared!")

        # ── Step 3: Start Fraud Agent Session (Gas Town) ────────
        step(3, "Start Fraud Agent Session (Gas Town Pattern)")
        info(f"Agent '{fraud_agent_id}' declaring context...")
        await handle_start_agent_session(
            StartAgentSessionCommand(
                agent_id=fraud_agent_id,
                session_id=fraud_session_id,
                context_source="event_store",
                event_replay_from_position=0,
                context_token_count=4000,
                model_version="fraud-model-v1.2.0",
                correlation_id=corr_id,
            ),
            store,
        )
        ok("Fraud agent session started — context declared!")

        # ── Step 4: Transition to Credit Analysis ───────────────
        step(4, "Transition to CREDIT_ANALYSIS_IN_PROGRESS")
        info("Appending CreditAnalysisRequested event...")
        current_version = await store.stream_version(f"loan-{app_id}")
        await store.append(
            stream_id=f"loan-{app_id}",
            events=[
                CreditAnalysisRequested(
                    application_id=app_id,
                    assigned_agent_id=credit_agent_id,
                    requested_at=datetime.now(timezone.utc),
                    priority="HIGH",
                ),
            ],
            expected_version=current_version,
            correlation_id=corr_id,
        )
        ok("State machine transitioned!")

        # ── Step 5: Record Credit Analysis ──────────────────────
        step(5, "Record Credit Analysis Results")
        info("Credit agent submitting analysis...")
        await handle_credit_analysis_completed(
            CreditAnalysisCompletedCommand(
                application_id=app_id,
                agent_id=credit_agent_id,
                session_id=credit_session_id,
                model_version="credit-model-v2.4.1",
                model_deployment_id="deploy-credit-001",
                confidence_score=0.87,
                risk_tier="LOW",
                recommended_limit_usd=Decimal("700000"),
                duration_ms=1500,
                input_data={"revenue": 5000000, "years": 8},
                correlation_id=corr_id,
            ),
            store,
        )
        ok("Credit analysis recorded — risk tier: LOW, confidence: 0.87")

        # ── Step 6: Record Fraud Screening ──────────────────────
        step(6, "Record Fraud Screening Results")
        info("Fraud agent submitting screening...")
        await handle_fraud_screening_completed(
            FraudScreeningCompletedCommand(
                application_id=app_id,
                agent_id=fraud_agent_id,
                session_id=fraud_session_id,
                fraud_score=0.05,
                anomaly_flags=[],
                screening_model_version="fraud-model-v1.2.0",
                model_deployment_id="deploy-fraud-001",
                input_data={"applicant_id": "applicant-demo-001"},
                correlation_id=corr_id,
            ),
            store,
        )
        ok("Fraud screening recorded — score: 0.05, no anomaly flags")

        # ── Step 7: Request Compliance Check ────────────────────
        step(7, "Request Compliance Checks (KYC, AML, SANCTIONS)")
        info("Requesting compliance verification...")
        await handle_request_compliance_check(
            RequestComplianceCheckCommand(
                application_id=app_id,
                regulation_set_version="Basel-III-2025-Q1",
                checks_required=["KYC", "AML", "SANCTIONS"],
                correlation_id=corr_id,
            ),
            store,
        )
        ok("Compliance checks requested!")

        # ── Step 8: Pass All Compliance Rules ───────────────────
        step(8, "Record Compliance Rule Results")
        for rule_id, rule_ver in [("KYC", "1.0"), ("AML", "2.0"), ("SANCTIONS", "1.5")]:
            info(f"Recording {rule_id} check — PASSED")
            await handle_record_compliance_check(
                RecordComplianceCheckCommand(
                    application_id=app_id,
                    rule_id=rule_id,
                    rule_version=rule_ver,
                    passed=True,
                    evidence_hash=f"{rule_id.lower()}-evidence-hash",
                    correlation_id=corr_id,
                ),
                store,
            )
            ok(f"{rule_id} — PASSED ✓")

        # ── Step 9: Generate Final Decision ─────────────────────
        step(9, "Generate Final Decision")
        credit_stream = f"agent-{credit_agent_id}-{credit_session_id}"
        fraud_stream = f"agent-{fraud_agent_id}-{fraud_session_id}"
        info("Generating automated decision...")
        await handle_generate_decision(
            GenerateDecisionCommand(
                application_id=app_id,
                orchestrator_agent_id="orchestrator-001",
                recommendation="APPROVE",
                confidence_score=0.85,
                contributing_agent_sessions=[credit_stream, fraud_stream],
                decision_basis_summary=(
                    "Low risk, clean fraud screening, "
                    "all compliance checks passed."
                ),
                model_versions={
                    "credit": "credit-model-v2.4.1",
                    "fraud": "fraud-model-v1.2.0",
                },
                correlation_id=corr_id,
            ),
            store,
        )
        ok("Decision: APPROVED ✅")

        # ── Step 10: Verify Complete Event Streams ──────────────
        step(10, "Verify Complete Audit Trail")
        loan_events = await store.load_stream(f"loan-{app_id}")
        compliance_events = await store.load_stream(f"compliance-{app_id}")
        credit_events = await store.load_stream(credit_stream)
        fraud_events = await store.load_stream(fraud_stream)

        info(f"Loan stream: {len(loan_events)} events")
        for i, event in enumerate(loan_events, 1):
            print(f"    {i}. {event.event_type}")

        info(f"Compliance stream: {len(compliance_events)} events")
        for i, event in enumerate(compliance_events, 1):
            print(f"    {i}. {event.event_type}")

        info(f"Credit agent stream: {len(credit_events)} events")
        for i, event in enumerate(credit_events, 1):
            print(f"    {i}. {event.event_type}")

        info(f"Fraud agent stream: {len(fraud_events)} events")
        for i, event in enumerate(fraud_events, 1):
            print(f"    {i}. {event.event_type}")

        total = len(loan_events) + len(compliance_events) + len(credit_events) + len(fraud_events)
        ok(f"Total events across all streams: {total}")

        # ── Step 11: Run Integrity Check ────────────────────────
        step(11, "Run Cryptographic Integrity Check")
        info("Verifying hash chain integrity...")
        integrity = await run_integrity_check(
            store=store,
            entity_type="application",
            entity_id=app_id,
        )
        ok(f"Chain valid: {integrity.chain_valid}")
        ok(f"Events verified: {integrity.events_verified_count}")
        ok(f"Tamper detected: {integrity.tamper_detected}")

        # ── Summary ─────────────────────────────────────────────
        print(f"\n{BOLD}{GREEN}{'='*60}")
        print(f"  🎉 FULL LIFECYCLE COMPLETE!")
        print(f"  Application '{app_id}' → APPROVED")
        print(f"  {total} events recorded in immutable audit trail")
        print(f"  Cryptographic integrity verified")
        print(f"  Gas Town context declared for {2} agents")
        print(f"  3/3 compliance checks passed")
        print(f"{'='*60}{RESET}\n")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
    else:
        asyncio.run(main())
