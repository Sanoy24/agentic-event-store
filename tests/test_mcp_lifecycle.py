from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from urllib.parse import urlencode
from uuid import uuid4

import pytest

from src.mcp.resources import register_resources
from src.mcp.server import LedgerFastMCP, LedgerServer
from src.mcp.tools import register_tools


async def _read_json(mcp: LedgerFastMCP, uri: str) -> dict:
    contents = await mcp.read_resource(uri)
    assert contents
    return json.loads(contents[0].content)


async def _wait_for_json(
    mcp: LedgerFastMCP,
    uri: str,
    predicate,
    *,
    timeout_s: float = 3.0,
) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout_s
    last = None
    while asyncio.get_running_loop().time() < deadline:
        last = await _read_json(mcp, uri)
        if predicate(last):
            return last
        await asyncio.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {uri}. Last payload: {last}")


@pytest.fixture
async def mcp_runtime(db_url: str):
    pytest.importorskip("mcp.server.fastmcp")
    if LedgerFastMCP is None:  # pragma: no cover - guarded by importorskip
        pytest.skip("FastMCP is not installed")

    server = LedgerServer(db_url)
    async with server.lifespan():
        mcp = LedgerFastMCP("TRP1 Ledger Test")
        register_tools(mcp, server)
        register_resources(mcp, server)
        yield mcp


@pytest.mark.asyncio
async def test_mcp_contract_exposes_required_tools_and_resources(mcp_runtime) -> None:
    mcp = mcp_runtime

    tool_names = {tool.name for tool in await mcp.list_tools()}
    assert tool_names == {
        "submit_application",
        "start_agent_session",
        "record_credit_analysis",
        "record_fraud_screening",
        "record_compliance_check",
        "generate_decision",
        "record_human_review",
        "run_integrity_check",
    }

    resource_uris = {str(resource.uri) for resource in await mcp.list_resources()}
    resource_uris.update(
        template.uriTemplate for template in await mcp.list_resource_templates()
    )
    assert resource_uris == {
        "ledger://applications/{application_id}",
        "ledger://applications/{application_id}/compliance",
        "ledger://applications/{application_id}/audit-trail",
        "ledger://agents/{agent_id}/performance",
        "ledger://agents/{agent_id}/sessions/{session_id}",
        "ledger://ledger/health",
    }


@pytest.mark.asyncio
async def test_mcp_lifecycle_runs_through_fastmcp_only(mcp_runtime) -> None:
    mcp = mcp_runtime
    application_id = f"app-{uuid4().hex[:8]}"
    credit_agent_id = "credit-agent-01"
    credit_session_id = f"session-{uuid4().hex[:6]}"
    credit_session_stream_id = f"agent-{credit_agent_id}-{credit_session_id}"
    fraud_agent_id = "fraud-agent-01"
    fraud_session_id = f"session-{uuid4().hex[:6]}"
    fraud_session_stream_id = f"agent-{fraud_agent_id}-{fraud_session_id}"

    submit_result = await mcp.call_tool(
        "submit_application",
        {
            "application_id": application_id,
            "applicant_id": "borrower-001",
            "applicant_name": "Northwind Manufacturing",
            "requested_amount_usd": 250000,
            "loan_purpose": "WORKING_CAPITAL",
            "submission_channel": "agent",
        },
    )
    assert submit_result["success"] is True

    session_result = await mcp.call_tool(
        "start_agent_session",
        {
            "agent_id": credit_agent_id,
            "session_id": credit_session_id,
            "context_source": "event_store",
            "event_replay_from_position": 0,
            "context_token_count": 4096,
            "model_version": "credit-model-v3.1",
        },
    )
    assert session_result["success"] is True

    analysis_result = await mcp.call_tool(
        "record_credit_analysis",
        {
            "application_id": application_id,
            "agent_id": credit_agent_id,
            "session_id": credit_session_id,
            "model_version": "credit-model-v3.1",
            "model_deployment_id": "deploy-credit-v3.1",
            "confidence_score": 0.91,
            "risk_tier": "LOW",
            "recommended_limit_usd": 200000,
            "analysis_duration_ms": 850,
            "input_data": {"cashflow": "healthy"},
        },
    )
    assert analysis_result["success"] is True

    fraud_session_result = await mcp.call_tool(
        "start_agent_session",
        {
            "agent_id": fraud_agent_id,
            "session_id": fraud_session_id,
            "context_source": "event_store",
            "event_replay_from_position": 0,
            "context_token_count": 2048,
            "model_version": "fraud-model-v2.4",
        },
    )
    assert fraud_session_result["success"] is True

    fraud_result = await mcp.call_tool(
        "record_fraud_screening",
        {
            "application_id": application_id,
            "agent_id": fraud_agent_id,
            "session_id": fraud_session_id,
            "fraud_score": 0.08,
            "screening_model_version": "fraud-model-v2.4",
            "model_deployment_id": "deploy-fraud-v2.4",
            "anomaly_flags": [],
            "input_data": {"device_risk": "low"},
        },
    )
    assert fraud_result["success"] is True

    first_check = await mcp.call_tool(
        "record_compliance_check",
        {
            "application_id": application_id,
            "rule_id": "KYC",
            "rule_version": "kyc-v1.2",
            "passed": True,
            "evidence_hash": "evidence-kyc",
            "regulation_set_version": "Basel-III-2026-Q1",
            "checks_required": ["KYC", "AML"],
        },
    )
    assert first_check["success"] is True
    assert first_check["compliance_status"] == "IN_PROGRESS"

    as_of_marker = datetime.now(timezone.utc).isoformat()
    await asyncio.sleep(0.02)

    second_check = await mcp.call_tool(
        "record_compliance_check",
        {
            "application_id": application_id,
            "rule_id": "AML",
            "rule_version": "aml-v3.0",
            "passed": True,
            "evidence_hash": "evidence-aml",
        },
    )
    assert second_check["success"] is True
    assert second_check["compliance_status"] == "CLEARED"

    decision_result = await mcp.call_tool(
        "generate_decision",
        {
            "application_id": application_id,
            "orchestrator_agent_id": "orchestrator-01",
            "recommendation": "APPROVE",
            "confidence_score": 0.88,
            "contributing_agent_sessions": [
                credit_session_stream_id,
                fraud_session_stream_id,
            ],
            "decision_basis_summary": "Credit and compliance checks passed.",
            "model_versions": {
                "orchestrator": "decision-model-v1",
                "credit": "credit-model-v3.1",
            },
        },
    )
    assert decision_result["success"] is True

    review_result = await mcp.call_tool(
        "record_human_review",
        {
            "application_id": application_id,
            "reviewer_id": "loan-officer-01",
            "override": False,
            "final_decision": "APPROVE",
        },
    )
    assert review_result["success"] is True

    role_error = await mcp.call_tool(
        "run_integrity_check",
        {
            "entity_type": "loan",
            "entity_id": application_id,
            "actor_role": "credit",
        },
    )
    assert role_error["success"] is False
    assert role_error["suggested_action"] == "check_preconditions"

    integrity_result = await mcp.call_tool(
        "run_integrity_check",
        {
            "entity_type": "loan",
            "entity_id": application_id,
            "actor_role": "compliance",
        },
    )
    assert integrity_result["success"] is True
    assert integrity_result["chain_valid"] is True

    rate_limited = await mcp.call_tool(
        "run_integrity_check",
        {
            "entity_type": "loan",
            "entity_id": application_id,
            "actor_role": "compliance",
        },
    )
    assert rate_limited["success"] is False
    assert rate_limited["suggested_action"] == "retry_later"

    application_view = await _wait_for_json(
        mcp,
        f"ledger://applications/{application_id}",
        lambda payload: payload.get("state") == "FINAL_APPROVED",
    )
    assert application_view["application_id"] == application_id
    assert application_view["state"] == "FINAL_APPROVED"

    current_compliance = await _wait_for_json(
        mcp,
        f"ledger://applications/{application_id}/compliance",
        lambda payload: payload.get("compliance_status") == "CLEARED",
    )
    assert current_compliance["compliance_status"] == "CLEARED"
    assert sorted(current_compliance["checks_passed"]) == ["AML", "KYC"]

    historical_compliance = await _read_json(
        mcp,
        (
            f"ledger://applications/{application_id}/compliance?"
            f"{urlencode({'as_of': as_of_marker})}"
        ),
    )
    assert historical_compliance["compliance_status"] == "IN_PROGRESS"
    assert historical_compliance["checks_passed"] == ["KYC"]
    assert historical_compliance["clearance_issued"] is False

    audit_trail = await _read_json(
        mcp,
        (
            f"ledger://applications/{application_id}/audit-trail?"
            f"{urlencode({'from': 1, 'to': 20})}"
        ),
    )
    event_types = {event["event_type"] for event in audit_trail["events"]}
    assert "ApplicationSubmitted" in event_types
    assert "CreditAnalysisCompleted" in event_types
    assert "FraudScreeningCompleted" in event_types
    assert "ComplianceRulePassed" in event_types
    assert "DecisionGenerated" in event_types
    assert "AuditIntegrityCheckRun" in event_types
    assert any(
        event["source_stream_id"] == credit_session_stream_id
        for event in audit_trail["events"]
        if event["source_stream_id"] is not None
    )
    assert any(
        event["source_stream_id"] == fraud_session_stream_id
        for event in audit_trail["events"]
        if event["source_stream_id"] is not None
    )

    health = await _wait_for_json(
        mcp,
        "ledger://ledger/health",
        lambda payload: payload.get("projections", {})
        .get("ComplianceAuditView", {})
        .get("lag_ms")
        == 0,
    )
    assert health["projection_failures"]["pending"] == 0
    assert health["projections"]["ComplianceAuditView"]["lag_ms"] == 0
