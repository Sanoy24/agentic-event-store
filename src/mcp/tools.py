# src/mcp/tools.py
# =============================================================================
# TRP1 LEDGER — MCP Tools (Command Side)
# =============================================================================
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import structlog

from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.aggregates.loan_application import ApplicationState, LoanApplicationAggregate
from src.commands.handlers import (
    CreditAnalysisCompletedCommand,
    FraudScreeningCompletedCommand,
    GenerateDecisionCommand,
    HumanReviewCompletedCommand,
    RecordComplianceCheckCommand,
    RequestComplianceCheckCommand,
    StartAgentSessionCommand,
    SubmitApplicationCommand,
    handle_credit_analysis_completed,
    handle_fraud_screening_completed,
    handle_generate_decision,
    handle_human_review_completed,
    handle_record_compliance_check,
    handle_request_compliance_check,
    handle_start_agent_session,
    handle_submit_application,
)
from src.integrity.audit_chain import run_integrity_check as execute_integrity_check
from src.models.events import (
    CreditAnalysisRequested,
    DomainError,
    OptimisticConcurrencyError,
)

logger = structlog.get_logger()


def register_tools(mcp: Any, server: Any) -> None:
    """Register all MCP tools with the FastMCP server."""
    try:
        from mcp.server.fastmcp import FastMCP  # noqa: F401

        _register_with_fastmcp(mcp, server)
    except ImportError:
        logger.info("mcp_tools_registered_standalone")


def _register_with_fastmcp(mcp: Any, server: Any) -> None:
    """Register tools using FastMCP decorator API."""

    @mcp.tool()
    async def submit_application(
        application_id: str,
        applicant_id: str,
        applicant_name: str,
        requested_amount_usd: float,
        loan_purpose: str,
        submission_channel: str = "agent",
        correlation_id: str = "",
    ) -> dict:
        """
        Submit a new loan application.

        Preconditions: none. This creates the first event in the loan stream.
        """
        result = await _execute_command(
            server,
            handle_submit_application,
            SubmitApplicationCommand(
                application_id=application_id,
                applicant_id=applicant_id,
                applicant_name=applicant_name,
                requested_amount_usd=Decimal(str(requested_amount_usd)),
                loan_purpose=loan_purpose,
                submission_channel=submission_channel,
                correlation_id=correlation_id or f"mcp-{application_id}",
            ),
        )
        if not result["success"]:
            return result

        store = await server.get_store()
        return {
            "success": True,
            "stream_id": f"loan-{application_id}",
            "initial_version": await store.stream_version(f"loan-{application_id}"),
        }

    @mcp.tool()
    async def start_agent_session(
        agent_id: str,
        session_id: str,
        context_source: str,
        event_replay_from_position: int,
        context_token_count: int,
        model_version: str,
        correlation_id: str = "",
    ) -> dict:
        """
        Start an agent session.

        Preconditions: must be called before any agent decision tools. Calling
        decision tools without an active session will return a DomainError.
        """
        result = await _execute_command(
            server,
            handle_start_agent_session,
            StartAgentSessionCommand(
                agent_id=agent_id,
                session_id=session_id,
                context_source=context_source,
                event_replay_from_position=event_replay_from_position,
                context_token_count=context_token_count,
                model_version=model_version,
                correlation_id=correlation_id or f"mcp-agent-{agent_id}",
            ),
        )
        if not result["success"]:
            return result

        return {
            "success": True,
            "session_id": session_id,
            "context_position": event_replay_from_position,
        }

    @mcp.tool()
    async def record_credit_analysis(
        application_id: str,
        agent_id: str,
        session_id: str,
        model_version: str,
        model_deployment_id: str,
        confidence_score: float,
        risk_tier: str,
        recommended_limit_usd: float,
        analysis_duration_ms: int,
        input_data: dict | None = None,
        correlation_id: str = "",
    ) -> dict:
        """
        Record completion of credit analysis by an AI agent.

        Preconditions:
        - start_agent_session must already have been called for this session
        - the application must be awaiting analysis; if it is freshly submitted,
          this tool will open the analysis request before recording the result
        """
        effective_correlation = correlation_id or f"mcp-credit-{application_id}"
        bootstrap_error = await _ensure_credit_analysis_requested(
            server,
            application_id=application_id,
            assigned_agent_id=agent_id,
            correlation_id=effective_correlation,
        )
        if bootstrap_error is not None:
            return bootstrap_error

        result = await _execute_command(
            server,
            handle_credit_analysis_completed,
            CreditAnalysisCompletedCommand(
                application_id=application_id,
                agent_id=agent_id,
                session_id=session_id,
                model_version=model_version,
                model_deployment_id=model_deployment_id,
                confidence_score=confidence_score,
                risk_tier=risk_tier,
                recommended_limit_usd=Decimal(str(recommended_limit_usd)),
                duration_ms=analysis_duration_ms,
                input_data=input_data or {},
                correlation_id=effective_correlation,
            ),
        )
        if not result["success"]:
            return result

        stream_id = f"agent-{agent_id}-{session_id}"
        latest_event = await _latest_event(await server.get_store(), stream_id)
        return {
            "success": True,
            "event_id": str(latest_event.event_id) if latest_event else None,
            "new_stream_version": latest_event.stream_position if latest_event else None,
        }

    @mcp.tool()
    async def record_fraud_screening(
        application_id: str,
        agent_id: str,
        session_id: str,
        fraud_score: float,
        screening_model_version: str,
        model_deployment_id: str,
        anomaly_flags: list[str] | None = None,
        input_data: dict | None = None,
        correlation_id: str = "",
    ) -> dict:
        """
        Record completion of fraud screening by a FraudDetection agent.

        Preconditions:
        - start_agent_session must already have been called for this session
        - fraud_score must be between 0.0 and 1.0 inclusive
        """
        result = await _execute_command(
            server,
            handle_fraud_screening_completed,
            FraudScreeningCompletedCommand(
                application_id=application_id,
                agent_id=agent_id,
                session_id=session_id,
                fraud_score=fraud_score,
                anomaly_flags=anomaly_flags or [],
                screening_model_version=screening_model_version,
                model_deployment_id=model_deployment_id,
                input_data=input_data or {},
                correlation_id=correlation_id or f"mcp-fraud-{application_id}",
            ),
        )
        if not result["success"]:
            return result

        stream_id = f"agent-{agent_id}-{session_id}"
        latest_event = await _latest_event(await server.get_store(), stream_id)
        return {
            "success": True,
            "event_id": str(latest_event.event_id) if latest_event else None,
            "new_stream_version": latest_event.stream_position if latest_event else None,
        }

    @mcp.tool()
    async def record_compliance_check(
        application_id: str,
        rule_id: str,
        rule_version: str,
        passed: bool,
        failure_reason: str | None = None,
        remediation_required: bool = False,
        evidence_hash: str = "",
        regulation_set_version: str | None = None,
        checks_required: list[str] | None = None,
        correlation_id: str = "",
    ) -> dict:
        """
        Record a compliance rule result.

        Preconditions:
        - the rule must belong to an active regulation set
        - if this is the first compliance event for the application, include
          regulation_set_version and checks_required to initialize the record
        """
        effective_correlation = correlation_id or f"mcp-compliance-{application_id}"
        bootstrap_error = await _ensure_compliance_requested(
            server,
            application_id=application_id,
            regulation_set_version=regulation_set_version,
            checks_required=checks_required,
            correlation_id=effective_correlation,
        )
        if bootstrap_error is not None:
            return bootstrap_error

        result = await _execute_command(
            server,
            handle_record_compliance_check,
            RecordComplianceCheckCommand(
                application_id=application_id,
                rule_id=rule_id,
                rule_version=rule_version,
                passed=passed,
                failure_reason=failure_reason,
                remediation_required=remediation_required,
                evidence_hash=evidence_hash,
                correlation_id=effective_correlation,
            ),
        )
        if not result["success"]:
            return result

        store = await server.get_store()
        events = await store.load_stream(f"compliance-{application_id}")
        check_event = next(
            (
                event
                for event in reversed(events)
                if event.event_type in {"ComplianceRulePassed", "ComplianceRuleFailed"}
                and event.payload.get("rule_id") == rule_id
            ),
            None,
        )
        compliance = await ComplianceRecordAggregate.load(store, application_id)
        return {
            "success": True,
            "check_id": str(check_event.event_id) if check_event else None,
            "compliance_status": _compliance_status(compliance),
        }

    @mcp.tool()
    async def generate_decision(
        application_id: str,
        orchestrator_agent_id: str,
        recommendation: str,
        confidence_score: float,
        contributing_agent_sessions: list[str],
        decision_basis_summary: str,
        model_versions: dict[str, str],
        correlation_id: str = "",
    ) -> dict:
        """
        Generate a loan decision via the DecisionOrchestrator.

        Preconditions:
        - all required analyses and compliance clearance must already exist
        - confidence_score below 0.6 will be forced to REFER
        """
        result = await _execute_command(
            server,
            handle_generate_decision,
            GenerateDecisionCommand(
                application_id=application_id,
                orchestrator_agent_id=orchestrator_agent_id,
                recommendation=recommendation,
                confidence_score=confidence_score,
                contributing_agent_sessions=contributing_agent_sessions,
                decision_basis_summary=decision_basis_summary,
                model_versions=model_versions,
                correlation_id=correlation_id or f"mcp-decision-{application_id}",
            ),
        )
        if not result["success"]:
            return result

        latest_event = await _latest_event(
            await server.get_store(),
            f"loan-{application_id}",
        )
        return {
            "success": True,
            "decision_id": str(latest_event.event_id) if latest_event else None,
            "recommendation": latest_event.payload.get("recommendation")
            if latest_event
            else recommendation,
        }

    @mcp.tool()
    async def record_human_review(
        application_id: str,
        reviewer_id: str,
        override: bool,
        final_decision: str,
        override_reason: str | None = None,
        correlation_id: str = "",
    ) -> dict:
        """
        Record a human review of an AI-generated recommendation.

        Preconditions:
        - the application must be pending human review
        - if override is true, override_reason is required
        """
        result = await _execute_command(
            server,
            handle_human_review_completed,
            HumanReviewCompletedCommand(
                application_id=application_id,
                reviewer_id=reviewer_id,
                override=override,
                final_decision=final_decision,
                override_reason=override_reason,
                correlation_id=correlation_id or f"mcp-review-{application_id}",
            ),
        )
        if not result["success"]:
            return result

        app = await LoanApplicationAggregate.load(await server.get_store(), application_id)
        return {
            "success": True,
            "final_decision": final_decision,
            "application_state": str(app.state) if app.state is not None else None,
        }

    @mcp.tool()
    async def run_integrity_check(
        entity_type: str,
        entity_id: str,
        actor_role: str = "compliance",
    ) -> dict:
        """
        Run a cryptographic integrity check on an entity audit chain.

        Preconditions:
        - actor_role must be `compliance`
        - the same entity may only be checked once per minute
        """
        try:
            store = await server.get_store()
            validation_error = await _ensure_integrity_check_allowed(
                store,
                entity_type=entity_type,
                entity_id=entity_id,
                actor_role=actor_role,
            )
            if validation_error is not None:
                return validation_error

            result = await execute_integrity_check(
                store=store,
                entity_type=entity_type,
                entity_id=entity_id,
            )
            return {
                "success": True,
                "check_result": {
                    "entity_type": result.entity_type,
                    "entity_id": result.entity_id,
                    "events_verified_count": result.events_verified_count,
                    "integrity_hash": result.integrity_hash,
                    "previous_hash": result.previous_hash,
                },
                "chain_valid": result.chain_valid,
                "tamper_detected": result.tamper_detected,
            }
        except Exception as e:
            return {
                "success": False,
                "error_type": type(e).__name__,
                "message": str(e),
                "context": {
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                },
                "suggested_action": "retry_or_contact_support",
            }


async def _execute_command(server: Any, handler_fn: Any, command: Any) -> dict:
    try:
        store = await server.get_store()
        await handler_fn(command, store)
        return {"success": True}
    except OptimisticConcurrencyError as e:
        return {
            "success": False,
            "error_type": "OptimisticConcurrencyError",
            "message": str(e),
            "context": {
                "stream_id": e.stream_id,
                "expected_version": e.expected_version,
                "actual_version": e.actual_version,
            },
            "suggested_action": e.suggested_action,
        }
    except DomainError as e:
        return {
            "success": False,
            "error_type": "DomainError",
            "message": str(e),
            "context": {
                "command_type": type(command).__name__,
            },
            "suggested_action": e.suggested_action or "check_preconditions",
        }
    except Exception as e:
        logger.exception("mcp_tool_error", command_type=type(command).__name__)
        return {
            "success": False,
            "error_type": type(e).__name__,
            "message": str(e),
            "context": {
                "command_type": type(command).__name__,
            },
            "suggested_action": "retry_or_contact_support",
        }


async def _latest_event(store: Any, stream_id: str) -> Any | None:
    events = await store.load_stream(stream_id)
    return events[-1] if events else None


async def _ensure_credit_analysis_requested(
    server: Any,
    *,
    application_id: str,
    assigned_agent_id: str,
    correlation_id: str,
) -> dict | None:
    store = await server.get_store()
    app = await LoanApplicationAggregate.load(store, application_id)
    if app.state == ApplicationState.SUBMITTED:
        await store.append(
            stream_id=f"loan-{application_id}",
            events=[
                CreditAnalysisRequested(
                    application_id=application_id,
                    assigned_agent_id=assigned_agent_id,
                    requested_at=datetime.now(timezone.utc),
                    priority="HIGH",
                )
            ],
            expected_version=app.version,
            correlation_id=correlation_id,
        )
    elif app.state not in {
        ApplicationState.AWAITING_ANALYSIS,
        ApplicationState.ANALYSIS_COMPLETE,
        ApplicationState.COMPLIANCE_REVIEW,
        ApplicationState.PENDING_DECISION,
        ApplicationState.APPROVED_PENDING_HUMAN,
        ApplicationState.DECLINED_PENDING_HUMAN,
        ApplicationState.FINAL_APPROVED,
        ApplicationState.FINAL_DECLINED,
    }:
        return {
            "success": False,
            "error_type": "DomainError",
            "message": f"Application {application_id} is not ready for analysis.",
            "context": {
                "application_id": application_id,
                "current_state": str(app.state) if app.state else "unknown",
            },
            "suggested_action": "check_preconditions",
        }
    return None


async def _ensure_compliance_requested(
    server: Any,
    *,
    application_id: str,
    regulation_set_version: str | None,
    checks_required: list[str] | None,
    correlation_id: str,
) -> dict | None:
    store = await server.get_store()
    compliance = await ComplianceRecordAggregate.load(store, application_id)
    if compliance.version > 0:
        return None
    if not regulation_set_version or not checks_required:
        return {
            "success": False,
            "error_type": "DomainError",
            "message": (
                "First compliance check requires regulation_set_version and "
                "checks_required."
            ),
            "context": {
                "application_id": application_id,
                "missing_fields": [
                    f for f in ["regulation_set_version", "checks_required"]
                    if not locals().get(f)
                ],
            },
            "suggested_action": "provide_regulation_set_version_and_checks_required",
        }

    await handle_request_compliance_check(
        RequestComplianceCheckCommand(
            application_id=application_id,
            regulation_set_version=regulation_set_version,
            checks_required=checks_required,
            correlation_id=correlation_id,
        ),
        store,
    )
    return None


def _compliance_status(compliance: ComplianceRecordAggregate) -> str:
    if compliance.clearance_issued:
        return "CLEARED"
    if compliance.failed_checks:
        return "FAILED"
    if compliance.required_checks:
        return "IN_PROGRESS"
    return "NO_RECORD"


async def _ensure_integrity_check_allowed(
    store: Any,
    *,
    entity_type: str,
    entity_id: str,
    actor_role: str,
) -> dict | None:
    if actor_role.lower() != "compliance":
        return {
            "success": False,
            "error_type": "DomainError",
            "message": "run_integrity_check may only be called by compliance role.",
            "context": {
                "provided_role": actor_role,
                "required_role": "compliance",
            },
            "suggested_action": "use_compliance_role",
        }

    audit_events = await store.load_stream(f"audit-{entity_type}-{entity_id}")
    for audit_event in reversed(audit_events):
        if audit_event.event_type != "AuditIntegrityCheckRun":
            continue
        delta = datetime.now(timezone.utc) - audit_event.recorded_at
        if delta.total_seconds() < 60:
            return {
                "success": False,
                "error_type": "DomainError",
                "message": (
                    "run_integrity_check is rate-limited to one call per minute "
                    f"for {entity_type}/{entity_id}."
                ),
                "context": {
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "seconds_since_last_check": int(delta.total_seconds()),
                },
                "suggested_action": "retry_after_60_seconds",
            }
        break

    return None
