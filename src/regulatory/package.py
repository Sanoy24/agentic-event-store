# =============================================================================
# TRP1 LEDGER - Regulatory Examination Package (Phase 6 Bonus)
# =============================================================================
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import structlog

from src.event_store import EventStore
from src.integrity.audit_chain import _compute_chain_hash, _hash_event
from src.models.events import StoredEvent

logger = structlog.get_logger()


@dataclass
class RegulatoryPackage:
    """Self-contained regulatory examination package."""

    package_id: str
    generated_at: str
    examination_date: str
    application_id: str
    event_stream: list[dict]
    projection_states: dict[str, Any]
    agent_sessions: list[dict]
    integrity_check: dict[str, Any]
    model_provenance: list[dict[str, Any]]
    narrative: list[str]
    package_hash: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


async def generate_regulatory_package(
    store: EventStore,
    application_id: str,
    examination_date: datetime,
) -> RegulatoryPackage:
    """
    Generate a self-contained regulatory package as of an examination date.

    The package is read-only: it never appends new integrity-check events or
    otherwise mutates the store during generation.
    """
    if examination_date.tzinfo is None:
        examination_date = examination_date.replace(tzinfo=timezone.utc)

    generated_at = datetime.now(timezone.utc)
    application_events = await store.load_application_events(application_id)
    events_as_of = [
        event for event in application_events if event.recorded_at <= examination_date
    ]

    agent_session_streams = _collect_agent_session_streams(events_as_of)
    agent_sessions = []
    for stream_id in sorted(agent_session_streams):
        session_events = [
            event
            for event in await store.load_stream(stream_id)
            if event.recorded_at <= examination_date
        ]
        if not session_events:
            continue
        agent_id, session_id = _parse_session_identity(stream_id, session_events)
        agent_sessions.append(
            {
                "agent_id": agent_id,
                "session_id": session_id,
                "stream_id": stream_id,
                "events": [_serialize_event(event) for event in session_events],
                "reconstructed_context": _session_context_from_events(
                    agent_id,
                    session_id,
                    session_events,
                ),
            }
        )

    compliance_events = [
        event
        for event in events_as_of
        if event.stream_id == f"compliance-{application_id}"
    ]
    projection_states = {
        "application_summary": _application_summary_from_events(
            application_id,
            events_as_of,
        ),
        "compliance_audit_view": _compliance_summary_from_events(
            application_id,
            compliance_events,
        ),
        "agent_performance_ledger": _agent_performance_from_events(events_as_of),
    }
    integrity_check = await _integrity_as_of(store, application_id, examination_date)
    model_provenance = _collect_model_provenance(events_as_of, agent_sessions)
    narrative = _generate_narrative(events_as_of)

    package = RegulatoryPackage(
        package_id=f"reg-pkg-{application_id}-{generated_at.strftime('%Y%m%d%H%M%S')}",
        generated_at=generated_at.isoformat(),
        examination_date=examination_date.isoformat(),
        application_id=application_id,
        event_stream=[_serialize_event(event) for event in events_as_of],
        projection_states=projection_states,
        agent_sessions=agent_sessions,
        integrity_check=integrity_check,
        model_provenance=model_provenance,
        narrative=narrative,
        metadata={
            "generator": "TRP1 Ledger Regulatory Package Generator",
            "version": "2.0.0",
            "event_count": len(events_as_of),
            "agent_session_count": len(agent_sessions),
        },
    )

    package.package_hash = hashlib.sha256(
        json.dumps(asdict(package), sort_keys=True, default=str).encode()
    ).hexdigest()

    logger.info(
        "regulatory_package_generated",
        application_id=application_id,
        examination_date=examination_date.isoformat(),
        event_count=len(events_as_of),
    )
    return package


def export_package_to_json(package: RegulatoryPackage) -> str:
    """Export a regulatory package to pretty-printed JSON."""
    return json.dumps(asdict(package), indent=2, default=str)


def _serialize_event(event: StoredEvent) -> dict[str, Any]:
    return {
        "event_id": str(event.event_id),
        "stream_id": event.stream_id,
        "stream_position": event.stream_position,
        "global_position": event.global_position,
        "event_type": event.event_type,
        "event_version": event.event_version,
        "payload": event.payload,
        "metadata": event.metadata,
        "recorded_at": event.recorded_at.isoformat(),
    }


def _collect_agent_session_streams(events: list[StoredEvent]) -> set[str]:
    session_streams = {
        event.stream_id for event in events if event.stream_id.startswith("agent-")
    }
    for event in events:
        if event.event_type == "DecisionGenerated":
            session_streams.update(event.payload.get("contributing_agent_sessions", []))
    return session_streams


def _parse_session_identity(
    stream_id: str,
    events: list[StoredEvent],
) -> tuple[str, str]:
    for event in events:
        agent_id = event.payload.get("agent_id")
        session_id = event.payload.get("session_id")
        if agent_id and session_id:
            return agent_id, session_id
    fallback = stream_id.removeprefix("agent-")
    if "-session-" in fallback:
        agent_id, _, session_suffix = fallback.rpartition("-session-")
        if agent_id:
            return agent_id, f"session-{session_suffix}"
    return fallback, "unknown-session"


def _session_context_from_events(
    agent_id: str,
    session_id: str,
    events: list[StoredEvent],
) -> dict[str, Any]:
    context_loaded = next(
        (event for event in events if event.event_type == "AgentContextLoaded"),
        None,
    )
    completed_applications = sorted(
        {
            event.payload.get("application_id")
            for event in events
            if event.payload.get("application_id")
            and event.event_type in {"CreditAnalysisCompleted", "FraudScreeningCompleted"}
        }
    )
    return {
        "agent_id": agent_id,
        "session_id": session_id,
        "session_health": "HEALTHY" if context_loaded else "NO_CONTEXT",
        "model_version": context_loaded.payload.get("model_version")
        if context_loaded
        else None,
        "context_source": context_loaded.payload.get("context_source")
        if context_loaded
        else None,
        "total_events": len(events),
        "completed_applications": completed_applications,
        "last_event_position": events[-1].stream_position if events else 0,
    }


def _application_summary_from_events(
    application_id: str,
    events: list[StoredEvent],
) -> dict[str, Any]:
    """Reconstruct the ApplicationSummary projection state as-of a timestamp."""
    state = {
        "application_id": application_id,
        "state": None,
        "applicant_id": None,
        "applicant_name": None,
        "requested_amount_usd": None,
        "risk_tier": None,
        "confidence_score": None,
        "fraud_score": None,
        "compliance_status": None,
        "decision": None,
        "human_reviewer_id": None,
        "override": None,
        "override_reason": None,
        "final_decision": None,
        "approved_amount_usd": None,
        "interest_rate": None,
        "conditions": [],
        "final_decision_at": None,
        "last_event_type": None,
        "last_event_at": None,
        "agent_sessions": [],
        "decline_reasons": [],
        "created_at": None,
        "updated_at": None,
    }

    for event in events:
        payload = event.payload
        if payload.get("application_id") != application_id and event.stream_id != f"loan-{application_id}":
            continue

        state["last_event_type"] = event.event_type
        state["last_event_at"] = event.recorded_at.isoformat()
        state["updated_at"] = event.recorded_at.isoformat()
        if state["created_at"] is None:
            state["created_at"] = event.recorded_at.isoformat()

        if event.event_type == "ApplicationSubmitted":
            state["state"] = "SUBMITTED"
            state["applicant_id"] = payload.get("applicant_id")
            state["applicant_name"] = payload.get("applicant_name")
            state["requested_amount_usd"] = str(payload.get("requested_amount_usd"))
        elif event.event_type == "CreditAnalysisRequested":
            state["state"] = "AWAITING_ANALYSIS"
        elif event.event_type == "CreditAnalysisCompleted":
            state["state"] = "ANALYSIS_COMPLETE"
            state["risk_tier"] = payload.get("risk_tier")
            state["confidence_score"] = payload.get("confidence_score")
            state["agent_sessions"].append(payload.get("session_id", ""))
        elif event.event_type == "FraudScreeningCompleted":
            state["fraud_score"] = payload.get("fraud_score")
        elif event.event_type == "ComplianceCheckRequested":
            state["state"] = "COMPLIANCE_REVIEW"
            state["compliance_status"] = "IN_PROGRESS"
        elif event.event_type == "ComplianceClearanceIssued":
            state["state"] = "PENDING_DECISION"
            state["compliance_status"] = "CLEARED"
        elif event.event_type == "DecisionGenerated":
            recommendation = payload.get("recommendation")
            state["state"] = (
                "APPROVED_PENDING_HUMAN"
                if recommendation == "APPROVE"
                else "DECLINED_PENDING_HUMAN"
            )
            state["decision"] = recommendation
            state["confidence_score"] = payload.get("confidence_score")
        elif event.event_type == "ApplicationUnderReview":
            state["human_reviewer_id"] = payload.get("assigned_reviewer_id")
        elif event.event_type == "HumanReviewCompleted":
            final_decision = payload.get("final_decision")
            state["state"] = (
                "FINAL_APPROVED"
                if final_decision == "APPROVE"
                else "FINAL_DECLINED" if final_decision == "DECLINE" else state["state"]
            )
            state["human_reviewer_id"] = payload.get("reviewer_id")
            state["override"] = payload.get("override", False)
            state["override_reason"] = payload.get("override_reason")
            state["final_decision"] = final_decision
            state["final_decision_at"] = event.recorded_at.isoformat()
        elif event.event_type == "ApplicationApproved":
            state["state"] = "FINAL_APPROVED"
            state["approved_amount_usd"] = str(payload.get("approved_amount_usd"))
            state["interest_rate"] = str(payload.get("interest_rate"))
            state["conditions"] = list(payload.get("conditions", []))
            state["final_decision"] = "APPROVED"
            state["final_decision_at"] = payload.get("effective_date")
        elif event.event_type == "ApplicationDeclined":
            state["state"] = "FINAL_DECLINED"
            state["decline_reasons"] = list(payload.get("decline_reasons", []))
            state["final_decision"] = "DECLINED"
            state["final_decision_at"] = event.recorded_at.isoformat()

    return state


def _compliance_summary_from_events(
    application_id: str,
    events: list[StoredEvent],
) -> dict[str, Any]:
    regulation_set_version = None
    checks_required: set[str] = set()
    checks_passed: set[str] = set()
    checks_failed: set[str] = set()
    rule_results: dict[str, dict[str, Any]] = {}
    clearance_timestamp = None
    clearance_issued_by = None
    last_event_id = None
    last_global_position = 0
    last_event_type = None
    last_event_at = None

    for event in events:
        last_event_id = str(event.event_id)
        last_global_position = event.global_position
        last_event_type = event.event_type
        last_event_at = event.recorded_at.isoformat()
        if event.event_type == "ComplianceCheckRequested":
            regulation_set_version = event.payload.get("regulation_set_version")
            checks_required = set(event.payload.get("checks_required", []))
        elif event.event_type == "ComplianceRulePassed":
            rule_id = event.payload.get("rule_id")
            if rule_id:
                checks_passed.add(rule_id)
                rule_results[rule_id] = {
                    "rule_version": event.payload.get("rule_version"),
                    "verdict": "PASS",
                    "event_timestamp": event.recorded_at.isoformat(),
                }
        elif event.event_type == "ComplianceRuleFailed":
            rule_id = event.payload.get("rule_id")
            if rule_id:
                checks_failed.add(rule_id)
                rule_results[rule_id] = {
                    "rule_version": event.payload.get("rule_version"),
                    "verdict": "FAIL",
                    "event_timestamp": event.recorded_at.isoformat(),
                    "failure_reason": event.payload.get("failure_reason"),
                }
        elif event.event_type == "ComplianceClearanceIssued":
            clearance_timestamp = event.payload.get("clearance_timestamp")
            clearance_issued_by = event.payload.get("issued_by")

    if clearance_timestamp:
        status = "CLEARED"
    elif checks_failed:
        status = "FAILED"
    elif checks_required:
        status = "IN_PROGRESS"
    else:
        status = "NO_RECORD"

    return {
        "application_id": application_id,
        "regulation_set_version": regulation_set_version,
        "checks_required": sorted(checks_required),
        "checks_passed": sorted(checks_passed),
        "checks_failed": sorted(checks_failed),
        "rule_results": rule_results,
        "compliance_status": status,
        "clearance_issued": clearance_timestamp is not None,
        "clearance_timestamp": clearance_timestamp,
        "clearance_issued_by": clearance_issued_by,
        "events_processed": len(events),
        "last_event_id": last_event_id,
        "last_global_position": last_global_position,
        "last_event_type": last_event_type,
        "last_event_at": last_event_at,
        "updated_at": last_event_at,
    }


def _agent_performance_from_events(
    events_as_of: list[StoredEvent],
) -> list[dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for event in events_as_of:
        payload = event.payload
        if event.event_type == "CreditAnalysisCompleted":
            key = (payload.get("agent_id", "unknown"), payload.get("model_version", "unknown"))
            row = _ensure_agent_performance_row(rows, key, event.recorded_at.isoformat())
            row["analyses_completed"] += 1
            row["total_confidence"] += float(payload.get("confidence_score", 0.0))
            row["total_duration_ms"] += int(payload.get("analysis_duration_ms", 0))
            row["last_seen_at"] = event.recorded_at.isoformat()
        elif event.event_type == "FraudScreeningCompleted":
            key = (
                payload.get("agent_id", "unknown"),
                payload.get("screening_model_version", "unknown"),
            )
            row = _ensure_agent_performance_row(rows, key, event.recorded_at.isoformat())
            row["analyses_completed"] += 1
            row["last_seen_at"] = event.recorded_at.isoformat()
        elif event.event_type == "DecisionGenerated":
            model_version = payload.get("model_versions", {}).get("orchestrator", "unknown")
            key = (payload.get("orchestrator_agent_id", "unknown"), model_version)
            row = _ensure_agent_performance_row(rows, key, event.recorded_at.isoformat())
            row["decisions_generated"] += 1
            recommendation = payload.get("recommendation")
            if recommendation == "APPROVE":
                row["approve_count"] += 1
            elif recommendation == "DECLINE":
                row["decline_count"] += 1
            elif recommendation == "REFER":
                row["refer_count"] += 1
            row["last_seen_at"] = event.recorded_at.isoformat()
        elif event.event_type == "HumanReviewCompleted" and payload.get("override", False):
            key = (payload.get("reviewer_id", "unknown"), "human")
            row = _ensure_agent_performance_row(rows, key, event.recorded_at.isoformat())
            row["human_override_count"] += 1
            row["last_seen_at"] = event.recorded_at.isoformat()
        elif event.event_type == "AgentDecisionSuperseded":
            key = (payload.get("agent_id", "unknown"), payload.get("model_version", "unknown"))
            row = _ensure_agent_performance_row(rows, key, event.recorded_at.isoformat())
            row["superseded_count"] += 1
            row["last_seen_at"] = event.recorded_at.isoformat()

    return [
        rows[key]
        for key in sorted(rows, key=lambda item: (item[0], item[1]))
    ]


def _ensure_agent_performance_row(
    rows: dict[tuple[str, str], dict[str, Any]],
    key: tuple[str, str],
    seen_at: str,
) -> dict[str, Any]:
    return rows.setdefault(
        key,
        {
            "agent_id": key[0],
            "model_version": key[1],
            "analyses_completed": 0,
            "decisions_generated": 0,
            "approve_count": 0,
            "decline_count": 0,
            "refer_count": 0,
            "human_override_count": 0,
            "superseded_count": 0,
            "total_confidence": 0.0,
            "total_duration_ms": 0,
            "first_seen_at": seen_at,
            "last_seen_at": seen_at,
        },
    )


async def _integrity_as_of(
    store: EventStore,
    application_id: str,
    examination_date: datetime,
) -> dict[str, Any]:
    audit_events = [
        event
        for event in await store.load_stream(f"audit-loan-{application_id}")
        if event.recorded_at <= examination_date
    ]
    linked_events = [event for event in audit_events if event.event_type == "AuditEventLinked"]
    previous_hash = "genesis"
    verified_count = 0
    checked_at = None
    for audit_event in reversed(audit_events):
        if audit_event.event_type != "AuditIntegrityCheckRun":
            continue
        previous_hash = audit_event.payload.get("integrity_hash", "genesis")
        verified_count = audit_event.payload.get("events_verified_count", 0)
        checked_at = audit_event.payload.get("check_timestamp") or audit_event.recorded_at.isoformat()
        break

    verified_slice = linked_events[:verified_count]
    expected_hash = _compute_chain_hash(
        "genesis",
        [_hash_event(event) for event in verified_slice],
    )
    tamper_detected = verified_count > 0 and expected_hash != previous_hash
    return {
        "chain_valid": not tamper_detected,
        "tamper_detected": tamper_detected,
        "events_verified_count": verified_count,
        "integrity_hash": previous_hash,
        "checked_at": checked_at,
    }


def _collect_model_provenance(
    events_as_of: list[StoredEvent],
    agent_sessions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    provenance: list[dict[str, Any]] = []
    for session in agent_sessions:
        context = session["reconstructed_context"]
        for event in session["events"]:
            payload = event["payload"]
            if event["event_type"] == "CreditAnalysisCompleted":
                provenance.append(
                    {
                        "agent_id": payload.get("agent_id"),
                        "session_id": session["session_id"],
                        "decision_type": "CreditAnalysisCompleted",
                        "model_version": payload.get("model_version") or context.get("model_version"),
                        "confidence_score": payload.get("confidence_score"),
                        "input_data_hash": payload.get("input_data_hash"),
                    }
                )
            elif event["event_type"] == "FraudScreeningCompleted":
                provenance.append(
                    {
                        "agent_id": payload.get("agent_id"),
                        "session_id": session["session_id"],
                        "decision_type": "FraudScreeningCompleted",
                        "model_version": payload.get("screening_model_version") or context.get("model_version"),
                        "confidence_score": None,
                        "input_data_hash": payload.get("input_data_hash"),
                    }
                )

    for event in events_as_of:
        if event.event_type != "DecisionGenerated":
            continue
        provenance.append(
            {
                "agent_id": event.payload.get("orchestrator_agent_id"),
                "session_id": None,
                "decision_type": "DecisionGenerated",
                "model_version": event.payload.get("model_versions", {}).get("orchestrator"),
                "confidence_score": event.payload.get("confidence_score"),
                "input_data_hash": None,
            }
        )
    return provenance


_EVENT_NARRATIVES = {
    "ApplicationSubmitted": lambda p: (
        f"Loan application submitted by {p.get('applicant_name', 'unknown')} for "
        f"${p.get('requested_amount_usd', '?')}."
    ),
    "CreditAnalysisRequested": lambda p: (
        f"Credit analysis requested for agent {p.get('assigned_agent_id', 'unknown')}."
    ),
    "CreditAnalysisCompleted": lambda p: (
        f"Credit analysis completed with risk tier {p.get('risk_tier', '?')} and confidence "
        f"{p.get('confidence_score', '?')}."
    ),
    "FraudScreeningCompleted": lambda p: (
        f"Fraud screening completed with score {p.get('fraud_score', '?')}."
    ),
    "ComplianceCheckRequested": lambda p: (
        f"Compliance review opened under regulation set {p.get('regulation_set_version', 'unknown')}."
    ),
    "ComplianceRulePassed": lambda p: (
        f"Compliance rule {p.get('rule_id', '?')} passed."
    ),
    "ComplianceRuleFailed": lambda p: (
        f"Compliance rule {p.get('rule_id', '?')} failed."
    ),
    "ComplianceClearanceIssued": lambda p: "Compliance clearance issued.",
    "DecisionGenerated": lambda p: (
        f"Decision generated: {p.get('recommendation', '?')}."
    ),
    "HumanReviewCompleted": lambda p: (
        f"Human review completed with final decision {p.get('final_decision', '?')}."
    ),
    "ApplicationApproved": lambda p: (
        f"Application approved for ${p.get('approved_amount_usd', '?')}."
    ),
    "ApplicationDeclined": lambda p: "Application declined.",
    "AgentContextLoaded": lambda p: (
        f"Agent context loaded from {p.get('context_source', '?')} using model "
        f"{p.get('model_version', '?')}."
    ),
}


def _generate_narrative(events: list[StoredEvent]) -> list[str]:
    narrative: list[str] = []
    for event in events:
        narrator = _EVENT_NARRATIVES.get(event.event_type)
        if narrator is None:
            continue
        timestamp = event.recorded_at.isoformat()
        narrative.append(f"[{timestamp}] {narrator(event.payload)}")
    return narrative
