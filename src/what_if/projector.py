# =============================================================================
# TRP1 LEDGER - What-If Counterfactual Projections (Phase 6 Bonus)
# =============================================================================
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import structlog

from src.event_store import EventStore
from src.models.events import BaseEvent, StoredEvent

logger = structlog.get_logger()


@dataclass
class WhatIfScenario:
    """Definition of a counterfactual scenario."""

    name: str
    description: str
    stream_id: str
    inject_at_position: int
    hypothetical_events: list[BaseEvent]
    remove_event_types: list[str] = field(default_factory=list)


@dataclass
class WhatIfResult:
    """Result of a counterfactual analysis."""

    scenario_name: str
    original_event_count: int
    modified_event_count: int
    hypothetical_stream: list[StoredEvent]
    divergence_point: int
    state_differences: dict[str, Any] = field(default_factory=dict)
    analysis_notes: list[str] = field(default_factory=list)


async def run_what_if(
    store: EventStore,
    scenario: WhatIfScenario,
) -> WhatIfResult:
    """
    Run a counterfactual scenario without mutating the real event store.

    The projector loads the affected application history, removes events that
    are downstream of the branch point, injects hypothetical facts, and then
    derives a counterfactual ApplicationSummary-style outcome by replaying the
    modified event history through aggregate logic.
    """
    application_id = await _infer_application_id(store, scenario)
    if application_id is not None:
        real_events = await store.load_application_events(application_id)
    else:
        real_events = await store.load_stream(scenario.stream_id)

    if not real_events:
        return WhatIfResult(
            scenario_name=scenario.name,
            original_event_count=0,
            modified_event_count=0,
            hypothetical_stream=[],
            divergence_point=0,
            analysis_notes=["Stream is empty - no events to modify"],
        )

    target_stream_events = [
        event for event in real_events if event.stream_id == scenario.stream_id
    ]
    events_before = [
        event
        for event in target_stream_events
        if event.stream_position <= scenario.inject_at_position
    ]
    events_after = [
        event
        for event in target_stream_events
        if event.stream_position > scenario.inject_at_position
    ]
    removed_event_ids = {str(event.event_id) for event in events_after}
    dependent_event_ids = _collect_dependent_event_ids(real_events, removed_event_ids)
    dependent_event_ids.update(
        str(event.event_id)
        for event in events_after
        if event.event_type in scenario.remove_event_types
    )

    branch_global_position = (
        events_before[-1].global_position if events_before else scenario.inject_at_position
    )
    heuristic_removed_types = _dependent_event_types_for_stream(scenario.stream_id)
    dependent_event_ids.update(
        str(event.event_id)
        for event in real_events
        if event.global_position > branch_global_position
        and event.event_type in heuristic_removed_types
    )

    hypothetical_stored = _build_hypothetical_events(scenario)
    modified_stream = _apply_counterfactual(
        real_events=real_events,
        scenario=scenario,
        hypothetical_events=hypothetical_stored,
        dependent_event_ids=dependent_event_ids,
    )

    original_summary = await _application_summary_from_events(real_events, application_id)
    counterfactual_summary = await _application_summary_from_events(
        modified_stream,
        application_id,
    )
    if (
        counterfactual_summary.get("state") == "ANALYSIS_COMPLETE"
        and counterfactual_summary.get("compliance_required_checks")
        and set(counterfactual_summary.get("compliance_passed_checks", []))
        >= set(counterfactual_summary.get("compliance_required_checks", []))
    ):
        counterfactual_summary["state"] = "PENDING_DECISION"

    changed_fields = {
        key: {
            "before": original_summary.get(key),
            "after": counterfactual_summary.get(key),
        }
        for key in counterfactual_summary
        if original_summary.get(key) != counterfactual_summary.get(key)
    }
    decision_recomputation_required = (
        original_summary.get("state") in {"APPROVED_PENDING_HUMAN", "DECLINED_PENDING_HUMAN", "FINAL_APPROVED", "FINAL_DECLINED"}
        and counterfactual_summary.get("state") in {"PENDING_DECISION", "COMPLIANCE_REVIEW", "ANALYSIS_COMPLETE"}
    )

    result = WhatIfResult(
        scenario_name=scenario.name,
        original_event_count=len(real_events),
        modified_event_count=len(modified_stream),
        hypothetical_stream=modified_stream,
        divergence_point=scenario.inject_at_position + 1,
        state_differences={
            "application_id": application_id,
            "original_event_types": [event.event_type for event in real_events],
            "modified_event_types": [event.event_type for event in modified_stream],
            "injected_events": [event.event_type for event in scenario.hypothetical_events],
            "removed_events": [
                event.event_type
                for event in real_events
                if str(event.event_id) in dependent_event_ids
            ],
            "original_application_summary": original_summary,
            "counterfactual_application_summary": counterfactual_summary,
            "changed_fields": changed_fields,
            "decision_recomputation_required": decision_recomputation_required,
        },
        analysis_notes=[
            f"Original history contained {len(real_events)} events",
            f"Counterfactual history contains {len(modified_stream)} events",
            f"Branch point is stream position {scenario.inject_at_position + 1}",
            f"Injected {len(scenario.hypothetical_events)} hypothetical events",
            f"Filtered {len(dependent_event_ids)} downstream events after branching",
            (
                "Upstream change invalidated the downstream decision path; "
                "a fresh decision must be generated from the counterfactual state."
                if decision_recomputation_required
                else "Counterfactual state remains decision-complete after pruning."
            ),
        ],
    )

    logger.info(
        "what_if_completed",
        scenario=scenario.name,
        original_count=len(real_events),
        modified_count=len(modified_stream),
        changed_fields=sorted(changed_fields),
    )
    return result


async def _infer_application_id(
    store: EventStore,
    scenario: WhatIfScenario,
) -> str | None:
    if scenario.stream_id.startswith("loan-"):
        return scenario.stream_id.removeprefix("loan-")
    if scenario.stream_id.startswith("compliance-"):
        return scenario.stream_id.removeprefix("compliance-")

    for event in scenario.hypothetical_events:
        application_id = event.model_dump(mode="json").get("application_id")
        if application_id:
            return application_id

    for event in await store.load_stream(scenario.stream_id):
        application_id = event.payload.get("application_id")
        if application_id:
            return application_id

    return None


def _collect_dependent_event_ids(
    events: list[StoredEvent],
    seed_event_ids: set[str],
) -> set[str]:
    dependent = set(seed_event_ids)
    expanded = True
    while expanded:
        expanded = False
        for event in events:
            causation_id = event.metadata.get("causation_id")
            if causation_id and causation_id in dependent and str(event.event_id) not in dependent:
                dependent.add(str(event.event_id))
                expanded = True
    return dependent


def _dependent_event_types_for_stream(stream_id: str) -> set[str]:
    if stream_id.startswith(("agent-", "compliance-")):
        return {
            "DecisionGenerated",
            "HumanReviewCompleted",
            "ApplicationApproved",
            "ApplicationDeclined",
            "ApplicationUnderReview",
        }
    return set()


def _build_hypothetical_events(scenario: WhatIfScenario) -> list[StoredEvent]:
    now = datetime.now(timezone.utc)
    hypothetical_events: list[StoredEvent] = []
    for index, event in enumerate(scenario.hypothetical_events, start=1):
        hypothetical_events.append(
            StoredEvent(
                event_id=uuid4(),
                stream_id=scenario.stream_id,
                stream_position=scenario.inject_at_position + index,
                global_position=0,
                event_type=event.event_type,
                event_version=event.event_version,
                payload=event.model_dump(mode="json"),
                metadata={"hypothetical": True, "scenario": scenario.name},
                recorded_at=now,
            )
        )
    return hypothetical_events


def _apply_counterfactual(
    *,
    real_events: list[StoredEvent],
    scenario: WhatIfScenario,
    hypothetical_events: list[StoredEvent],
    dependent_event_ids: set[str],
) -> list[StoredEvent]:
    modified_stream: list[StoredEvent] = []
    inserted = False

    for event in real_events:
        if (
            event.stream_id == scenario.stream_id
            and event.stream_position > scenario.inject_at_position
        ):
            continue
        if str(event.event_id) in dependent_event_ids:
            continue

        if (
            not inserted
            and scenario.inject_at_position == 0
            and event.stream_id == scenario.stream_id
        ):
            modified_stream.extend(hypothetical_events)
            inserted = True

        modified_stream.append(event)

        if (
            not inserted
            and event.stream_id == scenario.stream_id
            and event.stream_position == scenario.inject_at_position
        ):
            modified_stream.extend(hypothetical_events)
            inserted = True

    if not inserted:
        modified_stream.extend(hypothetical_events)

    return modified_stream


async def _application_summary_from_events(
    events: list[StoredEvent],
    application_id: str | None,
) -> dict[str, Any]:
    if application_id is None:
        return {}

    state: dict[str, Any] = {
        "application_id": application_id,
        "state": None,
        "risk_tier": None,
        "fraud_score": None,
        "confidence_score": None,
        "recommendation": None,
        "approved_amount_usd": None,
        "compliance_required_checks": [],
        "compliance_passed_checks": [],
        "credit_analysis_done": False,
        "human_review_override": False,
    }

    for event in events:
        if (
            event.stream_id != f"loan-{application_id}"
            and event.payload.get("application_id") != application_id
        ):
            continue

        payload = event.payload
        if event.event_type == "ApplicationSubmitted":
            state["state"] = "SUBMITTED"
        elif event.event_type == "CreditAnalysisRequested":
            state["state"] = "AWAITING_ANALYSIS"
        elif event.event_type == "CreditAnalysisCompleted":
            state["state"] = "ANALYSIS_COMPLETE"
            state["credit_analysis_done"] = True
            state["risk_tier"] = payload.get("risk_tier")
            state["confidence_score"] = payload.get("confidence_score")
        elif event.event_type == "FraudScreeningCompleted":
            state["fraud_score"] = payload.get("fraud_score")
        elif event.event_type == "ComplianceCheckRequested":
            state["state"] = "COMPLIANCE_REVIEW"
            state["compliance_required_checks"] = sorted(
                payload.get("checks_required", [])
            )
        elif event.event_type == "ComplianceRulePassed":
            rule_id = payload.get("rule_id")
            if rule_id and rule_id not in state["compliance_passed_checks"]:
                state["compliance_passed_checks"].append(rule_id)
                state["compliance_passed_checks"].sort()
        elif event.event_type == "ComplianceClearanceIssued":
            state["state"] = "PENDING_DECISION"
            state["compliance_passed_checks"] = sorted(
                set(state["compliance_passed_checks"])
                | set(payload.get("checks_passed", []))
            )
        elif event.event_type == "DecisionGenerated":
            state["recommendation"] = payload.get("recommendation")
            state["confidence_score"] = payload.get("confidence_score")
            state["state"] = (
                "APPROVED_PENDING_HUMAN"
                if payload.get("recommendation") == "APPROVE"
                else "DECLINED_PENDING_HUMAN"
            )
        elif event.event_type == "HumanReviewCompleted":
            state["human_review_override"] = bool(payload.get("override", False))
            final_decision = payload.get("final_decision")
            if final_decision == "APPROVE":
                state["state"] = "FINAL_APPROVED"
            elif final_decision == "DECLINE":
                state["state"] = "FINAL_DECLINED"
        elif event.event_type == "ApplicationApproved":
            state["state"] = "FINAL_APPROVED"
            approved_amount = payload.get("approved_amount_usd")
            state["approved_amount_usd"] = (
                str(approved_amount) if approved_amount is not None else None
            )
        elif event.event_type == "ApplicationDeclined":
            state["state"] = "FINAL_DECLINED"

    return state
