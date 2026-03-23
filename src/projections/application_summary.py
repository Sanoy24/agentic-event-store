# src/projections/application_summary.py
# =============================================================================
# TRP1 LEDGER — ApplicationSummary Projection
# =============================================================================
# Source: Challenge Doc Phase 3 p.12 (ApplicationSummary)
#
# One denormalised row per application. Populated by upsert on each relevant
# event. This is the primary read model for dashboards and API queries.
#
# SLO: projection lag < 500ms (p99).
#
# Idempotency: uses INSERT ... ON CONFLICT DO UPDATE. Replaying the same
# event always produces the same row state.
# =============================================================================
from __future__ import annotations

import json
from typing import Any

from src.models.events import StoredEvent
from src.projections.daemon import Projection


_INTERESTED_EVENTS = {
    "ApplicationSubmitted",
    "CreditAnalysisRequested",
    "CreditAnalysisCompleted",
    "FraudScreeningCompleted",
    "ComplianceCheckRequested",
    "ComplianceClearanceIssued",
    "DecisionGenerated",
    "HumanReviewCompleted",
    "ApplicationApproved",
    "ApplicationDeclined",
    "ApplicationUnderReview",
}


class ApplicationSummaryProjection(Projection):
    """
    Projection: one row per loan application in application_summary table.

    Each event handler updates the relevant columns via upsert.
    """

    @property
    def name(self) -> str:
        return "ApplicationSummary"

    def interested_in(self, event_type: str) -> bool:
        return event_type in _INTERESTED_EVENTS

    def event_types(self) -> set[str] | None:
        return set(_INTERESTED_EVENTS)

    async def handle(self, event: StoredEvent, conn: Any) -> None:
        """Route event to specific handler."""
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            await handler(event, conn)

    async def handle_batch(self, events: list[StoredEvent], conn: Any) -> None:
        if not events:
            return

        application_ids = {
            event.payload["application_id"]
            for event in events
            if "application_id" in event.payload
        }
        states = await self._load_states(conn, application_ids)
        dirty_states: dict[str, dict[str, Any]] = {}

        for event in events:
            application_id = event.payload.get("application_id")
            if application_id is None:
                continue

            state = states.setdefault(
                application_id,
                self._empty_state(application_id, event.recorded_at),
            )
            self._apply_to_state(state, event)
            dirty_states[application_id] = state

        if not dirty_states:
            return

        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO application_summary (
                    application_id,
                    state,
                    applicant_id,
                    applicant_name,
                    requested_amount_usd,
                    risk_tier,
                    confidence_score,
                    fraud_score,
                    compliance_status,
                    decision,
                    human_reviewer_id,
                    override,
                    override_reason,
                    final_decision,
                    approved_amount_usd,
                    interest_rate,
                    conditions,
                    final_decision_at,
                    last_event_type,
                    last_event_at,
                    agent_sessions,
                    decline_reasons,
                    created_at,
                    updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s::JSONB, %s, %s, %s, %s::JSONB,
                    %s::JSONB, %s, NOW()
                )
                ON CONFLICT (application_id) DO UPDATE SET
                    state = EXCLUDED.state,
                    applicant_id = EXCLUDED.applicant_id,
                    applicant_name = EXCLUDED.applicant_name,
                    requested_amount_usd = EXCLUDED.requested_amount_usd,
                    risk_tier = EXCLUDED.risk_tier,
                    confidence_score = EXCLUDED.confidence_score,
                    fraud_score = EXCLUDED.fraud_score,
                    compliance_status = EXCLUDED.compliance_status,
                    decision = EXCLUDED.decision,
                    human_reviewer_id = EXCLUDED.human_reviewer_id,
                    override = EXCLUDED.override,
                    override_reason = EXCLUDED.override_reason,
                    final_decision = EXCLUDED.final_decision,
                    approved_amount_usd = EXCLUDED.approved_amount_usd,
                    interest_rate = EXCLUDED.interest_rate,
                    conditions = EXCLUDED.conditions,
                    final_decision_at = EXCLUDED.final_decision_at,
                    last_event_type = EXCLUDED.last_event_type,
                    last_event_at = EXCLUDED.last_event_at,
                    agent_sessions = EXCLUDED.agent_sessions,
                    decline_reasons = EXCLUDED.decline_reasons,
                    created_at = COALESCE(application_summary.created_at, EXCLUDED.created_at),
                    updated_at = NOW()
                """,
                [
                    (
                        state["application_id"],
                        state["state"],
                        state["applicant_id"],
                        state["applicant_name"],
                        state["requested_amount_usd"],
                        state["risk_tier"],
                        state["confidence_score"],
                        state["fraud_score"],
                        state["compliance_status"],
                        state["decision"],
                        state["human_reviewer_id"],
                        state["override"],
                        state["override_reason"],
                        state["final_decision"],
                        state["approved_amount_usd"],
                        state["interest_rate"],
                        json.dumps(state["conditions"]) if state["conditions"] is not None else None,
                        state["final_decision_at"],
                        state["last_event_type"],
                        state["last_event_at"],
                        json.dumps(state["agent_sessions"]),
                        json.dumps(state["decline_reasons"]),
                        state["created_at"],
                    )
                    for state in dirty_states.values()
                ],
            )

    async def _load_states(
        self,
        conn: Any,
        application_ids: set[str],
    ) -> dict[str, dict[str, Any]]:
        if not application_ids:
            return {}

        result = await conn.execute(
            """
            SELECT
                application_id,
                state,
                applicant_id,
                applicant_name,
                requested_amount_usd,
                risk_tier,
                confidence_score,
                fraud_score,
                compliance_status,
                decision,
                human_reviewer_id,
                override,
                override_reason,
                final_decision,
                approved_amount_usd,
                interest_rate,
                conditions,
                final_decision_at,
                last_event_type,
                last_event_at,
                agent_sessions,
                decline_reasons,
                created_at
            FROM application_summary
            WHERE application_id = ANY(%s)
            """,
            (list(application_ids),),
        )
        rows = await result.fetchall()

        states: dict[str, dict[str, Any]] = {}
        for row in rows:
            states[row[0]] = {
                "application_id": row[0],
                "state": row[1],
                "applicant_id": row[2],
                "applicant_name": row[3],
                "requested_amount_usd": row[4],
                "risk_tier": row[5],
                "confidence_score": row[6],
                "fraud_score": row[7],
                "compliance_status": row[8],
                "decision": row[9],
                "human_reviewer_id": row[10],
                "override": row[11],
                "override_reason": row[12],
                "final_decision": row[13],
                "approved_amount_usd": row[14],
                "interest_rate": row[15],
                "conditions": row[16],
                "final_decision_at": row[17],
                "last_event_type": row[18],
                "last_event_at": row[19],
                "agent_sessions": row[20] or [],
                "decline_reasons": row[21] or [],
                "created_at": row[22],
            }
        return states

    def _empty_state(
        self,
        application_id: str,
        recorded_at: Any,
    ) -> dict[str, Any]:
        return {
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
            "conditions": None,
            "final_decision_at": None,
            "last_event_type": None,
            "last_event_at": None,
            "agent_sessions": [],
            "decline_reasons": [],
            "created_at": recorded_at,
        }

    def _apply_to_state(self, state: dict[str, Any], event: StoredEvent) -> None:
        p = event.payload
        state["last_event_type"] = event.event_type
        state["last_event_at"] = event.recorded_at
        state["created_at"] = state["created_at"] or event.recorded_at

        match event.event_type:
            case "ApplicationSubmitted":
                state["state"] = "SUBMITTED"
                state["applicant_id"] = p["applicant_id"]
                state["applicant_name"] = p.get("applicant_name", "")
                state["requested_amount_usd"] = p["requested_amount_usd"]
            case "CreditAnalysisRequested":
                state["state"] = "AWAITING_ANALYSIS"
            case "CreditAnalysisCompleted":
                state["state"] = "ANALYSIS_COMPLETE"
                state["risk_tier"] = p.get("risk_tier")
                state["confidence_score"] = p.get("confidence_score")
                state["agent_sessions"] = state["agent_sessions"] + [
                    p.get("session_id", "")
                ]
            case "FraudScreeningCompleted":
                state["fraud_score"] = p.get("fraud_score")
            case "ComplianceCheckRequested":
                state["state"] = "COMPLIANCE_REVIEW"
                state["compliance_status"] = "IN_PROGRESS"
            case "ComplianceClearanceIssued":
                state["state"] = "PENDING_DECISION"
                state["compliance_status"] = "CLEARED"
            case "DecisionGenerated":
                recommendation = p.get("recommendation", "")
                state["state"] = (
                    "APPROVED_PENDING_HUMAN"
                    if recommendation == "APPROVE"
                    else "DECLINED_PENDING_HUMAN"
                )
                state["decision"] = recommendation
                state["confidence_score"] = p.get("confidence_score")
            case "ApplicationUnderReview":
                state["human_reviewer_id"] = p.get("assigned_reviewer_id")
            case "HumanReviewCompleted":
                final_decision = p.get("final_decision")
                if final_decision == "APPROVE":
                    state["state"] = "FINAL_APPROVED"
                elif final_decision == "DECLINE":
                    state["state"] = "FINAL_DECLINED"
                state["human_reviewer_id"] = p.get("reviewer_id")
                state["override"] = p.get("override", False)
                state["override_reason"] = p.get("override_reason")
                state["final_decision"] = final_decision
                state["final_decision_at"] = event.recorded_at
            case "ApplicationApproved":
                state["state"] = "FINAL_APPROVED"
                state["approved_amount_usd"] = p.get("approved_amount_usd")
                state["interest_rate"] = p.get("interest_rate")
                state["conditions"] = p.get("conditions", [])
                state["final_decision"] = "APPROVED"
                state["final_decision_at"] = p.get("effective_date")
            case "ApplicationDeclined":
                state["state"] = "FINAL_DECLINED"
                state["decline_reasons"] = p.get("decline_reasons", [])
                state["final_decision"] = "DECLINED"
                state["final_decision_at"] = event.recorded_at

    # =========================================================================
    # Event Handlers
    # =========================================================================

    async def _on_ApplicationSubmitted(self, event: StoredEvent, conn: Any) -> None:
        p = event.payload
        await conn.execute(
            """
            INSERT INTO application_summary (
                application_id, state, applicant_id, applicant_name,
                requested_amount_usd, last_event_type, last_event_at, created_at, updated_at
            ) VALUES (%s, 'SUBMITTED', %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (application_id) DO UPDATE SET
                state = 'SUBMITTED',
                applicant_id = EXCLUDED.applicant_id,
                applicant_name = EXCLUDED.applicant_name,
                requested_amount_usd = EXCLUDED.requested_amount_usd,
                last_event_type = EXCLUDED.last_event_type,
                last_event_at = EXCLUDED.last_event_at,
                updated_at = NOW()
            """,
            (
                p["application_id"],
                p["applicant_id"],
                p.get("applicant_name", ""),
                p["requested_amount_usd"],
                event.event_type,
                event.recorded_at,
            ),
        )

    async def _on_CreditAnalysisRequested(self, event: StoredEvent, conn: Any) -> None:
        p = event.payload
        await conn.execute(
            """
            UPDATE application_summary SET
                state = 'AWAITING_ANALYSIS',
                last_event_type = %s,
                last_event_at = %s,
                updated_at = NOW()
            WHERE application_id = %s
            """,
            (event.event_type, event.recorded_at, p["application_id"]),
        )

    async def _on_CreditAnalysisCompleted(self, event: StoredEvent, conn: Any) -> None:
        p = event.payload
        # Update risk tier and add session to agent_sessions
        await conn.execute(
            """
            UPDATE application_summary SET
                state = 'ANALYSIS_COMPLETE',
                risk_tier = %s,
                confidence_score = %s,
                agent_sessions = agent_sessions || %s::JSONB,
                last_event_type = %s,
                last_event_at = %s,
                updated_at = NOW()
            WHERE application_id = %s
            """,
            (
                p.get("risk_tier"),
                p.get("confidence_score"),
                json.dumps([p.get("session_id", "")]),
                event.event_type,
                event.recorded_at,
                p["application_id"],
            ),
        )

    async def _on_FraudScreeningCompleted(self, event: StoredEvent, conn: Any) -> None:
        p = event.payload
        await conn.execute(
            """
            UPDATE application_summary SET
                fraud_score = %s,
                last_event_type = %s,
                last_event_at = %s,
                updated_at = NOW()
            WHERE application_id = %s
            """,
            (
                p.get("fraud_score"),
                event.event_type,
                event.recorded_at,
                p["application_id"],
            ),
        )

    async def _on_ComplianceCheckRequested(self, event: StoredEvent, conn: Any) -> None:
        p = event.payload
        await conn.execute(
            """
            UPDATE application_summary SET
                state = 'COMPLIANCE_REVIEW',
                compliance_status = 'IN_PROGRESS',
                last_event_type = %s,
                last_event_at = %s,
                updated_at = NOW()
            WHERE application_id = %s
            """,
            (event.event_type, event.recorded_at, p["application_id"]),
        )

    async def _on_ComplianceClearanceIssued(
        self, event: StoredEvent, conn: Any
    ) -> None:
        p = event.payload
        await conn.execute(
            """
            UPDATE application_summary SET
                state = 'PENDING_DECISION',
                compliance_status = 'CLEARED',
                last_event_type = %s,
                last_event_at = %s,
                updated_at = NOW()
            WHERE application_id = %s
            """,
            (event.event_type, event.recorded_at, p["application_id"]),
        )

    async def _on_DecisionGenerated(self, event: StoredEvent, conn: Any) -> None:
        p = event.payload
        recommendation = p.get("recommendation", "")
        # Map recommendation to pending state
        if recommendation == "APPROVE":
            state = "APPROVED_PENDING_HUMAN"
        else:
            state = "DECLINED_PENDING_HUMAN"

        await conn.execute(
            """
            UPDATE application_summary SET
                state = %s,
                decision = %s,
                confidence_score = %s,
                last_event_type = %s,
                last_event_at = %s,
                updated_at = NOW()
            WHERE application_id = %s
            """,
            (
                state,
                recommendation,
                p.get("confidence_score"),
                event.event_type,
                event.recorded_at,
                p["application_id"],
            ),
        )

    async def _on_ApplicationUnderReview(self, event: StoredEvent, conn: Any) -> None:
        p = event.payload
        await conn.execute(
            """
            UPDATE application_summary SET
                human_reviewer_id = %s,
                last_event_type = %s,
                last_event_at = %s,
                updated_at = NOW()
            WHERE application_id = %s
            """,
            (
                p.get("assigned_reviewer_id"),
                event.event_type,
                event.recorded_at,
                p["application_id"],
            ),
        )

    async def _on_HumanReviewCompleted(self, event: StoredEvent, conn: Any) -> None:
        p = event.payload
        final_decision = p.get("final_decision")
        state = None
        if final_decision == "APPROVE":
            state = "FINAL_APPROVED"
        elif final_decision == "DECLINE":
            state = "FINAL_DECLINED"
        await conn.execute(
            """
            UPDATE application_summary SET
                state = COALESCE(%s, state),
                human_reviewer_id = %s,
                override = %s,
                override_reason = %s,
                final_decision = %s,
                final_decision_at = %s,
                last_event_type = %s,
                last_event_at = %s,
                updated_at = NOW()
            WHERE application_id = %s
            """,
            (
                state,
                p.get("reviewer_id"),
                p.get("override", False),
                p.get("override_reason"),
                p.get("final_decision"),
                event.recorded_at,
                event.event_type,
                event.recorded_at,
                p["application_id"],
            ),
        )

    async def _on_ApplicationApproved(self, event: StoredEvent, conn: Any) -> None:
        p = event.payload
        await conn.execute(
            """
            UPDATE application_summary SET
                state = 'FINAL_APPROVED',
                approved_amount_usd = %s,
                interest_rate = %s,
                conditions = %s::JSONB,
                final_decision = 'APPROVED',
                final_decision_at = %s,
                last_event_type = %s,
                last_event_at = %s,
                updated_at = NOW()
            WHERE application_id = %s
            """,
            (
                p.get("approved_amount_usd"),
                p.get("interest_rate"),
                json.dumps(p.get("conditions", [])),
                p.get("effective_date"),
                event.event_type,
                event.recorded_at,
                p["application_id"],
            ),
        )

    async def _on_ApplicationDeclined(self, event: StoredEvent, conn: Any) -> None:
        p = event.payload
        await conn.execute(
            """
            UPDATE application_summary SET
                state = 'FINAL_DECLINED',
                decline_reasons = %s::JSONB,
                final_decision = 'DECLINED',
                final_decision_at = %s,
                last_event_type = %s,
                last_event_at = %s,
                updated_at = NOW()
            WHERE application_id = %s
            """,
            (
                json.dumps(p.get("decline_reasons", [])),
                event.recorded_at,
                event.event_type,
                event.recorded_at,
                p["application_id"],
            ),
        )


def reconstruct_application_summary_from_events(
    application_id: str,
    events: list[StoredEvent],
) -> dict[str, Any]:
    """
    Reconstruct the ApplicationSummary row shape from event history.

    This reuses the projection's own state-transition logic so bonus features
    such as the regulatory package can return projection-faithful state.
    """
    projection = ApplicationSummaryProjection()
    state = projection._empty_state(application_id, None)
    seen_relevant_event = False

    for event in events:
        if (
            event.stream_id != f"loan-{application_id}"
            and event.payload.get("application_id") != application_id
        ):
            continue
        projection._apply_to_state(state, event)
        seen_relevant_event = True

    if not seen_relevant_event:
        return {"application_id": application_id}

    return {
        "application_id": state["application_id"],
        "state": state["state"],
        "applicant_id": state["applicant_id"],
        "applicant_name": state["applicant_name"],
        "requested_amount_usd": state["requested_amount_usd"],
        "risk_tier": state["risk_tier"],
        "confidence_score": state["confidence_score"],
        "fraud_score": state["fraud_score"],
        "compliance_status": state["compliance_status"],
        "decision": state["decision"],
        "human_reviewer_id": state["human_reviewer_id"],
        "override": state["override"],
        "override_reason": state["override_reason"],
        "final_decision": state["final_decision"],
        "approved_amount_usd": state["approved_amount_usd"],
        "interest_rate": state["interest_rate"],
        "conditions": state["conditions"] or [],
        "final_decision_at": (
            state["final_decision_at"].isoformat()
            if hasattr(state["final_decision_at"], "isoformat")
            else state["final_decision_at"]
        ),
        "last_event_type": state["last_event_type"],
        "last_event_at": (
            state["last_event_at"].isoformat()
            if hasattr(state["last_event_at"], "isoformat")
            else state["last_event_at"]
        ),
        "agent_sessions": list(state["agent_sessions"]),
        "decline_reasons": list(state["decline_reasons"]),
        "created_at": (
            state["created_at"].isoformat()
            if hasattr(state["created_at"], "isoformat")
            else state["created_at"]
        ),
        "updated_at": (
            state["last_event_at"].isoformat()
            if hasattr(state["last_event_at"], "isoformat")
            else state["last_event_at"]
        ),
    }
