# src/projections/agent_performance.py
# =============================================================================
# TRP1 LEDGER — AgentPerformanceLedger Projection
# =============================================================================
# Source: Challenge Doc Phase 3 p.12 (AgentPerformanceLedger)
#
# Aggregated metrics per agent + model version combination.
# Enables: model comparison dashboards, agent reliability tracking.
#
# Design: stores raw totals (total_confidence, total_duration_ms) rather
# than averages. Averages are computed at query time as total/count.
# This avoids precision loss from running averages and ensures correct
# results during projection rebuilds.
# =============================================================================
from __future__ import annotations

from typing import Any

from src.models.events import StoredEvent
from src.projections.daemon import Projection


_INTERESTED_EVENTS = {
    "CreditAnalysisCompleted",
    "FraudScreeningCompleted",
    "DecisionGenerated",
    "HumanReviewCompleted",
    "AgentDecisionSuperseded",
}


class AgentPerformanceProjection(Projection):
    """
    Projection: aggregated metrics per agent + model version
    in agent_performance_ledger table.
    """

    @property
    def name(self) -> str:
        return "AgentPerformanceLedger"

    def interested_in(self, event_type: str) -> bool:
        return event_type in _INTERESTED_EVENTS

    def event_types(self) -> set[str] | None:
        return set(_INTERESTED_EVENTS)

    async def handle(self, event: StoredEvent, conn: Any) -> None:
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            await handler(event, conn)

    # =========================================================================
    # Event Handlers
    # =========================================================================

    async def _on_CreditAnalysisCompleted(self, event: StoredEvent, conn: Any) -> None:
        p = event.payload
        agent_id = p.get("agent_id", "unknown")
        model_version = p.get("model_version", "unknown")
        confidence = p.get("confidence_score", 0.0)
        duration = p.get("analysis_duration_ms", 0)

        await conn.execute(
            """
            INSERT INTO agent_performance_ledger (
                agent_id, model_version, analyses_completed,
                total_confidence, total_duration_ms,
                first_seen_at, last_seen_at
            ) VALUES (%s, %s, 1, %s, %s, %s, %s)
            ON CONFLICT (agent_id, model_version) DO UPDATE SET
                analyses_completed = agent_performance_ledger.analyses_completed + 1,
                total_confidence = agent_performance_ledger.total_confidence + EXCLUDED.total_confidence,
                total_duration_ms = agent_performance_ledger.total_duration_ms + EXCLUDED.total_duration_ms,
                last_seen_at = EXCLUDED.last_seen_at
            """,
            (
                agent_id,
                model_version,
                confidence,
                duration,
                event.recorded_at,
                event.recorded_at,
            ),
        )

    async def _on_FraudScreeningCompleted(self, event: StoredEvent, conn: Any) -> None:
        p = event.payload
        agent_id = p.get("agent_id", "unknown")
        model_version = p.get("screening_model_version", "unknown")

        await conn.execute(
            """
            INSERT INTO agent_performance_ledger (
                agent_id, model_version, analyses_completed,
                first_seen_at, last_seen_at
            ) VALUES (%s, %s, 1, %s, %s)
            ON CONFLICT (agent_id, model_version) DO UPDATE SET
                analyses_completed = agent_performance_ledger.analyses_completed + 1,
                last_seen_at = EXCLUDED.last_seen_at
            """,
            (agent_id, model_version, event.recorded_at, event.recorded_at),
        )

    async def _on_DecisionGenerated(self, event: StoredEvent, conn: Any) -> None:
        p = event.payload
        agent_id = p.get("orchestrator_agent_id", "unknown")
        recommendation = p.get("recommendation", "")

        # Determine recommendation column to increment
        approve_inc = 1 if recommendation == "APPROVE" else 0
        decline_inc = 1 if recommendation == "DECLINE" else 0
        refer_inc = 1 if recommendation == "REFER" else 0

        # For DecisionGenerated, model_versions is a dict
        model_versions = p.get("model_versions", {})
        model_version = (
            model_versions.get("orchestrator", "unknown")
            if isinstance(model_versions, dict)
            else "unknown"
        )

        await conn.execute(
            """
            INSERT INTO agent_performance_ledger (
                agent_id, model_version, decisions_generated,
                approve_count, decline_count, refer_count,
                first_seen_at, last_seen_at
            ) VALUES (%s, %s, 1, %s, %s, %s, %s, %s)
            ON CONFLICT (agent_id, model_version) DO UPDATE SET
                decisions_generated = agent_performance_ledger.decisions_generated + 1,
                approve_count = agent_performance_ledger.approve_count + EXCLUDED.approve_count,
                decline_count = agent_performance_ledger.decline_count + EXCLUDED.decline_count,
                refer_count = agent_performance_ledger.refer_count + EXCLUDED.refer_count,
                last_seen_at = EXCLUDED.last_seen_at
            """,
            (
                agent_id,
                model_version,
                approve_inc,
                decline_inc,
                refer_inc,
                event.recorded_at,
                event.recorded_at,
            ),
        )

    async def _on_HumanReviewCompleted(self, event: StoredEvent, conn: Any) -> None:
        p = event.payload
        if p.get("override", False):
            # Human overrode the AI decision — increment override count
            # We attribute this to the reviewer (human agent)
            reviewer_id = p.get("reviewer_id", "unknown")
            await conn.execute(
                """
                INSERT INTO agent_performance_ledger (
                    agent_id, model_version, human_override_count,
                    first_seen_at, last_seen_at
                ) VALUES (%s, 'human', 1, %s, %s)
                ON CONFLICT (agent_id, model_version) DO UPDATE SET
                    human_override_count = agent_performance_ledger.human_override_count + 1,
                    last_seen_at = EXCLUDED.last_seen_at
                """,
                (reviewer_id, event.recorded_at, event.recorded_at),
            )

    async def _on_AgentDecisionSuperseded(self, event: StoredEvent, conn: Any) -> None:
        p = event.payload
        agent_id = p.get("agent_id", "unknown")

        await conn.execute(
            """
            UPDATE agent_performance_ledger SET
                superseded_count = superseded_count + 1,
                last_seen_at = %s
            WHERE agent_id = %s
            """,
            (event.recorded_at, agent_id),
        )


def reconstruct_agent_performance_from_events(
    events: list[StoredEvent],
) -> list[dict[str, Any]]:
    """
    Reconstruct AgentPerformanceLedger row shapes from event history.

    This mirrors the projection update semantics closely enough for
    examination-package time travel without reading the live current-state table.
    """
    rows: dict[tuple[str, str], dict[str, Any]] = {}

    def ensure_row(agent_id: str, model_version: str, seen_at: Any) -> dict[str, Any]:
        return rows.setdefault(
            (agent_id, model_version),
            {
                "agent_id": agent_id,
                "model_version": model_version,
                "analyses_completed": 0,
                "decisions_generated": 0,
                "approve_count": 0,
                "decline_count": 0,
                "refer_count": 0,
                "human_override_count": 0,
                "superseded_count": 0,
                "total_confidence": 0.0,
                "total_duration_ms": 0,
                "first_seen_at": seen_at.isoformat() if hasattr(seen_at, "isoformat") else seen_at,
                "last_seen_at": seen_at.isoformat() if hasattr(seen_at, "isoformat") else seen_at,
            },
        )

    for event in events:
        p = event.payload
        seen_at = event.recorded_at
        if event.event_type == "CreditAnalysisCompleted":
            row = ensure_row(
                p.get("agent_id", "unknown"),
                p.get("model_version", "unknown"),
                seen_at,
            )
            row["analyses_completed"] += 1
            row["total_confidence"] += float(p.get("confidence_score", 0.0))
            row["total_duration_ms"] += int(p.get("analysis_duration_ms", 0))
            row["last_seen_at"] = seen_at.isoformat()
        elif event.event_type == "FraudScreeningCompleted":
            row = ensure_row(
                p.get("agent_id", "unknown"),
                p.get("screening_model_version", "unknown"),
                seen_at,
            )
            row["analyses_completed"] += 1
            row["last_seen_at"] = seen_at.isoformat()
        elif event.event_type == "DecisionGenerated":
            model_versions = p.get("model_versions", {})
            row = ensure_row(
                p.get("orchestrator_agent_id", "unknown"),
                model_versions.get("orchestrator", "unknown")
                if isinstance(model_versions, dict)
                else "unknown",
                seen_at,
            )
            row["decisions_generated"] += 1
            recommendation = p.get("recommendation")
            if recommendation == "APPROVE":
                row["approve_count"] += 1
            elif recommendation == "DECLINE":
                row["decline_count"] += 1
            elif recommendation == "REFER":
                row["refer_count"] += 1
            row["last_seen_at"] = seen_at.isoformat()
        elif event.event_type == "HumanReviewCompleted" and p.get("override", False):
            row = ensure_row(
                p.get("reviewer_id", "unknown"),
                "human",
                seen_at,
            )
            row["human_override_count"] += 1
            row["last_seen_at"] = seen_at.isoformat()
        elif event.event_type == "AgentDecisionSuperseded":
            row = ensure_row(
                p.get("agent_id", "unknown"),
                p.get("model_version", "unknown"),
                seen_at,
            )
            row["superseded_count"] += 1
            row["last_seen_at"] = seen_at.isoformat()

    return [
        rows[key]
        for key in sorted(rows, key=lambda item: (item[0], item[1]))
    ]
