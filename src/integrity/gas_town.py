# src/integrity/gas_town.py
# =============================================================================
# TRP1 LEDGER — Gas Town Agent Memory Pattern
# =============================================================================
# Source: Challenge Doc Phase 4C p.15 + Manual Part II Cluster E p.13
#
# "The key insight: the event store IS the agent's memory."
#
# An AI agent that crashes mid-session must be able to restart and
# reconstruct its exact context from the event store, then continue
# where it left off without repeating completed work.
# =============================================================================
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

import structlog

from src.event_store import EventStore
from src.models.events import StoredEvent

logger = structlog.get_logger()


class SessionHealthStatus(StrEnum):
    """Health status of a reconstructed agent session."""

    HEALTHY = "HEALTHY"
    NEEDS_RECONCILIATION = "NEEDS_RECONCILIATION"
    NO_CONTEXT = "NO_CONTEXT"
    EMPTY = "EMPTY"


@dataclass
class PendingWorkItem:
    """A work item that was started but not completed."""

    application_id: str
    event_type: str
    started_at: datetime | None = None
    description: str = ""


@dataclass
class AgentContext:
    """
    Reconstructed agent context from event store replay.

    This is what the agent receives after a crash recovery.
    It contains enough information for the agent to continue
    correctly without repeating completed work.
    """

    agent_id: str
    session_id: str
    context_text: str
    last_event_position: int
    pending_work: list[PendingWorkItem] = field(default_factory=list)
    session_health_status: SessionHealthStatus = SessionHealthStatus.EMPTY
    model_version: str | None = None
    context_source: str | None = None
    completed_applications: list[str] = field(default_factory=list)
    total_events: int = 0


# Decision event types — events that indicate work output
_DECISION_EVENTS = {
    "CreditAnalysisCompleted",
    "FraudScreeningCompleted",
}

# Completion indicators — events that confirm work is done
_COMPLETION_EVENTS = {
    "CreditAnalysisCompleted",
    "FraudScreeningCompleted",
    "AgentDecisionSuperseded",
}


def _summarize_events(events: list[StoredEvent], max_tokens: int) -> str:
    """
    Summarize old events into a token-efficient prose description.

    Simple token estimation: ~4 chars per token (English average).
    """
    if not events:
        return ""

    char_budget = max_tokens * 4
    lines: list[str] = []
    used = 0

    for event in events:
        line = (
            f"[pos={event.stream_position}] {event.event_type}: "
            f"{_brief_payload(event.payload)}"
        )
        if used + len(line) > char_budget:
            lines.append(f"... ({len(events) - len(lines)} older events summarized)")
            break
        lines.append(line)
        used += len(line)

    return "\n".join(lines)


def _brief_payload(payload: dict) -> str:
    """Create a brief summary of payload fields."""
    important_keys = [
        "application_id",
        "agent_id",
        "risk_tier",
        "confidence_score",
        "fraud_score",
        "recommendation",
        "model_version",
    ]
    parts = []
    for key in important_keys:
        if key in payload:
            parts.append(f"{key}={payload[key]}")
    if not parts:
        # Fall back to first few keys
        for key in list(payload.keys())[:3]:
            parts.append(f"{key}={payload[key]}")
    return ", ".join(parts)


def _format_event_verbatim(event: StoredEvent) -> str:
    """Format a single event for verbatim inclusion in context."""
    return (
        f"[position={event.stream_position}, type={event.event_type}, "
        f"version={event.event_version}]\n"
        f"  payload: {event.payload}\n"
        f"  metadata: {event.metadata}\n"
        f"  recorded_at: {event.recorded_at}"
    )


async def reconstruct_agent_context(
    store: EventStore,
    agent_id: str,
    session_id: str,
    token_budget: int = 8000,
) -> AgentContext:
    """
    Reconstruct an agent's context from the event store after a crash.

    Steps:
    1. Load full AgentSession stream for agent_id + session_id
    2. Identify: last completed action, pending work items, current state
    3. Summarize old events into prose (token-efficient)
    4. Preserve verbatim: last 3 events, any PENDING or ERROR state events
    5. Return AgentContext with context_text, last_event_position,
       pending_work[], session_health_status

    CRITICAL: if the agent's last event was a partial decision (no
    corresponding completion event), flag the context as NEEDS_RECONCILIATION.
    """
    stream_id = f"agent-{agent_id}-{session_id}"
    events = await store.load_stream(stream_id)

    if not events:
        logger.info("agent_context_empty", agent_id=agent_id, session_id=session_id)
        return AgentContext(
            agent_id=agent_id,
            session_id=session_id,
            context_text="No events found for this agent session.",
            last_event_position=0,
            session_health_status=SessionHealthStatus.EMPTY,
        )

    # Determine session health and extract state
    context_loaded = False
    model_version = None
    context_source = None
    completed_apps: list[str] = []
    pending_work: list[PendingWorkItem] = []

    for event in events:
        if event.event_type == "AgentContextLoaded":
            context_loaded = True
            model_version = event.payload.get("model_version")
            context_source = event.payload.get("context_source")
        elif event.event_type in _DECISION_EVENTS:
            app_id = event.payload.get("application_id", "unknown")
            completed_apps.append(app_id)
        elif event.event_type == "AgentDecisionSuperseded":
            app_id = event.payload.get("application_id", "unknown")
            if app_id in completed_apps:
                completed_apps.remove(app_id)

    # Determine health status
    last_event = events[-1]
    health = SessionHealthStatus.HEALTHY

    if not context_loaded:
        health = SessionHealthStatus.NO_CONTEXT
    elif last_event.event_type in _DECISION_EVENTS:
        # Last event is a decision — check if there's a matching completion
        # If it's the last event, it's complete (the decision IS the completion)
        health = SessionHealthStatus.HEALTHY
    elif last_event.event_type == "AgentContextLoaded" and len(events) == 1:
        # Only context loaded, no work done yet
        health = SessionHealthStatus.HEALTHY
    elif (
        last_event.event_type not in _COMPLETION_EVENTS
        and last_event.event_type != "AgentContextLoaded"
    ):
        # Last event is something unexpected — needs reconciliation
        health = SessionHealthStatus.NEEDS_RECONCILIATION
        pending_work.append(
            PendingWorkItem(
                application_id=last_event.payload.get("application_id", "unknown"),
                event_type=last_event.event_type,
                description=f"Partial state detected: last event was {last_event.event_type}",
            )
        )

    # Build context text
    # Reserve ~30% of token budget for verbatim events
    verbatim_budget = int(token_budget * 0.3)
    summary_budget = token_budget - verbatim_budget

    # Summarize older events
    if len(events) > 3:
        older_events = events[:-3]
        summary_text = _summarize_events(older_events, summary_budget)
    else:
        summary_text = ""

    # Preserve last 3 events verbatim
    recent_events = events[-3:]
    verbatim_text = "\n\n".join(_format_event_verbatim(e) for e in recent_events)

    # Compose full context
    context_parts = [
        "=== Agent Session Context ===",
        f"Agent: {agent_id}, Session: {session_id}",
        f"Model: {model_version or 'unknown'}",
        f"Context Source: {context_source or 'unknown'}",
        f"Total Events: {len(events)}",
        f"Health: {health.value}",
        f"Completed Applications: {', '.join(completed_apps) or 'none'}",
    ]

    if pending_work:
        context_parts.append("\n=== PENDING WORK (requires attention) ===")
        for pw in pending_work:
            context_parts.append(f"  - {pw.application_id}: {pw.description}")

    if summary_text:
        context_parts.append("\n=== Event History (summarized) ===")
        context_parts.append(summary_text)

    context_parts.append("\n=== Recent Events (verbatim) ===")
    context_parts.append(verbatim_text)

    context_text = "\n".join(context_parts)

    result = AgentContext(
        agent_id=agent_id,
        session_id=session_id,
        context_text=context_text,
        last_event_position=events[-1].stream_position,
        pending_work=pending_work,
        session_health_status=health,
        model_version=model_version,
        context_source=context_source,
        completed_applications=completed_apps,
        total_events=len(events),
    )

    logger.info(
        "agent_context_reconstructed",
        agent_id=agent_id,
        session_id=session_id,
        total_events=len(events),
        health=health.value,
        completed_apps=len(completed_apps),
        pending_items=len(pending_work),
    )

    return result
