# src/aggregates/agent_session.py
# =============================================================================
# TRP1 LEDGER — AgentSession Aggregate
# =============================================================================
# Source: Challenge Doc Phase 2 pages 9-11 (AgentSession aggregate)
#        + Manual Part II Cluster E p.13 (Gas Town pattern)
#
# The AgentSession aggregate tracks all actions taken by a specific AI agent
# instance during a work session. The critical invariant is the Gas Town
# pattern: no agent may make a decision without first declaring its context
# source via AgentContextLoaded.
#
# "The key insight: the event store IS the agent's memory."
# (Manual p.13)
# =============================================================================
from __future__ import annotations

from src.event_store import EventStore
from src.models.events import DomainError, StoredEvent


class AgentSessionAggregate:
    """
    Consistency boundary for an AI agent work session.

    Enforces the Gas Town pattern: AgentContextLoaded must be the first
    event in every session before any decision events can be appended.
    This ensures every agent decision is traceable to its context source,
    model version, and input data.
    """

    def __init__(self, agent_id: str, session_id: str):
        self.agent_id = agent_id
        self.session_id = session_id
        self.version: int = 0
        self.context_loaded: bool = False       # Gas Town: must be True before decisions
        self.model_version: str | None = None
        self.context_source: str | None = None
        self.context_position: int | None = None
        self.decisions: list[str] = []          # application_ids decided in this session
        self.is_active: bool = True

    @classmethod
    async def load(
        cls,
        store: EventStore,
        agent_id: str,
        session_id: str,
    ) -> AgentSessionAggregate:
        """
        Replay AgentSession stream to reconstruct state.

        Stream ID format: "agent-{agent_id}-{session_id}"
        """
        events = await store.load_stream(f"agent-{agent_id}-{session_id}")
        agg = cls(agent_id=agent_id, session_id=session_id)
        for event in events:
            agg._apply(event)
        return agg

    # =========================================================================
    # Event Application (State Reconstruction)
    # =========================================================================

    def _apply(self, event: StoredEvent) -> None:
        """Dispatch to typed handler. Update version after every event."""
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position

    def _on_AgentContextLoaded(self, event: StoredEvent) -> None:
        """
        Gas Town: first event in every session. Sets context_loaded = True.

        This event IS the declaration of memory source. Without it, the agent
        has no declared context and cannot make any decisions.
        (Manual p.13)
        """
        self.context_loaded = True
        self.model_version = event.payload["model_version"]
        self.context_source = event.payload["context_source"]
        self.context_position = event.payload["event_replay_from_position"]

    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        """Record that a credit analysis decision was made in this session."""
        application_id = event.payload.get("application_id")
        if application_id:
            self.decisions.append(application_id)

    def _on_FraudScreeningCompleted(self, event: StoredEvent) -> None:
        """Record that a fraud screening decision was made in this session."""
        application_id = event.payload.get("application_id")
        if application_id:
            self.decisions.append(application_id)

    def _on_AgentDecisionSuperseded(self, event: StoredEvent) -> None:
        """
        Record that a previous decision in this session was superseded.
        Remove the application_id from active decisions list.
        """
        application_id = event.payload.get("application_id")
        if application_id and application_id in self.decisions:
            self.decisions.remove(application_id)

    # =========================================================================
    # Business Rule Assertions
    # =========================================================================

    def assert_context_loaded(self) -> None:
        """
        Gas Town enforcement (Challenge Doc p.10, Rule 2):

        An AgentSession MUST have AgentContextLoaded as its first event before
        any decision event can be appended. No agent may make a decision without
        first declaring its context source.

        Raises DomainError if context_loaded is False.

        Reference: Manual p.13 — "The key insight: the event store IS the agent's
        memory." The AgentContextLoaded event IS the declaration of memory source.
        """
        if not self.context_loaded:
            raise DomainError(
                f"AgentSession {self.agent_id}/{self.session_id}: "
                "AgentContextLoaded event required before any decision event. "
                "Call start_agent_session tool first. (Gas Town pattern)"
            )

    def assert_model_version_current(self, model_version: str) -> None:
        """
        Verify the model version in the command matches what was declared at
        context load. Prevents silent model version drift between session start
        and decision.

        This catches the scenario where an agent declares model v2.0 at session
        start but uses model v2.1 for a decision — breaking the reproducibility
        guarantee.
        """
        if self.model_version and self.model_version != model_version:
            raise DomainError(
                f"Model version mismatch: session declared {self.model_version}, "
                f"command uses {model_version}"
            )
