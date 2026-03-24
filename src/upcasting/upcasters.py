from __future__ import annotations

from datetime import datetime
from typing import Any

from src.upcasting.registry import UpcasterRegistry

# Global registry instance — imported by EventStore
registry = UpcasterRegistry()


# =============================================================================
# CreditAnalysisCompleted v1 → v2
# =============================================================================
# v1 fields: application_id, agent_id, session_id, confidence_score,
#             risk_tier, recommended_limit_usd, analysis_duration_ms,
#             input_data_hash
# v2 adds:   model_version, model_deployment_id, regulatory_basis
# =============================================================================


@registry.register("CreditAnalysisCompleted", from_version=1)
def upcast_credit_v1_to_v2(
    payload: dict,
    *,
    current_event: Any | None = None,
    context: dict[str, Any] | None = None,
) -> dict:
    """
    Upcast CreditAnalysisCompleted from v1 to v2.

    Inference strategy:
    - model_version: Inferred from recorded_at timestamp using known
      deployment timeline. ~15% error rate during Q3 2025 canary rollout.
    - model_deployment_id: Set to "unknown-pre-v2" — genuinely unknown
      for v1 events, as deployment tracking was not yet implemented.
    - confidence_score: Normalized to None for v1 events. Historical
      confidence values are treated as unavailable for strict challenge-doc
      alignment; preserving an ambiguous legacy value would be worse than null
      for audit, replay, and analytics consumers.
    - regulatory_basis: Inferred from recorded_at using known regulatory
      framework timeline.

    See DESIGN.md Section 4 for full inference analysis.
    """
    recorded_at = payload.get("_recorded_at")
    if recorded_at is None and current_event is not None:
        recorded_at = getattr(current_event, "recorded_at", None)

    return {
        **payload,
        "model_version": payload.get(
            "model_version",
            _infer_model_version(recorded_at),
        ),
        "model_deployment_id": payload.get("model_deployment_id", "unknown-pre-v2"),
        "confidence_score": None,
        "regulatory_basis": payload.get(
            "regulatory_basis",
            [_infer_regulatory_basis(recorded_at)],
        ),
    }


# =============================================================================
# DecisionGenerated v1 → v2
# =============================================================================
# v1 fields: application_id, orchestrator_agent_id, recommendation,
#             confidence_score, contributing_agent_sessions,
#             decision_basis_summary
# v2 adds:   model_versions{}
# =============================================================================


@registry.register("DecisionGenerated", from_version=1)
async def upcast_decision_v1_to_v2(
    payload: dict,
    *,
    current_event: Any | None = None,
    context: dict[str, Any] | None = None,
) -> dict:
    """
    Upcast DecisionGenerated from v1 to v2.

    Inference strategy for model_versions:
    - Ideally reconstructed from contributing_agent_sessions by loading
      each session's AgentContextLoaded event. However, this requires
      a store lookup during upcasting, which has performance implications:
      each v1 DecisionGenerated load triggers N additional stream loads
      (one per contributing session).
    - For the upcaster (which must be pure — no I/O), we set model_versions
      to an empty dict with a marker indicating reconstruction is needed.
    - The full reconstruction is deferred to read-time enrichment in the
      projection layer, where the store is available.

    See DESIGN.md Section 4 for performance analysis.
    """
    if "model_versions" in payload:
        return payload

    store = (context or {}).get("store")
    model_versions = await _reconstruct_model_versions(
        store,
        payload.get("contributing_agent_sessions", []),
    )

    return {
        **payload,
        "model_versions": model_versions,
    }


# =============================================================================
# Inference Helpers
# =============================================================================


def _infer_model_version(recorded_at: str | None) -> str:
    """
    Infer model version from timestamp using known deployment timeline.

    Timeline:
      pre-2025-01-01:           "credit-model-v1.x-legacy"
      2025-01-01 to 2025-06-30: "credit-model-v2.0"
      2025-07-01 onwards:       "credit-model-v2.2"

    Error rate: ~15% for Q3 2025 events (canary rollout period where
    multiple versions were deployed simultaneously).
    """
    if not recorded_at:
        return "credit-model-v1.x-legacy"

    try:
        if isinstance(recorded_at, str):
            ts = datetime.fromisoformat(recorded_at)
        else:
            ts = recorded_at

        if ts.year < 2025:
            return "credit-model-v1.x-legacy"
        elif ts < datetime(2025, 7, 1, tzinfo=ts.tzinfo):
            return "credit-model-v2.0"
        else:
            return "credit-model-v2.2"
    except (ValueError, TypeError):
        return "credit-model-v1.x-legacy"


def _infer_regulatory_basis(recorded_at: str | None) -> str:
    """
    Infer the regulatory framework version active at the time of decision.

    Timeline:
      pre-2025:     "Basel-III-2024-Q4"
      2025 H1:      "Basel-III-2025-Q1"
      2025 H2+:     "Basel-IV-2025-Q3"
    """
    if not recorded_at:
        return "Basel-III-2024-Q4"

    try:
        if isinstance(recorded_at, str):
            ts = datetime.fromisoformat(recorded_at)
        else:
            ts = recorded_at

        if ts.year < 2025:
            return "Basel-III-2024-Q4"
        elif ts < datetime(2025, 7, 1, tzinfo=ts.tzinfo):
            return "Basel-III-2025-Q1"
        else:
            return "Basel-IV-2025-Q3"
    except (ValueError, TypeError):
        return "Basel-III-2024-Q4"


async def _reconstruct_model_versions(
    store: Any | None,
    session_stream_ids: list[str],
) -> dict[str, str]:
    """
    Reconstruct per-session model versions from AgentContextLoaded events.

    This performs read-time lookups, which is precisely the performance tradeoff
    documented in DESIGN.md for DecisionGenerated v1 historical replay.
    """
    if store is None:
        return {
            "_needs_reconstruction": True,
            "_reason": "v1 event — EventStore context unavailable during load",
        }

    model_versions: dict[str, str] = {}
    missing_sessions: list[str] = []

    for session_stream_id in session_stream_ids:
        session_events = await store.load_stream(session_stream_id)
        model_version = None
        for event in session_events:
            if event.event_type == "AgentContextLoaded":
                model_version = event.payload.get("model_version")
                break
        if model_version:
            model_versions[session_stream_id] = model_version
        else:
            missing_sessions.append(session_stream_id)

    if missing_sessions:
        model_versions["_missing_sessions"] = ",".join(sorted(missing_sessions))

    if not model_versions:
        return {
            "_needs_reconstruction": True,
            "_reason": "v1 event — no contributing session model versions found",
        }

    return model_versions
