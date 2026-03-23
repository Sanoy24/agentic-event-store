from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import structlog

from src.integrity.gas_town import reconstruct_agent_context
from src.projections.compliance_audit import get_compliance_at, get_current_compliance
from src.projections.compliance_audit import (
    get_projection_lag as get_compliance_projection_lag,
)

logger = structlog.get_logger()


def _json(data: dict[str, Any]) -> str:
    return json.dumps(data, indent=2, default=str)


async def _read_compliance(
    server: Any,
    application_id: str,
    *,
    as_of: str | None = None,
) -> str:
    pool = await server.get_pool()
    async with pool.connection() as conn:
        if as_of:
            try:
                at_time = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
            except ValueError:
                return _json({"error": f"Invalid timestamp: {as_of}"})
            state = await get_compliance_at(conn, application_id, at_time)
        else:
            state = await get_current_compliance(conn, application_id)
        return _json(state)


async def _read_application_audit_trail(
    server: Any,
    application_id: str,
    *,
    from_position: int | None = None,
    to_position: int | None = None,
) -> str:
    store = await server.get_store()
    events = await store.load_stream(f"audit-loan-{application_id}")

    trail = []
    for event in events:
        if from_position is not None and event.stream_position < from_position:
            continue
        if to_position is not None and event.stream_position > to_position:
            continue

        payload = event.payload
        presented_event_type = event.event_type
        presented_version = event.event_version
        presented_metadata = event.metadata
        presented_recorded_at = (
            event.recorded_at.isoformat() if event.recorded_at else None
        )
        source_stream_id = None
        source_global_position = event.global_position

        if event.event_type == "AuditEventLinked":
            presented_event_type = payload.get("source_event_type", event.event_type)
            presented_version = payload.get("source_event_version", event.event_version)
            presented_metadata = payload.get("metadata_snapshot", {})
            presented_recorded_at = payload.get(
                "source_recorded_at", presented_recorded_at
            )
            source_stream_id = payload.get("source_stream_id")
            source_global_position = payload.get(
                "source_global_position", event.global_position
            )
            payload = payload.get("payload_snapshot", {})

        trail.append(
            {
                "event_id": str(event.event_id),
                "stream_id": event.stream_id,
                "stream_position": event.stream_position,
                "global_position": source_global_position,
                "event_type": presented_event_type,
                "event_version": presented_version,
                "audit_event_type": event.event_type,
                "source_stream_id": source_stream_id,
                "payload": payload,
                "metadata": presented_metadata,
                "recorded_at": presented_recorded_at,
            }
        )

    return _json(
        {
            "application_id": application_id,
            "event_count": len(trail),
            "events": trail,
        }
    )


def register_resources(mcp: Any, server: Any) -> None:
    """Register all MCP resources with the FastMCP server."""

    try:
        from mcp.server.fastmcp import FastMCP  # noqa: F401

        _register_with_fastmcp(mcp, server)
    except ImportError:
        logger.info("mcp_resources_registered_standalone")


def _register_with_fastmcp(mcp: Any, server: Any) -> None:
    """Register resources using FastMCP decorator API."""

    if not hasattr(mcp, "_ledger_query_handlers"):
        mcp._ledger_query_handlers = {}

    @mcp.resource("ledger://applications/{application_id}")
    async def get_application(application_id: str) -> str:
        pool = await server.get_pool()
        async with pool.connection() as conn:
            result = await conn.execute(
                "SELECT * FROM application_summary WHERE application_id = %s",
                (application_id,),
            )
            row = await result.fetchone()
            if not row:
                return _json({"error": f"Application {application_id} not found"})

            columns = [desc[0] for desc in result.description]
            data = dict(zip(columns, row))
            for key, val in data.items():
                if hasattr(val, "isoformat"):
                    data[key] = val.isoformat()
                elif not isinstance(
                    val,
                    (type(None), bool, int, float, str, list, dict),
                ):
                    data[key] = str(val)
            return _json(data)

    @mcp.resource("ledger://applications/{application_id}/compliance")
    async def get_application_compliance(application_id: str) -> str:
        return await _read_compliance(server, application_id)

    async def get_application_compliance_as_of(
        application_id: str,
        as_of: str | None = None,
    ) -> str:
        return await _read_compliance(server, application_id, as_of=as_of)

    mcp._ledger_query_handlers["ledger://applications/{application_id}/compliance"] = (
        get_application_compliance_as_of
    )

    @mcp.resource("ledger://applications/{application_id}/audit-trail")
    async def get_application_audit_trail(application_id: str) -> str:
        return await _read_application_audit_trail(server, application_id)

    async def get_application_audit_trail_ranged(
        application_id: str,
        from_: str | None = None,
        to: str | None = None,
        from_position: str | None = None,
        to_position: str | None = None,
    ) -> str:
        raw_from = from_ if from_ is not None else from_position
        raw_to = to if to is not None else to_position

        if raw_from is not None or raw_to is not None:
            try:
                from_value = int(raw_from) if raw_from is not None else None
                to_value = int(raw_to) if raw_to is not None else None
            except ValueError:
                return _json({"error": "from and to must be integer audit positions"})
        else:
            from_value = None
            to_value = None

        return await _read_application_audit_trail(
            server,
            application_id,
            from_position=from_value,
            to_position=to_value,
        )

    mcp._ledger_query_handlers["ledger://applications/{application_id}/audit-trail"] = (
        get_application_audit_trail_ranged
    )

    @mcp.resource("ledger://agents/{agent_id}/performance")
    async def get_agent_performance(agent_id: str) -> str:
        pool = await server.get_pool()
        async with pool.connection() as conn:
            result = await conn.execute(
                """
                SELECT *,
                    CASE WHEN analyses_completed > 0
                         THEN total_confidence / analyses_completed
                         ELSE 0
                    END AS avg_confidence_score,
                    CASE WHEN analyses_completed > 0
                         THEN total_duration_ms / analyses_completed
                         ELSE 0
                    END AS avg_duration_ms
                FROM agent_performance_ledger
                WHERE agent_id = %s
                ORDER BY last_seen_at DESC
                """,
                (agent_id,),
            )
            rows = await result.fetchall()
            if not rows:
                return _json({"agent_id": agent_id, "models": []})

            columns = [desc[0] for desc in result.description]
            models = []
            for row in rows:
                data = dict(zip(columns, row))
                for key, val in data.items():
                    if hasattr(val, "isoformat"):
                        data[key] = val.isoformat()
                    elif not isinstance(val, (type(None), bool, int, float, str)):
                        data[key] = str(val)
                models.append(data)

            return _json(
                {
                    "agent_id": agent_id,
                    "model_count": len(models),
                    "models": models,
                }
            )

    @mcp.resource("ledger://agents/{agent_id}/sessions/{session_id}")
    async def get_agent_session(agent_id: str, session_id: str) -> str:
        store = await server.get_store()
        context = await reconstruct_agent_context(
            store=store,
            agent_id=agent_id,
            session_id=session_id,
        )
        return _json(
            {
                "agent_id": context.agent_id,
                "session_id": context.session_id,
                "session_health": context.session_health_status.value,
                "model_version": context.model_version,
                "context_source": context.context_source,
                "total_events": context.total_events,
                "last_event_position": context.last_event_position,
                "completed_applications": context.completed_applications,
                "pending_work": [
                    {
                        "application_id": pw.application_id,
                        "event_type": pw.event_type,
                        "description": pw.description,
                    }
                    for pw in context.pending_work
                ],
                "context_text": context.context_text,
            }
        )

    @mcp.resource("ledger://ledger/health")
    async def get_ledger_health() -> str:
        daemon = await server.get_daemon()
        pool = await server.get_pool()
        async with pool.connection() as conn:
            res = await conn.execute(
                "SELECT COUNT(*), MAX(global_position) FROM events"
            )
            row = await res.fetchone()
            total_events = row[0] if row else 0
            head_position = row[1] if row else 0

            res = await conn.execute("SELECT COUNT(*) FROM event_streams")
            row = await res.fetchone()
            total_streams = row[0] if row else 0

            lag_by_projection = await daemon.get_all_lags()
            res = await conn.execute(
                """
                SELECT projection_name, last_position, updated_at
                FROM projection_checkpoints
                """
            )
            rows = await res.fetchall()
            projections = {}
            for name, position, updated in rows:
                projections[name] = {
                    "last_position": position,
                    "lag_events": lag_by_projection.get(
                        name,
                        (head_position or 0) - (position or 0),
                    ),
                    "updated_at": updated.isoformat() if updated else None,
                }

            if "ComplianceAuditView" in projections:
                projections["ComplianceAuditView"]["lag_ms"] = (
                    await get_compliance_projection_lag(conn)
                )

            res = await conn.execute(
                """
                SELECT COUNT(*) FILTER (WHERE skipped_at IS NULL),
                       COUNT(*) FILTER (WHERE skipped_at IS NOT NULL)
                FROM projection_failures
                """
            )
            row = await res.fetchone()
            projection_failures = {
                "pending": row[0] if row else 0,
                "skipped": row[1] if row else 0,
            }

            res = await conn.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE published_at IS NULL) AS pending,
                    COUNT(*) FILTER (WHERE published_at IS NOT NULL) AS published,
                    COUNT(*) FILTER (WHERE attempts > 3) AS failed
                FROM outbox
                """
            )
            row = await res.fetchone()
            outbox = {
                "pending": row[0] if row else 0,
                "published": row[1] if row else 0,
                "failed": row[2] if row else 0,
            }

            return _json(
                {
                    "status": "healthy",
                    "total_events": total_events,
                    "head_position": head_position,
                    "total_streams": total_streams,
                    "projections": projections,
                    "projection_failures": projection_failures,
                    "outbox": outbox,
                }
            )
