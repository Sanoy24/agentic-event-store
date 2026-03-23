from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime
from typing import Any

import structlog
from psycopg_pool import AsyncConnectionPool

from src.event_store import EventStore
from src.models.events import StoredEvent
from src.projections.daemon import Projection

logger = structlog.get_logger()

SNAPSHOT_VERSION = 1
SNAPSHOT_INTERVAL = 3

_INTERESTED_EVENTS = {
    "ComplianceCheckRequested",
    "ComplianceCheckInitiated",
    "ComplianceRuleNoted",
    "ComplianceCheckCompleted",
    "ComplianceRulePassed",
    "ComplianceRuleFailed",
    "ComplianceClearanceIssued",
}

_FINALISING_EVENTS = {
    "ComplianceCheckCompleted",
    "ComplianceClearanceIssued",
    "ComplianceRuleFailed",
}


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _normalise_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return None


def _empty_state(application_id: str) -> dict[str, Any]:
    return {
        "application_id": application_id,
        "regulation_set_version": "",
        "checks_required": [],
        "checks_passed": [],
        "checks_failed": [],
        "rule_results": {},
        "compliance_status": "UNKNOWN",
        "clearance_issued": False,
        "clearance_timestamp": None,
        "clearance_issued_by": None,
        "events_processed": 0,
        "last_event_id": None,
        "last_global_position": 0,
        "last_event_type": None,
        "last_event_at": None,
    }


def _normalise_state(state: dict[str, Any]) -> dict[str, Any]:
    normalised = deepcopy(state)
    normalised["checks_required"] = list(normalised.get("checks_required", []))
    normalised["checks_passed"] = list(normalised.get("checks_passed", []))
    normalised["checks_failed"] = list(normalised.get("checks_failed", []))
    normalised["rule_results"] = dict(normalised.get("rule_results", {}))
    normalised["clearance_timestamp"] = _normalise_timestamp(
        normalised.get("clearance_timestamp")
    )
    normalised["last_event_at"] = _normalise_timestamp(normalised.get("last_event_at"))
    normalised["events_processed"] = int(normalised.get("events_processed", 0) or 0)
    normalised["last_global_position"] = int(
        normalised.get("last_global_position", 0) or 0
    )
    return normalised


def _serialise_state(state: dict[str, Any]) -> dict[str, Any]:
    serialised = deepcopy(state)
    for key in ("clearance_timestamp", "last_event_at"):
        value = serialised.get(key)
        if isinstance(value, datetime):
            serialised[key] = value.isoformat()
    return serialised


def _present_state(state: dict[str, Any], *, as_of: datetime | None = None) -> dict[str, Any]:
    presented = _serialise_state(state)
    rule_results = presented.get("rule_results", {})
    presented["rules"] = [
        {"rule_id": rule_id, **detail}
        for rule_id, detail in sorted(rule_results.items())
    ]
    if as_of is not None:
        presented["as_of"] = as_of.isoformat()
    return presented


def _ensure_rule_entry(
    state: dict[str, Any],
    rule_id: str,
    *,
    regulation_set_version: str | None = None,
) -> dict[str, Any]:
    existing = dict(state["rule_results"].get(rule_id, {}))
    existing.setdefault("status", "PENDING")
    existing.setdefault("regulation_set_version", regulation_set_version or "")
    state["rule_results"][rule_id] = existing
    return existing


def _sorted_unique(values: list[str]) -> list[str]:
    return sorted({value for value in values if value})


def _apply_event_to_state(
    state: dict[str, Any],
    event_type: str,
    payload: dict[str, Any],
    *,
    recorded_at: datetime | None = None,
    event_id: Any = None,
    global_position: int | None = None,
) -> dict[str, Any]:
    state = _normalise_state(state)
    regulation_set_version = payload.get(
        "regulation_set_version", state.get("regulation_set_version", "")
    )

    if event_type == "ComplianceCheckRequested":
        state["regulation_set_version"] = regulation_set_version or ""
        state["checks_required"] = list(payload.get("checks_required", []))
        for rule_id in state["checks_required"]:
            _ensure_rule_entry(
                state,
                rule_id,
                regulation_set_version=state["regulation_set_version"],
            )
        state["compliance_status"] = "IN_PROGRESS"

    elif event_type == "ComplianceCheckInitiated":
        state["compliance_status"] = (
            "FAILED" if state["checks_failed"] else "IN_PROGRESS"
        )

    elif event_type == "ComplianceRuleNoted":
        rule_id = payload.get("rule_id", "")
        if rule_id:
            rule_entry = _ensure_rule_entry(
                state,
                rule_id,
                regulation_set_version=state.get("regulation_set_version", ""),
            )
            notes = list(rule_entry.get("notes", []))
            notes.append(
                {
                    "note_type": payload.get("note_type", ""),
                    "note": payload.get("note", ""),
                    "recorded_at": recorded_at.isoformat() if recorded_at else None,
                }
            )
            rule_entry["notes"] = notes

    elif event_type == "ComplianceCheckCompleted":
        if payload.get("has_hard_block", False):
            state["compliance_status"] = "FAILED"
        elif not state["clearance_issued"]:
            state["compliance_status"] = "IN_PROGRESS"

    elif event_type == "ComplianceRulePassed":
        rule_id = payload.get("rule_id", "")
        if rule_id:
            state["checks_passed"] = _sorted_unique(state["checks_passed"] + [rule_id])
            state["checks_failed"] = [
                existing for existing in state["checks_failed"] if existing != rule_id
            ]
            rule_entry = _ensure_rule_entry(
                state,
                rule_id,
                regulation_set_version=state.get("regulation_set_version", ""),
            )
            rule_entry.update(
                {
                    "status": "PASSED",
                    "rule_version": payload.get("rule_version"),
                    "regulation_set_version": state.get("regulation_set_version", ""),
                    "evaluation_timestamp": payload.get("evaluation_timestamp"),
                    "evidence_hash": payload.get("evidence_hash", ""),
                    "failure_reason": None,
                    "remediation_required": False,
                }
            )
            if not state["clearance_issued"] and not state["checks_failed"]:
                state["compliance_status"] = "IN_PROGRESS"

    elif event_type == "ComplianceRuleFailed":
        rule_id = payload.get("rule_id", "")
        if rule_id:
            state["checks_failed"] = _sorted_unique(state["checks_failed"] + [rule_id])
            state["checks_passed"] = [
                existing for existing in state["checks_passed"] if existing != rule_id
            ]
            rule_entry = _ensure_rule_entry(
                state,
                rule_id,
                regulation_set_version=state.get("regulation_set_version", ""),
            )
            rule_entry.update(
                {
                    "status": "FAILED",
                    "rule_version": payload.get("rule_version"),
                    "regulation_set_version": state.get("regulation_set_version", ""),
                    "failure_reason": payload.get("failure_reason", ""),
                    "remediation_required": payload.get(
                        "remediation_required", False
                    ),
                }
            )
            state["compliance_status"] = "FAILED"

    elif event_type == "ComplianceClearanceIssued":
        state["regulation_set_version"] = regulation_set_version or state.get(
            "regulation_set_version", ""
        )
        for rule_id in payload.get("checks_passed", []):
            state["checks_passed"] = _sorted_unique(state["checks_passed"] + [rule_id])
            rule_entry = _ensure_rule_entry(
                state,
                rule_id,
                regulation_set_version=state.get("regulation_set_version", ""),
            )
            rule_entry.setdefault("status", "PASSED")
            rule_entry["regulation_set_version"] = state.get(
                "regulation_set_version", ""
            )
        state["clearance_issued"] = True
        state["clearance_timestamp"] = _normalise_timestamp(
            payload.get("clearance_timestamp")
        )
        state["clearance_issued_by"] = payload.get("issued_by", "")
        state["compliance_status"] = "CLEARED"

    if recorded_at is not None:
        state["last_event_at"] = recorded_at
    if event_id is not None:
        state["last_event_id"] = event_id
    if global_position is not None:
        state["last_global_position"] = global_position
    if event_type:
        state["last_event_type"] = event_type
        state["events_processed"] = int(state.get("events_processed", 0) or 0) + 1

    return state


def _event_log_row(event: StoredEvent, regulation_set_version: str) -> dict[str, Any]:
    payload = event.payload
    verdict = "RECORDED"
    rule_id = payload.get("rule_id")
    rule_version = payload.get("rule_version")

    if event.event_type == "ComplianceCheckRequested":
        verdict = "REQUESTED"
    elif event.event_type == "ComplianceRulePassed":
        verdict = "PASSED"
    elif event.event_type == "ComplianceRuleFailed":
        verdict = "FAILED"
    elif event.event_type == "ComplianceClearanceIssued":
        verdict = "CLEARED"
    elif event.event_type == "ComplianceCheckCompleted":
        verdict = payload.get("overall_verdict", "COMPLETED")
    elif event.event_type == "ComplianceCheckInitiated":
        verdict = "INITIATED"
    elif event.event_type == "ComplianceRuleNoted":
        verdict = payload.get("note_type", "NOTED")

    return {
        "event_id": event.event_id,
        "global_position": event.global_position,
        "application_id": payload["application_id"],
        "event_type": event.event_type,
        "regulation_set_version": payload.get(
            "regulation_set_version", regulation_set_version
        ),
        "rule_id": rule_id,
        "rule_version": rule_version,
        "verdict": verdict,
        "detail": payload,
        "event_timestamp": event.recorded_at,
    }


class ComplianceAuditProjection(Projection):
    """
    Regulatory read model for compliance.

    Design:
    - `compliance_audit_view` stores the current full compliance record.
    - `compliance_audit_events` stores every compliance event immutably.
    - `compliance_audit_snapshots` stores periodic state snapshots to accelerate
      temporal queries while keeping historical replay exact.
    """

    def __init__(
        self,
        *,
        view_table: str = "compliance_audit_view",
        event_table: str = "compliance_audit_events",
        snapshot_table: str = "compliance_audit_snapshots",
        projection_name: str = "ComplianceAuditView",
    ) -> None:
        self._view_table = view_table
        self._event_table = event_table
        self._snapshot_table = snapshot_table
        self._projection_name = projection_name

    @property
    def name(self) -> str:
        return self._projection_name

    def interested_in(self, event_type: str) -> bool:
        return event_type in _INTERESTED_EVENTS

    def event_types(self) -> set[str] | None:
        return set(_INTERESTED_EVENTS)

    async def handle(self, event: StoredEvent, conn: Any) -> None:
        application_id = event.payload.get("application_id")
        if not application_id:
            return

        current_state = await self._load_state(conn, application_id)
        if current_state is not None:
            current_state = _normalise_state(current_state)
            if event.global_position <= current_state.get("last_global_position", 0):
                return
        else:
            current_state = _empty_state(application_id)

        next_state = _apply_event_to_state(
            current_state,
            event.event_type,
            event.payload,
            recorded_at=event.recorded_at,
            event_id=event.event_id,
            global_position=event.global_position,
        )

        await self._upsert_state(conn, next_state)
        await self._log_event(conn, event, next_state)

        if self._should_snapshot(next_state, event):
            await self._write_snapshot(conn, next_state, event)

    async def handle_batch(self, events: list[StoredEvent], conn: Any) -> None:
        application_ids = sorted(
            {
                event.payload.get("application_id")
                for event in events
                if event.payload.get("application_id")
            }
        )
        if not application_ids:
            return

        state_cache = await self._load_states(conn, application_ids)
        dirty_states: dict[str, dict[str, Any]] = {}
        event_rows: list[tuple[Any, ...]] = []
        snapshot_rows: list[tuple[Any, ...]] = []

        for event in events:
            application_id = event.payload.get("application_id")
            if not application_id:
                continue

            current_state = _normalise_state(
                state_cache.get(application_id, _empty_state(application_id))
            )
            if event.global_position <= current_state.get("last_global_position", 0):
                continue

            next_state = _apply_event_to_state(
                current_state,
                event.event_type,
                event.payload,
                recorded_at=event.recorded_at,
                event_id=event.event_id,
                global_position=event.global_position,
            )

            state_cache[application_id] = next_state
            dirty_states[application_id] = next_state

            event_row = _event_log_row(
                event,
                next_state.get("regulation_set_version", ""),
            )
            event_rows.append(
                (
                    event_row["event_id"],
                    event_row["global_position"],
                    event_row["application_id"],
                    event_row["event_type"],
                    event_row["regulation_set_version"],
                    event_row["rule_id"],
                    event_row["rule_version"],
                    event_row["verdict"],
                    json.dumps(event_row["detail"], default=_json_default),
                    event_row["event_timestamp"],
                )
            )

            if self._should_snapshot(next_state, event):
                snapshot_rows.append(
                    (
                        next_state["application_id"],
                        SNAPSHOT_VERSION,
                        event.event_id,
                        event.global_position,
                        event.recorded_at,
                        json.dumps(_serialise_state(next_state), default=_json_default),
                    )
                )

        if dirty_states:
            async with conn.cursor() as cur:
                await cur.executemany(
                    f"""
                    INSERT INTO {self._view_table} (
                        application_id,
                        regulation_set_version,
                        checks_required,
                        checks_passed,
                        checks_failed,
                        rule_results,
                        compliance_status,
                        clearance_issued,
                        clearance_timestamp,
                        clearance_issued_by,
                        events_processed,
                        last_event_id,
                        last_global_position,
                        last_event_type,
                        last_event_at,
                        updated_at
                    ) VALUES (
                        %s, %s, %s::JSONB, %s::JSONB, %s::JSONB, %s::JSONB,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                    )
                    ON CONFLICT (application_id) DO UPDATE SET
                        regulation_set_version = EXCLUDED.regulation_set_version,
                        checks_required = EXCLUDED.checks_required,
                        checks_passed = EXCLUDED.checks_passed,
                        checks_failed = EXCLUDED.checks_failed,
                        rule_results = EXCLUDED.rule_results,
                        compliance_status = EXCLUDED.compliance_status,
                        clearance_issued = EXCLUDED.clearance_issued,
                        clearance_timestamp = EXCLUDED.clearance_timestamp,
                        clearance_issued_by = EXCLUDED.clearance_issued_by,
                        events_processed = EXCLUDED.events_processed,
                        last_event_id = EXCLUDED.last_event_id,
                        last_global_position = EXCLUDED.last_global_position,
                        last_event_type = EXCLUDED.last_event_type,
                        last_event_at = EXCLUDED.last_event_at,
                        updated_at = NOW()
                    """,
                    [
                        (
                            state["application_id"],
                            state.get("regulation_set_version", ""),
                            json.dumps(state.get("checks_required", []), default=_json_default),
                            json.dumps(state.get("checks_passed", []), default=_json_default),
                            json.dumps(state.get("checks_failed", []), default=_json_default),
                            json.dumps(state.get("rule_results", {}), default=_json_default),
                            state.get("compliance_status", "UNKNOWN"),
                            state.get("clearance_issued", False),
                            state.get("clearance_timestamp"),
                            state.get("clearance_issued_by"),
                            state.get("events_processed", 0),
                            state.get("last_event_id"),
                            state.get("last_global_position", 0),
                            state.get("last_event_type"),
                            state.get("last_event_at"),
                        )
                        for state in dirty_states.values()
                    ],
                )

        if event_rows:
            async with conn.cursor() as cur:
                await cur.executemany(
                    f"""
                    INSERT INTO {self._event_table} (
                        event_id,
                        global_position,
                        application_id,
                        event_type,
                        regulation_set_version,
                        rule_id,
                        rule_version,
                        verdict,
                        detail,
                        event_timestamp
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s::JSONB, %s
                    )
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    event_rows,
                )

        if snapshot_rows:
            async with conn.cursor() as cur:
                await cur.executemany(
                    f"""
                    INSERT INTO {self._snapshot_table} (
                        application_id,
                        snapshot_version,
                        source_event_id,
                        source_global_position,
                        snapshot_taken_at,
                        state
                    ) VALUES (%s, %s, %s, %s, %s, %s::JSONB)
                    ON CONFLICT (source_event_id) DO NOTHING
                    """,
                    snapshot_rows,
                )

    async def rebuild_from_scratch(
        self,
        store: EventStore,
        pool: AsyncConnectionPool,
        batch_size: int = 100,
    ) -> int | None:
        if self._projection_name != "ComplianceAuditView":
            return None

        shadow_view = f"{self._view_table}__rebuild"
        shadow_events = f"{self._event_table}__rebuild"
        shadow_snapshots = f"{self._snapshot_table}__rebuild"

        async with pool.connection() as conn:
            await conn.execute(f"DROP TABLE IF EXISTS {shadow_snapshots}")
            await conn.execute(f"DROP TABLE IF EXISTS {shadow_events}")
            await conn.execute(f"DROP TABLE IF EXISTS {shadow_view}")
            await conn.execute(
                f"CREATE TABLE {shadow_view} (LIKE {self._view_table} INCLUDING ALL)"
            )
            await conn.execute(
                f"CREATE TABLE {shadow_events} (LIKE {self._event_table} INCLUDING ALL)"
            )
            await conn.execute(
                f"ALTER TABLE {shadow_events} ALTER COLUMN id DROP DEFAULT"
            )
            await conn.execute(
                f"ALTER TABLE {shadow_events} "
                f"ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY"
            )
            await conn.execute(
                f"CREATE TABLE {shadow_snapshots} "
                f"(LIKE {self._snapshot_table} INCLUDING ALL)"
            )
            await conn.commit()

        shadow_projection = ComplianceAuditProjection(
            view_table=shadow_view,
            event_table=shadow_events,
            snapshot_table=shadow_snapshots,
            projection_name=f"{self._projection_name}__rebuild",
        )

        last_position = 0
        total_processed = 0

        while True:
            batch: list[StoredEvent] = []
            async for stored_event in store.load_all(
                from_global_position=last_position,
                batch_size=batch_size,
            ):
                batch.append(stored_event)
                if len(batch) >= batch_size:
                    break

            if not batch:
                break

            async with pool.connection() as conn:
                for stored_event in batch:
                    if shadow_projection.interested_in(stored_event.event_type):
                        await shadow_projection.handle(stored_event, conn)
                await conn.commit()

            last_position = batch[-1].global_position
            total_processed += sum(
                1 for stored_event in batch if self.interested_in(stored_event.event_type)
            )

        async with pool.connection() as conn:
            async with conn.transaction():
                await conn.execute(f"ALTER TABLE {self._view_table} RENAME TO {self._view_table}__old")
                await conn.execute(
                    f"ALTER TABLE {self._event_table} RENAME TO {self._event_table}__old"
                )
                await conn.execute(
                    f"ALTER TABLE {self._snapshot_table} RENAME TO {self._snapshot_table}__old"
                )
                await conn.execute(
                    f"ALTER TABLE {shadow_view} RENAME TO {self._view_table}"
                )
                await conn.execute(
                    f"ALTER TABLE {shadow_events} RENAME TO {self._event_table}"
                )
                await conn.execute(
                    f"ALTER TABLE {shadow_snapshots} RENAME TO {self._snapshot_table}"
                )
                await conn.execute(
                    """
                    INSERT INTO projection_checkpoints (
                        projection_name, last_position, updated_at
                    ) VALUES (%s, %s, NOW())
                    ON CONFLICT (projection_name) DO UPDATE
                    SET last_position = EXCLUDED.last_position,
                        updated_at = NOW()
                    """,
                    (self._projection_name, last_position),
                )

            await conn.execute(
                f"DROP TABLE IF EXISTS {self._snapshot_table}__old CASCADE"
            )
            await conn.execute(
                f"DROP TABLE IF EXISTS {self._event_table}__old CASCADE"
            )
            await conn.execute(
                f"DROP TABLE IF EXISTS {self._view_table}__old CASCADE"
            )
            await conn.commit()

        logger.info(
            "compliance_audit_rebuilt",
            processed_events=total_processed,
            checkpoint=last_position,
        )
        return total_processed

    async def _load_state(self, conn: Any, application_id: str) -> dict[str, Any] | None:
        result = await conn.execute(
            f"""
            SELECT
                application_id,
                regulation_set_version,
                checks_required,
                checks_passed,
                checks_failed,
                rule_results,
                compliance_status,
                clearance_issued,
                clearance_timestamp,
                clearance_issued_by,
                events_processed,
                last_event_id,
                last_global_position,
                last_event_type,
                last_event_at
            FROM {self._view_table}
            WHERE application_id = %s
            """,
            (application_id,),
        )
        row = await result.fetchone()
        if row is None:
            return None

        return {
            "application_id": row[0],
            "regulation_set_version": row[1] or "",
            "checks_required": row[2] or [],
            "checks_passed": row[3] or [],
            "checks_failed": row[4] or [],
            "rule_results": row[5] or {},
            "compliance_status": row[6] or "UNKNOWN",
            "clearance_issued": bool(row[7]),
            "clearance_timestamp": row[8],
            "clearance_issued_by": row[9],
            "events_processed": row[10] or 0,
            "last_event_id": row[11],
            "last_global_position": row[12] or 0,
            "last_event_type": row[13],
            "last_event_at": row[14],
        }

    async def _load_states(
        self,
        conn: Any,
        application_ids: list[str],
    ) -> dict[str, dict[str, Any]]:
        if not application_ids:
            return {}

        result = await conn.execute(
            f"""
            SELECT
                application_id,
                regulation_set_version,
                checks_required,
                checks_passed,
                checks_failed,
                rule_results,
                compliance_status,
                clearance_issued,
                clearance_timestamp,
                clearance_issued_by,
                events_processed,
                last_event_id,
                last_global_position,
                last_event_type,
                last_event_at
            FROM {self._view_table}
            WHERE application_id = ANY(%s)
            """,
            (application_ids,),
        )
        rows = await result.fetchall()
        return {
            row[0]: {
                "application_id": row[0],
                "regulation_set_version": row[1] or "",
                "checks_required": row[2] or [],
                "checks_passed": row[3] or [],
                "checks_failed": row[4] or [],
                "rule_results": row[5] or {},
                "compliance_status": row[6] or "UNKNOWN",
                "clearance_issued": bool(row[7]),
                "clearance_timestamp": row[8],
                "clearance_issued_by": row[9],
                "events_processed": row[10] or 0,
                "last_event_id": row[11],
                "last_global_position": row[12] or 0,
                "last_event_type": row[13],
                "last_event_at": row[14],
            }
            for row in rows
        }

    async def _upsert_state(self, conn: Any, state: dict[str, Any]) -> None:
        await conn.execute(
            f"""
            INSERT INTO {self._view_table} (
                application_id,
                regulation_set_version,
                checks_required,
                checks_passed,
                checks_failed,
                rule_results,
                compliance_status,
                clearance_issued,
                clearance_timestamp,
                clearance_issued_by,
                events_processed,
                last_event_id,
                last_global_position,
                last_event_type,
                last_event_at,
                updated_at
            ) VALUES (
                %s, %s, %s::JSONB, %s::JSONB, %s::JSONB, %s::JSONB,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
            )
            ON CONFLICT (application_id) DO UPDATE SET
                regulation_set_version = EXCLUDED.regulation_set_version,
                checks_required = EXCLUDED.checks_required,
                checks_passed = EXCLUDED.checks_passed,
                checks_failed = EXCLUDED.checks_failed,
                rule_results = EXCLUDED.rule_results,
                compliance_status = EXCLUDED.compliance_status,
                clearance_issued = EXCLUDED.clearance_issued,
                clearance_timestamp = EXCLUDED.clearance_timestamp,
                clearance_issued_by = EXCLUDED.clearance_issued_by,
                events_processed = EXCLUDED.events_processed,
                last_event_id = EXCLUDED.last_event_id,
                last_global_position = EXCLUDED.last_global_position,
                last_event_type = EXCLUDED.last_event_type,
                last_event_at = EXCLUDED.last_event_at,
                updated_at = NOW()
            """,
            (
                state["application_id"],
                state.get("regulation_set_version", ""),
                json.dumps(state.get("checks_required", []), default=_json_default),
                json.dumps(state.get("checks_passed", []), default=_json_default),
                json.dumps(state.get("checks_failed", []), default=_json_default),
                json.dumps(state.get("rule_results", {}), default=_json_default),
                state.get("compliance_status", "UNKNOWN"),
                state.get("clearance_issued", False),
                state.get("clearance_timestamp"),
                state.get("clearance_issued_by"),
                state.get("events_processed", 0),
                state.get("last_event_id"),
                state.get("last_global_position", 0),
                state.get("last_event_type"),
                state.get("last_event_at"),
            ),
        )

    async def _log_event(
        self,
        conn: Any,
        event: StoredEvent,
        state: dict[str, Any],
    ) -> None:
        event_row = _event_log_row(
            event,
            state.get("regulation_set_version", ""),
        )
        await conn.execute(
            f"""
            INSERT INTO {self._event_table} (
                event_id,
                global_position,
                application_id,
                event_type,
                regulation_set_version,
                rule_id,
                rule_version,
                verdict,
                detail,
                event_timestamp
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s::JSONB, %s
            )
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                event_row["event_id"],
                event_row["global_position"],
                event_row["application_id"],
                event_row["event_type"],
                event_row["regulation_set_version"],
                event_row["rule_id"],
                event_row["rule_version"],
                event_row["verdict"],
                json.dumps(event_row["detail"], default=_json_default),
                event_row["event_timestamp"],
            ),
        )

    def _should_snapshot(self, state: dict[str, Any], event: StoredEvent) -> bool:
        event_count = int(state.get("events_processed", 0) or 0)
        return (
            event_count > 0
            and (
                event.event_type in _FINALISING_EVENTS
                or event_count % SNAPSHOT_INTERVAL == 0
            )
        )

    async def _write_snapshot(
        self,
        conn: Any,
        state: dict[str, Any],
        event: StoredEvent,
    ) -> None:
        await conn.execute(
            f"""
            INSERT INTO {self._snapshot_table} (
                application_id,
                snapshot_version,
                source_event_id,
                source_global_position,
                snapshot_taken_at,
                state
            ) VALUES (%s, %s, %s, %s, %s, %s::JSONB)
            ON CONFLICT (source_event_id) DO NOTHING
            """,
            (
                state["application_id"],
                SNAPSHOT_VERSION,
                event.event_id,
                event.global_position,
                event.recorded_at,
                json.dumps(_serialise_state(state), default=_json_default),
            ),
        )


async def get_current_compliance(conn: Any, application_id: str) -> dict[str, Any]:
    projection = ComplianceAuditProjection()
    state = await projection._load_state(conn, application_id)
    if state is None:
        return {
            "application_id": application_id,
            "compliance_status": "NO_RECORD",
            "checks_required": [],
            "checks_passed": [],
            "checks_failed": [],
            "rule_results": {},
            "rules": [],
        }
    return _present_state(state)


async def get_compliance_at(
    conn: Any,
    application_id: str,
    at_timestamp: datetime,
) -> dict[str, Any]:
    snapshot_result = await conn.execute(
        """
        SELECT state, source_global_position
        FROM compliance_audit_snapshots
        WHERE application_id = %s
          AND snapshot_version = %s
          AND snapshot_taken_at <= %s
        ORDER BY snapshot_taken_at DESC, source_global_position DESC
        LIMIT 1
        """,
        (application_id, SNAPSHOT_VERSION, at_timestamp),
    )
    snapshot_row = await snapshot_result.fetchone()

    if snapshot_row is None:
        state = _empty_state(application_id)
        from_position = 0
    else:
        state = _normalise_state(snapshot_row[0])
        from_position = snapshot_row[1]

    result = await conn.execute(
        """
        SELECT
            global_position,
            event_type,
            detail,
            event_timestamp,
            event_id
        FROM compliance_audit_events
        WHERE application_id = %s
          AND event_timestamp <= %s
          AND global_position > %s
        ORDER BY event_timestamp ASC, global_position ASC
        """,
        (application_id, at_timestamp, from_position),
    )
    rows = await result.fetchall()

    for global_position, event_type, detail, event_timestamp, event_id in rows:
        state = _apply_event_to_state(
            state,
            event_type,
            detail or {},
            recorded_at=event_timestamp,
            event_id=event_id,
            global_position=global_position,
        )

    return _present_state(state, as_of=at_timestamp)


async def get_state_at(
    conn: Any,
    application_id: str,
    at_timestamp: datetime,
) -> dict[str, Any]:
    return await get_compliance_at(conn, application_id, at_timestamp)


async def get_projection_lag(conn: Any) -> int | None:
    result = await conn.execute(
        """
        WITH head AS (
            SELECT global_position, recorded_at
            FROM events
            WHERE event_type = ANY(%s)
            ORDER BY global_position DESC
            LIMIT 1
        ),
        checkpoint AS (
            SELECT last_position
            FROM projection_checkpoints
            WHERE projection_name = 'ComplianceAuditView'
        ),
        processed AS (
            SELECT e.global_position, e.recorded_at
            FROM checkpoint c
            LEFT JOIN events e ON e.global_position = c.last_position
        )
        SELECT CASE
            WHEN NOT EXISTS (SELECT 1 FROM head) THEN 0
            WHEN NOT EXISTS (SELECT 1 FROM processed WHERE global_position IS NOT NULL) THEN NULL
            WHEN (SELECT global_position FROM processed) >= (SELECT global_position FROM head) THEN 0
            ELSE GREATEST(
                0,
                FLOOR(
                    EXTRACT(
                        EPOCH FROM (
                            (SELECT recorded_at FROM head) -
                            (SELECT recorded_at FROM processed)
                        )
                    ) * 1000
                )::INT
            )
        END
        """
        ,
        (list(_INTERESTED_EVENTS),),
    )
    row = await result.fetchone()
    return row[0] if row else None


async def rebuild_from_scratch(
    store: EventStore,
    pool: AsyncConnectionPool,
    batch_size: int = 100,
) -> int:
    projection = ComplianceAuditProjection()
    rebuilt = await projection.rebuild_from_scratch(store, pool, batch_size)
    return rebuilt or 0
