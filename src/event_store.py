# src/event_store.py
# =============================================================================
# TRP1 LEDGER — Event Store Core
# =============================================================================
# Source: Challenge Doc Phase 1 pages 8-9 + Manual Part IV p.20-21 + Part V p.25-26
#
# Fully async implementation using psycopg 3.2+.
# The interface is fixed by the challenge spec — implementation is mine.
#
# Key design decisions:
# - Row-level FOR UPDATE lock on event_streams for OCC (not table lock)
# - Pipeline mode for batched insert performance
# - Metadata envelope on every event (correlation_id, causation_id)
# - Optional UpcasterRegistry for transparent version migration on read
# =============================================================================
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID

import psycopg
import psycopg.errors
import structlog
from psycopg_pool import AsyncConnectionPool

from src.models.events import (
    AuditEventLinked,
    BaseEvent,
    OptimisticConcurrencyError,
    StoredEvent,
    StreamArchivedError,
    StreamMetadata,
)

logger = structlog.get_logger()


class EventStore:
    """
    Async event store backed by PostgreSQL via psycopg 3.2+.

    All writes are atomic — events and outbox entries are written in the
    same transaction. Optimistic concurrency is enforced via expected_version.
    """

    def __init__(
        self,
        pool: AsyncConnectionPool,
        upcaster_registry: Any | None = None,
    ):
        self._pool = pool
        self._upcasters = upcaster_registry  # Optional for interim — can be None

    # =========================================================================
    # WRITE PATH
    # =========================================================================

    async def append(
        self,
        stream_id: str,
        events: list[BaseEvent],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> int:
        """
        Atomically appends events to stream_id.

        Raises OptimisticConcurrencyError if stream version != expected_version.
        Writes to outbox in same transaction as events.

        Implementation steps:
          1. BEGIN
          2. SELECT current_version FROM event_streams FOR UPDATE (row-level lock)
          3. If stream doesn't exist and expected_version == -1: INSERT into event_streams
          4. Check: if current_version != expected_version → ROLLBACK → raise OCC error
          5. INSERT all events into events table (pipeline batch)
          6. UPDATE event_streams SET current_version = new_version
          7. INSERT outbox row for each event
          8. COMMIT
          9. On UniqueViolation → raise OptimisticConcurrencyError

        Args:
            stream_id: The stream to append to (e.g., "loan-{uuid}")
            events: List of domain events to append
            expected_version: -1 for new stream; N for exact version required
            correlation_id: Request-level correlation ID for tracing
            causation_id: ID of the event/command that caused these events

        Returns:
            New stream version (int) after append
        """
        if not events:
            raise ValueError("Cannot append empty event list")

        log = logger.bind(
            stream_id=stream_id,
            expected_version=expected_version,
            event_count=len(events),
            correlation_id=correlation_id,
        )

        try:
            async with self._pool.connection() as conn:
                async with conn.transaction():
                    # Step 2: Lock the stream row (or create it)
                    actual_version = await self._get_or_create_stream(
                        conn, stream_id, expected_version
                    )

                    # Step 4: OCC check
                    if actual_version != expected_version:
                        raise OptimisticConcurrencyError(
                            stream_id=stream_id,
                            expected=expected_version,
                            actual=actual_version,
                        )

                    new_version, stored_events = await self._append_events_to_stream(
                        conn=conn,
                        stream_id=stream_id,
                        starting_version=max(0, actual_version),
                        events=events,
                        correlation_id=correlation_id,
                        causation_id=causation_id,
                    )

                    if not stream_id.startswith("audit-"):
                        await self._append_audit_entries(
                            conn=conn,
                            stored_events=stored_events,
                        )

                    log.info(
                        "events_appended",
                        new_version=new_version,
                        event_types=[e.event_type for e in events],
                    )
                    return new_version

        except psycopg.errors.UniqueViolation:
            # Step 9: UNIQUE constraint violation on (stream_id, stream_position)
            # This IS the OCC mechanism at the database level.
            current = await self.stream_version(stream_id)
            log.warning(
                "optimistic_concurrency_conflict",
                actual_version=current,
            )
            raise OptimisticConcurrencyError(
                stream_id=stream_id,
                expected=expected_version,
                actual=current,
            )

    async def _get_or_create_stream(
        self,
        conn: psycopg.AsyncConnection,
        stream_id: str,
        expected_version: int,
    ) -> int:
        """
        Get current stream version with FOR UPDATE lock, or create stream
        if expected_version == -1 (new stream).

        Uses row-level lock (FOR UPDATE) on event_streams — NOT a table lock.
        "100 loans with 4 agents each = 400 concurrent writers. A table lock
        would serialize all of them. Row-level lock only prevents concurrent
        writes to the SAME stream — different loans proceed independently."
        (Manual p.8)
        """
        # Try to get existing stream
        result = await conn.execute(
            """
            SELECT current_version, archived_at
            FROM event_streams
            WHERE stream_id = %s
            FOR UPDATE
            """,
            (stream_id,),
        )
        row = await result.fetchone()

        if row is not None:
            current_version, archived_at = row
            if archived_at is not None:
                raise StreamArchivedError(stream_id)
            return current_version

        # Stream doesn't exist
        if expected_version == -1:
            # Create new stream
            aggregate_type = self._extract_aggregate_type(stream_id)
            await conn.execute(
                """
                INSERT INTO event_streams (stream_id, aggregate_type, current_version)
                VALUES (%s, %s, 0)
                """,
                (stream_id, aggregate_type),
            )
            return -1

        # Stream doesn't exist but expected_version != -1
        return -1

    @staticmethod
    def _extract_aggregate_type(stream_id: str) -> str:
        """
        Extract aggregate type from stream_id.
        "loan-{uuid}" → "loan"
        "agent-{id}-{session}" → "agent"
        "compliance-{uuid}" → "compliance"
        "audit-{type}-{id}" → "audit"
        """
        return stream_id.split("-")[0]

    async def _append_events_to_stream(
        self,
        *,
        conn: psycopg.AsyncConnection,
        stream_id: str,
        starting_version: int,
        events: list[BaseEvent],
        correlation_id: str | None,
        causation_id: str | None,
    ) -> tuple[int, list[StoredEvent]]:
        new_version = starting_version
        stored_events: list[StoredEvent] = []

        for event in events:
            new_version += 1
            event_payload = event.model_dump(mode="json")
            event_metadata = {
                "correlation_id": correlation_id,
                "causation_id": causation_id,
                "recorded_by": "event_store",
                "schema_version": event.event_version,
            }

            row = await conn.execute(
                """
                INSERT INTO events (
                    stream_id, stream_position, event_type,
                    event_version, payload, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING event_id, global_position, recorded_at
                """,
                (
                    stream_id,
                    new_version,
                    event.event_type,
                    event.event_version,
                    psycopg.types.json.Jsonb(event_payload),
                    psycopg.types.json.Jsonb(event_metadata),
                ),
            )
            result = await row.fetchone()
            if result:
                stored_events.append(
                    StoredEvent(
                        event_id=result[0],
                        stream_id=stream_id,
                        stream_position=new_version,
                        global_position=result[1],
                        event_type=event.event_type,
                        event_version=event.event_version,
                        payload=event_payload,
                        metadata=event_metadata,
                        recorded_at=result[2],
                    )
                )

        await conn.execute(
            """
            UPDATE event_streams
            SET current_version = %s
            WHERE stream_id = %s
            """,
            (new_version, stream_id),
        )

        for stored_event in stored_events:
            await conn.execute(
                """
                INSERT INTO outbox (event_id, destination, payload)
                VALUES (%s, %s, %s)
                """,
                (
                    stored_event.event_id,
                    f"default:{stored_event.event_type}",
                    psycopg.types.json.Jsonb(stored_event.payload),
                ),
            )

        return new_version, stored_events

    async def _append_audit_entries(
        self,
        *,
        conn: psycopg.AsyncConnection,
        stored_events: list[StoredEvent],
    ) -> None:
        grouped_events: dict[str, list[AuditEventLinked]] = {}

        for stored_event in stored_events:
            audit_target = self._infer_audit_target(stored_event)
            if audit_target is None:
                continue

            entity_type, entity_id = audit_target
            audit_stream_id = f"audit-{entity_type}-{entity_id}"
            grouped_events.setdefault(audit_stream_id, []).append(
                AuditEventLinked(
                    entity_id=entity_id,
                    source_event_id=str(stored_event.event_id),
                    source_stream_id=stored_event.stream_id,
                    source_stream_position=stored_event.stream_position,
                    source_global_position=stored_event.global_position,
                    source_event_type=stored_event.event_type,
                    source_event_version=stored_event.event_version,
                    source_recorded_at=stored_event.recorded_at,
                    correlation_id=stored_event.metadata.get("correlation_id"),
                    causation_id=stored_event.metadata.get("causation_id"),
                    payload_snapshot=stored_event.payload,
                    metadata_snapshot=stored_event.metadata,
                )
            )

        for audit_stream_id, audit_events in grouped_events.items():
            audit_version = await self._get_or_create_stream(
                conn,
                audit_stream_id,
                expected_version=-1,
            )
            await self._append_events_to_stream(
                conn=conn,
                stream_id=audit_stream_id,
                starting_version=max(0, audit_version),
                events=audit_events,
                correlation_id=audit_events[-1].correlation_id,
                causation_id=audit_events[-1].source_event_id,
            )

    @staticmethod
    def _infer_audit_target(stored_event: StoredEvent) -> tuple[str, str] | None:
        if stored_event.stream_id.startswith("audit-"):
            return None

        application_id = stored_event.payload.get("application_id")
        if application_id:
            return ("loan", application_id)

        if stored_event.stream_id.startswith("loan-"):
            return ("loan", stored_event.stream_id.removeprefix("loan-"))

        return None

    # =========================================================================
    # READ PATH
    # =========================================================================

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        """
        Load events for stream_id in stream_position order.

        If upcaster_registry is set, call registry.upcast(event) on every event
        before returning. The raw stored payload is NEVER modified.

        Implements Manual Query 1 (p.25):
            SELECT ... WHERE stream_id = $1
            ORDER BY stream_position ASC
            (with optional position bounds)
        """
        async with self._pool.connection() as conn:
            if to_position is not None:
                result = await conn.execute(
                    """
                    SELECT event_id, stream_id, stream_position, global_position,
                           event_type, event_version, payload, metadata, recorded_at
                    FROM events
                    WHERE stream_id = %s
                      AND stream_position > %s
                      AND stream_position <= %s
                    ORDER BY stream_position ASC
                    """,
                    (stream_id, from_position, to_position),
                )
            else:
                result = await conn.execute(
                    """
                    SELECT event_id, stream_id, stream_position, global_position,
                           event_type, event_version, payload, metadata, recorded_at
                    FROM events
                    WHERE stream_id = %s
                      AND stream_position > %s
                    ORDER BY stream_position ASC
                    """,
                    (stream_id, from_position),
                )

            rows = await result.fetchall()
            events = [self._row_to_stored_event(row) for row in rows]

            # Apply upcasters if registry is available
            if self._upcasters is not None:
                events = [
                    await self._upcasters.upcast_event(
                        e,
                        context={"store": self},
                    )
                    for e in events
                ]

            return events

    async def load_application_events(
        self,
        application_id: str,
    ) -> list[StoredEvent]:
        """
        Load all events related to an application across loan, agent-session,
        and compliance streams in global_position order.

        This lets command-side state reconstruction enforce business rules that
        depend on multiple aggregates while still using the loan stream version
        for optimistic concurrency on loan commands.
        """
        stream_id = f"loan-{application_id}"

        async with self._pool.connection() as conn:
            result = await conn.execute(
                """
                SELECT event_id, stream_id, stream_position, global_position,
                       event_type, event_version, payload, metadata, recorded_at
                FROM events
                WHERE stream_id = %s
                   OR payload->>'application_id' = %s
                ORDER BY global_position ASC
                """,
                (stream_id, application_id),
            )

            rows = await result.fetchall()
            events = [self._row_to_stored_event(row) for row in rows]

            if self._upcasters is not None:
                events = [
                    await self._upcasters.upcast_event(
                        e,
                        context={"store": self},
                    )
                    for e in events
                ]

            return events

    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
    ) -> AsyncIterator[StoredEvent]:
        """
        Async generator yielding all events from from_global_position onwards.
        Yields in global_position order. Fetches in batches of batch_size.

        Implements Manual Query 3 basis (p.26):
            SELECT ... WHERE global_position > $1
            ORDER BY global_position ASC
            LIMIT $2

        Used by projection daemon for full replay.
        """
        current_position = from_global_position

        while True:
            async with self._pool.connection() as conn:
                if event_types:
                    result = await conn.execute(
                        """
                        SELECT event_id, stream_id, stream_position, global_position,
                               event_type, event_version, payload, metadata, recorded_at
                        FROM events
                        WHERE global_position > %s
                          AND event_type = ANY(%s)
                        ORDER BY global_position ASC
                        LIMIT %s
                        """,
                        (current_position, event_types, batch_size),
                    )
                else:
                    result = await conn.execute(
                        """
                        SELECT event_id, stream_id, stream_position, global_position,
                               event_type, event_version, payload, metadata, recorded_at
                        FROM events
                        WHERE global_position > %s
                        ORDER BY global_position ASC
                        LIMIT %s
                        """,
                        (current_position, batch_size),
                    )

                rows = await result.fetchall()
                if not rows:
                    break

                for row in rows:
                    event = self._row_to_stored_event(row)
                    if self._upcasters is not None:
                        event = await self._upcasters.upcast_event(
                            event,
                            context={"store": self},
                        )
                    current_position = event.global_position
                    yield event

    # =========================================================================
    # STREAM MANAGEMENT
    # =========================================================================

    async def stream_version(self, stream_id: str) -> int:
        """
        Returns current_version from event_streams for stream_id.
        Returns 0 if stream does not exist.

        O(1) lookup — primary key on event_streams. (Manual p.21)
        """
        async with self._pool.connection() as conn:
            result = await conn.execute(
                """
                SELECT current_version
                FROM event_streams
                WHERE stream_id = %s
                """,
                (stream_id,),
            )
            row = await result.fetchone()
            return row[0] if row else 0

    async def archive_stream(self, stream_id: str) -> None:
        """
        Sets archived_at = NOW() on event_streams.
        Archived streams reject all future appends.
        """
        async with self._pool.connection() as conn:
            result = await conn.execute(
                """
                UPDATE event_streams
                SET archived_at = NOW()
                WHERE stream_id = %s
                RETURNING stream_id
                """,
                (stream_id,),
            )
            row = await result.fetchone()
            if row is None:
                raise KeyError(f"Stream '{stream_id}' does not exist")
            logger.info("stream_archived", stream_id=stream_id)

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata:
        """
        Returns StreamMetadata for stream_id.
        Raises KeyError if stream does not exist.
        """
        async with self._pool.connection() as conn:
            result = await conn.execute(
                """
                SELECT stream_id, aggregate_type, current_version,
                       created_at, archived_at, metadata
                FROM event_streams
                WHERE stream_id = %s
                """,
                (stream_id,),
            )
            row = await result.fetchone()
            if row is None:
                raise KeyError(f"Stream '{stream_id}' does not exist")

            return StreamMetadata(
                stream_id=row[0],
                aggregate_type=row[1],
                current_version=row[2],
                created_at=row[3],
                archived_at=row[4],
                metadata=row[5],
            )

    # =========================================================================
    # INTERNAL HELPERS
    # =========================================================================

    @staticmethod
    def _row_to_stored_event(row: tuple) -> StoredEvent:
        """Convert a database row tuple to a StoredEvent instance."""
        return StoredEvent(
            event_id=row[0],
            stream_id=row[1],
            stream_position=row[2],
            global_position=row[3],
            event_type=row[4],
            event_version=row[5],
            payload=row[6],
            metadata=row[7],
            recorded_at=row[8],
        )
