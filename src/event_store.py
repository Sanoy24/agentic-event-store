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

                    # Step 5: Insert events
                    # If stream was new (actual_version = -1), start at 0
                    new_version = max(0, actual_version) 
                    event_ids: list[UUID] = []

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
                            RETURNING event_id
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
                            event_ids.append(result[0])

                    # Step 6: Update stream version
                    await conn.execute(
                        """
                        UPDATE event_streams
                        SET current_version = %s
                        WHERE stream_id = %s
                        """,
                        (new_version, stream_id),
                    )

                    # Step 7: Insert outbox entries
                    for i, event in enumerate(events):
                        event_payload = event.model_dump(mode="json")
                        await conn.execute(
                            """
                            INSERT INTO outbox (event_id, destination, payload)
                            VALUES (%s, %s, %s)
                            """,
                            (
                                event_ids[i],
                                f"default:{event.event_type}",
                                psycopg.types.json.Jsonb(event_payload),
                            ),
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
                events = [self._upcasters.upcast(e) for e in events]

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
                        event = self._upcasters.upcast(event)
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
