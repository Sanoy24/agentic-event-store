from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any

import structlog
from psycopg_pool import AsyncConnectionPool

from src.event_store import EventStore
from src.models.events import StoredEvent

logger = structlog.get_logger()


class Projection(ABC):
    """
    Base class for all projections.

    Subclasses must implement:
    - name: unique identifier used for checkpoint tracking
    - handle(event, conn): process a single event (must be idempotent!)
    - interested_in(event_type): whether this projection cares about the event
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique projection name, used as key in projection_checkpoints."""
        ...

    @abstractmethod
    async def handle(self, event: StoredEvent, conn: Any) -> None:
        """
        Process a single event. Must be idempotent.

        Uses INSERT ... ON CONFLICT DO UPDATE (upsert) or similar
        patterns to ensure replaying the same event produces the same state.
        """
        ...

    @abstractmethod
    def interested_in(self, event_type: str) -> bool:
        """Return True if this projection should process this event type."""
        ...

    def event_types(self) -> set[str] | None:
        """
        Optional explicit event-type declaration.

        Returning a concrete set lets the daemon fetch only relevant events
        from the store, which keeps lag accounting and replay throughput aligned
        with the projection's real workload. Returning None falls back to the
        predicate-only path.
        """
        return None

    async def rebuild_from_scratch(
        self,
        store: EventStore,
        pool: AsyncConnectionPool,
        batch_size: int = 100,
    ) -> int | None:
        """
        Optional projection-specific rebuild path.

        Return an integer to signal that the projection handled its own rebuild.
        Return None to let the daemon fall back to generic replay.
        """
        return None

    async def handle_batch(
        self,
        events: list[StoredEvent],
        conn: Any,
    ) -> None:
        """
        Optional batch-processing hook.

        Projections can override this to process a list of relevant events
        inside a single savepoint. The daemon falls back to event-by-event
        handling if the batch path raises.
        """
        raise NotImplementedError


class ProjectionDaemon:
    """
    Async daemon that continuously processes events and routes them
    to registered projections.

    Architecture:
      1. Find the lowest checkpoint across all projections
      2. Fetch a batch of events starting from that position
      3. Route each event to interested projections
      4. Update each projection's checkpoint
      5. Sleep and repeat
    """

    def __init__(
        self,
        store: EventStore,
        pool: AsyncConnectionPool,
        projections: list[Projection],
        batch_size: int = 100,
        poll_interval_ms: int = 100,
        max_retries_per_event: int = 3,
    ):
        self._store = store
        self._pool = pool
        self._projections = {projection.name: projection for projection in projections}
        self._batch_size = batch_size
        self._poll_interval = poll_interval_ms / 1000.0
        self._max_retries_per_event = max(1, max_retries_per_event)
        self._running = False
        self._checkpoints: dict[str, int] = {}

    async def start(self) -> None:
        """Start the projection daemon. Runs until stop() is called."""
        self._running = True
        await self._load_checkpoints()

        logger.info(
            "projection_daemon_started",
            projections=list(self._projections.keys()),
            checkpoints=self._checkpoints,
            max_retries_per_event=self._max_retries_per_event,
        )

        while self._running:
            processed = await self._process_batch()
            if processed == 0:
                await asyncio.sleep(self._poll_interval)

    async def stop(self) -> None:
        """Signal the daemon to stop after the current batch."""
        self._running = False
        logger.info("projection_daemon_stopping")

    async def run_once(self) -> int:
        """
        Process a single batch of events. Useful for testing.
        Returns number of events processed.
        """
        await self._load_checkpoints()
        return await self._process_batch()

    async def get_lag(self, projection_name: str) -> int:
        """
        Get the lag for a specific projection in event count.

        Returns: global_position_head - last_processed_position
        """
        projection = self._projections[projection_name]
        event_types = await self._resolve_projection_event_types(projection)
        async with self._pool.connection() as conn:
            if event_types:
                result = await conn.execute(
                    """
                    SELECT COALESCE(MAX(global_position), 0)
                    FROM events
                    WHERE event_type = ANY(%s)
                    """,
                    (list(event_types),),
                )
            else:
                result = await conn.execute(
                    "SELECT COALESCE(MAX(global_position), 0) FROM events"
                )
            row = await result.fetchone()
            head = row[0] if row else 0
            checkpoint = self._checkpoints.get(projection_name, 0)
            return head - checkpoint

    async def get_all_lags(self) -> dict[str, int]:
        """Get lag for all registered projections."""
        return {
            projection_name: await self.get_lag(projection_name)
            for projection_name in self._projections
        }

    async def rebuild_projection(self, projection_name: str) -> int:
        """
        Rebuild a projection from scratch.

        Uses a custom projection rebuild path when one exists, otherwise falls
        back to resetting the checkpoint and replaying from position 0.
        """
        if projection_name not in self._projections:
            raise ValueError(f"Unknown projection: {projection_name}")

        projection = self._projections[projection_name]
        custom_rebuild = await projection.rebuild_from_scratch(
            self._store,
            self._pool,
            self._batch_size,
        )
        if custom_rebuild is not None:
            await self._load_checkpoints()
            logger.info(
                "projection_rebuild_completed",
                projection=projection_name,
                total_events=custom_rebuild,
                mode="custom",
            )
            return custom_rebuild

        async with self._pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO projection_checkpoints (projection_name, last_position, updated_at)
                VALUES (%s, 0, NOW())
                ON CONFLICT (projection_name) DO UPDATE
                SET last_position = 0, updated_at = NOW()
                """,
                (projection_name,),
            )
            await conn.commit()

        self._checkpoints[projection_name] = 0
        logger.info("projection_rebuild_started", projection=projection_name)

        total = 0
        while True:
            processed = await self._process_batch_for(projection_name)
            total += processed
            if processed == 0:
                break

        logger.info(
            "projection_rebuild_completed",
            projection=projection_name,
            total_events=total,
            mode="generic",
        )
        return total

    async def _load_checkpoints(self) -> None:
        """Load all projection checkpoints from the database."""
        async with self._pool.connection() as conn:
            for name in self._projections:
                result = await conn.execute(
                    "SELECT last_position FROM projection_checkpoints WHERE projection_name = %s",
                    (name,),
                )
                row = await result.fetchone()
                self._checkpoints[name] = row[0] if row else 0

    async def _process_batch(self) -> int:
        """
        Process a batch of events for all projections.
        Returns total events fetched.
        """
        if not self._checkpoints:
            return 0

        min_position = min(self._checkpoints.values())
        event_types_filter = await self._union_event_types()
        events: list[StoredEvent] = []
        async for event in self._store.load_all(
            from_global_position=min_position,
            event_types=event_types_filter,
            batch_size=self._batch_size,
        ):
            events.append(event)
            if len(events) >= self._batch_size:
                break

        if not events:
            return 0

        async with self._pool.connection() as conn:
            async with conn.transaction():
                for name, projection in self._projections.items():
                    pending_events = [
                        event
                        for event in events
                        if event.global_position > self._checkpoints.get(name, 0)
                    ]
                    if not pending_events:
                        continue

                    relevant_events = [
                        event
                        for event in pending_events
                        if projection.interested_in(event.event_type)
                    ]

                    batch_processed = False
                    if relevant_events and self._has_custom_batch_handler(projection):
                        try:
                            async with conn.transaction():
                                await projection.handle_batch(relevant_events, conn)
                        except Exception:
                            logger.exception(
                                "projection_batch_handler_error",
                                projection=name,
                                event_count=len(relevant_events),
                            )
                        else:
                            await self._clear_projection_failures(
                                conn,
                                projection_name=name,
                                global_positions=[
                                    event.global_position for event in relevant_events
                                ],
                            )
                            self._checkpoints[name] = pending_events[-1].global_position
                            batch_processed = True

                    if batch_processed:
                        continue

                    await self._process_projection_events(
                        conn,
                        name,
                        projection,
                        pending_events,
                    )

                for name in self._projections:
                    checkpoint = self._checkpoints.get(name, 0)
                    await conn.execute(
                        """
                        INSERT INTO projection_checkpoints (projection_name, last_position, updated_at)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT (projection_name) DO UPDATE
                        SET last_position = %s, updated_at = NOW()
                        """,
                        (name, checkpoint, checkpoint),
                    )

        return len(events)

    async def _process_projection_events(
        self,
        conn: Any,
        projection_name: str,
        projection: Projection,
        pending_events: list[StoredEvent],
    ) -> None:
        for event in pending_events:
            processed_successfully = True
            failure_skipped = False

            if projection.interested_in(event.event_type):
                try:
                    async with conn.transaction():
                        await projection.handle(event, conn)
                except Exception:
                    retry_count = await self._record_projection_failure(
                        conn,
                        projection_name=projection_name,
                        event=event,
                    )
                    if retry_count >= self._max_retries_per_event:
                        processed_successfully = True
                        failure_skipped = True
                        await self._mark_projection_failure_skipped(
                            conn,
                            projection_name=projection_name,
                            event=event,
                        )
                        logger.exception(
                            "projection_event_skipped",
                            projection=projection_name,
                            event_type=event.event_type,
                            global_position=event.global_position,
                            retry_count=retry_count,
                            max_retries=self._max_retries_per_event,
                        )
                    else:
                        processed_successfully = False
                        logger.exception(
                            "projection_handler_error",
                            projection=projection_name,
                            event_type=event.event_type,
                            global_position=event.global_position,
                            retry_count=retry_count,
                            max_retries=self._max_retries_per_event,
                        )

            if processed_successfully:
                if projection.interested_in(event.event_type) and not failure_skipped:
                    await self._clear_projection_failure(
                        conn,
                        projection_name=projection_name,
                        global_position=event.global_position,
                    )
                self._checkpoints[projection_name] = max(
                    self._checkpoints.get(projection_name, 0),
                    event.global_position,
                )

    async def _process_batch_for(self, projection_name: str) -> int:
        """Process a batch of events for a single projection (used by rebuild)."""
        projection = self._projections[projection_name]
        checkpoint = self._checkpoints.get(projection_name, 0)
        event_types_filter = await self._resolve_projection_event_types(projection)

        events: list[StoredEvent] = []
        async for event in self._store.load_all(
            from_global_position=checkpoint + 1,
            event_types=list(event_types_filter) if event_types_filter else None,
            batch_size=self._batch_size,
        ):
            events.append(event)
            if len(events) >= self._batch_size:
                break

        if not events:
            return 0

        processed_events = 0
        async with self._pool.connection() as conn:
            for event in events:
                if not projection.interested_in(event.event_type):
                    continue
                try:
                    await projection.handle(event, conn)
                    processed_events += 1
                except Exception:
                    logger.exception(
                        "projection_rebuild_error",
                        projection=projection_name,
                        event_type=event.event_type,
                        global_position=event.global_position,
                    )
                    await conn.rollback()
                    raise

            new_checkpoint = events[-1].global_position
            self._checkpoints[projection_name] = new_checkpoint
            await conn.execute(
                """
                INSERT INTO projection_checkpoints (projection_name, last_position, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (projection_name) DO UPDATE
                SET last_position = %s, updated_at = NOW()
                """,
                (projection_name, new_checkpoint, new_checkpoint),
            )
            await conn.commit()

        return processed_events

    async def _record_projection_failure(
        self,
        conn: Any,
        *,
        projection_name: str,
        event: StoredEvent,
    ) -> int:
        result = await conn.execute(
            """
            INSERT INTO projection_failures (
                projection_name,
                global_position,
                stream_id,
                event_type,
                retry_count,
                last_failed_at
            ) VALUES (%s, %s, %s, %s, 1, NOW())
            ON CONFLICT (projection_name, global_position) DO UPDATE
            SET retry_count = projection_failures.retry_count + 1,
                stream_id = EXCLUDED.stream_id,
                event_type = EXCLUDED.event_type,
                last_failed_at = NOW()
            RETURNING retry_count
            """,
            (
                projection_name,
                event.global_position,
                event.stream_id,
                event.event_type,
            ),
        )
        row = await result.fetchone()
        return row[0] if row else 1

    async def _mark_projection_failure_skipped(
        self,
        conn: Any,
        *,
        projection_name: str,
        event: StoredEvent,
    ) -> None:
        await conn.execute(
            """
            UPDATE projection_failures
            SET skipped_at = NOW()
            WHERE projection_name = %s
              AND global_position = %s
            """,
            (projection_name, event.global_position),
        )

    async def _clear_projection_failure(
        self,
        conn: Any,
        *,
        projection_name: str,
        global_position: int,
    ) -> None:
        await conn.execute(
            """
            DELETE FROM projection_failures
            WHERE projection_name = %s
              AND global_position = %s
            """,
            (projection_name, global_position),
        )

    async def _clear_projection_failures(
        self,
        conn: Any,
        *,
        projection_name: str,
        global_positions: list[int],
    ) -> None:
        if not global_positions:
            return
        await conn.execute(
            """
            DELETE FROM projection_failures
            WHERE projection_name = %s
              AND global_position = ANY(%s)
            """,
            (projection_name, global_positions),
        )

    async def _union_event_types(self) -> list[str] | None:
        declared_event_types: set[str] = set()
        for projection in self._projections.values():
            event_types = await self._resolve_projection_event_types(projection)
            if not event_types:
                continue
            declared_event_types.update(event_types)
        return sorted(declared_event_types) if declared_event_types else None

    async def _resolve_projection_event_types(
        self,
        projection: Projection,
    ) -> set[str] | None:
        explicit = projection.event_types()
        if explicit is not None:
            return set(explicit)

        async with self._pool.connection() as conn:
            result = await conn.execute("SELECT DISTINCT event_type FROM events")
            rows = await result.fetchall()

        inferred = {
            row[0]
            for row in rows
            if row and row[0] and projection.interested_in(row[0])
        }
        return inferred or None

    @staticmethod
    def _has_custom_batch_handler(projection: Projection) -> bool:
        return type(projection).handle_batch is not Projection.handle_batch
