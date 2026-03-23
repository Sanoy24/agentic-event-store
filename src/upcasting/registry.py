# src/upcasting/registry.py
# =============================================================================
# TRP1 LEDGER — Upcaster Registry
# =============================================================================
# Source: Challenge Doc Phase 4A p.13 + Manual Part II Cluster C
#
# Centralized registry that automatically applies version migrations
# whenever old events are loaded from the store. The event loading path
# calls the registry transparently — callers never manually invoke upcasters.
#
# Key guarantee: the raw stored payload in the database is NEVER modified.
# Upcasting happens at read time only, producing new StoredEvent instances
# via StoredEvent.with_payload() (immutable copy).
# =============================================================================
from __future__ import annotations

from collections.abc import Callable
from inspect import isawaitable
from typing import Any

from src.models.events import StoredEvent


class UpcasterRegistry:
    """
    Registry for event version migration functions (upcasters).

    Upcasters transform old event payloads into new ones at read time,
    without touching stored events. They are registered as a chain:
    v1→v2→v3, applied automatically when old events are loaded.

    Usage:
        registry = UpcasterRegistry()

        @registry.register("CreditAnalysisCompleted", from_version=1)
        def upcast_credit_v1_to_v2(payload: dict) -> dict:
            return {**payload, "model_version": "legacy-pre-2026"}
    """

    def __init__(self) -> None:
        self._upcasters: dict[tuple[str, int], Callable[..., dict[Any, Any]]] = {}

    def register(
        self, event_type: str, from_version: int
    ) -> Callable[[Callable[..., dict[Any, Any]]], Callable[..., dict[Any, Any]]]:
        """
        Decorator. Registers fn as upcaster from event_type@from_version.

        The registered function receives the old payload dict and must
        return a new payload dict with the additional/modified fields.
        """

        def decorator(
            fn: Callable[..., dict[Any, Any]]
        ) -> Callable[..., dict[Any, Any]]:
            self._upcasters[(event_type, from_version)] = fn
            return fn

        return decorator

    def upcast(self, event: StoredEvent) -> StoredEvent:
        """
        Apply all registered upcasters for this event type in version order.

        Walks the version chain: if event is v1 and upcasters exist for
        v1→v2 and v2→v3, the event is upcasted through both, returning
        a v3 StoredEvent with the fully migrated payload.

        The original StoredEvent is never mutated — each step produces
        a new instance via StoredEvent.with_payload().
        """
        current = event
        v = event.event_version
        while (event.event_type, v) in self._upcasters:
            upcaster_fn = self._upcasters[(event.event_type, v)]
            new_payload = self._invoke_upcaster(
                upcaster_fn,
                current,
                context=None,
            )
            if isawaitable(new_payload):
                raise RuntimeError(
                    "Async upcaster requires UpcasterRegistry.upcast_event()."
                )
            current = current.with_payload(new_payload, version=v + 1)
            v += 1
        return current

    async def upcast_event(
        self,
        event: StoredEvent,
        *,
        context: dict[str, Any] | None = None,
    ) -> StoredEvent:
        """
        Async upcasting entry point used by EventStore load paths.

        This supports both pure synchronous upcasters and async upcasters that
        need read-time context such as an EventStore for reconstruction.
        """
        current = event
        v = event.event_version
        while (event.event_type, v) in self._upcasters:
            upcaster_fn = self._upcasters[(event.event_type, v)]
            new_payload = self._invoke_upcaster(
                upcaster_fn,
                current,
                context=context or {},
            )
            if isawaitable(new_payload):
                new_payload = await new_payload
            current = current.with_payload(new_payload, version=v + 1)
            v += 1
        return current

    def has_upcaster(self, event_type: str, version: int) -> bool:
        """Check if an upcaster is registered for a specific type+version."""
        return (event_type, version) in self._upcasters

    @property
    def registered_count(self) -> int:
        """Number of registered upcasters."""
        return len(self._upcasters)

    @staticmethod
    def _invoke_upcaster(
        upcaster_fn: Callable[..., dict[Any, Any]],
        event: StoredEvent,
        *,
        context: dict[str, Any] | None,
    ) -> Any:
        try:
            return upcaster_fn(
                event.payload,
                current_event=event,
                context=context,
            )
        except TypeError as exc:
            if "unexpected keyword argument" not in str(exc):
                raise
            return upcaster_fn(event.payload)
