# src/aggregates/audit_ledger.py
# =============================================================================
# TRP1 LEDGER — AuditLedger Aggregate
# =============================================================================
# Source: Challenge Doc Phase 2 (AuditLedger aggregate)
#        + Manual Part II Cluster A (Aggregate boundaries)
#
# The AuditLedger aggregate maintains a cryptographic hash chain over events
# for a given entity. Each integrity check records a SHA-256 hash linking to
# the previous check, forming a blockchain-style tamper-evident audit trail.
#
# Stream format: audit-{entity_type}-{entity_id}
# =============================================================================
from __future__ import annotations

from src.event_store import EventStore
from src.models.events import DomainError, StoredEvent


class AuditLedgerAggregate:
    """
    Consistency boundary for the cryptographic audit chain.

    Maintains an append-only hash chain over events for a given entity.
    Each AuditIntegrityCheckRun records a hash that links to the previous
    check's hash, enabling tamper detection at any point in the chain.

    If tampering is detected, an AuditTamperDetected event is appended
    and the aggregate enters a tampered state, blocking further integrity
    checks until the issue is investigated.
    """

    def __init__(self, entity_type: str, entity_id: str):
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.version: int = 0
        self.last_integrity_hash: str | None = None
        self.previous_hash: str | None = None
        self.tamper_detected: bool = False
        self.events_verified_count: int = 0
        self.check_count: int = 0

    @property
    def stream_id(self) -> str:
        return f"audit-{self.entity_type}-{self.entity_id}"

    @classmethod
    async def load(
        cls,
        store: EventStore,
        entity_type: str,
        entity_id: str,
    ) -> AuditLedgerAggregate:
        """
        Replay audit stream to reconstruct state.

        Stream ID format: "audit-{entity_type}-{entity_id}"
        """
        agg = cls(entity_type=entity_type, entity_id=entity_id)
        events = await store.load_stream(agg.stream_id)
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

    def _on_AuditIntegrityCheckRun(self, event: StoredEvent) -> None:
        """
        Record a successful integrity check. Updates the hash chain.
        """
        self.previous_hash = self.last_integrity_hash
        self.last_integrity_hash = event.payload.get("integrity_hash")
        self.events_verified_count = event.payload.get("events_verified_count", 0)
        self.check_count += 1

    def _on_AuditEventLinked(self, event: StoredEvent) -> None:
        """
        Business events mirrored into the audit stream do not change integrity
        state directly; they exist so the audit trail can be queried from the
        dedicated AuditLedger stream.
        """
        return None

    def _on_AuditTamperDetected(self, event: StoredEvent) -> None:
        """
        Record that tampering was detected. The aggregate enters a
        tampered state — further integrity checks are blocked until
        investigation is complete.
        """
        self.tamper_detected = True

    # =========================================================================
    # Business Rule Assertions
    # =========================================================================

    def assert_chain_intact(self) -> None:
        """
        Verify that no tampering has been detected in the audit chain.

        Raises DomainError if tamper_detected is True. This prevents
        further integrity checks from running on a compromised chain
        until the issue is investigated and resolved.
        """
        if self.tamper_detected:
            raise DomainError(
                f"AuditLedger {self.entity_type}/{self.entity_id}: "
                "Tampering previously detected. Chain integrity compromised. "
                "Investigation required before further checks."
            )
