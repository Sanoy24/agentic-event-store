# src/integrity/audit_chain.py
# =============================================================================
# TRP1 LEDGER — Cryptographic Audit Chain
# =============================================================================
# Source: Challenge Doc Phase 4B p.14 + Manual Part II Cluster F
#
# Regulatory-grade tamper evidence via SHA-256 hash chain over the event log.
# Each AuditIntegrityCheckRun event records a hash of all preceding events
# plus the previous integrity hash, forming a blockchain-style chain.
# Any post-hoc modification of events breaks the chain.
# =============================================================================
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone

import structlog

from src.event_store import EventStore
from src.models.events import (
    AuditIntegrityCheckRun,
    AuditTamperDetected,
    StoredEvent,
)

logger = structlog.get_logger()


@dataclass(frozen=True)
class IntegrityCheckResult:
    """Result of a cryptographic integrity check."""

    entity_type: str
    entity_id: str
    events_verified_count: int
    chain_valid: bool
    tamper_detected: bool
    integrity_hash: str
    previous_hash: str
    checked_at: datetime


def _hash_event(event: StoredEvent) -> str:
    """
    Compute SHA-256 hash of an event's payload + metadata.

    Uses canonical JSON serialization (sorted keys) to ensure
    deterministic hashing regardless of dict insertion order.
    """
    content = json.dumps(
        {
            "event_id": str(event.event_id),
            "stream_id": event.stream_id,
            "stream_position": event.stream_position,
            "event_type": event.event_type,
            "event_version": event.event_version,
            "payload": event.payload,
            "metadata": event.metadata,
        },
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(content.encode()).hexdigest()


def _compute_chain_hash(previous_hash: str, event_hashes: list[str]) -> str:
    """
    Compute the chain hash: sha256(previous_hash + concatenated event_hashes).

    This creates a blockchain-style chain where each integrity check
    links to the previous one. Breaking any event in the chain
    invalidates all subsequent integrity hashes.
    """
    combined = previous_hash + "".join(event_hashes)
    return hashlib.sha256(combined.encode()).hexdigest()


async def run_integrity_check(
    store: EventStore,
    entity_type: str,
    entity_id: str,
) -> IntegrityCheckResult:
    """
    Run a cryptographic integrity check on the audit chain.

    Steps:
    1. Load all events for the entity's primary stream
    2. Load the last AuditIntegrityCheckRun event (if any)
    3. Hash the payloads of all events since the last check
    4. Verify hash chain: new_hash = sha256(previous_hash + event_hashes)
    5. Append new AuditIntegrityCheckRun event to audit-{entity_type}-{entity_id}
    6. Return result with: events_verified, chain_valid, tamper_detected
    """
    primary_stream = f"{entity_type}-{entity_id}"
    audit_stream = f"audit-{entity_type}-{entity_id}"
    primary_events = await store.load_stream(primary_stream)
    audit_events = await store.load_stream(audit_stream)
    linked_events = [event for event in audit_events if event.event_type == "AuditEventLinked"]

    # Once the AuditLedger stream exists, the integrity check should verify the
    # cross-stream audit facts rather than only the primary aggregate stream.
    events_for_chain = linked_events or primary_events

    previous_hash = "genesis"  # Initial chain hash
    last_verified_position = 0

    # Find the last AuditIntegrityCheckRun event
    for audit_event in reversed(audit_events):
        if audit_event.event_type == "AuditIntegrityCheckRun":
            previous_hash = audit_event.payload.get("integrity_hash", "genesis")
            last_verified_position = audit_event.payload.get("events_verified_count", 0)
            break

    # Step 3: Hash events since last check
    events_to_verify = events_for_chain[last_verified_position:]
    event_hashes = [_hash_event(e) for e in events_to_verify]

    # Step 4: Compute chain hash
    new_hash = _compute_chain_hash(previous_hash, event_hashes)

    # Verify chain integrity — re-hash all events from scratch to detect tampering
    chain_valid = True
    tamper_detected = False

    if last_verified_position > 0:
        # Re-verify previously verified events haven't been modified
        # Check against expected hash from the full chain
        expected_full = _compute_chain_hash(
            "genesis",
            [_hash_event(e) for e in events_for_chain[:last_verified_position]],
        )
        if expected_full != previous_hash:
            chain_valid = False
            tamper_detected = True

    checked_at = datetime.now(timezone.utc)

    # Step 5: Append integrity check event
    audit_version = await store.stream_version(audit_stream)
    expected_version = audit_version if audit_version > 0 else -1

    if tamper_detected:
        # Append tamper detection event
        await store.append(
            stream_id=audit_stream,
            events=[
                AuditTamperDetected(
                    entity_id=entity_id,
                    detection_timestamp=checked_at,
                    expected_hash=previous_hash,
                    actual_hash=new_hash,
                    affected_event_range=f"positions 0-{len(primary_events)}",
                    severity="CRITICAL",
                    alert_sent=False,
                ),
            ],
            expected_version=expected_version,
            correlation_id=f"integrity-check-{entity_id}",
        )
        # Update version for the next append
        expected_version = expected_version + 1 if expected_version >= 0 else 1

    # Always append the integrity check result
    await store.append(
        stream_id=audit_stream,
        events=[
            AuditIntegrityCheckRun(
                entity_id=entity_id,
                check_timestamp=checked_at,
                events_verified_count=len(events_for_chain),
                integrity_hash=new_hash,
                previous_hash=previous_hash,
            ),
        ],
        expected_version=expected_version,
        correlation_id=f"integrity-check-{entity_id}",
    )

    result = IntegrityCheckResult(
        entity_type=entity_type,
        entity_id=entity_id,
        events_verified_count=len(events_to_verify),
        chain_valid=chain_valid,
        tamper_detected=tamper_detected,
        integrity_hash=new_hash,
        previous_hash=previous_hash,
        checked_at=checked_at,
    )

    logger.info(
        "integrity_check_completed",
        entity_type=entity_type,
        entity_id=entity_id,
        events_verified=result.events_verified_count,
        chain_valid=result.chain_valid,
        tamper_detected=result.tamper_detected,
    )

    return result


async def inspect_integrity_chain(
    store: EventStore,
    entity_type: str,
    entity_id: str,
) -> IntegrityCheckResult:
    """
    Verify the current audit chain state without appending new audit events.

    This is used by read-only workflows such as regulatory-package generation,
    where we need an independently verifiable integrity result without mutating
    the ledger during the read.
    """
    primary_stream = f"{entity_type}-{entity_id}"
    audit_stream = f"audit-{entity_type}-{entity_id}"

    primary_events = await store.load_stream(primary_stream)
    audit_events = await store.load_stream(audit_stream)
    linked_events = [event for event in audit_events if event.event_type == "AuditEventLinked"]
    events_for_chain = linked_events or primary_events

    previous_hash = "genesis"
    verified_count = 0
    for audit_event in reversed(audit_events):
        if audit_event.event_type != "AuditIntegrityCheckRun":
            continue
        previous_hash = audit_event.payload.get("integrity_hash", "genesis")
        verified_count = audit_event.payload.get("events_verified_count", 0)
        break

    current_hash = _compute_chain_hash(
        "genesis",
        [_hash_event(event) for event in events_for_chain[:verified_count]],
    )
    checked_at = datetime.now(timezone.utc)
    tamper_detected = verified_count > 0 and current_hash != previous_hash
    chain_valid = not tamper_detected

    if verified_count < len(events_for_chain):
        incremental_hash = _compute_chain_hash(
            previous_hash,
            [_hash_event(event) for event in events_for_chain[verified_count:]],
        )
    else:
        incremental_hash = previous_hash

    return IntegrityCheckResult(
        entity_type=entity_type,
        entity_id=entity_id,
        events_verified_count=len(events_for_chain),
        chain_valid=chain_valid,
        tamper_detected=tamper_detected,
        integrity_hash=incremental_hash,
        previous_hash=previous_hash,
        checked_at=checked_at,
    )
