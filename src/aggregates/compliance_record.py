from __future__ import annotations

from src.event_store import EventStore
from src.models.events import StoredEvent


class ComplianceRecordAggregate:
    """
    Consistency boundary for application compliance checks.

    This aggregate owns the authoritative view of which checks were required,
    which passed, and whether clearance was issued for the application.
    """

    def __init__(self, application_id: str):
        self.application_id = application_id
        self.version: int = 0
        self.regulation_set_version: str | None = None
        self.required_checks: list[str] = []
        self.passed_checks: list[str] = []
        self.failed_checks: list[str] = []
        self.clearance_issued: bool = False

    @classmethod
    async def load(
        cls,
        store: EventStore,
        application_id: str,
    ) -> ComplianceRecordAggregate:
        events = await store.load_stream(f"compliance-{application_id}")
        agg = cls(application_id=application_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: StoredEvent) -> None:
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position

    def _on_ComplianceCheckRequested(self, event: StoredEvent) -> None:
        self.regulation_set_version = event.payload.get("regulation_set_version")
        self.required_checks = list(event.payload.get("checks_required", []))

    def _on_ComplianceRulePassed(self, event: StoredEvent) -> None:
        rule_id = event.payload.get("rule_id")
        if rule_id and rule_id not in self.passed_checks:
            self.passed_checks.append(rule_id)

    def _on_ComplianceRuleFailed(self, event: StoredEvent) -> None:
        rule_id = event.payload.get("rule_id")
        if rule_id and rule_id not in self.failed_checks:
            self.failed_checks.append(rule_id)

    def _on_ComplianceClearanceIssued(self, event: StoredEvent) -> None:
        self.clearance_issued = True

    def missing_required_checks(self) -> list[str]:
        return sorted(set(self.required_checks) - set(self.passed_checks))
