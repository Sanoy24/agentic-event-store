# src/mcp/server.py
# =============================================================================
# TRP1 LEDGER — MCP Server Entry Point
# =============================================================================
# Source: Challenge Doc Phase 5 p.15 (MCP Server)
#
# Model Context Protocol server providing structured access to the event
# store for AI agents. Commands are exposed as tools, queries as resources.
#
# The MCP server is the interface layer — all business logic flows through
# the existing command handlers and projections.
# =============================================================================
from __future__ import annotations

import asyncio
import contextlib
import json
import sys
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator
from urllib.parse import parse_qs, urlsplit, urlunsplit

import structlog
from psycopg_pool import AsyncConnectionPool

from src.event_store import EventStore
from src.mcp.resources import register_resources
from src.mcp.tools import register_tools
from src.projections.agent_performance import AgentPerformanceProjection
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.compliance_audit import ComplianceAuditProjection
from src.projections.daemon import ProjectionDaemon
from src.upcasting.upcasters import registry as upcaster_registry

logger = structlog.get_logger()

try:
    from mcp.server.fastmcp import FastMCP
    from mcp.server.lowlevel.helper_types import ReadResourceContents
except ImportError:  # pragma: no cover - fallback exercised when MCP isn't installed
    FastMCP = None  # type: ignore[assignment]
    ReadResourceContents = None  # type: ignore[assignment]

# Default database URL — override via MCP_DATABASE_URL env variable
DEFAULT_DB_URL = "postgresql://postgres:postgres@localhost:5432/trp1_ledger"


if FastMCP is not None:

    class LedgerFastMCP(FastMCP):
        """
        FastMCP wrapper that understands URI query strings for resources.

        FastMCP's stock resource matcher only considers the template path.
        The challenge contract uses URIs like:
        `ledger://applications/{id}/compliance?as_of=...`
        so we normalize the path, merge in parsed query parameters, and then
        invoke the matched template function with both sets of arguments.
        """

        async def read_resource(self, uri: Any) -> list[ReadResourceContents]:
            uri_str = str(uri)
            if "?" not in uri_str:
                return list(await super().read_resource(uri_str))

            parsed = urlsplit(uri_str)
            normalized_uri = urlunsplit(
                (parsed.scheme, parsed.netloc, parsed.path, "", "")
            )
            query_params = {
                key: values[-1]
                for key, values in parse_qs(parsed.query, keep_blank_values=True).items()
            }
            if "from" in query_params:
                query_params["from_"] = query_params.pop("from")
            context = self.get_context()
            query_handlers = getattr(self, "_ledger_query_handlers", {})

            resource = self._resource_manager._resources.get(normalized_uri)
            if resource is not None:
                content = await resource.read()
                return [
                    ReadResourceContents(
                        content=content,
                        mime_type=resource.mime_type,
                        meta=resource.meta,
                    )
                ]

            for template in self._resource_manager._templates.values():
                if params := template.matches(normalized_uri):
                    template_key = (
                        getattr(template, "uri_template", None)
                        or getattr(template, "uriTemplate", None)
                        or str(template)
                    )
                    handler = query_handlers.get(template_key)
                    if handler is not None:
                        content = await handler(**{**params, **query_params})
                        return [
                            ReadResourceContents(
                                content=content,
                                mime_type="text/plain",
                                meta=None,
                            )
                        ]

                    merged_params = {**params, **query_params}
                    resource = await template.create_resource(
                        uri_str,
                        merged_params,
                        context=context,
                    )
                    content = await resource.read()
                    return [
                        ReadResourceContents(
                            content=content,
                            mime_type=resource.mime_type,
                            meta=resource.meta,
                        )
                    ]

            return list(await super().read_resource(uri_str))

        async def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> Any:
            """
            Normalize FastMCP tool responses to plain Python data.

            The test suite drives the server the way an agent would and expects
            `call_tool()` to yield the underlying JSON payload rather than the
            SDK's content-wrapper objects.
            """
            raw_result = await super().call_tool(name, arguments or {})
            return self._normalise_tool_result(raw_result)

        def _normalise_tool_result(self, result: Any) -> Any:
            if isinstance(result, dict):
                return result
            if isinstance(result, str):
                try:
                    return json.loads(result)
                except json.JSONDecodeError:
                    return result
            if isinstance(result, list):
                if len(result) == 1:
                    return self._normalise_tool_result(result[0])
                return [self._normalise_tool_result(item) for item in result]
            if hasattr(result, "content"):
                return self._normalise_tool_result(getattr(result, "content"))
            if hasattr(result, "text"):
                return self._normalise_tool_result(getattr(result, "text"))
            if hasattr(result, "data"):
                return self._normalise_tool_result(getattr(result, "data"))
            return result

else:  # pragma: no cover - only used when MCP isn't installed
    LedgerFastMCP = None


class LedgerServer:
    """
    MCP Server for the TRP1 Ledger event store.

    Initialises the event store, projection daemon, and registers
    all MCP tools and resources.
    """

    def __init__(self, db_url: str | None = None):
        import os

        self._db_url = db_url or os.environ.get("MCP_DATABASE_URL", DEFAULT_DB_URL)
        self._pool: AsyncConnectionPool | None = None
        self._store: EventStore | None = None
        self._daemon: ProjectionDaemon | None = None

    @asynccontextmanager
    async def lifespan(self) -> AsyncIterator[dict[str, Any]]:
        """
        Async context manager for server lifecycle.

        Sets up the connection pool, event store, and projection daemon.
        Yields a context dict with the store and pool for use by tools/resources.
        """
        # Windows event loop fix
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        async with AsyncConnectionPool(self._db_url) as pool:
            self._pool = pool
            self._store = EventStore(pool, upcaster_registry=upcaster_registry)

            # Set up projections
            projections = [
                ApplicationSummaryProjection(),
                AgentPerformanceProjection(),
                ComplianceAuditProjection(),
            ]
            self._daemon = ProjectionDaemon(
                store=self._store,
                pool=pool,
                projections=projections,
            )
            daemon_task = asyncio.create_task(self._daemon.start())

            logger.info("ledger_server_started", db_url=self._db_url)

            try:
                yield {
                    "store": self._store,
                    "pool": pool,
                    "daemon": self._daemon,
                }
            finally:
                logger.info("ledger_server_stopping")
                await self._daemon.stop()
                try:
                    await asyncio.wait_for(daemon_task, timeout=2)
                except TimeoutError:
                    daemon_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await daemon_task

    async def get_store(self) -> EventStore:
        if self._store is None:
            await self._initialize()
        return self._store

    async def get_pool(self) -> AsyncConnectionPool:
        if self._pool is None:
            await self._initialize()
        return self._pool

    async def _initialize(self) -> None:
        if self._pool is not None:
            return

        self._pool = AsyncConnectionPool(self._db_url, open=False)
        await self._pool.open()
        self._store = EventStore(self._pool, upcaster_registry=upcaster_registry)

        # Set up projections
        projections = [
            ApplicationSummaryProjection(),
            AgentPerformanceProjection(),
            ComplianceAuditProjection(),
        ]
        self._daemon = ProjectionDaemon(
            store=self._store,
            pool=self._pool,
            projections=projections,
        )

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._daemon.start())
        except RuntimeError:
            pass

        logger.info("ledger_server_started_lazily", db_url=self._db_url)

    async def get_daemon(self) -> ProjectionDaemon:
        if self._daemon is None:
            await self._initialize()
        return self._daemon


def create_mcp_app(db_url: str | None = None) -> Any:
    """
    Create and configure the MCP application.

    This function creates a FastMCP server with all tools and resources
    registered. It can be used directly by the MCP runtime.

    Note: This requires the 'mcp' package to be installed.
    If not available, returns a simple dict describing the server.
    """
    if LedgerFastMCP is None:
        logger.warning(
            "mcp_package_not_available",
            message="The 'mcp' package is not installed. Install with: pip install mcp",
        )
        return _create_standalone_server(db_url)

    server = LedgerServer(db_url)

    mcp = LedgerFastMCP(
        "TRP1 Ledger",
        instructions=(
            "Agentic Event Store — immutable memory and governance "
            "backbone for multi-agent AI systems."
        ),
    )

    register_tools(mcp, server)
    register_resources(mcp, server)

    return mcp


def _create_standalone_server(db_url: str | None = None) -> dict:
    """
    Fallback when mcp package is not installed.
    Returns a descriptor of available tools and resources.
    """
    return {
        "name": "TRP1 Ledger",
        "description": "Agentic Event Store MCP Server",
        "tools": [
            "submit_application",
            "start_agent_session",
            "record_credit_analysis",
            "record_fraud_screening",
            "record_compliance_check",
            "generate_decision",
            "record_human_review",
            "run_integrity_check",
        ],
        "resources": [
            "ledger://applications/{id}",
            "ledger://applications/{id}/compliance",
            "ledger://applications/{id}/audit-trail",
            "ledger://agents/{id}/performance",
            "ledger://agents/{id}/sessions/{session_id}",
            "ledger://ledger/health",
        ],
        "requires": ["mcp>=1.0"],
    }


if __name__ == "__main__":
    # psycopg3 requires SelectorEventLoop on Windows — ProactorEventLoop
    # (the default) does not support the socket operations psycopg needs.
    # This MUST be set before FastMCP.run() creates its own event loop.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    app = create_mcp_app()
    if hasattr(app, "run"):
        app.run()
    else:
        import json

        print(json.dumps(app, indent=2))
