

**TRP1  ·  THE LEDGER  ·  ARC 5: INTEGRATION & PROTOCOL ARCHITECTURE**

**Event Sourcing**

**Practitioner Manual**

*Complete reference for knowledge, skills, behaviours, and competencies — covering conceptual foundations, enterprise tool stack, schema design, query patterns, solution architectures, and guidance for novice and experienced practitioners.*

| AUDIENCE Audience — onboarding and upskilling companion Enterprise candidates — pre-engagement reference | CONTENTS I  ·  Conceptual Landscape & Pattern Map II  ·  Deep Jargon Glossary (7 clusters, 18 terms) III  ·  KSBC Framework (Knowledge, Skills, Behaviours, Competencies) IV  ·  Event Schema Design Reference V  ·  Query Pattern Reference (8 canonical queries) VI  ·  Solution Architecture Reference (4 patterns) VII  ·  Guidance for Mastery: Self-Assessment and Advanced Concepts  |
| :---- | :---- |

TRP1 FDE Program  ·  March 2026  ·  Confidential Training Resource

  **HOW TO USE THIS MANUAL**  

*A guide to getting value from this document quickly*

| If you are preparing for Arc 5  | Start with Part I (Conceptual Map) to verify your mental model is precise. Then read Part II (Jargon Glossary) — specifically the Notes on each term. Read Part VII (Guidance for Mastery) before the week begins so the diagnostic moments are familiar when they appear.  |
| :---- | :---- |
| **If you are new to event sourcing**  | Read Part I completely before starting Phase 0 of the challenge. Use Part II as a reference during implementation — look up terms when you encounter them. Part III (KSBC) is your self-assessment framework: be honest about where you are.  |
| **If you are an experienced ES practitioner** | Skim Part I to verify alignment; focus on Part II Clusters D and E (enterprise and AI-era patterns). Part VI (Architectures) contains the AI multi-agent coordination pattern and the greenfield decision framework — these are the most relevant to your current work. Part VII Checkpoint 5-8 describes what to probe in your own design.  |
| **If you are preparing for an enterprise ES role** | Parts IV and V (Schema Reference and Query Patterns) are your technical reference. Part VI (Architectures 1-4) contains the design templates. The KSBC Level 3 (Principal) descriptors in Part III describe what the role requires. |
| **If you are using this as a reference during a client engagement** | Part VI Architecture 4 (Greenfield Decision Framework) is the advisory framework. The callout "The One-Way Door Conversation" in Part VI is the single most important piece of client communication guidance in this document. |

*A note on the Notes throughout this manual: they are written for all practitioners. Understanding why a concept is commonly misunderstood — and how to recognise when you have misunderstood it — is itself a form of expertise. The most common misconceptions in Part VII.4 are the result of accurate-but-incomplete mental models. Reading them is inoculation.*

  **PART I**

**The Conceptual Landscape**

*Event Sourcing, its relatives, and why most engineers confuse them*

# **1.1  The Family of Patterns — A Precise Map**

Event sourcing belongs to a family of related architectural patterns. A common failure is treating them as interchangeable or as a stack that is always used together. They are not. Each pattern solves a different problem, and they combine in specific ways for specific reasons. Your first job is to ensure you have a precise mental model of the relationships before any implementation begins.

| PATTERN | CORE PROBLEM SOLVED | PRIMARY PRIMITIVE | RELATIONSHIP TO ES | COMMON CONFUSION |
| :---- | :---- | :---- | :---- | :---- |
| Event Sourcing (ES) | How do we store state so that history is never lost and any past state is reproducible? | Immutable, append-only event stream as the source of truth | — (the subject of this manual) | Confused with EDA because both use "events" |
| Event-Driven Architecture (EDA) | How do services communicate without tight coupling? | Events as messages passed between services (can be dropped, deduplicated, lost) | Complementary: an event-sourced system can publish domain events to an EDA bus; they are not the same | You might think EDA \= ES because both involve "events flowing"  |
| CQRS | How do we scale reads and writes independently when their requirements diverge? | Separate command (write) model and query (read) model | Frequently paired with ES: commands append events, queries read projections; but CQRS does not require ES and ES does not require CQRS | Practitioners often implement both together and cannot explain which part is which  |
| Domain-Driven Design (DDD) | How do we model complex business domains so code reflects business language? | Bounded contexts, aggregates, domain events, ubiquitous language | ES implements the persistence layer for DDD aggregates using their domain events; DDD is design methodology, ES is persistence strategy | You might think DDD is required for ES; it is not — ES is useful without DDD  |
| Saga / Process Manager | How do we coordinate multi-step business processes that span multiple aggregates or services? | Long-running process that reacts to events and issues commands | Sagas are implemented on top of ES: they listen to events in the store and produce commands; they are not part of the store itself | Commonly conflating the saga coordinator with the event store  |
| Outbox Pattern | How do we guarantee that a state change and an external notification happen atomically? | Write to the outbox table in the same DB transaction as the state change; poll outbox for delivery | Used to reliably publish ES domain events to external systems (Kafka, RabbitMQ); solves the dual-write problem | Skipping the outbox often leads to projections missing events under load  |
| CRDT (Conflict-free Replicated Data Type) | How do we merge state from multiple nodes without coordination? | Mathematically merge-safe data structures | Alternative to optimistic concurrency — relevant in distributed edge deployments; not a substitute for an event store in high-integrity scenarios | Rarely confused with ES but occasionally proposed as an alternative by distributed systems practitioners |

# **1.2  The Critical Distinction — EDA vs. Event Sourcing**

This is the most important distinction to establish early, because the confusion between EDA and Event Sourcing causes the most significant architectural mistakes. If you have worked with Kafka, RabbitMQ, or any pub/sub system, you will arrive with EDA intuitions and attempt to apply them to event sourcing. These intuitions are wrong in specific, predictable ways.

| DIMENSION | EVENT-DRIVEN ARCHITECTURE | EVENT SOURCING |
| :---- | :---- | :---- |
| Purpose of events | Notification: "something happened, whoever cares should react" | Persistence: "this is the authoritative record of what happened — the database IS the events" |
| What happens if an event is lost? | Downstream consumers miss a notification — this is often acceptable and handled by retry logic | The system's history is incomplete and state reconstruction is impossible — this is catastrophic |
| Event retention | Usually short-lived (hours to days); Kafka default retention is 7 days | Permanent by design; events from 5 years ago must be as readable as events from today |
| Consumer role | Consumers react to events and maintain their own state independently | The event stream IS the state; all state is derived from events |
| Schema evolution | Consumers can often tolerate schema changes with consumer-side adaptation | Historical events cannot be modified; all schema evolution happens at read time via upcasters |
| Ordering guarantee | Usually per-partition/per-topic ordering; global ordering is expensive | Strict ordering within a stream (by stream\_position); global ordering via global\_position |
| Concurrency model | Typically at-least-once delivery with consumer-side idempotency | Optimistic concurrency control — each write specifies expected version; conflicts are explicit |
| Typical tools | Kafka, RabbitMQ, Redis Streams, AWS EventBridge, NATS | EventStoreDB, Marten+PostgreSQL, Axon Framework, custom PostgreSQL schemas |
| Mutable state? | Consumers usually maintain mutable projections in separate databases | Projections are derived and rebuildable; the events are immutable; the projection IS mutable |

In short, Event-Driven Architecture (EDA) focuses on the propagation of state changes, while Event Sourcing (ES) focuses on the definition of state.

## **The Mathematical Distinction**

**Event Sourcing (The Log is the Truth)**

* In ES, the state *S* at time *t* is a pure function of the history of events *e* from the beginning of time.

*S*t \= *f*( *S*0, \[*e*1, *e*2, ..., *e*t \] )

* **State**: Is transient or a projection (cache). It can be deleted and perfectly recalculated by replaying the event log.  
* **Storage**: You store the operands (the events), not the result. 


**EDA / Event-Driven (The Message is the Trigger).** 

* In a standard EDA system, events are used to notify other services. The state *S* is usually updated in place (CRUD) and the event is a side effect.

*S*t \= *S*t-1 \+ Δ

* **State**: Is the "Source of Truth." Once the database record is overwritten, the previous state is lost unless specifically archived.  
* **Storage**: You store the result, and the event is an ephemeral notification ($e\_t$) sent over a wire. 

Key Comparisons

| Feature | Event Sourcing (ES) | Event-Driven Architecture (EDA) |
| :---- | :---- | :---- |
| State Origin | Derived from the sum of all events. | Derived from the current record in a DB. |
| Auditability | Perfect (mathematically guaranteed). | Partial (depends on logging/tables). |
| Complexity | High (requires "folding" or snapshots). | Lower (standard pub/sub models). |
| Primary Goal | Data integrity and history. | Decoupling and scalability. |

The Hybrid Reality: Most modern systems use both. You might use Event Sourcing inside a single microservice to keep a perfect history, and then emit those events via EDA to notify the rest of the ecosystem. Should we look into a specific code implementation or how snapshots are used to optimize ES performance?

| The "What If" Test  | When you find yourself confusing EDA and ES, use this diagnostic: "What happens if my message bus crashes and loses the last 100 events? In my current design, can I reconstruct the system's state?" If the answer is no, you have EDA, not event sourcing. The ability to reconstruct current state from first principles — by replaying from event 1 — is the definitional property of event sourcing. If the system cannot do this, it is not event-sourced.  |
| :---- | :---- |

# **1.3  CQRS — What It Is and Is Not**

CQRS is the architectural pattern most frequently paired with event sourcing, but they are independent decisions. A system can use CQRS without event sourcing (separate read and write databases with synchronisation). A system can use event sourcing without CQRS (reading state by replaying the aggregate stream on every query — expensive at scale but valid for small systems). Most production event-sourced systems use both.

## **The CQRS Contract**

| SIDE | NAME | PRIMITIVE | CHARACTERISTICS | ES IMPLEMENTATION |
| :---- | :---- | :---- | :---- | :---- |
| Write | Command Side | Command → Domain Logic → Events | Strongly consistent; optimistic concurrency enforced; returns acknowledgement not data | append\_events() to the event store; aggregate validates business rules before appending |
| Read | Query Side | Events → Projection → Read Model | Eventually consistent; optimised for query patterns; can have multiple read models from same events | Projections built by the async daemon; stored in Postgres tables optimised for the specific query |

The most important property of CQRS in the event sourcing context: the write side never reads from the query side. Commands are validated against the aggregate's current state — which is reconstructed by replaying events — not against a projection. Projections can be stale; the aggregate event stream is always current. If a command handler reads from a projection to make a decision, the CQRS boundary has been violated.

| Diagnostic Check: The Projection Read in Command Handler  | Examine your credit analysis command handler. Where does it read the application's current state? If it points to a database table (a projection), you have violated the CQRS boundary. The command handler must reconstruct the LoanApplicationAggregate by replaying its event stream. This is slower than reading a projection but guarantees the command sees the authoritative state. The performance concern is real — it is solved by snapshotting, not by reading projections in commands.  |
| :---- | :---- |

# **1.4  DDD Integration — Aggregates, Bounded Contexts, Domain Events**

Event sourcing is the natural persistence layer for DDD aggregates. The DDD aggregate is a consistency boundary — changes to the aggregate must be atomic and its invariants must always hold. An event is the DDD concept of a "domain event" made durable. The combination gives you: DDD for modelling (what are the right aggregates and events?), event sourcing for persistence (how do we store aggregate state?), and CQRS for access (how do we query efficiently?).

## **The DDD → Event Sourcing Mapping**

| DDD CONCEPT | EVENT SOURCING EQUIVALENT | IMPLEMENTATION NOTE |
| :---- | :---- | :---- |
| Aggregate | Event stream | One stream per aggregate instance: "loan-{id}", "agent-{id}-{session\_id}" |
| Aggregate root | The entity identified by stream\_id | stream\_id encodes both aggregate type and identity |
| Domain event | Event stored in the stream | Events are named in past tense ("ApplicationSubmitted"), are immutable, and represent facts that occurred |
| Command | Input to the command handler | Commands are validated; they MAY produce events but they are not stored directly — only the resulting events are stored |
| Invariant | Business rule checked in aggregate.apply() before append | If the invariant is violated, append is rejected before writing to the store |
| Value object | Embedded in event payload | Value objects become JSON fields in the event payload; they are not aggregates and have no streams |
| Bounded context | Separate event stream namespace | Events in different bounded contexts use different stream\_id prefixes; cross-context communication via published domain events, not shared streams |
| Anti-corruption layer | Event translator / upcaster at context boundary | Translates events from one bounded context's schema to another's; prevents schema leakage between contexts |

| The Ubiquitous Language Test  | Read your event type names aloud to a non-technical stakeholder. If they understand what happened, the event names are correct domain language. If names like "UpdateUserStatus", "SetApplicationFlag", or "ModifyRecord" appear, you have mapped CRUD operations to events rather than modelling domain facts. CRUD events ("Updated", "Deleted", "Modified") are common early-stage mistakes. Real domain events: "ApplicationSubmitted", "CreditLimitExceeded", "FraudAlertRaised", "ComplianceRulePassed".  |
| :---- | :---- |

  **PART II**

**The Deep Jargon Glossary**

*Every term a tutor needs to know with precision — including how trainees misuse them*

This glossary is organised by conceptual cluster, not alphabetically. Terms that are regularly confused are grouped together so you can address the confusion directly. Each entry includes a note on the most common misunderstanding.

  **CLUSTER A  ·  Core Storage Primitives**


**Event Store**

A database specialised for storing events in order. Not a general-purpose database with events bolted on. The event store guarantees: append-only writes (no update or delete), strict ordering within a stream via stream\_position, global ordering across all streams via global\_position, and ACID guarantees for appends. The two dominant implementations in 2026 are EventStoreDB (purpose-built) and PostgreSQL with a carefully designed events table (the approach used in The Ledger challenge).

**In practice:** 

‣  EventStoreDB: the purpose-built standard, with native gRPC streaming and persistent subscriptions

‣  PostgreSQL \+ Marten (.NET): enterprise-grade, single-database, production-proven

‣  PostgreSQL \+ psycopg3 (Python): what trainees build in the challenge — same patterns, no framework magic

| Note  | You might say "I'm using an event store" when using a message broker (Kafka) or a time-series database (InfluxDB). Test yourself: "Can I replay all events from position 0 to reconstruct current state, guaranteed, without data loss?" Kafka cannot guarantee this after retention expires. InfluxDB is not append-only-per-stream with optimistic concurrency. Neither is an event store.  |
| :---- | :---- |

**Event Stream**

The ordered sequence of events for a single aggregate instance. A stream is identified by a stream\_id (e.g., "loan-a1b2c3") and contains all events that have ever been applied to that aggregate. Streams are the unit of optimistic concurrency — you cannot span a transaction across two streams without accepting that they may be at different versions at commit time. Streams are also the unit of archival — when an aggregate's lifecycle is complete, its stream can be archived (moved to cold storage) without affecting other streams.

**In practice:** 

‣  "loan-abc123" contains: ApplicationSubmitted, CreditAnalysisRequested, CreditAnalysisCompleted, DecisionGenerated, ApplicationApproved — the complete immutable history of that loan

‣  "agent-worker-1-session-42" contains the full action history of one AI agent's work session

| Note  | A common confusion: if you have used Kafka, you might think a "stream" is a topic with multiple producers. An event store stream is a single aggregate instance's history — it has one logical writer at a time (enforced by optimistic concurrency). The Kafka topic is the EDA concept; the event store stream is the ES concept.  |
| :---- | :---- |

**Stream Position vs. Global Position**

Stream position is the version number of an event within a single stream — starts at 1, increments by 1, unique per stream. Global position is a monotonically increasing sequence number across ALL events in the store — the order in which events were physically written. Both are needed: stream position for optimistic concurrency (expected\_version=5 means "this stream should currently have 5 events"); global position for projection daemons (process all events from global\_position=1000 onwards, regardless of which stream they belong to).

**In practice:** 

‣  Optimistic concurrency uses stream\_position: "I read the loan stream and it was at version 3; I expect it to still be at version 3 when I append"

‣  Projection daemon uses global\_position: "I last processed global position 8500; give me all events with global\_position \> 8500"

| Note  | If your projection daemon crashes and restarts, it must know where to resume. The answer is global\_position stored in projection\_checkpoints. If you replay from the beginning, you have not implemented checkpointing — your daemon is O(total\_events) on every restart.  |
| :---- | :---- |

**Aggregate**

*AG-reh-gate (noun) or ag-REH-gate (verb)*  **·  DDD / Consistency Boundary**

A consistency boundary — a cluster of domain objects that must be modified atomically and whose invariants must always hold. In event sourcing, an aggregate's state is the result of replaying all events in its stream. You cannot partially apply events — all events must be applied in order, or none. The aggregate is the unit of transactional consistency: you can guarantee consistency within one aggregate; you cannot guarantee it across two aggregates in a single operation without distributed transactions (which event sourcing deliberately avoids).

**In practice:** 

‣  LoanApplication is an aggregate: all state changes to a loan happen in the "loan-{id}" stream, atomically

‣  AgentSession is a separate aggregate: its events live in "agent-{id}-{session\_id}" — a different consistency boundary

| Note  | The hardest aggregate design question: "How large should my aggregate be?" The temptation is a God Aggregate that contains everything. The problem: every write to ANY part of the aggregate contends for the same stream\_position. 100 agents writing to one LoanApplication aggregate simultaneously \= 99 OptimisticConcurrencyErrors per second. The correct response: make aggregates as small as the consistency requirement allows — which is usually smaller than you expect.  |
| :---- | :---- |

**Optimistic Concurrency Control (OCC)**

The mechanism that prevents two concurrent writers from creating an inconsistent event stream. Each write operation specifies the expected\_version — the stream version the writer observed before making its decision. The event store checks: if current\_version \== expected\_version, append succeeds and current\_version increments. If current\_version \> expected\_version (someone else appended since the writer read), raise OptimisticConcurrencyError. The writer must reload the stream, re-apply its business logic to the updated state, and retry if appropriate. No locks are held between read and write — hence "optimistic" (we optimistically assume no conflict; we verify atomically at write time).

**In practice:** 

‣  Agent A reads loan stream at version 3, computes credit decision. Agent B also reads loan stream at version 3, also computes fraud score. Both try to append at expected\_version=3. One succeeds (version becomes 4). The other gets OptimisticConcurrencyError, reloads at version 4, and retries.

‣  For LoanApplication: on retry, the agent must check whether the analysis it was about to submit is still relevant given the new events — it may have been superseded

| Note  | When you first encounter OCC, you might ask: "Can't I just use a database lock?" Yes — but locking creates contention. If 100 agents are processing 100 different loans, OCC means each loan's stream has independent concurrency — no global lock bottleneck. With pessimistic locking, all 100 agents contend for a global lock. OCC is why event stores can handle high write throughput.  |
| :---- | :---- |

**Snapshot**

A periodic save of an aggregate's computed state that acts as a checkpoint during event replay. Without snapshots, loading an aggregate requires replaying every event from position 1\. For a LoanApplication with 50 events, this is fast. For an aggregate with 50,000 events (a customer account with 10 years of transactions), this is unacceptably slow. A snapshot stores the aggregate's state at a specific stream\_position. On load: find the most recent snapshot, load from that position, replay only the events since the snapshot. Snapshotting has hidden complexity: snapshot invalidation (if the aggregate's apply logic changes, old snapshots are invalid), snapshot versioning (snapshots must carry a schema version), and snapshot storage (where to keep them without bloating the events table).

**In practice:** 

‣  Snapshot trigger options: every N events (e.g., every 100), every T time (e.g., daily), on-demand (after specific event types)

‣  Snapshot storage options: same events table with a special event\_type, separate snapshots table, Redis cache for hot aggregates

| Note  | The snapshot trap: you might implement snapshots and forget snapshot invalidation. If you change the aggregate's \_apply() logic (e.g., add a new computed field), all existing snapshots are now wrong. You must either invalidate snapshots on deploy or carry a snapshot\_schema\_version field and reject snapshots that don't match the current schema.  |
| :---- | :---- |

  **CLUSTER B  ·  Projections & Read Models**


**Projection**

A read-optimised view of data, built by processing events from the event store. A projection subscribes to one or more event types and maintains state (usually in a database table) as events are processed. Projections are eventually consistent with the event store — there is always a lag between an event being appended and the projection being updated. Projections are rebuildable: drop the projection table, replay all events from position 0, and the projection is reconstructed. This rebuildability is one of the most powerful properties of event sourcing — you can add a new projection at any time and backfill it with all historical data.

**In practice:** 

‣  ApplicationSummary projection: one row per loan application, updated as each event type is processed — the read model for the loan officer's dashboard

‣  AgentPerformanceLedger projection: aggregated statistics per model version — the read model for the AI ops team

| Note  | If your product team wants a new report: average time between ApplicationSubmitted and DecisionGenerated, by applicant industry sector. In a CRUD system, this might require a schema migration. In an event-sourced system, you create a new projection that subscribes to ApplicationSubmitted and DecisionGenerated events, backfill from position 0, and query the projection. No schema migration. No data loss. This "infinite flexibility" is real, but only if projections are designed correctly.  |
| :---- | :---- |

**Inline Projection vs. Async Projection**

Inline projections are updated synchronously in the same database transaction as the event write. Guaranteed consistent with the event store at all times. Higher write latency (the projection update is on the critical path). Async projections are updated by a background daemon after the event is written. Lower write latency (the write completes immediately; the projection catches up asynchronously). Eventually consistent — there is a lag window where the projection may not reflect the latest events. The choice depends on the read model's consistency requirements: if a UI must immediately reflect a write, use inline; if it can tolerate a few hundred milliseconds of lag, use async.

**In practice:** 

‣  ApplicationSummary: could be inline (immediate consistency for the loan officer dashboard) or async with \<500ms SLO

‣  AgentPerformanceLedger: always async — stale-by-seconds is acceptable for an analytics dashboard

| Note  | The consistency trap: you might default to inline projections because they're simpler. But inline projections mean every write pays the cost of updating every projection. As projections multiply, write latency grows proportionally. Production systems almost always move critical read models to async with tight SLOs.  |
| :---- | :---- |

**Projection Checkpoint / Daemon Checkpoint**

The last global\_position that a projection daemon has successfully processed. Stored in a projection\_checkpoints table. When the daemon restarts (crash, deploy, scale), it reads its checkpoint and resumes from that position — not from 0\. Without checkpointing, every restart causes a full replay which is O(total\_events) and gets slower as the system matures. Checkpointing is what makes the async daemon production-safe. The checkpoint must be updated atomically with the projection update — if the projection table is updated but the checkpoint is not, the daemon will reprocess events and the projection must be idempotent (handling the same event twice must produce the same result as handling it once).

**In practice:** 

‣  After processing events 1–8500, checkpoint \= 8500; on restart, daemon queries "SELECT \* FROM events WHERE global\_position \> 8500"

‣  Idempotency requirement: if ProcessedApplicationSubmitted is called twice with the same application\_id, the second call must not create a duplicate row

| Note  | The idempotency test: deliberately process the same batch of events twice through a projection and assert the state is identical to processing it once. Every projection must pass this test to avoid corrupting state after a daemon restart.  |
| :---- | :---- |

**Projection Rebuild / Replay**

The process of discarding a projection's current state and rebuilding it by replaying all events from position 0\. This is how you add new projections to a system with historical data, fix projection bugs, and recover from corruption. Rebuilding must happen without downtime to live reads — the old projection must remain queryable while the new version is being built, then swapped atomically. The Blue/Green projection pattern: build the new projection in a "green" table while the "blue" table serves live traffic; swap at completion.

**In practice:** 

‣  Adding a new projection to a 3-year-old system: create the table, set checkpoint to 0, start daemon — it replays 3 years of events to backfill

‣  Fixing a projection bug: rename current table to \_backup, create new table, replay from 0, verify, drop \_backup

| Note  | How long does a full rebuild take on your system? You should know this number to understand your system's operational characteristics. A rebuild that takes 10 minutes is acceptable; 10 hours is not — you need to fix either event volume or build efficiency before going to production.  |
| :---- | :---- |

  **CLUSTER C  ·  Schema Evolution & Migration**


**Upcaster**

A function that transforms an event of an old schema version into an event of a newer schema version, applied at read time without modifying the stored event. Upcasters are the event sourcing solution to schema evolution. The stored event is immutable — you can never change what was written. The upcaster transforms the payload in memory as events are loaded, so the rest of the system always sees the current schema. Upcasters are chained: if an event evolves from v1 to v3, the chain v1→v2→v2→v3 is applied automatically. Every upcaster must be deterministic (same input, same output), must not call external services, and must never write to the database.

**In practice:** 

‣  v1 CreditAnalysisCompleted: {application\_id, risk\_tier, recommended\_limit\_usd}

‣  v2 CreditAnalysisCompleted: adds {model\_version, confidence\_score}

‣  Upcaster v1→v2: infer model\_version from recorded\_at timestamp; set confidence\_score=null (genuinely unknown — fabrication is worse than null)

| Note  | The null vs. fabrication decision is a hard judgment call. When a new required field is added, you must provide a value for historical events. Rule: never fabricate a value that will be treated as accurate data. If a compliance system reads it, a fabricated value is worse than null — it will cause incorrect regulatory decisions.  |
| :---- | :---- |

**Event Schema Versioning**

The practice of tagging each stored event with the schema version it was created under (event\_version field in the events table). This enables the upcaster registry to know which transformation chain to apply when loading an event. Without version tracking, upcasters must infer the version from payload content — fragile and error-prone. Schema versioning should be established from day one even if no upcasters exist yet: it costs nothing to store, and retrofitting version tracking onto a production system with millions of unversioned events is extremely painful.

**In practice:** 

‣  event\_version=1: original schema; no upcaster needed

‣  event\_version=2: added regulatory\_basis field; upcaster v1→v2 infers it from rule versions active at recorded\_at

‣  event\_version=3: renamed risk\_tier to risk\_level; upcaster v2→v3 renames the field

| Note  | What is the current event\_version of your CreditAnalysisCompleted events? If you cannot answer immediately, version tracking is not implemented correctly. It should be a constant on the event class.  |
| :---- | :---- |

**Backward Compatibility vs. Breaking Change (in event schemas)**

Backward-compatible changes allow existing consumers to process new events without modification: adding new optional fields, adding new event types. Breaking changes require upcasters or consumer updates: renaming fields, removing fields, changing field types, changing the semantics of a field (e.g., changing a field from representing cents to dollars). The key test for backward compatibility: can every existing upcaster and projection still function correctly if it receives the new event schema without modification? If yes, the change is backward-compatible.

**In practice:** 

‣  Backward-compatible: add optional field "branch\_code" to ApplicationSubmitted — existing projections ignore it

‣  Breaking: rename "amount\_cents" to "amount\_usd" — existing projections reading "amount\_cents" get null, causing silent data corruption

| Note  | The silent corruption problem is the worst failure mode. Always ask: "What happens to every existing projection if I make this change?" The answer should be explicit, not assumed.  |
| :---- | :---- |

**Aggregate Root Versioning vs. Event Versioning**

Two different things called "version" in event sourcing. Aggregate version (also called stream\_position) is the number of events in the stream — used for optimistic concurrency control. Event schema version (event\_version) is the version of the event's payload schema — used for upcasting. These are completely independent: a stream at aggregate version 47 might contain events with event\_version 1 (old schema) and events with event\_version 2 (new schema) mixed together. The aggregate version tracks "how many events have happened"; the event version tracks "what schema was used when this event was created".

**In practice:** 

‣  stream\_position=47 means this aggregate has had 47 events applied to it — used in expected\_version check

‣  event\_version=2 on a particular event means its payload must be processed through the v1→v2 upcaster chain

| Note  | This distinction causes persistent confusion because both are called "version." Clarify: are you looking at the aggregate stream version for concurrency or the event schema version for upcasting? They are different columns with completely different semantics.  |
| :---- | :---- |

  **CLUSTER D  ·  Enterprise & Distributed Patterns**


**Outbox Pattern**

The solution to the dual-write problem: writing to the event store and publishing to an external message bus (Kafka, Redis Streams) atomically. Without the outbox, these two writes happen in separate transactions — if the event store write succeeds but the Kafka publish fails, your projections and external systems are inconsistent. The outbox table lives in the same database as the event store. On event append: write the event to the events table AND write a corresponding row to the outbox table, in the same database transaction. A separate poller reads the outbox and publishes to Kafka, marking rows as published. Transactional Outbox Pattern is the formal name; "outbox" is the universal term.

**In practice:** 

‣  Events table write \+ outbox table write \= one PostgreSQL transaction — either both succeed or both fail

‣  Outbox poller reads unacknowledged outbox rows, publishes to Kafka, marks published\_at — idempotent, retryable

| Note  | The outbox pattern works with any database \+ any message bus combination. It is the implementation-agnostic solution for enterprise integration.  |
| :---- | :---- |

**Saga / Process Manager**

A stateful process that coordinates multi-step business workflows by reacting to events and issuing commands. In event-sourced systems, a saga listens to domain events and, based on its current state, decides whether to issue the next command in a workflow. Sagas have their own event stream (or state store) to track their current position in the workflow. The saga pattern comes in two flavours: Choreography (each service reacts to events and emits its own without central coordination) and Orchestration (a dedicated saga coordinator explicitly issues commands to each participant service). Both are implemented on top of the event store.

**In practice:** 

‣  Loan approval saga: on CreditAnalysisCompleted → issue RequestFraudScreening command; on FraudScreeningCompleted → issue RequestComplianceCheck; on all checks complete → issue GenerateDecision command

‣  Timeout handling: if FraudScreeningCompleted is not received within 30 seconds, the saga issues an EscalateToHuman command

| Note  | Don't conflate the saga with the aggregate. The aggregate enforces business rules for a single entity; the saga orchestrates workflows across multiple aggregates. They are different things and should never be merged.  |
| :---- | :---- |

**Eventual Consistency vs. Strong Consistency**

In CQRS systems with async projections, the write side (event store) and read side (projections) are eventually consistent — there is a window of time where a read might return stale data. The duration of this window is the projection lag. Strong consistency means reads always reflect the latest write. In event-sourced systems: aggregate state reconstruction via event replay is strongly consistent (you see every event); projection reads are eventually consistent (you see the projection as of the last daemon checkpoint). The consistency requirement for a feature determines which to use for that feature's read operations.

**In practice:** 

‣  User just approved a loan — they expect to immediately see "Approved" on the dashboard: either use inline projection (strongly consistent) or implement read-after-write consistency at the application layer

‣  Analytics dashboard showing approval rates by risk tier: eventual consistency with 5-second lag is acceptable

| Note  | The UI timing problem: a user takes an action and immediately navigates to a screen that reads from a projection. If the projection has not caught up, they see stale data. The solution is read-after-write consistency, which must be explicitly designed.  |
| :---- | :---- |

**Idempotency (in event processing)**

An operation is idempotent if performing it multiple times produces the same result as performing it once. In event sourcing, projection handlers must be idempotent because at-least-once delivery guarantees may cause an event to be delivered to a projection more than once (after daemon restart, after network failure, during replay). A non-idempotent projection will accumulate duplicate state. Idempotency strategies: primary key on the event\_id in the projection table (duplicate inserts fail gracefully), tracking processed event IDs in a processed\_events table, using UPSERT (insert or update) semantics instead of INSERT.

**In practice:** 

‣  Non-idempotent: INSERT INTO application\_summary VALUES (...) — second call creates duplicate row

‣  Idempotent: INSERT INTO application\_summary VALUES (...) ON CONFLICT (application\_id) DO UPDATE SET ... — second call updates existing row to same state

| Note  | The idempotency test should be part of every projection's test suite. This is a production safety requirement, not just good practice.  |
| :---- | :---- |

  **CLUSTER E  ·  AI-Era Extensions**


**Agent Memory via Event Replay (Gas Town Pattern)**

The pattern of using an agent's event stream as its durable memory, enabling full context reconstruction after a crash or restart. An AI agent appends an AgentContextLoaded event at session start (recording what data it accessed and its model version), then appends an event for every significant action it takes. If the agent crashes, it can be restarted, its event stream replayed, and a context summary reconstructed from the event history — enabling it to continue without repeating work or losing decisions made before the crash. Named "Gas Town" because it solves the "ephemeral memory" problem of command-line agents described in the Tenacious infrastructure work.

**In practice:** 

‣  Agent starts: appends AgentContextLoaded with {model\_version, context\_source, token\_count}

‣  Agent makes decision: appends CreditAnalysisCompleted with {decision, confidence, input\_data\_hash}

‣  Agent crashes; is restarted: calls reconstruct\_agent\_context() which replays the session stream and produces a context summary for the LLM

| Note  | The key insight: the event store IS the agent's memory. Reconstructing summaries from the event store enables agents to work on tasks spanning hours without context window exhaustion.  |
| :---- | :---- |

**Causal Chain (correlation\_id \+ causation\_id)**

The mechanism for tracing why an event occurred — not just what occurred. Every event carries two metadata fields: correlation\_id (the ID of the original request or workflow that initiated the entire causal chain — stays the same across all events in the same business transaction) and causation\_id (the ID of the specific event that directly caused this event to be created). Together, they form a graph: following causation\_ids from any event backwards traces the complete causal history of that decision. This is mandatory for regulatory compliance ("what data and model caused this credit decision?") and essential for debugging ("which recommendation caused this schedule adjustment?").

**In practice:** 

‣  Request comes in with correlation\_id=C42. ApplicationSubmitted has correlation\_id=C42, causation\_id=null (it is the root). CreditAnalysisRequested has correlation\_id=C42, causation\_id=event\_id of ApplicationSubmitted. CreditAnalysisCompleted has correlation\_id=C42, causation\_id=event\_id of CreditAnalysisRequested.

‣  Full causal chain: ApplicationSubmitted → CreditAnalysisRequested → CreditAnalysisCompleted → DecisionGenerated → ApplicationApproved

| Note  | The counterfactual test: can you trace ApplicationApproved back to the exact model version and input data hash? If any link is missing or causation\_ids are null, your audit trail is incomplete and the system will fail regulatory examination.  |
| :---- | :---- |

**Counterfactual Projection / What-If Analysis**

A read-only operation that replays a portion of the event stream with one or more events substituted (the "counterfactual") and evaluates what the system state would have been under that alternative history. Used for: regulatory examination ("what would the decision have been with last month's model?"), model improvement analysis ("how many decisions would have changed if we had used the v3 risk model?"), and debugging ("was the agent's decision wrong because of bad input data or bad model logic?"). Counterfactual events exist only in memory during the projection run — they are NEVER written to the event store.

**In practice:** 

‣  Counterfactual: substitute CreditAnalysisCompleted with risk\_tier=HIGH instead of MEDIUM; re-run the decision projection; check if the final decision changes

‣  Causal dependency filtering: events that are causally dependent on the substituted event must be replaced by counterfactual consequences; events that are causally independent (e.g., a parallel fraud screening) are replayed unchanged

| Note  | Causal dependency filtering is critical. If you substitute an event, you must also substitute or skip all downstream events that trace back to it, otherwise the counterfactual is contaminated.  |
| :---- | :---- |

  **PART III**

**The KSBC Framework**

*Knowledge, Skills, Behaviours & Competencies — what mastery looks like at each level*

The KSBC framework maps four dimensions of expertise. Knowledge is what you know. Skills are what you can do. Behaviours are your thought patterns and habits. Competencies are demonstrations of all three under real conditions. Use this as a self-assessment tool as you prepare for enterprise roles.

| How to Use This Framework  | Use this as a diagnostic lens for your own development. Identify which domain is your current bottleneck. If you know patterns but cannot implement them, you have a Skill gap. If you can implement but cannot explain tradeoffs, you have a Behaviour gap. Focus your learning on the areas where you need the most growth.  |
| :---- | :---- |

  **KNOWLEDGE — What You Must Know Before You Can Reason Correctly**


| KNOWLEDGE DOMAIN | Observable Indicators — What is present when you master this  |
| :---- | :---- |
| **Core ES Primitives** *Events, streams, global position, append-only semantics* | You can define an event store and distinguish it from a CRUD database without prompting You can explain stream\_position vs. global\_position without confusion You can articulate why the past is immutable and what that means for schema evolution You know the ACID guarantees required of an event store and why they matter |
| **Pattern Relationships** *ES, EDA, CQRS, DDD, Saga — distinct and related* | You can map the relationships between ES, EDA, CQRS, DDD without mixing them You can give an example of each pattern used without the others You can explain why CQRS does not require ES and ES does not require CQRS You can identify when a Saga is appropriate vs. when aggregate design should be changed |
| **Concurrency & Consistency** *OCC, eventual consistency, consistency boundaries* | You can explain optimistic concurrency control with a concrete example You can articulate the consistency model of async projections (eventual consistency) You can identify the consistency requirement for a given business feature and choose the appropriate read model You know the difference between aggregate-level consistency and cross-aggregate coordination |
| **Schema Evolution** *Upcasting, versioning, backward compatibility* | You can classify schema changes as backward-compatible or breaking You can write an upcaster for a realistic schema change You understand null vs. inferred vs. default value tradeoffs for missing historical fields You can explain why snapshot invalidation is required after aggregate apply() logic changes |
| **Enterprise Stack** *EventStoreDB, Marten, PostgreSQL, Kafka integration* | You can map EventStoreDB concepts to PostgreSQL equivalents You understand the Marten Async Daemon pattern and can implement its equivalent in Python You know when to use the Outbox Pattern and can implement it You can explain the difference between Kafka as event bus vs. PostgreSQL as event store |
| **AI-Era Patterns** *Agent memory, causal chains, counterfactual projections* | You can explain the Gas Town persistent memory pattern and implement AgentContextLoaded You understand correlation\_id \+ causation\_id and can trace a complete causal chain You know the counterfactual projection concept and its causal dependency constraint You can explain how event sourcing enables regulatory auditability for AI decisions |

  **SKILLS — What You Must Be Able to Do Under Timed Conditions**


| SKILL AREA | Observable Indicators — What is present when you master this  |
| :---- | :---- |
| **Event Store Implementation** *Build a production-quality append-only store* | You implement the events table schema correctly (all required columns, indexes, constraints) You implement append\_events() with optimistic concurrency in a single database transaction You implement load\_stream() with upcaster application transparently You implement load\_all() as an async generator with configurable batch sizes You pass the double-decision test (concurrent append conflict handling |
| **Aggregate Design & Domain Logic** *Model business rules as aggregates* | You identify correct aggregate boundaries for a given business domain You implement the load() → apply events → validate → append pattern correctly You enforce all business invariants in domain logic You implement state machine transitions and rejects invalid state changes You name events in past-tense domain language |
| **Projection Building** *Build efficient, rebuildable read models* | You implement a projection handler with correct idempotency You implement the ProjectionDaemon with checkpoint management You expose projection lag as a measurable metric You can rebuild a projection from scratch without downtime to live reads You meet stated SLOs under simulated load |
| **Upcaster Implementation** *Handle schema evolution without store mutation* | You implement UpcasterRegistry with automatic chain application You write a realistic upcaster with justified inference strategy You pass the immutability test (store is never written during upcasting) You implement upcaster tests You understand and can articulate null vs. fabrication tradeoffs |
| **MCP Tool & Resource Design** *Design event store interfaces for AI agent consumption* | You design Tools (Commands) and Resources (Queries) as structural CQRS implementation You implement structured error types that LLM consumers can reason about You document preconditions in tool descriptions You ensure Resources read only from projections You pass full lifecycle integration tests using only MCP tool calls |

  **BEHAVIOURS — Thought Patterns That Emerge From Experience**


Behaviours are the hardest dimension to teach because they are not transferable through instruction — they emerge from experience navigating real systems and real failures. The tutor's role is to create the conditions under which these behaviours develop, not to explain them. The following table describes what to look for and how to create opportunities for each behaviour to emerge.

| BEHAVIOUR | Observable Indicators — What is present when you master this  |
| :---- | :---- |
| **Fact-Not-State Thinking** *Instinctively models the world as facts that happened rather than state that exists* | You spontaneously describe business requirements using past-tense domain events When confronted with a new requirement, you ask "what are the facts I need to record?" before "what fields do I need to store?" You catch yourself naming events with CRUD verbs and self-correct to domain language You can walk through a business scenario and generate a complete event catalogue independently |
| **Consistency Boundary Instinct** *Automatically identifies where consistency requirements begin and end* | When designing aggregates, your first question is "what is the smallest consistent unit?" You recognise aggregate-size bloat and articulate the concurrency cost When given a multi-aggregate business operation, you design a saga rather than cross-aggregate transactions You can immediately identify which aggregate owns a given business rule when a new requirement arrives |
| **Schema Immortality Awareness** *Treats the event store as containing data that will outlive the current codebase* | Before adding any field to an event, you ask "what will the upcaster look like when this field must change?" You add event\_version to every event schema from day one You never fabricate historical field values in upcasters without documentation You ask "what happens to events written 3 years from now that use this schema?" |
| **Causal Tracing Reflex** *Automatically thinks in terms of why things happened, not just what happened* | Every event has correlation\_id and causation\_id populated When debugging, your first question is "what is the causal chain that led to this state?" You design agent actions so every automated decision references the data and model version used You can answer "who authorised this action and what made them authorise it?" for any event |
| **Lag Consciousness** *Treats projection lag as a system property that must be designed for, not an acceptable background condition* | You report projection lag in milliseconds, not "fast" or "low" You define projection SLOs before implementing projections When designing a new feature, your first question about reads is "what is the acceptable consistency window?" You design read-after-write consistency at the application layer when needed |
| **Immutability as a Design Constraint** *Designs new features assuming the event log already exists and cannot be modified* | When asked to "just update old events," you explain why this violates the fundamental guarantee and propose an upcaster You treat the event catalogue as a contract with the future When a bug is found in historical data, your first response is "design a compensating event" not "run an UPDATE" You understand that append-only is a feature, and can explain why to a stakeholder |
| **Projection Rebuild Orientation** *Treats every projection as a temporary view that might need to be rebuilt* | You design projections with rebuild in mind from the start When a new reporting requirement arrives, your first question is "can I build a new projection from existing events?" When optimising a slow query, you consider rebuilding the projection before adding indexes You know and monitor projection rebuild time as a system health metric |

  **COMPETENCIES — Integrated Performance Under Real Conditions**


Competencies are demonstrated by navigating ambiguous, realistic scenarios — not by passing checklist tests. The following competency definitions describe what a trainee must be able to do to be ready for an enterprise event sourcing role. Each competency has a Level 1 (trainee), Level 2 (practitioner), and Level 3 (principal) descriptor.

| COMPETENCY | LEVEL 1 — Trainee | LEVEL 2 — Practitioner | LEVEL 3 — Principal Practitioner  |
| :---- | :---- | :---- | :---- |
| Event Catalogue Design | Can generate events for a domain with guidance; uses some CRUD verbs; misses edge cases  | Generates complete event catalogue independently; all domain language; identifies edge cases  | Designs event catalogues that are immediately readable by domain experts; identifies missing events that requirements didn't specify; anticipates schema evolution needs |
| Aggregate Boundary Decisions | Implements aggregates that work but are too large or arbitrarily bounded  | Correctly sizes aggregates to consistency requirements; can justify boundary choices  | Identifies aggregate boundary traps in existing systems; redesigns boundaries under live concurrency pressure; can explain the coupling cost of any boundary choice in business terms |
| Production Incident Response | Needs guidance to replay events for debugging  | Uses event replay as the first debugging tool; reconstructs state from history  | Leads regulatory examinations using event streams; writes counterfactual projections to isolate root causes; produces tamper-evident audit packages on demand |
| Schema Migration Planning | Can write a upcaster with guidance; does not plan ahead for evolution  | Writes upcasters independently; plans evolution paths before implementing changes  | Reviews schema changes for long-term upcasting implications; maintains a schema evolution roadmap; makes null-vs-fabrication decisions with documented rationale |
| Performance Under Load | Implements working system; has not considered high-volume characteristics  | Has modelled throughput; knows aggregate hot spots; has tested OCC retry rates  | Can predict OCC collision rates from business logic analysis; optimises write throughput via QuickAppend modes and stream partitioning; designs snapshot strategies from first principles |
| Client Communication | Can explain event sourcing technically to engineers  | Can explain business value (auditability, flexibility) to non-technical stakeholders  | Can conduct the "should we use event sourcing?" conversation with an enterprise client; communicates one-way door decision correctly  |

  **PART IV**

**The Event Schema Design Reference**

*Canonical schemas, field-level rationale, and design decision patterns for production event stores*

This reference section provides production-ready schema definitions with the rationale for every design decision. The tutor should use these as the canonical answer when trainees question why a particular column exists or a constraint is defined a certain way. Each decision in these schemas was made because its absence caused a production failure somewhere in the industry.

  **4.1  Core Event Store Tables**


  **events (Primary table — the source of truth)**

*Every state change in the system is stored here. This is the only table that grows forever and can never be updated or deleted.*

**event\_id**  UUID PK DEFAULT gen\_random\_uuid()   *— Globally unique identifier. Always UUID — never integer sequences (which leak event volume and are hard to shard). gen\_random\_uuid() is Postgres 14+ native; use uuid-ossp extension on older versions.*

**stream\_id**  TEXT NOT NULL   *— Encodes both aggregate type and instance: "loan-{uuid}", "agent-{id}-{session}". Text not UUID: stream IDs must be human-readable for debugging and operational queries. Use consistent naming convention enforced by application layer.*

**stream\_position**  BIGINT NOT NULL   *— Position of this event within its stream. Starts at 1\. Combined with stream\_id forms the unique constraint for optimistic concurrency. BIGINT not INT — systems running for years can exceed INT range on active streams.*

**global\_position**  BIGINT GENERATED ALWAYS AS IDENTITY   *— Monotonically increasing across ALL events. Used by projection daemons to resume from checkpoint. GENERATED ALWAYS prevents application from setting it manually — this is intentional: the database owns global ordering.*

**event\_type**  TEXT NOT NULL   *— String name of the event: "CreditAnalysisCompleted". Use consistent naming: PascalCase, past tense, no abbreviations. This is the routing key for upcasters and projections.*

**event\_version**  SMALLINT NOT NULL DEFAULT 1   *— Schema version of this event's payload. Starts at 1\. Increment when payload schema changes. SMALLINT (2 bytes) is sufficient — event schemas rarely exceed version 10 in practice.*

**payload**  JSONB NOT NULL   *— The event's data. JSONB (binary JSON) not JSON: JSONB indexes are efficient, JSONB queries are fast, JSONB validates structure. Never store PII here without encryption — this table is permanent and auditable.*

**metadata**  JSONB NOT NULL DEFAULT '{}'   *— Cross-cutting context: correlation\_id, causation\_id, agent\_id, model\_version, user\_id, ip\_address, trace\_id. Kept separate from payload to avoid polluting domain event schemas with infrastructure concerns.*

**recorded\_at**  TIMESTAMPTZ NOT NULL DEFAULT clock\_timestamp()   *— When the event was recorded. clock\_timestamp() not NOW() — NOW() returns the transaction start time; clock\_timestamp() returns the actual write time. For events within one transaction, NOW() gives them identical timestamps which is misleading.*

**Key Indices:**

UNIQUE (stream\_id, stream\_position) — the concurrency control constraint

INDEX (stream\_id, stream\_position) — range scan for load\_stream()

INDEX (global\_position) — range scan for projection daemon

INDEX (event\_type) — filter queries by event type

INDEX (recorded\_at) — time-range queries for audit and compliance

BRIN INDEX (recorded\_at) — efficient for append-only time-ordered data; much smaller than B-tree

| Design Note | The UNIQUE constraint on (stream\_id, stream\_position) is what makes optimistic concurrency work. The database enforces it atomically — two concurrent inserts with the same (stream\_id, stream\_position) cannot both succeed. One gets a unique constraint violation, which the application translates to OptimisticConcurrencyError. This is the only lock-free concurrency mechanism needed. |
| :---- | :---- |

  **event\_streams (Stream metadata registry)**

*Tracks the current version and metadata of each aggregate stream. Updated atomically with every event append.*

**stream\_id**  TEXT PRIMARY KEY   *— Matches events.stream\_id. The canonical identifier for this aggregate instance.*

**aggregate\_type**  TEXT NOT NULL   *— The type component of stream\_id: "loan", "agent", "compliance". Enables queries like "all active loan streams".*

**current\_version**  BIGINT NOT NULL DEFAULT 0   *— The current stream\_position of the most recent event. Updated on every append. Used for optimistic concurrency check without a COUNT() query on the events table.*

**created\_at**  TIMESTAMPTZ NOT NULL DEFAULT NOW()   *— When the stream was first created (first event appended).*

**archived\_at**  TIMESTAMPTZ (nullable)   *— Set when the stream is archived. Archived streams are logically closed — no new events can be appended. Queries filter on archived\_at IS NULL for active streams.*

**metadata**  JSONB NOT NULL DEFAULT '{}'   *— Stream-level metadata: tags, owning team, data classification (PII, financial, regulated). Used for operational management and compliance queries.*

**Key Indices:**

INDEX (aggregate\_type, archived\_at) — list active streams by type

| Design Note | current\_version in event\_streams enables the optimistic concurrency check to be O(1): "SELECT current\_version FROM event\_streams WHERE stream\_id \= $1" is a primary key lookup. Without this table, the check requires "SELECT MAX(stream\_position) FROM events WHERE stream\_id \= $1" which becomes slower as the stream grows. This is the performance optimisation that makes high-throughput append viable. |
| :---- | :---- |

  **projection\_checkpoints (Daemon state tracking)**

*Tracks the last successfully processed global\_position for each projection. The daemon reads this on startup to resume without full replay.*

**projection\_name**  TEXT PRIMARY KEY   *— Unique identifier for this projection: "application\_summary", "agent\_performance\_ledger"*

**last\_position**  BIGINT NOT NULL DEFAULT 0   *— The global\_position of the last event successfully processed. On restart, daemon queries events WHERE global\_position \> last\_position.*

**updated\_at**  TIMESTAMPTZ NOT NULL DEFAULT NOW()   *— When the checkpoint was last updated. Used for lag monitoring: NOW() \- updated\_at gives the approximate lag for a daemon that has not processed any events recently.*

**Key Indices:**

PRIMARY KEY (projection\_name) — single row per projection, fast upsert

| Design Note | The checkpoint must be updated in the same transaction as the projection table update — not before, not after. If the projection is updated but the checkpoint is not, the daemon will reprocess events on restart (requiring idempotent handlers). If the checkpoint is updated but the projection is not (due to a crash), the daemon will skip events — which is catastrophic. Always: UPDATE projection tables AND UPDATE projection\_checkpoints in a single database transaction. |
| :---- | :---- |

  **outbox (Reliable event publishing)**

*Events to be published to external systems. Written in the same transaction as the events table. Polled by the outbox publisher for delivery to Kafka, Redis Streams, or other buses.*

**id**  UUID PRIMARY KEY DEFAULT gen\_random\_uuid()   *— Independent ID from event\_id — one event may generate multiple outbox entries (e.g., publish to Kafka AND send a webhook).*

**event\_id**  UUID NOT NULL REFERENCES events(event\_id)   *— Foreign key to the source event. Enables the publisher to retrieve full event context if needed.*

**destination**  TEXT NOT NULL   *— The target: "kafka:loan-events", "redis:agent-actions", "webhook:compliance-api". Allows the publisher to route to the correct bus.*

**payload**  JSONB NOT NULL   *— The message payload to publish. May differ from the event payload (e.g., stripped of internal fields, transformed to the external event schema).*

**created\_at**  TIMESTAMPTZ NOT NULL DEFAULT NOW()   *— When the outbox entry was created.*

**published\_at**  TIMESTAMPTZ (nullable)   *— NULL \= not yet published. Set by the publisher on successful delivery. The publisher polls WHERE published\_at IS NULL.*

**attempts**  SMALLINT NOT NULL DEFAULT 0   *— Delivery attempt count. Used to implement backoff and dead-lettering (if attempts \> max\_retries, move to dead letter).*

**Key Indices:**

INDEX (published\_at, created\_at) WHERE published\_at IS NULL — efficient poll for unpublished entries

| Design Note | The outbox publisher must be idempotent at the destination. If the publisher delivers a message to Kafka and then crashes before marking published\_at, it will redeliver on restart. The Kafka consumer (projection or external system) must handle duplicate messages gracefully. This is the at-least-once delivery guarantee. Exactly-once requires distributed transaction support (Kafka transactions) which adds significant complexity — at-least-once with idempotent consumers is the standard enterprise pattern. |
| :---- | :---- |

  **snapshots (Performance optimisation for high-volume streams)**

*Periodic saves of aggregate state to avoid full-stream replay. Use only when load\_stream() latency exceeds acceptable thresholds — typically when streams exceed \~500 events for latency-sensitive operations.*

**snapshot\_id**  UUID PRIMARY KEY DEFAULT gen\_random\_uuid()   *— Unique identifier for this snapshot.*

**stream\_id**  TEXT NOT NULL REFERENCES event\_streams(stream\_id)   *— The stream this snapshot is for.*

**stream\_position**  BIGINT NOT NULL   *— The stream\_position of the most recent event included in this snapshot.*

**aggregate\_type**  TEXT NOT NULL   *— Used to select the correct deserialiser when loading the snapshot.*

**snapshot\_version**  INT NOT NULL   *— Schema version of the snapshot data — different from event\_version. Must be validated against current aggregate apply() logic version on load.*

**state**  JSONB NOT NULL   *— The serialised aggregate state at stream\_position. Must be the complete state needed to continue applying events from stream\_position+1.*

**created\_at**  TIMESTAMPTZ NOT NULL DEFAULT NOW()   *— When the snapshot was taken.*

**Key Indices:**

INDEX (stream\_id, stream\_position DESC) — find most recent snapshot for a stream

| Design Note | snapshot\_version is the field most often forgotten in snapshot implementations. When aggregate apply() logic changes (e.g., a new computed field is added), snapshots taken with the old logic are invalid — they will produce incorrect state if loaded. On load, check: if snapshot.snapshot\_version \!= CURRENT\_AGGREGATE\_SNAPSHOT\_VERSION, discard the snapshot and fall back to full replay. The performance cost of an occasional full replay is much lower than the correctness cost of loading a stale snapshot. |
| :---- | :---- |

  **4.2  Event Payload Schema Patterns**


Event payload schemas are the contracts between the present and the future. Well-designed payloads are: minimal (only the facts that happened, no derived or computed values), self-contained (no foreign keys that require cross-aggregate lookups to understand), and evolution-friendly (designed with the first upcaster already in mind).

## **Pattern 1 — Identity Events (stream initiation)**

Every stream starts with an identity event that records the aggregate's initial facts. The identity event must contain enough information to understand the aggregate's context without loading any other stream.

*Identity event — correct vs. incorrect*

\# CORRECT: Self-contained identity event  
class ApplicationSubmitted(BaseEvent):  
    event\_type: str \= "ApplicationSubmitted"  
    event\_version: int \= 1  
    \# Identity fields  
    application\_id: UUID  
    applicant\_id: UUID  
    applicant\_name: str            \# Denormalised from applicant record  
    \# Business facts  
    requested\_amount\_usd: Decimal  
    loan\_purpose: LoanPurpose      \# Enum, not string — enforced vocabulary  
    submission\_channel: str        \# "web" | "mobile" | "agent" | "branch"  
    submitted\_at: datetime

\# WRONG: Thin identity event requiring cross-aggregate lookup  
class ApplicationSubmitted(BaseEvent):  
    application\_id: UUID           \# All other data requires separate query  
    applicant\_id: UUID             \# Who is this? Cannot tell without loading applicant stream

| The Denormalisation Rule | Event payloads should denormalise data that will be needed to understand the event in isolation. A future auditor or regulator looking at an event in isolation must be able to understand it without loading related aggregates. This means: include the applicant's name in ApplicationSubmitted even though it's also in the applicant record; include the session's context summary in AgentContextLoaded even though it's also in the context database. The cost is storage (cheap); the benefit is auditability (invaluable). |
| :---- | :---- |

## **Pattern 2 — Decision Events with Causal Chain**

Events that record a decision must carry the full causal chain — not just what was decided, but the data and model version that informed the decision. This is the event sourcing implementation of the AI governance requirement.

*Decision event with full provenance chain*

class CreditAnalysisCompleted(BaseEvent):  
    event\_type: str \= "CreditAnalysisCompleted"  
    event\_version: int \= 2  
    \# What was decided  
    application\_id: UUID  
    risk\_tier: RiskTier            \# HIGH | MEDIUM | LOW  
    recommended\_limit\_usd: Decimal  
    confidence\_score: float        \# 0.0 \- 1.0  
    \# How it was decided (causal provenance)  
    agent\_id: str  
    session\_id: UUID  
    model\_version: str             \# "credit-model-v2.4.1"  
    model\_deployment\_id: UUID      \# Specific deployment, not just version  
    input\_data\_hash: str           \# SHA-256 of all input data — not the data itself  
    analysis\_duration\_ms: int  
    \# Regulatory context  
    regulatory\_basis: list\[str\]    \# List of regulation IDs this analysis satisfies

\# metadata (set by EventStore, not by the domain event):  
\# {  
\#   "correlation\_id": "...",       \# Original request ID  
\#   "causation\_id": "...",         \# event\_id of CreditAnalysisRequested  
\#   "agent\_id": "credit-worker-3", \# Who appended this event  
\# }

## **Pattern 3 — Compensating Events (not corrections, not deletions)**

When something goes wrong — a wrong amount was entered, a rule was misapplied, a fraud score was recalculated — the correct response is a compensating event, not a modification of the original. Compensating events preserve the history of what went wrong and create an audit trail of the correction.

*Compensating event pattern*

\# A CreditAnalysisCompleted was submitted with an incorrect risk tier  
\# WRONG: update the stored event (breaks immutability)  
\# UPDATE events SET payload \= ... WHERE event\_id \= ... \-- NEVER DO THIS

\# CORRECT: append a compensating event  
class CreditAnalysisCorrected(BaseEvent):  
    event\_type: str \= "CreditAnalysisCorrected"  
    event\_version: int \= 1  
    application\_id: UUID  
    original\_event\_id: UUID        \# References the incorrect CreditAnalysisCompleted  
    corrected\_risk\_tier: RiskTier  
    corrected\_limit\_usd: Decimal  
    correction\_reason: str         \# Required — must explain why  
    corrected\_by: str              \# Who authorised the correction  
    corrected\_at: datetime

\# The aggregate's apply() handles CreditAnalysisCorrected by overwriting  
\# the risk\_tier and limit in the reconstructed state — but the original  
\# CreditAnalysisCompleted remains in the stream, permanently, as evidence  
\# of what happened and when the correction was made.

| The Correction Audit Requirement | In regulated industries, corrections are themselves regulated events. A loan risk assessment that was corrected after the fact must show: the original assessment, the correction, who authorised it, and why. An event store that allows modification of stored events cannot satisfy this requirement. An event store that uses compensating events satisfies it automatically — the complete correction history is in the stream. |
| :---- | :---- |

  **4.3  Event Naming Convention Reference**


| NAMING RULE | CORRECT EXAMPLES | INCORRECT EXAMPLES | WHY IT MATTERS |
| :---- | :---- | :---- | :---- |
| Past tense — facts that occurred | ApplicationSubmitted, CreditAnalysisCompleted, ComplianceRulePassed | SubmitApplication, CompleteCreditAnalysis, PassComplianceRule | Commands are present tense (intentions); events are past tense (facts). Mixing them causes confusion about whether a concept is an input or an output |
| Domain language — no CRUD verbs | SessionBooked, FraudAlertRaised, ApplicationWithdrawn | UserCreated, RecordUpdated, StatusChanged, DataModified | CRUD verbs describe database operations, not business facts. "ApplicationWithdrawn" tells you something happened in the business domain; "RecordUpdated" tells you a row changed |
| Specific — not generic | SeatReserved, LimitExceeded, AgentContextLoaded | EventCreated, StatusUpdated, ProcessCompleted | Generic names force readers to examine the payload to understand what happened. Specific names communicate intent immediately |
| Noun \+ past participle | OrderShipped, InvoicePaid, PolicyRenewed | ShippedOrder, PaidInvoice, RenewedPolicy | The noun-first convention groups related events together alphabetically and matches domain language ("the order was shipped") |
| No abbreviations | CustomerIdentityVerified, AmlCheckCompleted | CustIdVerif, AmlChk | Events are permanent records. Abbreviations that make sense today are cryptic 3 years from now when the original team has moved on |
| No technical prefixes | CreditLimitExceeded, SessionExpired | ON\_CREDIT\_LIMIT\_EXCEEDED, evt:session\_expired | Technical prefixes are for messaging infrastructure, not domain events. Events should be readable by domain experts |

  **PART V**

**Query Pattern Reference**

*The canonical queries every event-sourced system needs — with SQL and Python patterns*

Query patterns in event-sourced systems fall into two categories: event store queries (direct reads against the events table, used for aggregate reconstruction and audit) and projection queries (reads against read-optimised projection tables). The canonical rule: use projection queries for application features; use event store queries for audit, compliance, and debugging.

  **5.1  Event Store Queries — Aggregate Reconstruction**


## **Query 1 — Load Aggregate Stream (the fundamental query)**

*Core aggregate load — O(n) without snapshots, O(events since snapshot) with*

\-- Load all events for an aggregate, in order  
SELECT  
    event\_id, stream\_position, event\_type, event\_version,  
    payload, metadata, recorded\_at  
FROM events  
WHERE stream\_id \= $1  
ORDER BY stream\_position ASC;

\-- With snapshot optimisation (load only events after most recent snapshot)  
SELECT e.\*  
FROM events e  
WHERE e.stream\_id \= $1  
  AND e.stream\_position \> (  
    SELECT COALESCE(MAX(stream\_position), 0\)  
    FROM snapshots  
    WHERE stream\_id \= $1  
      AND snapshot\_version \= $2  \-- current aggregate snapshot schema version  
  )  
ORDER BY e.stream\_position ASC;

## **Query 2 — Optimistic Concurrency Append (atomic check \+ insert)**

*Optimistic concurrency — uses row-level lock on event\_streams, not table lock*

\-- PostgreSQL: check current version and append in one transaction  
BEGIN;

\-- Step 1: Lock the stream row and check version  
SELECT current\_version FROM event\_streams  
WHERE stream\_id \= $1  
FOR UPDATE;  \-- Row-level lock prevents concurrent version updates

\-- Step 2: Verify expected version (application layer does this check)  
\-- if current\_version \!= expected\_version: ROLLBACK and raise OptimisticConcurrencyError

\-- Step 3: Insert events  
INSERT INTO events (stream\_id, stream\_position, event\_type, event\_version, payload, metadata)  
SELECT $1, current\_version \+ ROW\_NUMBER() OVER (ORDER BY ordinal),  
       event\_type, event\_version, payload, metadata  
FROM unnest($2::jsonb\[\]) WITH ORDINALITY AS t(ev, ordinal)  
CROSS JOIN LATERAL (  
    SELECT ev-\>\>'event\_type' AS event\_type,  
           (ev-\>\>'event\_version')::int AS event\_version,  
           ev-\>'payload' AS payload,  
           ev-\>'metadata' AS metadata  
) AS unpacked;

\-- Step 4: Update stream version  
UPDATE event\_streams  
SET current\_version \= current\_version \+ $3,  \-- number of events appended  
    updated\_at \= NOW()  
WHERE stream\_id \= $1;

COMMIT;

## **Query 3 — Projection Daemon Polling**

*Daemon polling — use LISTEN/NOTIFY to reduce polling overhead in production*

\-- Get next batch of events for a projection to process  
SELECT  
    e.event\_id, e.stream\_id, e.stream\_position,  
    e.global\_position, e.event\_type, e.event\_version,  
    e.payload, e.metadata, e.recorded\_at  
FROM events e  
WHERE e.global\_position \> (  
    SELECT last\_position  
    FROM projection\_checkpoints  
    WHERE projection\_name \= $1  
)  
ORDER BY e.global\_position ASC  
LIMIT $2;  \-- batch\_size, typically 100-500

\-- PostgreSQL LISTEN/NOTIFY for push-based notification (more efficient than polling)  
\-- In the append transaction:  
NOTIFY event\_appended, '{"stream\_id": "loan-abc", "global\_position": 8501}';  
\-- Projection daemon:  
LISTEN event\_appended;  \-- blocks until notification received, then polls

## **Query 4 — Temporal Query (state at a specific timestamp)**

*Temporal query — enables regulatory time-travel and counterfactual analysis*

\-- Load all events for a stream up to a specific timestamp  
\-- Used for temporal queries, audit examinations, what-if projections  
SELECT  
    event\_id, stream\_position, event\_type, event\_version,  
    payload, metadata, recorded\_at  
FROM events  
WHERE stream\_id \= $1  
  AND recorded\_at \<= $2  \-- the target timestamp  
ORDER BY stream\_position ASC;

\-- Optimised version with snapshot before target timestamp  
WITH latest\_snapshot AS (  
    SELECT stream\_position, state  
    FROM snapshots  
    WHERE stream\_id \= $1  
      AND created\_at \<= $2  
      AND snapshot\_version \= $3  
    ORDER BY stream\_position DESC  
    LIMIT 1  
)  
SELECT e.\*  
FROM events e  
LEFT JOIN latest\_snapshot ls ON true  
WHERE e.stream\_id \= $1  
  AND e.recorded\_at \<= $2  
  AND e.stream\_position \> COALESCE(ls.stream\_position, 0\)  
ORDER BY e.stream\_position ASC;

  **5.2  Projection Queries — Application Features**


## **Query 5 — Causal Chain Traversal (audit and debugging)**

*Causal chain traversal — recursive CTE following causation\_id links*

\-- Find all events in a causal chain starting from a root correlation\_id  
\-- This is the query that answers: "what led to this decision?"  
WITH RECURSIVE causal\_chain AS (  
    \-- Base case: all events with this correlation\_id  
    SELECT  
        event\_id, stream\_id, event\_type, payload, metadata,  
        recorded\_at, 0 AS depth  
    FROM events  
    WHERE metadata-\>\>'correlation\_id' \= $1

    UNION ALL

    \-- Recursive: follow causation\_id references  
    SELECT  
        e.event\_id, e.stream\_id, e.event\_type, e.payload, e.metadata,  
        e.recorded\_at, cc.depth \+ 1  
    FROM events e  
    INNER JOIN causal\_chain cc  
        ON e.event\_id::text \= cc.metadata-\>\>'causation\_id'  
    WHERE cc.depth \< 20  \-- guard against cycles  
)  
SELECT \* FROM causal\_chain ORDER BY recorded\_at ASC;

## **Query 6 — Cross-Stream Timeline (regulatory package)**

*Cross-stream timeline — the basis for generating regulatory packages*

\-- All events related to a business entity across all streams  
\-- Used for regulatory examination packages  
SELECT  
    e.event\_id,  
    e.stream\_id,  
    e.event\_type,  
    e.payload-\>'application\_id' AS application\_id,  
    e.recorded\_at,  
    e.metadata-\>\>'agent\_id' AS agent\_id,  
    e.metadata-\>\>'correlation\_id' AS correlation\_id,  
    e.metadata-\>\>'causation\_id' AS causation\_id  
FROM events e  
WHERE  
    \-- Events in the primary application stream  
    e.stream\_id \= $1  
    OR  
    \-- Events in related streams that reference this application  
    e.payload-\>\>'application\_id' \= $2  
ORDER BY e.recorded\_at ASC, e.global\_position ASC;

## **Query 7 — Projection Lag Monitoring**

*Lag monitoring — surface as a health endpoint in your MCP server*

\-- Monitor lag for all projections (run every 60 seconds)  
SELECT  
    pc.projection\_name,  
    pc.last\_position AS checkpoint\_position,  
    (SELECT MAX(global\_position) FROM events) AS latest\_position,  
    (SELECT MAX(global\_position) FROM events) \- pc.last\_position AS events\_behind,  
    EXTRACT(EPOCH FROM (NOW() \- pc.updated\_at)) \* 1000 AS lag\_ms,  
    CASE  
        WHEN EXTRACT(EPOCH FROM (NOW() \- pc.updated\_at)) \> 5 THEN 'CRITICAL'  
        WHEN EXTRACT(EPOCH FROM (NOW() \- pc.updated\_at)) \> 1 THEN 'WARNING'  
        ELSE 'OK'  
    END AS status  
FROM projection\_checkpoints pc  
ORDER BY lag\_ms DESC;

## **Query 8 — Schema Version Distribution (operational health)**

*Version distribution — run before every schema migration deployment*

\-- Understand what event\_versions are live in production  
\-- Critical before deploying schema changes or upcasters  
SELECT  
    event\_type,  
    event\_version,  
    COUNT(\*) AS event\_count,  
    MIN(recorded\_at) AS oldest,  
    MAX(recorded\_at) AS newest,  
    ROUND(100.0 \* COUNT(\*) / SUM(COUNT(\*)) OVER (PARTITION BY event\_type), 1\) AS pct  
FROM events  
GROUP BY event\_type, event\_version  
ORDER BY event\_type, event\_version;

  **PART VI**

**Solution Architecture Reference**

*Pattern templates for common enterprise event sourcing architectures*

This section provides architecture templates — reusable patterns that the tutor can use to guide trainees when designing systems. Each pattern includes: the problem it solves, the component structure, the critical design decisions, and the failure modes to guard against.

  **Architecture 1 — Single-Service Event Store (The Ledger Pattern)**


The starting pattern for all event sourced systems. One service owns the event store. All writes go through its command handlers. All reads are served from its projections via an MCP server or REST API. This is what trainees build in The Ledger challenge.

| COMPONENT | RESPONSIBILITY | TECHNOLOGY | CRITICAL DECISION |
| :---- | :---- | :---- | :---- |
| Event Store (PostgreSQL) | Append-only storage, optimistic concurrency, global ordering | PostgreSQL 14+ with JSONB, GENERATED ALWAYS identity | Use JSONB not JSON; use global\_position GENERATED ALWAYS to prevent application from setting it; BRIN index on recorded\_at |
| Command Handlers | Accept commands, validate against aggregate state, append events | Python asyncio with asyncpg | Load aggregate by replaying stream (or snapshot+replay), not by reading projections; enforce all business invariants before append |
| Projection Daemon | Rebuild read models from events; maintain checkpoints | Python asyncio background task with PostgreSQL LISTEN/NOTIFY | Checkpoint in same transaction as projection update; every handler must be idempotent; expose lag metric |
| Upcaster Registry | Transform old event schemas at read time | Python registry pattern; registered by (event\_type, from\_version) | Register upcasters before any other code runs; test immutability (store not written during upcasting); chain application must be automatic |
| MCP Server | Expose commands as Tools, projections as Resources | FastMCP or custom MCP server | Resources must read only from projections; Tools must return structured errors; document preconditions for LLM consumers |
| Outbox Publisher | Deliver events to external buses reliably | Background task polling outbox table with PostgreSQL LISTEN/NOTIFY | Publish at-least-once; consumers must be idempotent; dead-letter after max attempts |

  **Architecture 2 — Multi-Agent Coordination via Event Store**


Multiple AI agents collaborate on business processes by reading from and writing to shared event streams. No agent communicates directly with another — they coordinate exclusively through events. This is the event-driven equivalent of microservices coordination, applied to AI agent systems.

| CONCERN | PATTERN | IMPLEMENTATION | FAILURE MODE TO GUARD AGAINST |
| :---- | :---- | :---- | :---- |
| Agent isolation | Each agent has its own AgentSession stream; no shared mutable state | stream\_id \= "agent-{id}-{session\_id}" per agent per work session | Agents sharing state via a common writable projection rather than event streams — creates hidden coupling |
| Work coordination | Work items published as events; agents claim work by appending AgentContextLoaded | Saga coordinates workflow; OptimisticConcurrencyError handles claiming collisions | Two agents claiming the same work item — handled by OCC: first claim wins, second gets OptimisticConcurrencyError and moves to next item |
| Decision provenance | Every agent decision event references the specific data and model version used | input\_data\_hash, model\_version, model\_deployment\_id on every decision event | Agent decisions without model version tracking — undetectable when model changes cause decision quality regression |
| Agent memory on restart | Gas Town pattern: replay AgentSession stream to reconstruct context | AgentContextReconstructor with token budget management | Agent restarts and repeats already-completed work — prevented by checking last\_completed\_action in reconstructed context |
| Cross-agent consistency | Saga orchestrator subscribes to all agent decision events and coordinates next step | Saga has its own event stream (saga state); subscribes via projection daemon | Saga state stored only in memory — lost on restart; must be stored in event stream |
| Concurrent agent access to application | OCC on application stream prevents conflicting decisions | expected\_version on every append; retry logic with reload | Retry storms — if many agents collide on the same stream, implement exponential backoff with jitter |

  **Architecture 3 — Enterprise Integration (Event Store \+ Kafka)**


The event store is the system of record for one bounded context. Events from the store flow to Kafka for consumption by other services, analytics pipelines, and external systems. The Outbox Pattern guarantees exactly-once write semantics between the store and Kafka.

| COMPONENT | ROLE | KEY DESIGN DECISION |
| :---- | :---- | :---- |
| Event Store (PostgreSQL) | Source of truth for the bounded context; strong ACID consistency; append-only | Never use Kafka as the event store — Kafka is the integration bus; the store is the source of truth |
| Outbox Table | Bridge between strong consistency (store) and at-least-once delivery (Kafka) | Write to outbox in same transaction as events table; never write to Kafka directly from command handler |
| Outbox Publisher | Polls outbox, publishes to Kafka, marks published | Must be idempotent; use Kafka idempotent producer with message key \= event\_id; dead-letter after max retries |
| Kafka Topic (domain events) | Carries events to downstream consumers; partitioned by stream\_id for ordering | Partition key \= stream\_id ensures events for one aggregate arrive in order to each consumer |
| External Projections | Consumer services build their own read models from Kafka topic | Each consumer service has its own checkpoint (Kafka consumer group offset); consumer events are translated at the context boundary via Anti-Corruption Layer |
| Event Schema Registry | Enforces Avro/Protobuf schema compatibility for Kafka topics | Separate from event\_version in the store — Kafka schema compatibility is a transport concern; store versioning is a persistence concern |

  **Architecture 4 — Greenfield Design Decision Framework**


When a client asks "should we use event sourcing?", this is the decision framework. The tutor should be able to walk trainees through this logic so they can conduct the conversation with a client independently.

| FACTOR | STRONG YES | STRONG NO | NUANCED GUIDANCE |
| :---- | :---- | :---- | :---- |
| Auditability requirement | Regulatory mandate: every decision must be reproducible and tamper-evident | No audit requirement; simple CRUD operations are sufficient | If there is any current or foreseeable compliance requirement, use ES — retrofitting it is extremely expensive |
| State reconstruction need | Must be able to restore system to any past state for debugging or examination | Current state is the only relevant data; history is never queried | Even if not currently required, ask: "will we ever need to debug a production incident by replaying what happened?" |
| Business complexity | Complex domain with many state transitions, concurrent processes, rich business rules | Simple domain: create, update, delete a few entity types | Complexity can grow — a system that starts simple can become complex; ES handles this gracefully; CRUD with complex state management does not |
| Team experience | Team has prior ES or DDD experience; strong engineering discipline | Team has no ES experience; timeline is very short; rapid prototyping phase | The learning curve is real — 4-8 weeks for a team to become productive. Do not adopt ES under tight deadline pressure unless the team is experienced |
| Data volume | High write volume (\>10k events/day); need temporal queries; multiple read models | Low volume; single read model; simple queries | PostgreSQL \+ ES handles millions of events efficiently; volume alone is not a disqualifier if the architecture is correct |
| Migration cost | Greenfield or existing system with clean domain model | Legacy system with millions of CRUD records and no domain model; hard deadline | ES migration is one of the most expensive architectural refactors. Always assess migration cost honestly before recommending it for existing systems |

| The One-Way Door Conversation | The most important thing the tutor must communicate — and the trainee must be able to convey to a client — is this: "Event sourcing is a one-way door. Once you have production events in the store, migration away from event sourcing requires reconstructing current state for every aggregate, building a new CRUD schema from that state, and convincing every downstream system that the new schema is equivalent to the event history. This typically takes 6-18 months and costs more than the original implementation. Make the decision with full awareness of this commitment." A trainee who recommends event sourcing without disclosing this risk is not ready for client-facing work. |
| :---- | :---- |

  **PART VII**

**Guidance for Mastery: Self-Assessment and Advanced Concepts**

*Advanced checks and self-development strategies for mastering event sourcing*

This section provides diagnostic checks and self-correction strategies to help you reach mastery. The goal is to recognise common pitfalls and apply the correct mental shifts independently.

  **7.1  Self-Diagnosis of Experience Level**


Before moving to advanced concepts, diagnose your current understanding. The following questions distinguish a practitioner who has implemented the pattern from one who has only studied it:

| QUESTION | NOVICE LEVEL  | EXPERIENCED LEVEL  |
| :---- | :---- | :---- |
| "Name an event in your system that you almost added but decided not to." | Cannot name one; lists events in the catalogue without reflection | Names a specific rejected event; explains why it was a CRUD operation not a domain fact, or why it belonged in a different aggregate |
| "What is the current event\_version of your most-changed event type?" | Does not know; event\_version not tracked or always 1 | Knows immediately; can recite the upcaster chain; explains what inference decisions were made for historical fields |
| "What happens to your projections during a deploy?" | Not considered; projections update when the daemon restarts | Describes checkpoint management; discusses whether deploys cause lag spikes; mentions how projection schema migrations are handled |
| "Your most active stream has 10,000 events. What is the p99 load latency for that aggregate?" | Has not measured; "it's fast" | Has measured or can calculate from snapshot strategy; knows at what event count snapshots become necessary |
| "How do you detect a rogue agent that is appending events without being authorised?" | Application-layer authorisation check | Structural approach: agent\_id in event\_stream metadata with authorised\_agents list; any append from an unauthorised agent\_id is rejected at the store level; audit query to find stream\_ids with events from unexpected agent\_ids |

  **7.2  Core Self-Correction Strategies**


## **Self-Correction 1 — Reframing from State to Fact**

When you catch yourself designing events named "UserUpdated", "RecordModified", or "DataChanged":

***"Stop. What business thing just happened? Not what the database needs to do — what happened in the domain. A user completed their profile? Their address changed? Each of those is a different event with different consequences. 'Updated' tells you nothing about why."***

*Refine your thinking: Read your event name as if you're telling a colleague what just happened. If it's too vague, keep pressing until the name communicates intent without reading the payload.*

## **Self-Correction 2 — Testing Concurrency Integrity**

When you implement an append without expected\_version, or fail to handle OptimisticConcurrencyError:

***"Run a collision test. Simulate two concurrent writers trying to append to the same loan application. If both succeed, your system is broken. Optimistic concurrency is not optional; it is the fundamental correctness guarantee."***

*Remember: without OCC, your event store has no consistency guarantees, and any business logic built on top of it is unstable.*

## **Self-Correction 3 — Validating Checkpoint Reliability**

When you design a projection daemon without checkpoint management, or when checkpoints aren't updated atomically:

***"Perform a restart test. Kill your daemon process and restart it. If it replays from position 0, your checkpointing is failed. In production, with millions of events, that replay will make your system unusable."***

*Watching your daemon replay events instead of resuming from the last position is a critical lesson in operational efficiency.*

## **Self-Correction 4 — Thinking About the Schema's Future**

Before you finalise any event schema:

***"What field am I most likely to need in 6 months? When I add it, I will have to write an upcaster for all historical events. How will I infer that value, and is that inference safe?"***

*The goal is to develop Schema Immortality Awareness — treating every schema as a permanent contract with your future self.*

  **7.3  Advanced Practitioner Challenges**


If your technical implementation is solid, your growth lies in three areas: AI-era integration, client communication, and large-scale operations.

## **Self-Challenge 1 — Proving AI Decision Provenance**

If you treat AI actions as opaque results without causal context:

***"If you are audited, can you prove which model version made a specific assessment and exactly what data it saw? If not, you do not meet the AI governance requirement."***

*Extend your event sourcing knowledge to include AI provenance — track model versions and data hashes for every automated decision.*

## **Self-Challenge 2 — Assessing the One-Way Door**

Practise your ability to advise on whether a system should adopt event sourcing:

***"A client has 3 CRUD databases and wants to migrate to event sourcing in 6 months. Do you say yes? How do you explain the migration cost and the long-term commitment required?"***

*You must proactively raise migration costs and distinguish between greenfield and legacy scenarios. Principal practitioners know when to say no.*

## **Self-Challenge 3 — Predict Performance Under Load**

Move beyond functional design to quantitative analysis:

***"If you have 1,000 concurrent aggregates with 4 agents each, what is your expected OCC collision rate? How do you tune your backoff and retry strategy to keep throughput high?"***

*Mastering performance means being able to predict and manage the overhead of optimistic concurrency in high-volume environments.*

## **Self-Challenge 4 — Designing for Counterfactual Integrity**

Prove your expertise by handling complex replay scenarios:

***"If you substitute an event in a stream's history, how do you mathematically decide which subsequent events to keep and which to discard? Draw your dependency graph."***

*You must master the use of causation\_id chains to filter histories for counterfactual analysis without contaminating the results.*

  **7.4  Common Misconceptions — Preemptive Inoculation**


These misconceptions are common. Addressing them early will prevent fundamental implementation mistakes.

| MISCONCEPTION | COMMON BELIEF  | THE CORRECT UNDERSTANDING | HOW TO SURFACE IT |
| :---- | :---- | :---- | :---- |
| "Events are messages" | Events are packets of data that flow between services — like Kafka messages | Events are facts stored permanently in a database. They do not flow anywhere by default. The Outbox Pattern is how they flow. | Check: "If I turn off my message broker, does my event store still work?" The answer must be yes.  |
| "I can fix old events" | When a bug is found in historical data, the solution is to UPDATE the events table | Compensating events are the only mechanism for correcting history. The original event is permanent evidence. | Practise by deliberately introducing a "wrong" event in a test and implementing a compensating event to correct it.  |
| "Projections are optional" | The event store is the database; application features can just query the events table directly | Querying the events table for application features creates tight coupling to the event schema and is O(n) for every feature request | Try running a feature without a projection. Measure the performance and then consider the cost at 1,000 requests per second.  |
| "Every event needs a snapshot" | Snapshots are a core part of event sourcing that must be implemented from day one | Snapshots are a performance optimisation for high-volume streams. Start without them. Add them when load testing reveals unacceptable latency. | Check your longest stream count. If it's \<1,000, snapshots are premature. Mastery requires "measure first" discipline.  |
| "Event sourcing is eventually consistent" | The entire event sourcing system is eventually consistent | The event store (write side) is strongly consistent. Only async projections (read side) are eventually consistent. Command handlers read from strongly consistent aggregate state. | Ask: "If I append and immediately load the stream, do I see the event?" The answer must be yes.  |
| "Aggregate \= database table" | The aggregate in event sourcing corresponds to a database table | An aggregate is a consistency boundary, not a storage unit. Multiple instances of the same aggregate type share no mutable state — each has its own stream. | Map it out: aggregate type → one independent stream per instance. No shared mutable state.  |

TRP1 FDE Program  ·  Event Sourcing Practitioner Manual  ·  March 2026  ·  Confidential Practitioner Resource  
