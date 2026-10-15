# Lightweight PostgreSQL-interface database: proposed design

Status: approved design; initial local-development implementation is available. See docs/STATUS.md for delivered scope and outstanding release gates.

## Recommendation and assumptions

Build a small Rust server with PostgreSQL wire compatibility, a deliberately limited PostgreSQL SQL frontend, and bundled SQLite storage in WAL mode. Ship one executable and an optional container. Reuse the storage engine; implement the compatibility layer and resource controls.

This is a database product built on an existing storage engine, not a new storage engine. That distinction is intentional: building and validating a new durable pager, MVCC implementation, index manager, recovery system, and garbage collector is substantially more work than the requested feature set suggests. A custom engine is justified if independent engine development is itself a requirement or measured write contention invalidates this design.

Confirmed user requirements: PostgreSQL wire protocol plus basic SQL; read-heavy workloads with data exceeding memory. Proposed operating assumptions: single node, durable local SSD storage, and short transactions. No throughput claim is made before measurement.

Primary limitation: SQLite permits one active writer per database. Independent read transactions can run concurrently. Long write transactions block other writers; stale read transactions can fail when upgraded to writers even if the competing write touched unrelated rows. This is more restrictive than PostgreSQL's writer concurrency.

## Product contract

| Area | Version 1 contract |
|---|---|
| Deployment | Single executable, one command, automatically initialized data directory |
| Data | Persistent local disk; bounded caches; no requirement to load all rows into RAM |
| Interface | PostgreSQL protocol 3.0 baseline; ordinary SQL through psql and selected drivers |
| Operations | CREATE/DROP TABLE, CREATE/DROP INDEX, INSERT, SELECT, UPDATE, DELETE |
| Queries | Single table, projections, comparisons, AND/OR, IS NULL, IN, ORDER BY, LIMIT/OFFSET |
| Indexes | Primary key; single-column and composite B-tree secondary indexes; UNIQUE |
| Transactions | BEGIN, COMMIT, ROLLBACK; snapshot isolation; atomic autocommit |
| Durability | WAL plus synchronous=FULL; acknowledge commits after durable success |
| Types | INT4, INT8, BOOLEAN, TEXT, BYTEA initially; NULL with explicit constraints |
| Constraints | Explicit primary key, NOT NULL, UNIQUE; literal defaults |
| Boundaries | One database and public schema per server; explicit bounded resource settings |

Exclude joins, foreign keys, views, triggers, stored procedures, extensions, JSON operators, full-text search, replication, distributed transactions, online index construction, general catalog emulation, and broad ORM support. Reject unsupported syntax with an error; never silently ignore it. RETURNING can be added only for the explicitly tested CRUD subset. No PostgreSQL disk-format compatibility.

## Architecture

```text
psql / supported PostgreSQL drivers
                |
        bounded async TCP server
                |
    PostgreSQL session + protocol state
                |
     PostgreSQL AST parser and binder
                |
  type checking + supported-subset validation
                |
       SQL lowering + bound parameters
                |
 bounded blocking workers / SQLite connections
                |
      B-trees + WAL + background checkpoints
```

Rust keeps the runtime small and gives explicit resource ownership. Bundle a maintained SQLite release into the artifact so deployment does not depend on a system library. Select maintained parser/protocol crates during the feasibility stage after checking license, footprint, protocol completeness, and benchmark cost. Do not write a SQL parser or wire codec from scratch without evidence that existing libraries are unsuitable.

Use async I/O for sockets and bounded blocking execution for SQLite. A transaction leases one connection until completion, preserving snapshot and session state. Do not reserve an OS thread per idle client. Limit leased transactions, worker count, and queues. Ensure waiting writers cannot occupy every worker and starve the transaction that holds the writer lock; use short nonblocking busy responses with bounded async admission/retry for lock acquisition, never automatic replay of a partially executed transaction.

Cache prepared statements per connection with a small LRU and schema-generation invalidation. Stream results in bounded batches with socket backpressure. Apply deadlines to slow result consumers because open result sets can retain snapshots.

## PostgreSQL compatibility boundary

Implement startup, protocol version handling, simple queries, Parse/Bind/Describe/Execute/Close, Flush, Sync, Terminate, parameter metadata, row descriptions, command tags, SQLSTATE errors, and ReadyForQuery transaction status. Extended-query errors must discard messages until Sync as required. Support named and unnamed prepared statements/portals, parameter inference for supported expressions, binary and text representations of supported types, and bounded portal execution.

Implement cancellation with a separate cancellation key and an engine interrupt scoped to the executing request. Handle SSL negotiation explicitly. Default to loopback. A remotely exposed release needs tested TLS and SCRAM authentication with a single configured identity; role management is outside scope.

Support only a documented allowlist of startup/session settings and probes needed by the selected clients. Implement SHOW transaction_isolation truthfully. Default isolation is REPEATABLE READ (snapshot isolation), which differs from PostgreSQL's usual READ COMMITTED default. Reject explicit unsupported isolation modes rather than misreporting their semantics. Basic psql queries are supported; psql catalog meta-commands and arbitrary ORM introspection are not promised.

Use PostgreSQL parsing and typed binding, then generate SQLite SQL from a validated AST. Do not perform regex substitutions or pass arbitrary PostgreSQL SQL directly to SQLite. Bind values separately from generated SQL. Internal metadata tables and SQLite-specific operations are inaccessible through the SQL frontend.

SQLite STRICT tables alone do not establish PostgreSQL type semantics. Enforce integer bounds, checked arithmetic, boolean operations, parameter coercion, NULL three-valued logic, identifier folding, text comparison, and ordering in the frontend/lowering layer. Generate explicit NULL ordering. Restrict expressions when exact semantics cannot yet be guaranteed. INT4 maps to integer storage with range enforcement, INT8 to signed 64-bit integer, BOOLEAN to constrained integer, TEXT to text, BYTEA to blob. Use deterministic binary text ordering; locale-aware collations are excluded. Do not implicitly auto-generate integer primary keys when NULL is supplied.

## Snapshot isolation and transaction state

1. BEGIN creates a deferred transaction. Acquire the snapshot at the first data statement, and retain it through transaction end.
2. Reads observe committed data from that snapshot plus their own transaction's writes. Rows and secondary indexes belong to the same engine transaction and snapshot.
3. The first write obtains the engine's writer lock. Retain it through COMMIT or ROLLBACK. A write-only transaction may wait within its configured deadline; a stale snapshot cannot be refreshed to make a write succeed.
4. Map SQLITE_BUSY_SNAPSHOT to SQLSTATE 40001. The client must retry the whole transaction. Ordinary lock contention has a distinct bounded timeout/error path.
5. An error in an explicit transaction marks the session failed. Subsequent statements return 25P02 until rollback; COMMIT in a failed transaction performs rollback and reports the correct command tag.
6. Disconnect, cancellation, or timeout safely ends/aborts the affected work and releases connections and locks according to a tested session state machine.

Snapshot isolation is the promised contract; serializability is not advertised. The backend may reject more histories than a row-level MVCC engine. No predicate locks or custom read-set validation are added.

Required concurrency tests: repeatable reads, no dirty reads, stable range results after concurrent inserts/deletes, read-your-writes, rollback, competing updates, stale read-to-write upgrade, unique-key races, and indexed-column changes while an old snapshot remains open. Verify that an old snapshot can find an old index key and a new snapshot finds only the new key.

## Indexes and query execution

Use the engine's on-disk B-trees and planner. Composite indexes accelerate equality on leading columns followed by a range or compatible ordering. An index on (tenant_id, created_at) is the intended path for tenant-scoped ordered ranges. It does not promise an efficient lookup by created_at alone.

Maintain indexes in the same transaction as rows. Validate unique constraints through the engine, including concurrent writes. Build indexes transactionally during a maintenance window; do not pretend that index creation is online. Keep internal catalog updates atomic with DDL. Schema changes invalidate cached plans, and old transactions must finish or reach a deadline before incompatible DDL proceeds.

Use plan inspection and controlled statistics collection to verify access paths. Avoid redundant indexes: every extra index consumes space and adds write cost. Benchmark key width, row size, selectivity, cache size, and number of indexes. Prefer keyset pagination in examples; OFFSET remains supported but becomes expensive at high offsets.

## Durability, recovery, and resource limits

Let SQLite own WAL ordering, recovery, atomicity, and checkpoint mechanics; do not add a second transaction log. Set and verify WAL and FULL synchronization on the relevant connections. Never silently select NORMAL to improve benchmark results: it changes power-loss durability.

A maintenance connection performs passive checkpoints based on WAL growth and elapsed time, avoiding foreground checkpoint spikes where possible. Bound active transaction age and idle-in-transaction time. Long snapshots may prevent WAL recycling; measure WAL size, oldest snapshot age, and checkpoint progress. Under sustained pressure, reject new writes and expire transactions according to documented configured limits. Account for overshoot from an in-flight transaction; bound transaction size as well as queue size. Do not delete WAL files manually.

Enforce aggregate cache budgets across connections, query/result byte limits, statement timeouts, prepared-statement limits, maximum clients, and transaction limits. Provide minimal structured logs and counters for latency, busy errors, snapshot conflicts, memory, WAL size, and checkpoints.

On startup, verify schema format version, take a server-instance lock, open/recover the database, and become ready only after a test query succeeds. Do not rebuild indexes or preload all rows. Use local filesystems supported by WAL; network filesystems are outside scope. Shutdown drains briefly, rolls back unfinished work, and closes the engine. Use the engine backup API or a clean stopped copy; copying only the main file during live WAL activity is not a valid backup procedure.

## Performance and release gates

These are engineering targets, not measured claims:

- Empty and clean-start readiness under one second on a declared reference machine; runnable within five seconds once the executable/image is available. Download time and crash recovery are measured separately.
- Initial packaging budgets: stripped executable under 30 MiB and idle RSS under 40 MiB. Revisit with measurements rather than weakening correctness.
- Prepared indexed point-read throughput at least 70% of a minimal loopback server using the same SQLite query and durability settings. Report p50/p95/p99 latency and CPU, not throughput alone.
- Bound steady-state memory by configured cache/concurrency budgets and demonstrate stable WAL behavior under sustained writes and expired snapshots.

Benchmark raw embedded SQLite, a minimal transport baseline, this server, and PostgreSQL on the same hardware. Fix durability, datasets, cache conditions, clients, and transaction sizes. Include 1/8/32/128 clients, point reads, selective ranges, 95/5 and 50/50 read/write mixes, write-only loads, hot-key contention, and long readers. Use both RAM-resident and larger-than-cache datasets. Measure fsync cost, write amplification, restart time, abort rate, and writer queue latency. No generic claim of being faster than PostgreSQL follows from a smaller feature set.

If write contention prevents the agreed workload target, stop before expanding SQL support. Reconsider a transactional MVCC storage backend or a custom engine with row-level first-committer-wins validation. A custom replacement must separately design versioned secondary indexes, atomic visibility publication, durable commit ordering, recovery, snapshot-safe reclamation, and bounded restart time; it is a new architecture milestone, not a quick optimization.

## Implementation sequence

| Phase | Deliverable | Exit condition |
|---|---|---|
| 0: freeze contract | Workload profile, compatibility matrix, reference hardware, dependency choices | Explicit backend tradeoff and type/isolation boundaries accepted |
| 1: feasibility spike | In-process schema/index workload, WAL concurrency harness, minimal wire connection | Snapshot/upgrade behavior verified; writer bottleneck measured; drivers can connect |
| 2: CRUD vertical slice | Typed AST lowering, catalog, CRUD, indexes, transactions over simple protocol | SQL contract and atomicity tests pass; indexed paths verified |
| 3: driver compatibility | Extended protocol, codecs, portals, cancellation, transaction errors | Pinned psql, psycopg, and node-postgres integration suites pass |
| 4: resilience | Deadlines, admission control, WAL management, crash/recovery, backup/restore | Fault tests preserve atomicity; resource pressure remains bounded |
| 5: performance | Prepared caching, batching, profiling-driven changes | Published reproducible benchmark report meets agreed targets |
| 6: delivery | Release binary/container, initialization, readiness, TLS/auth for remote use, quickstart | Fresh-machine deployment and upgrade/restart smoke tests pass |

Do not implement all phases before testing the storage choice. Phase 1 is an explicit architecture gate.

Testing strategy: differential tests against PostgreSQL for the supported SQL semantics; property tests for typed expression lowering and randomized CRUD/index equivalence; deterministic concurrency schedules; protocol fragmentation/malformed-frame tests; subprocess crash injection around commits and checkpoints; disk-full/I/O failure injection; backup restoration; slow-client resource tests. Process termination tests alone do not establish power-loss durability, so use storage fault injection for the durability gate.

Implementation began after the user approved proceeding. The phase gates above remain the release criteria.

## References

- PostgreSQL wire overview: https://www.postgresql.org/docs/current/protocol-overview.html
- PostgreSQL protocol flow: https://www.postgresql.org/docs/17/protocol-flow.html
- PostgreSQL isolation: https://www.postgresql.org/docs/14/transaction-iso.html
- SQLite isolation and stale snapshot behavior: https://www.sqlite.org/isolation.html
- SQLite WAL and checkpoint constraints: https://www.sqlite.org/wal.html
- SQLite synchronization controls: https://www.sqlite.org/pragma.html#pragma_synchronous
- SQLite STRICT typing: https://www.sqlite.org/stricttables.html
