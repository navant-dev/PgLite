# Implementation status

The initial implementation is a usable local-development database server. The original design remains the target, not a claim that every production release gate has been completed.

## Implemented

- Rust executable with bundled SQLite, automatic initialization, local file locking, and loopback PostgreSQL listener.
- Typed SQL subset lowered from a PostgreSQL AST; unsupported constructs explicitly rejected.
- Durable disk-backed CRUD and composite B-tree secondary indexes.
- Snapshot isolation, read-your-writes, unique constraint enforcement, stale-write errors, failed-transaction state, transaction expiry.
- Simple and extended protocol, prepared plan reuse, binary/text type codecs, bounded portals, cancellation path, schema-version invalidation.
- Configurable connection/cache/result/time/WAL limits, background checkpoints, blocking worker isolation, and cancellable writer admission.
- Correctness, randomized index equivalence, actual-driver, abrupt process exit, and performance smoke tests.

## Deliberate first-release reductions

- Localhost only. Remote authentication/TLS and remote container deployment remain gated.
- Bounded result materialization instead of a fully streaming execution engine.
- A bounded asynchronous FIFO queue admits one writer. Stale snapshots still require whole-transaction retries; no automatic transaction replay.
- DDL invalidates prepared plans; clients must prepare again.
- Defaults, RETURNING, arithmetic, table aliases, and general driver session settings are not implemented.
- No online backup command, upgrade migrations, or published release artifacts.

## Outstanding production gates

- Independent psql, psycopg, and node-postgres integration matrix; broad ORM compatibility is out of scope.
- Power-loss/storage fault injection, disk-full/I/O failures, protocol fuzzing, slow-client tests, and continuous soak testing.
- Differential SQL semantics tests against PostgreSQL for every supported expression.
- Workloads larger than machine RAM, cold-cache runs, and a minimal wire-server/PostgreSQL benchmark comparison. Current smoke measurements exceed configured SQLite cache, not host RAM.
- Measurement of aggregate memory under worst-case clients, retained portals, and oversized incoming frames.
- Remote TLS/SCRAM, operational backup/restore automation, and packaging validation on Linux.

The single-writer architecture remains appropriate for a read-heavy initial workload, but a high-write-concurrency service still needs to account for serialized writes, queue latency, and snapshot-conflict retries. No cross-engine performance superiority is claimed.


## Final local verification

The optimized suite passed 25 tests including the performance smoke benchmark. The executable measured 5.35 MiB, 346 ms to readiness, and 3,204 KiB idle RSS on this host. The 95/5 mixed workload committed all 200 writes without conflicts. See [BENCHMARKS.md](BENCHMARKS.md) and the saved [verification output](verification.txt) for methodology and limitations.
