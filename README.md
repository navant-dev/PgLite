# PgLite server

A small, persistent database server for read-heavy CRUD workloads. It speaks a subset of PostgreSQL's wire protocol and SQL, with SQLite WAL providing disk-backed storage, B-tree indexes, durable commits, and snapshot isolation.

This is an early local-development release, not a PostgreSQL replacement. It accepts only loopback connections. Authentication, TLS, and a remote deployment profile are not implemented. It is unrelated to other projects using the PGlite name.

## Start

Requires Rust 1.99 and a C compiler to build. SQLite is bundled; no database installation or external service is needed.

```sh
cargo build --release --locked
./target/release/pglite-server --data ./data/app.db
```

If Rust was installed locally by this task, use `./scripts/cargo` instead of `cargo`. The server automatically creates the directory and database, then prints its readiness address. Building dependencies takes minutes; launching the already-built executable is the fast deployment path.

Connect with a PostgreSQL client:

```sh
psql 'host=127.0.0.1 port=5433 user=local dbname=app sslmode=disable'
```

Startup user/database names are currently labels; they do not select separate databases or authenticate users. Any local process able to connect can access the database. Basic SQL is supported; psql catalog commands such as `\d` are not implemented. The automated driver suite uses `tokio-postgres`; psql, psycopg, and node-postgres compatibility still need independent verification.

## Example

```sql
CREATE TABLE events (
    id BIGINT PRIMARY KEY,
    tenant_id INT NOT NULL,
    title TEXT,
    active BOOLEAN
);
CREATE INDEX events_tenant_title ON events (tenant_id, title);

INSERT INTO events VALUES (1, 10, 'Launch', true);
SELECT id, title FROM events WHERE tenant_id = 10 ORDER BY title LIMIT 20;

BEGIN;
UPDATE events SET title = 'Ready' WHERE id = 1;
SELECT title FROM events WHERE id = 1;
COMMIT;

DELETE FROM events WHERE id = 1;
```

Prepared statements support `$1`, `$2`, etc., including binary INT4, INT8, BOOLEAN, TEXT, and BYTEA parameters/results. Prepare again after DDL: the server rejects stale schema versions rather than using invalid result metadata. Explicitly typed parameters can be used when a query does not give enough context for inference.

## Implemented SQL

- CREATE/DROP TABLE with exactly one inline primary key; NOT NULL and inline UNIQUE.
- CREATE/DROP INDEX, including composite and unique B-tree indexes.
- INSERT VALUES, single-table SELECT, UPDATE, and DELETE.
- Column/literal projections, comparisons, AND/OR/NOT, IS NULL/IS NOT NULL, IN/NOT IN, ORDER BY, LIMIT/OFFSET.
- INT/INT4/INTEGER, BIGINT/INT8, BOOLEAN/BOOL, TEXT, BYTEA; nullable columns.
- BEGIN, COMMIT, ROLLBACK; explicit REPEATABLE READ accepted.
- Simple query batches, prepared statements, portals, text/binary codecs, cancellation, and PostgreSQL error codes.

Unsupported operations return errors. In particular: no joins, aggregations, arithmetic, aliases on tables, RETURNING, defaults, casts, UPSERT, ALTER TABLE, savepoints, additional isolation levels, locale collations, sequences, triggers, or general system catalogs. Comparison/coercion support is intentionally narrower than PostgreSQL; use correctly typed values. Text uses binary ordering. LIMIT/OFFSET must be nonnegative integer literals or parameters. Query result column aliases are supported.

## Transaction behavior

Transactions see a stable snapshot and their own writes. Index changes and row changes commit atomically. One writer can be active at a time. The server admits writers through a bounded FIFO queue. Short autocommit writes wait asynchronously for their turn; explicit write transactions retain admission until COMMIT or ROLLBACK. Writes based on an older snapshot can still conflict even when they touch unrelated rows.

- `40001`: stale snapshot could not be upgraded to a writer. Roll back and retry the whole transaction.
- `55P03`: backend writer lock busy (for example, an external process accessing the file). Retry with application-level backoff; no hidden server retries.
- `25P02`: transaction is aborted. ROLLBACK is required (COMMIT also rolls it back).
- `57014`: statement interrupted, execution deadline exceeded, or writer admission timed out.
- `54000`: resource limit reached.

The default is snapshot isolation, reported as `repeatable read`, unlike PostgreSQL's normal READ COMMITTED default. Do not rely on serializable isolation.

## Resource controls

| Option | Default | Meaning |
|---|---:|---|
| `--listen` | `127.0.0.1:5433` | Loopback address only |
| `--data` | `data/pglite.db` | Persistent database path |
| `--max-clients` | 32 | Active sockets; excess connections are closed |
| `--cache-kib` | 2048 | SQLite cache target per connection |
| `--max-result-bytes` | 8388608 | Accounted row values and cell overhead per result/batch |
| `--max-wal-bytes` | 67108864 | Soft write-admission threshold; in-flight writes may overshoot |
| `--statement-timeout-ms` | 10000 | SQLite execution deadline and separate writer-admission timeout |
| `--transaction-timeout-ms` | 30000 | Maximum explicit transaction age; expiry closes the connection |

Also limited: 1 MiB SQL text, 1 MiB bound parameter payload, 64 statements per batch, 32 prepared statements and 8 portals per connection. Results are materialized within the result budget and then encoded incrementally; the engine does not yet stream unbounded scans. Use LIMIT or keyset pagination. SQLite caches, result buffers, protocol frames, and runtime allocations are separate; cache size is not a total RSS cap. The OS page cache may grow with available memory.

Statements run on bounded blocking workers, outside async socket tasks. Transactions retain their connection, not an idle worker thread. Waiting writers do not occupy blocking workers, so the active writer can always submit COMMIT or ROLLBACK. A background task checkpoints WAL; old transactions expire to release retained snapshots. WAL pressure can temporarily reject writes. The server uses FULL synchronization and does not trade acknowledged-commit durability for benchmark speed.

One server owns a data path through an OS file lock. Keep the `.db`, `-wal`, and `-shm` files on a local filesystem. Do not copy only the main file while running. For this release, stop the server cleanly and copy the entire data directory for backup; restore into a separate stopped directory. An online backup API is not implemented.

## Verify

```sh
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --all -- --check
cargo test --release --test performance -- --ignored --nocapture
```

Tests cover indexed CRUD, randomized index/scan agreement, snapshot visibility, stale-write conflicts, rollback, durability after abrupt process exit, resource limits, real-driver prepared/binary queries, portals, and schema invalidation. Process-crash tests do not emulate a storage device losing power.

See [DESIGN.md](DESIGN.md) for the architecture, [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for measured results, and [docs/STATUS.md](docs/STATUS.md) for implementation scope and remaining release gates.
