# Modified BenchBase for PgLite

This runs the actual [CMU BenchBase](https://github.com/cmu-db/benchbase) Java framework and PostgreSQL JDBC driver against PgLite over TCP. It is a modified YCSB experiment, not an official YCSB score, TPC result, or a PostgreSQL comparison.

The initial run found a JDBC text-preservation bug and mixed-write failures. The fixes and fresh-dataset verification are documented in [FIXES.md](FIXES.md); [RESULTS.md](RESULTS.md) preserves the original failing baseline.

## Reproduce

Requires Git, Python 3, JDK 23 or newer, and a release PgLite binary. Maven is bootstrapped by upstream's wrapper. Run from the PgLite repository root:

```sh
BENCHBASE_DIR=/tmp/benchbase-pglite
# Use a new checkout and an empty database directory for each complete run.
git clone https://github.com/cmu-db/benchbase.git "$BENCHBASE_DIR"
git -C "$BENCHBASE_DIR" checkout "$(cat benchmarks/benchbase/upstream-revision.txt)"
python3 benchmarks/benchbase/adapt.py "$BENCHBASE_DIR"
(cd "$BENCHBASE_DIR" && ./mvnw -Ppostgres -DskipTests -Dfmt.skip=true \
  -Dmaven.compiler.release=23 -Ddescriptors=src/main/assembly/dir.xml package)
./target/release/pglite-server --data /tmp/pglite-benchbase-fresh/bench.db \
  --listen 127.0.0.1:55433
```

In a second terminal, with the same `BENCHBASE_DIR` set:

```sh
python3 benchmarks/benchbase/run.py \
  --distribution "$BENCHBASE_DIR/target/benchbase-postgres/benchbase-postgres" \
  --results /tmp/pglite-benchbase-results
```

Use `--java /absolute/path/to/java` if necessary. The results directory must not already exist. The server is user-managed; stop it after the run. The runner uses only port 55433. Do not point it at application data. `--skip-load` is only for a previously verified complete 100,000-row dataset. Generated configs embed the local schema path; regenerate them using `configure.py` after moving the checkout. No changes to the database server are needed.

## Exact modifications

- Fixed in-memory catalog for the single YCSB table replaces PostgreSQL catalog introspection. The adapter rejects other benchmark names.
- The initialization metadata connection closes before opening worker sockets, so 32 workers fit the existing 32-client limit.
- `SELECT ... ORDER BY ycsb_key DESC LIMIT 1` replaces initialization's unsupported MAX aggregate.
- The server already defaults to snapshot isolation. Worker initialization verifies `SHOW transaction_isolation` returns `repeatable read`, then rolls back its setup transaction, instead of sending unsupported session-level `SET TRANSACTION ISOLATION` commands.
- External DDL creates the original YCSB schema on an empty database, omitting `DROP TABLE IF EXISTS`.
- Loader issues atomic 64-row INSERTs, below PgLite's 1,024-parameter cap. Only loading retries SQLSTATE 55P03 (lock contention) or 53100 (WAL pressure), at most 100 retries with 20 ms waits. No retries are made for ambiguous connection failures. This changes setup, not timed operations.
- JDBC uses explicit parameter types and startup options; all timed SQL and workload procedures remain upstream implementations.
- Config `retries=1` means **one attempt**, with no transaction retry, in this upstream Worker implementation. Setting it to zero would execute no transaction at all.

Upstream Java code is Apache-2.0 licensed; this directory contains a transformation script rather than vendored Java sources. The pinned revision is in `upstream-revision.txt`.

## Method and interpretation

100,000 rows, ten 100-character text fields, integer primary key; Zipfian key distribution with theta 0.99 and seed 42. Reads retrieve the row; updates replace all ten fields. Scans use upstream's primary-key interval predicate with a random interval of 1–1,000 keys. This experiment does not exercise a secondary index. Seeded workers do not make concurrent scheduling deterministic.

Each main workload has a configured 10-second warmup and 30-second measurement window. Use the actual elapsed nanoseconds in each summary: the upstream timer can overshoot. Each transaction uses BEGIN / query / COMMIT through PostgreSQL JDBC, so these rates are not directly comparable with the earlier autocommit Rust-client smoke test. Successful commits retain SQLite WAL/FULL durability, default 2 MiB per-connection cache, default writer admission, and default 64 MiB WAL soft limit.

The upstream `Goodput` field divides warmup-inclusive success counts by measurement time. **Do not use it.** Report `Throughput (requests/second)` and measured latency percentiles. Histograms include warmup and completion-boundary transactions; they are not measurement-only counts. Raw CSVs contain timed transaction attempts but no success column, so interpret rates alongside failure histograms/logs.

At result collection, upstream's PostgreSQL parameter collector attempts an unsupported metadata expression. It logs one error and emits empty parameters / null DBMS version. This happens after timing and is not a workload transaction failure. The reported `DBMS Type: POSTGRES` denotes the JDBC adapter, not the underlying engine.

See [RESULTS.md](RESULTS.md) for this host's observations. These short, same-host client/server runs use warm OS cache and do not establish larger-than-RAM performance or production capacity.

## Verify stored data

After stopping the server cleanly, validate all one million text fields as well as the row/key counts and SQLite integrity:

```sh
python3 benchmarks/benchbase/verify.py \
  --database /tmp/pglite-benchbase-fresh/bench.db \
  --output /tmp/pglite-benchbase-results/post-run-validation.json
```

The validator exits nonzero on corruption, missing rows, unexpected field lengths/types, or a nonempty WAL that makes offline validation unsafe. `TextProbe.java` independently checks exact JDBC string round-tripping against an isolated server; see [FIXES.md](FIXES.md).
