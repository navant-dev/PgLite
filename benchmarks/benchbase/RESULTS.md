# Modified BenchBase results — 2026-10-15

**Historical failing baseline; see [FIXES.md](FIXES.md) for the corrected server and rerun.**

**Diagnostic results only: the run exposed a text data-preservation bug and mixed-workload write failures. These are not correctness-qualified performance scores.** The server implementation was not changed for this benchmark.

Actual CMU BenchBase YCSB, pinned revision `33c00473807ebd49304d114a6d769d2d2b2bbb34`, with the setup adapter described in [README.md](README.md). PostgreSQL JDBC 42.7.4; Temurin Java 25.0.4.1+1, compiled for Java 23. PgLite commit `f0e789580985c6bb71cef1c77700f3256a2982bd`, optimized binary.

Host: Intel Core i5-8257U, eight logical CPUs, 8 GiB RAM, macOS 15.7.7. Client and server share the machine. 100,000 rows with ten initially 100-character fields; database file 468,131,840 bytes (446.45 MiB). Default 2 MiB cache per connection, WAL/FULL durability, 32-client cap, 64 MiB WAL soft limit. Warm OS cache; the dataset does not exceed host RAM.

## Observations

| Workload | Clients | Measured attempts/sec | p50 ms | p95 ms | p99 ms | Failed attempts, including warmup |
|---|---:|---:|---:|---:|---:|---:|
| Point reads | 1 | 3,369 | 0.257 | 0.560 | 0.816 | 0 |
| Point reads | 8 | 8,043 | 0.963 | 1.369 | 1.632 | 0 |
| Point reads | 32 | 8,304 | 3.811 | 5.184 | 5.891 | 0 |
| 95% reads / 5% updates | 8 | 7,793 | 0.976 | 1.433 | 1.981 | 201 |
| Primary-key range scans | 8 | 219 | 35.207 | 71.031 | 85.043 | 0 |

Each workload ran once with 10 seconds configured warmup and 30 seconds configured measurement. Actual measurement times were about 30–31 seconds, as recorded in the summaries. Scans use a random key interval of 1–1,000; they are substantially larger operations than point reads. These workloads do not test secondary indexes. No timed transaction retries were enabled.

At 32 clients, read throughput increased only about 3% over eight clients while latency rose substantially. This suggests saturation near eight clients for this host and workload; it does not isolate the server from JDBC/client CPU overhead.

## Failures and correctness

The mixed run completed 293,781 reads and 15,330 updates across warmup and measurement. Another 201 updates failed: 189 `55P03` lock errors and 12 classified as retryable rejections by BenchBase. The latter SQLSTATEs are not present in its default log, so no more specific classification is asserted. With one allowed attempt, these rejections were not retried. That is approximately **1.29% of attempted updates** across the full run. Measurement-only success throughput cannot be recovered exactly from the upstream raw CSV, which omits transaction status. The table therefore reports attempts/sec, not successful commits/sec.

After clean server shutdown, SQLite `integrity_check` returned `ok`, and all 100,000 keys remained present (0–99,999). However, **16,639 rows contained at least one text field shorter than the supplied 100 characters**. Structural database integrity does not imply correct values.

A standalone PostgreSQL JDBC probe reproduces the defect:

```text
Bound input: a''b   (hex 61272762)
Stored/read: a'b    (hex 612762)
```

`src/wire.rs` decodes text parameters with `Portal::parameter::<String>`. In pgwire 0.41.1, the text decoder calls `parse_string_postgres`, which replaces doubled apostrophes with a single apostrophe. Bound parameter bytes must not undergo SQL-literal unescaping. This explains a concrete data-preservation failure on this JDBC path. The probe is [TextProbe.java](TextProbe.java), and its failing output is [results/text-probe.txt](results/text-probe.txt). This is the pre-fix observation; the correction and fresh-dataset rerun are recorded in [FIXES.md](FIXES.md).

Run the probe against a fresh isolated PgLite instance on port 55433:

```sh
java --class-path "$BENCHBASE_DIR/target/benchbase-postgres/benchbase-postgres/lib/postgresql-42.7.4.jar" \
  benchmarks/benchbase/TextProbe.java
```

Expected outcome on the original server: nonzero exit with an assertion failure. Other probe cases, including backslashes and empty strings, passed.

## Artifacts and caveats

[results/summary.json](results/summary.json) contains extracted measurements. Each workload directory contains the original summary, config, latency/throughput CSVs, histograms, log, and gzip-compressed raw samples. [results/environment.json](results/environment.json) records the server binary SHA-256 and settings; [results/post-run-validation.json](results/post-run-validation.json) records the failed field-length check.

Upstream's `Goodput` includes warmup completions divided by measurement time; it is intentionally not used. Its PostgreSQL parameter collector logs an unsupported metadata query after each run and leaves DBMS version null. That collection error is separate from transaction failures. The `POSTGRES` label in raw summaries identifies the adapter, not the storage engine.

The one- and eight-client runs preceded the adapter fix that closes the initialization connection before starting all workers. Their timed workload code is identical to the later runs. Setup failures during adaptation are excluded from the measurement table. There is no independent PostgreSQL baseline, repeated-trial confidence interval, cold-cache test, or larger-than-RAM result here.
