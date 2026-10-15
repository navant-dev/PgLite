# BenchBase bug fixes and verification — 2026-10-15

The fresh-dataset rerun completed every measured workload without transaction failures. All 100,000 rows and one million text fields passed offline validation. The JDBC probe now preserves consecutive apostrophes exactly.

## Changes

1. **Preserve bound text bytes.** Text and binary TEXT/VARCHAR parameters are decoded directly as UTF-8, preserving apostrophes, backslashes, whitespace, empty strings, and Unicode. NULL stays NULL; malformed UTF-8 returns `22021`. SQL literals continue to use normal SQL quoting. This bypasses pgwire 0.41.1's SQL-literal unescaping in its text-format String decoder.
2. **Coordinate checkpoints with writers.** Background checkpoint/truncation now acquires the same FIFO admission permit as writes. The permit remains owned by the blocking checkpoint job even if the async caller is cancelled. Checkpoints no longer compete with server writes for SQLite's writer lock.
3. **Admit transactional write planning before catalog reads.** Preparing a write inside an explicit transaction now obtains writer admission before reading schema metadata, which can establish a SQLite snapshot. This prevents a first write from using an accidentally stale pre-admission snapshot. Its permit is retained until transaction completion. Already-established read snapshots are never refreshed: genuine stale read/write conflicts still return `40001`.

Preparing a write inside an explicit transaction can now make later writers wait even before that prepared statement executes. Keep these transactions short; the existing transaction timeout bounds the wait. Preparing outside an explicit transaction does not retain writer admission.

No transaction replay, relaxed synchronization, increased cache/WAL limits, or benchmark data sanitization was used. The server remains WAL/FULL with its original resource limits.

## Regression coverage

- The new text-format wire regression first failed against the old decoder (`a''b` became `a'b`). It now covers exact text/binary round trips, NULL, Unicode, invalid UTF-8, and SQL-literal quoting.
- The writer test exercises the JDBC-style `BEGIN → Parse UPDATE → Execute → COMMIT` sequence with a competing writer, then verifies that a genuine stale snapshot still fails with `40001`.
- The checkpoint regression holds an active SQLite writer plus its admission permit, verifies that checkpointing waits, then commits and checks that WAL is reclaimed without losing data.
- Existing cancellation, timeout, CRUD/index, schema invalidation, crash recovery, and performance tests remain in the complete optimized suite.

The final optimized suite passes **27 tests**, including the opt-in performance test. See [regression-tests.txt](results-fixed/regression-tests.txt). Formatting and Clippy with warnings denied also pass.

## Fresh BenchBase rerun

Same pinned BenchBase adapter and workload configuration as the [original failing run](RESULTS.md): 100,000 rows, ten 100-character fields, 10-second warmup, 30-second configured measurement, no timed retries. Dataset rebuilt from source, not reused from the corrupted run. Host and configuration details are in [environment.json](results-fixed/environment.json).

| Workload | Clients | Measured transactions/sec | p50 ms | p95 ms | p99 ms | Failures including warmup |
|---|---:|---:|---:|---:|---:|---:|
| Point reads | 1 | 3,049 | 0.256 | 0.751 | 0.920 | 0 |
| Point reads | 8 | 8,145 | 0.946 | 1.374 | 1.693 | 0 |
| Point reads | 32 | 8,269 | 3.826 | 5.190 | 5.935 | 0 |
| 95% reads / 5% updates | 8 | 7,957 | 0.949 | 1.388 | 1.883 | 0 |
| Primary-key range scans | 8 | 222 | 35.357 | 69.416 | 84.864 | 0 |

The mixed run completed **301,915 reads and 15,788 updates** across warmup and measurement, with zero rejected/aborted/error attempts. Previously, 201 update attempts failed. Measurement-only counts and actual elapsed time are in each summary; the warmup-inclusive upstream `Goodput` field remains excluded. These single local runs are validation observations, not a controlled performance improvement claim. They remain warm-cache, below host RAM, and do not test secondary indexes.

The upstream PostgreSQL metadata collector still logs its known unsupported query after timing; this is unrelated to transaction outcomes and leaves DBMS version null. All outcome histograms, logs, summaries and compressed raw samples are retained under [results-fixed](results-fixed/summary.json).

## Data verification

After all workloads and the separate JDBC probe, the server was shut down cleanly. [Offline validation](results-fixed/post-run-validation.json) checked:

- SQLite integrity: `ok`.
- 100,000 rows, with keys 0 through 99,999.
- All ten fields of every row are non-NULL TEXT values of exactly 100 characters: **zero invalid rows**, versus 16,639 in the old run.
- Database size: 468,131,840 bytes.

The independent [JDBC probe output](results-fixed/text-probe.txt) shows all seven exact-byte comparisons passing, including `a''b` (`61272762` in both directions). Field-length checks are supplemented by exact-value wire regressions and this JDBC probe; length alone is not a proof of arbitrary-value correctness.

The fixes prevent future corruption; they cannot reconstruct apostrophes already removed by the old decoder. Previously affected values must be reloaded from their original source. The old benchmark database and reports were retained as the failing baseline.
