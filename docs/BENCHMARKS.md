# Local verification and performance

These are smoke measurements on the development host (x86_64 macOS 15.7.7), not production capacity guarantees. The full final test output is saved in [verification.txt](verification.txt).

## Method

- Release build, bundled SQLite 3.51.1, WAL mode, synchronous=FULL.
- 100,000 rows with a 512-byte text payload, a primary index, and a composite secondary index on `(tenant, id)`.
- Database size: 59,617,280 bytes; SQLite cache target: 2,048 KiB per connection.
- Network runs launch the actual server executable with its default two async workers, eight maximum blocking workers, and 32-client limit. Clients use tokio-postgres over loopback.
- Point reads select one payload by primary key. Secondary-index ranges select 20 IDs for a tenant in ID order.
- Mixed workload: eight clients, 4,000 total operations, 95% secondary-index reads and 5% autocommit writes. Successful writes and conflicts are counted separately.
- Embedded baselines call SQLite or the engine directly, without the network. The eight-writer embedded stress intentionally bypasses server writer admission to expose the underlying single-writer limitation.
- Throughput includes connection/preparation setup; query percentiles time each request. Mixed-workload operation rates include failed requests, if any; always read the write outcome counts alongside the rate.

The dataset exceeds each connection's SQLite cache but does **not** exceed host RAM. The OS page cache is warm. Cold-cache, larger-than-RAM, PostgreSQL comparison, minimal-wire-server comparison, long-duration soak, and power-loss fault-injection gates remain open.

## Writer admission decision

The first actual-server mixed run, before writer admission, committed 147 of 200 writes; 53 returned contention errors. This justified adding a FIFO asynchronous writer queue. The queue is bounded by accepted connections, has a timeout, supports cancellation, and holds a writer's permit through an explicit transaction. It does not replay transactions or refresh stale snapshots.

Regression tests verify successful concurrent autocommit writes, cancellation while queued, COMMIT making progress while another writer waits, and `40001` for a stale snapshot. The final mixed run committed all 200 writes with zero contention errors. This demonstrates the queue fix on this workload; it does not eliminate snapshot conflicts or the backend’s one-writer throughput ceiling.


## Final results

| Workload | Operations/sec | p50 (µs) | p95 (µs) | p99 (µs) |
|---|---:|---:|---:|---:|
| Embedded SQLite point read | 127,151 | 7 | 9 | 12 |
| Engine prepared point read | 88,464 | 10 | 12 | 28 |
| Server point read, 1 client | 8,226 | 113 | 151 | 205 |
| Server point read, 8 clients | 21,149 | 350 | 539 | 647 |
| Server point read, 32 clients | 23,754 | 1,270 | 1,869 | 2,247 |
| Server secondary range, 8 clients | 20,160 | 363 | 550 | 660 |
| Server 95% read / 5% write, 8 clients | 18,924 | 387 | 635 | 805 |

Mixed workload: **200/200 writes committed, zero contention errors**, without transaction retries. The separate direct-engine writer stress, which deliberately bypasses admission, committed 31/800 attempts; 743 encountered a busy writer and 26 encountered snapshot conflicts.

The final stripped executable is **5,611,184 bytes** (5.35 MiB). The deployment test measured **346 ms** from process launch to readiness and **3,204 KiB** of idle resident memory. These are single local observations; startup, RSS, and throughput vary with hardware, OS caching, and host load. The earlier and final runs are not a controlled throughput comparison.

The complete optimized run passed **25 tests**, including the explicitly enabled performance test. Formatting and Clippy with warnings denied also passed. The test total includes the subprocess crash helper. The CI workflow is authored but has not been run on GitHub/Linux.

## Subsequent BenchBase validation

The longer [modified BenchBase run](../benchmarks/benchbase/RESULTS.md) found mixed-workload write failures and a JDBC text-parameter preservation bug. The earlier short smoke measurements above do not establish correctness or reliability for this workload. See that report for the reproducer, raw results, and limitations.

Those defects are now fixed. The [fresh BenchBase rerun](../benchmarks/benchbase/FIXES.md) completed all workloads with zero transaction failures and validated all one million text fields. The original numbers remain above as historical observations.
