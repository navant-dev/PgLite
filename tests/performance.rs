//! Reproducible local smoke benchmark; not a production capacity claim.
use pglite_server::{
    engine::{self, Config, Session},
    sql,
};
use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
};
use std::{sync::Arc, time::Instant};
use tokio::task::JoinSet;
struct BenchServer(Child);
impl Drop for BenchServer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
use tokio_postgres::NoTls;
fn report(label: &str, mut samples: Vec<u128>, elapsed: f64) {
    samples.sort();
    println!(
        "{label}: n={} ops_s={:.0} p50_us={} p95_us={} p99_us={}",
        samples.len(),
        samples.len() as f64 / elapsed,
        samples[samples.len() / 2],
        samples[samples.len() * 95 / 100],
        samples[samples.len() * 99 / 100]
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "run explicitly in release mode"]
async fn disk_backed_read_and_write_smoke() {
    let d = tempfile::tempdir().unwrap();
    let config = Arc::new(Config::new(d.path().join("bench.db")));
    engine::initialize(&config).unwrap();
    let mut s = Session::open(config.clone()).unwrap();
    s.execute(
        "CREATE TABLE records(id BIGINT PRIMARY KEY, tenant INT, body TEXT)",
        &[],
        &[],
    )
    .unwrap();
    s.execute(
        "CREATE INDEX records_tenant ON records(tenant,id)",
        &[],
        &[],
    )
    .unwrap();
    s.conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = s
            .conn
            .prepare("INSERT INTO records VALUES(?1,?2,?3)")
            .unwrap();
        let body = "x".repeat(512);
        for id in 0..100_000_i64 {
            stmt.execute(rusqlite::params![id, id % 100, body]).unwrap();
        }
    }
    s.conn
        .execute_batch("COMMIT; PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    let bytes = std::fs::metadata(&config.path).unwrap().len();
    println!(
        "dataset_rows=100000 database_bytes={bytes} cache_kib_per_connection={} sqlite={}",
        config.cache_kib,
        rusqlite::version()
    );
    assert!(bytes > config.cache_kib as u64 * 1024 * 10);
    let mut samples = Vec::new();
    let start = Instant::now();
    {
        let mut stmt = s
            .conn
            .prepare_cached("SELECT body FROM records WHERE id=?1")
            .unwrap();
        for n in 0..4000 {
            let t = Instant::now();
            let _: String = stmt
                .query_row([(n * 7919) % 100_000], |r| r.get(0))
                .unwrap();
            samples.push(t.elapsed().as_micros());
        }
    }
    report(
        "embedded_sqlite_point",
        samples,
        start.elapsed().as_secs_f64(),
    );
    let plan = sql::plan(
        &s,
        &sql::parse("SELECT body FROM records WHERE id=$1").unwrap()[0],
        &[],
    )
    .unwrap();
    let version = s.schema_version().unwrap();
    let mut samples = Vec::new();
    let start = Instant::now();
    for n in 0..4000 {
        let t = Instant::now();
        s.execute_cached(
            &plan,
            version,
            &[rusqlite::types::Value::Integer((n * 7919) % 100_000)],
        )
        .unwrap();
        samples.push(t.elapsed().as_micros());
    }
    report(
        "engine_prepared_point",
        samples,
        start.elapsed().as_secs_f64(),
    );
    let mut server = BenchServer(
        Command::new(env!("CARGO_BIN_EXE_pglite-server"))
            .args([
                "--data",
                config.path.to_str().unwrap(),
                "--listen",
                "127.0.0.1:0",
            ])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut ready = String::new();
    BufReader::new(server.0.stdout.take().unwrap())
        .read_line(&mut ready)
        .unwrap();
    let addr: std::net::SocketAddr = ready
        .split_whitespace()
        .nth(1)
        .unwrap()
        .strip_prefix("addr=")
        .unwrap()
        .parse()
        .unwrap();
    for clients in [1, 8, 32] {
        let mut jobs = JoinSet::new();
        let start = Instant::now();
        for client in 0..clients {
            jobs.spawn(async move {
                let (c, connection) = tokio_postgres::connect(
                    &format!(
                        "host=127.0.0.1 port={} user=bench sslmode=disable",
                        addr.port()
                    ),
                    NoTls,
                )
                .await
                .unwrap();
                tokio::spawn(connection);
                let p = c
                    .prepare("SELECT body FROM records WHERE id=$1")
                    .await
                    .unwrap();
                let mut times = Vec::new();
                for n in 0..500_i64 {
                    let t = Instant::now();
                    c.query_one(&p, &[&((n * 7919 + client * 101) % 100_000)])
                        .await
                        .unwrap();
                    times.push(t.elapsed().as_micros());
                }
                times
            });
        }
        let mut samples = Vec::new();
        while let Some(r) = jobs.join_next().await {
            samples.extend(r.unwrap());
        }
        report(
            &format!("wire_point_clients_{clients}"),
            samples,
            start.elapsed().as_secs_f64(),
        );
    }
    for mixed in [false, true] {
        let mut jobs = JoinSet::new();
        let start = Instant::now();
        for client in 0..8_i64 {
            jobs.spawn(async move {
                let (c, connection) = tokio_postgres::connect(
                    &format!(
                        "host=127.0.0.1 port={} user=bench sslmode=disable",
                        addr.port()
                    ),
                    NoTls,
                )
                .await
                .unwrap();
                tokio::spawn(connection);
                let read = c
                    .prepare("SELECT id FROM records WHERE tenant=$1 ORDER BY id LIMIT 20")
                    .await
                    .unwrap();
                let write = c
                    .prepare("UPDATE records SET body=$1 WHERE id=$2")
                    .await
                    .unwrap();
                let (mut samples, mut committed, mut conflicts) = (Vec::new(), 0, 0);
                for n in 0..500_i64 {
                    let t = Instant::now();
                    if mixed && n % 20 == 0 {
                        match c.execute(&write, &[&"updated", &(client * 500 + n)]).await {
                            Ok(_) => committed += 1,
                            Err(e)
                                if e.code()
                                    .is_some_and(|c| matches!(c.code(), "40001" | "55P03")) =>
                            {
                                conflicts += 1
                            }
                            Err(e) => panic!("{e}"),
                        }
                    } else {
                        c.query(&read, &[&((n % 100) as i32)]).await.unwrap();
                    }
                    samples.push(t.elapsed().as_micros());
                }
                (samples, committed, conflicts)
            });
        }
        let (mut samples, mut committed, mut conflicts) = (Vec::new(), 0, 0);
        while let Some(r) = jobs.join_next().await {
            let (t, c, f) = r.unwrap();
            samples.extend(t);
            committed += c;
            conflicts += f;
        }
        let label = if mixed {
            "wire_secondary_95read_5write_clients8"
        } else {
            "wire_secondary_range_clients8"
        };
        report(label, samples, start.elapsed().as_secs_f64());
        if mixed {
            println!("mixed_write_committed={committed} mixed_write_conflicts={conflicts}");
        }
    }
    let mut jobs = JoinSet::new();
    let start = Instant::now();
    for client in 0..8 {
        let c = config.clone();
        jobs.spawn_blocking(move || {
            let mut s = Session::open(c).unwrap();
            let p = sql::plan(
                &s,
                &sql::parse("UPDATE records SET tenant=$1 WHERE id=$2").unwrap()[0],
                &[],
            )
            .unwrap();
            let v = s.schema_version().unwrap();
            let (mut ok, mut busy, mut conflict) = (0, 0, 0);
            for n in 0..100 {
                match s.execute_cached(
                    &p,
                    v,
                    &[
                        rusqlite::types::Value::Integer(7),
                        rusqlite::types::Value::Integer(client * 100 + n),
                    ],
                ) {
                    Ok(_) => ok += 1,
                    Err(e) if e.code == "55P03" => busy += 1,
                    Err(e) if e.code == "40001" => conflict += 1,
                    Err(e) => panic!("{e}"),
                }
            }
            (ok, busy, conflict)
        });
    }
    let (mut ok, mut busy, mut conflict) = (0, 0, 0);
    while let Some(r) = jobs.join_next().await {
        let (a, b, c) = r.unwrap();
        ok += a;
        busy += b;
        conflict += c;
    }
    println!(
        "embedded_eight_writers_without_admission: committed={ok} busy={busy} snapshot_conflicts={conflict} elapsed_ms={}",
        start.elapsed().as_millis()
    );
    drop(server);
}
