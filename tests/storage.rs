use pglite_server::{
    engine::{self, Config, Session, Tx},
    sql,
};
use rusqlite::types::Value as V;
use std::{sync::Arc, time::Duration};
fn setup() -> (tempfile::TempDir, Arc<Config>) {
    let d = tempfile::tempdir().unwrap();
    let c = Arc::new(Config::new(d.path().join("test.db")));
    engine::initialize(&c).unwrap();
    (d, c)
}
fn exec(s: &mut Session, q: &str) {
    s.execute(q, &[], &[]).unwrap();
}
fn rows(s: &mut Session, q: &str) -> Vec<Vec<V>> {
    s.execute(q, &[], &[]).unwrap().remove(0).rows
}
#[test]
fn crud_types_and_indexes() {
    let (_d, c) = setup();
    let mut s = Session::open(c).unwrap();
    exec(
        &mut s,
        "CREATE TABLE items (id BIGINT PRIMARY KEY, tenant INT NOT NULL, name TEXT UNIQUE, enabled BOOLEAN)",
    );
    exec(
        &mut s,
        "CREATE INDEX items_tenant_name ON items (tenant, name)",
    );
    exec(
        &mut s,
        "INSERT INTO items VALUES (1, 3, 'a', true), (2, 3, 'b', false), (3, 4, NULL, NULL)",
    );
    assert_eq!(
        rows(
            &mut s,
            "SELECT name FROM items WHERE tenant = 3 ORDER BY name"
        ),
        vec![vec![V::Text("a".into())], vec![V::Text("b".into())]]
    );
    assert_eq!(
        rows(
            &mut s,
            "SELECT id FROM items WHERE name IS NOT NULL ORDER BY id"
        )
        .len(),
        2
    );
    exec(
        &mut s,
        "UPDATE items SET name = 'new', enabled = false WHERE id = 1",
    );
    assert_eq!(
        rows(&mut s, "SELECT enabled FROM items WHERE name = 'new'"),
        vec![vec![V::Integer(0)]]
    );
    exec(&mut s, "DELETE FROM items WHERE id IN (1, 2)");
    assert_eq!(rows(&mut s, "SELECT * FROM items").len(), 1);
    let p = sql::plan(
        &s,
        &sql::parse("SELECT name FROM items WHERE tenant=3 ORDER BY name").unwrap()[0],
        &[],
    )
    .unwrap();
    let explain: String = s
        .conn
        .query_row(&format!("EXPLAIN QUERY PLAN {}", p.sql), [], |r| r.get(3))
        .unwrap();
    assert!(explain.contains("items_tenant_name"), "{explain}");
}
#[test]
fn snapshot_sees_old_rows_and_index_keys() {
    let (_d, c) = setup();
    let mut a = Session::open(c.clone()).unwrap();
    let mut b = Session::open(c).unwrap();
    exec(&mut a, "CREATE TABLE t (id BIGINT PRIMARY KEY, value TEXT)");
    exec(&mut a, "CREATE INDEX t_value ON t (value)");
    exec(&mut a, "INSERT INTO t VALUES (1,'old')");
    exec(&mut a, "BEGIN");
    assert_eq!(rows(&mut a, "SELECT id FROM t WHERE value='old'").len(), 1);
    exec(&mut b, "UPDATE t SET value='new' WHERE id=1");
    exec(&mut b, "INSERT INTO t VALUES (2,'old')");
    assert_eq!(
        rows(&mut a, "SELECT id FROM t WHERE value='old'"),
        vec![vec![V::Integer(1)]]
    );
    assert!(rows(&mut a, "SELECT id FROM t WHERE value='new'").is_empty());
    let e = a
        .execute("UPDATE t SET value='conflict' WHERE id=1", &[], &[])
        .unwrap_err();
    assert_eq!(e.code, "40001");
    assert_eq!(a.tx, Tx::Failed);
    assert_eq!(
        a.execute("SELECT * FROM t", &[], &[]).unwrap_err().code,
        "25P02"
    );
    exec(&mut a, "ROLLBACK");
    assert_eq!(
        rows(&mut a, "SELECT id FROM t WHERE value='new'"),
        vec![vec![V::Integer(1)]]
    );
}
#[test]
fn atomic_batches_constraints_and_rollback() {
    let (_d, c) = setup();
    let mut s = Session::open(c).unwrap();
    exec(
        &mut s,
        "CREATE TABLE t (id INT PRIMARY KEY, value TEXT UNIQUE)",
    );
    assert!(
        s.execute(
            "INSERT INTO t VALUES (1,'a'); INSERT INTO t VALUES (2,'a')",
            &[],
            &[]
        )
        .is_err()
    );
    assert!(rows(&mut s, "SELECT * FROM t").is_empty());
    assert_eq!(
        s.execute("INSERT INTO t VALUES (NULL,'a')", &[], &[])
            .unwrap_err()
            .code,
        "23502"
    );
    assert!(
        s.execute("INSERT INTO t VALUES (2147483648,'a')", &[], &[])
            .is_err()
    );
    exec(&mut s, "BEGIN");
    exec(&mut s, "INSERT INTO t VALUES (1,'a')");
    assert_eq!(rows(&mut s, "SELECT * FROM t").len(), 1);
    assert!(s.execute("INSERT INTO t VALUES (1,'b')", &[], &[]).is_err());
    assert_eq!(s.execute("COMMIT", &[], &[]).unwrap()[0].tag, "ROLLBACK");
    assert!(rows(&mut s, "SELECT * FROM t").is_empty());
}
#[test]
fn disk_reopen_and_ddl_rollback() {
    let (_d, c) = setup();
    let mut s = Session::open(c.clone()).unwrap();
    exec(&mut s, "BEGIN");
    exec(&mut s, "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    exec(&mut s, "ROLLBACK");
    assert!(s.table("t").is_err());
    exec(&mut s, "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    exec(&mut s, "INSERT INTO t VALUES (9)");
    drop(s);
    let mut s = Session::open(c).unwrap();
    assert_eq!(rows(&mut s, "SELECT id FROM t"), vec![vec![V::Integer(9)]]);
}
#[test]
fn limits_and_expiry() {
    let (_d, c) = setup();
    let mut c = (*c).clone();
    c.transaction_timeout = Duration::from_millis(1);
    c.max_result_bytes = 1024;
    let mut s = Session::open(Arc::new(c)).unwrap();
    exec(&mut s, "BEGIN");
    std::thread::sleep(Duration::from_millis(5));
    assert!(s.expire());
    assert_eq!(s.execute("SELECT 1", &[], &[]).unwrap_err().code, "25P02");
    exec(&mut s, "ROLLBACK");
    assert_eq!(rows(&mut s, "SELECT 1"), vec![vec![V::Integer(1)]]);
}
#[test]
fn rejected_sql_does_not_escape_frontend() {
    let (_d, c) = setup();
    let mut s = Session::open(c).unwrap();
    for query in [
        "PRAGMA synchronous=OFF",
        "ATTACH DATABASE '/tmp/escape.db' AS external",
        "SELECT * FROM sqlite_master",
        "SELECT * FROM _pglite_catalog",
        "SELECT 1.5",
        "SELECT 1+2",
        "BEGIN ISOLATION LEVEL SERIALIZABLE",
    ] {
        assert!(s.execute(query, &[], &[]).is_err(), "{query}");
    }
}
#[test]
fn unsupported_options_and_negative_pagination() {
    let (_d, c) = setup();
    let mut s = Session::open(c).unwrap();
    exec(&mut s, "CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT)");
    for q in [
        "CREATE TABLE other (id INT PRIMARY KEY) WITH (fillfactor=50)",
        "CREATE UNIQUE INDEX bad ON t (name) NULLS NOT DISTINCT",
        "SELECT * FROM t FOR UPDATE",
        "SELECT * FROM t LIMIT -1",
        "SELECT * FROM t OFFSET -2",
        "INSERT INTO t VALUES(1,'a') ON CONFLICT DO NOTHING",
    ] {
        assert!(s.execute(q, &[], &[]).is_err(), "{q}");
    }
    assert!(
        s.execute("SELECT * FROM t LIMIT $1", &[V::Integer(-1)], &[])
            .is_err()
    );
}
#[test]
fn writer_conflict_and_no_dirty_reads() {
    let (_d, c) = setup();
    let mut a = Session::open(c.clone()).unwrap();
    let mut b = Session::open(c).unwrap();
    exec(&mut a, "CREATE TABLE t (id INT PRIMARY KEY)");
    exec(&mut a, "BEGIN");
    exec(&mut a, "INSERT INTO t VALUES(1)");
    assert!(rows(&mut b, "SELECT * FROM t").is_empty());
    assert_eq!(
        b.execute("INSERT INTO t VALUES(2)", &[], &[])
            .unwrap_err()
            .code,
        "55P03"
    );
    exec(&mut a, "ROLLBACK");
    exec(&mut b, "INSERT INTO t VALUES(2)");
}
#[test]
fn result_budget_rolls_back_batch() {
    let (_d, c) = setup();
    let mut s = Session::open(c.clone()).unwrap();
    exec(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)");
    let mut c = (*c).clone();
    c.max_result_bytes = 128;
    let mut s = Session::open(Arc::new(c)).unwrap();
    let result=s.execute("INSERT INTO t VALUES (1,'abcdefghijklmnopqrstuvwxyz'),(2,'abcdefghijklmnopqrstuvwxyz'); SELECT * FROM t; SELECT * FROM t",&[],&[]);
    assert!(result.is_err());
    assert!(rows(&mut s, "SELECT * FROM t").is_empty());
}
#[test]
fn randomized_index_and_table_scan_agree() {
    let (_d, c) = setup();
    let mut s = Session::open(c).unwrap();
    exec(&mut s, "CREATE TABLE t (id BIGINT PRIMARY KEY, k INT)");
    exec(&mut s, "CREATE INDEX t_k ON t(k)");
    let mut seed = 12345_u64;
    for _ in 0..300 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let id = (seed % 50) as i64;
        let k = ((seed >> 16) % 7) as i64;
        match seed % 3 {
            0 => {
                let _ = s.execute(&format!("INSERT INTO t VALUES({id},{k})"), &[], &[]);
            }
            1 => exec(&mut s, &format!("UPDATE t SET k={k} WHERE id={id}")),
            _ => exec(&mut s, &format!("DELETE FROM t WHERE id={id}")),
        }
        let indexed = rows(&mut s, &format!("SELECT id FROM t WHERE k={k} ORDER BY id"));
        let mut stmt = s
            .conn
            .prepare("SELECT id FROM t NOT INDEXED WHERE k=? ORDER BY id")
            .unwrap();
        let raw = stmt
            .query_map([k], |r| Ok(vec![V::Integer(r.get(0)?)]))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(indexed, raw);
    }
}
#[test]
fn cached_plan_uses_snapshot_and_invalidates_after_ddl() {
    let (_d, c) = setup();
    let mut a = Session::open(c.clone()).unwrap();
    let mut b = Session::open(c).unwrap();
    exec(&mut a, "CREATE TABLE t (id INT PRIMARY KEY, value TEXT)");
    exec(&mut a, "INSERT INTO t VALUES(1,'old')");
    let p = sql::plan(
        &a,
        &sql::parse("SELECT value FROM t WHERE id=$1").unwrap()[0],
        &[],
    )
    .unwrap();
    let version = a.schema_version().unwrap();
    exec(&mut a, "BEGIN");
    assert_eq!(
        a.execute_cached(&p, version, &[V::Integer(1)])
            .unwrap()
            .rows[0][0],
        V::Text("old".into())
    );
    exec(&mut b, "UPDATE t SET value='new' WHERE id=1");
    assert_eq!(
        a.execute_cached(&p, version, &[V::Integer(1)])
            .unwrap()
            .rows[0][0],
        V::Text("old".into())
    );
    exec(&mut a, "ROLLBACK");
    exec(&mut b, "DROP TABLE t");
    assert_eq!(
        a.execute_cached(&p, version, &[V::Integer(1)])
            .unwrap_err()
            .code,
        "0A000"
    );
}
#[test]
fn wal_pressure_recovers_after_checkpoint() {
    let (_d, c) = setup();
    let mut c = (*c).clone();
    c.max_wal_bytes = 1024 * 1024;
    let c = Arc::new(c);
    let mut s = Session::open(c.clone()).unwrap();
    exec(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, value TEXT)");
    s.conn
        .execute("INSERT INTO t VALUES (1,?)", ["x".repeat(2 * 1024 * 1024)])
        .unwrap();
    assert_eq!(
        s.execute("INSERT INTO t VALUES(2,'small')", &[], &[])
            .unwrap_err()
            .code,
        "53100"
    );
    engine::checkpoint(&c.path, c.max_wal_bytes).unwrap();
    exec(&mut s, "INSERT INTO t VALUES(2,'small')");
}
#[test]
fn savepoint_rollback_is_not_silently_treated_as_full_rollback() {
    let (_d, c) = setup();
    let mut s = Session::open(c).unwrap();
    exec(&mut s, "BEGIN");
    assert_eq!(
        s.execute("ROLLBACK TO SAVEPOINT missing", &[], &[])
            .unwrap_err()
            .code,
        "0A000"
    );
    assert_eq!(s.tx, Tx::Failed);
    exec(&mut s, "ROLLBACK");
}
#[test]
fn rolled_back_schema_cookie_reuse_cannot_revive_stale_plan() {
    let (_d, c) = setup();
    let mut s = Session::open(c).unwrap();
    exec(&mut s, "BEGIN");
    exec(&mut s, "CREATE TABLE t (id INT PRIMARY KEY)");
    let p = sql::plan(&s, &sql::parse("INSERT INTO t VALUES($1)").unwrap()[0], &[]).unwrap();
    let v = s.schema_version().unwrap();
    exec(&mut s, "ROLLBACK");
    exec(&mut s, "CREATE TABLE t (id TEXT PRIMARY KEY)");
    assert_eq!(
        s.execute_cached(&p, v, &[V::Integer(1)]).unwrap_err().code,
        "0A000"
    );
    assert!(rows(&mut s, "SELECT * FROM t").is_empty());
}

#[tokio::test]
async fn checkpoint_waits_for_writer_and_reclaims_wal() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = pglite_server::engine::Config::new(dir.path().join("checkpoint-queue.db"));
    config.max_wal_bytes = 1024 * 1024;
    let config = std::sync::Arc::new(config);
    pglite_server::engine::initialize(&config).unwrap();
    let session = pglite_server::engine::Session::open(config.clone()).unwrap();
    let permit = config.writer.clone().acquire_owned().await.unwrap();
    session.conn.execute_batch("BEGIN IMMEDIATE; CREATE TABLE checkpoint_test(id INTEGER PRIMARY KEY, body BLOB); INSERT INTO checkpoint_test VALUES(1,zeroblob(700000));").unwrap();
    let task = tokio::spawn(pglite_server::engine::checkpoint_admitted(config.clone()));
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    assert!(
        !task.is_finished(),
        "checkpoint must queue behind the active writer"
    );
    session.conn.execute_batch("COMMIT").unwrap();
    drop(permit);
    let (_, pages, done) = tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(pages, done);
    assert_eq!(
        std::fs::metadata(format!("{}-wal", config.path.display()))
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        session
            .conn
            .query_row("SELECT length(body) FROM checkpoint_test", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        700000
    );
}
