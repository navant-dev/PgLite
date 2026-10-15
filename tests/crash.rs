use pglite_server::engine::{self, Config, Session};
use std::{process::Command, sync::Arc};
#[test]
fn crash_child() {
    let Ok(path) = std::env::var("PGLITE_CRASH_TEST_PATH") else {
        return;
    };
    let config = Arc::new(Config::new(path));
    let mut s = Session::open(config).unwrap();
    s.execute("INSERT INTO t VALUES(1)", &[], &[]).unwrap();
    s.execute("BEGIN; INSERT INTO t VALUES(2)", &[], &[])
        .unwrap();
    std::process::exit(77);
}
#[test]
fn committed_wal_survives_abrupt_exit_and_uncommitted_is_discarded() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("crash.db");
    let config = Arc::new(Config::new(&path));
    engine::initialize(&config).unwrap();
    let mut keeper = Session::open(config.clone()).unwrap();
    keeper
        .execute("CREATE TABLE t (id INT PRIMARY KEY)", &[], &[])
        .unwrap();
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child", "--nocapture"])
        .env("PGLITE_CRASH_TEST_PATH", &path)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(77));
    let mut recovered = Session::open(config).unwrap();
    let rows = recovered
        .execute("SELECT id FROM t", &[], &[])
        .unwrap()
        .remove(0)
        .rows;
    assert_eq!(rows, vec![vec![rusqlite::types::Value::Integer(1)]]);
    assert_eq!(
        recovered
            .conn
            .query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
}
