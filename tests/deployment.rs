use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};
struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[test]
fn starts_quickly_and_prevents_second_owner() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.db");
    let binary = env!("CARGO_BIN_EXE_pglite-server");
    let start = Instant::now();
    let mut server = Server(
        Command::new(binary)
            .args(["--data", path.to_str().unwrap(), "--listen", "127.0.0.1:0"])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let out = server.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        BufReader::new(out).read_line(&mut line).unwrap();
        let _ = tx.send(line);
    });
    let ready = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(ready.starts_with("ready addr=127.0.0.1:"), "{ready}");
    println!(
        "ready_ms={} binary_bytes={}",
        start.elapsed().as_millis(),
        std::fs::metadata(binary).unwrap().len()
    );
    #[cfg(unix)]
    if let Ok(output) = Command::new("ps")
        .args(["-o", "rss=", "-p", &server.0.id().to_string()])
        .output()
    {
        println!(
            "idle_rss_kib={}",
            String::from_utf8_lossy(&output.stdout).trim()
        );
    }
    let second = Command::new(binary)
        .args(["--data", path.to_str().unwrap(), "--listen", "127.0.0.1:0"])
        .output()
        .unwrap();
    assert!(!second.status.success());
    assert!(String::from_utf8_lossy(&second.stderr).contains("another server owns"));
}
#[test]
fn remote_listener_is_rejected() {
    let result = Command::new(env!("CARGO_BIN_EXE_pglite-server"))
        .args(["--listen", "0.0.0.0:5433"])
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("loopback"));
}
#[tokio::test]
async fn transaction_expiry_closes_connection_and_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("expiry.db");
    let mut server = Server(
        Command::new(env!("CARGO_BIN_EXE_pglite-server"))
            .args([
                "--data",
                path.to_str().unwrap(),
                "--listen",
                "127.0.0.1:0",
                "--transaction-timeout-ms",
                "100",
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
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test sslmode=disable",
            addr.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    let connection = tokio::spawn(connection);
    client
        .batch_execute("CREATE TABLE t (id INT PRIMARY KEY); BEGIN; INSERT INTO t VALUES(1)")
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(3), connection)
        .await
        .unwrap();
    assert!(client.is_closed());
    let conn = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM t", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}
