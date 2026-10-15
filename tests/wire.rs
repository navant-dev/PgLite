use pglite_server::{
    engine::{self, Config, Session},
    wire::{Backend, Handlers},
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;
#[tokio::test]
async fn driver_prepared_crud_transactions_and_binary_types() {
    let d = tempfile::tempdir().unwrap();
    let c = Arc::new(Config::new(d.path().join("wire.db")));
    engine::initialize(&c).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let backend = Backend::new(Session::open(c.clone()).unwrap());
            tokio::spawn(pgwire::tokio::process_socket(
                socket,
                None,
                Handlers(Arc::new(backend)),
            ));
        }
    });
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test sslmode=disable",
            addr.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    let conn = tokio::spawn(connection);
    client.batch_execute("CREATE TABLE t (id BIGINT PRIMARY KEY, n INT, active BOOLEAN, name TEXT, payload BYTEA)").await.unwrap();
    let insert = client
        .prepare("INSERT INTO t VALUES ($1,$2,$3,$4,$5)")
        .await
        .unwrap();
    assert_eq!(
        client
            .execute(
                &insert,
                &[&1_i64, &42_i32, &true, &"hello", &vec![0_u8, 1, 255]]
            )
            .await
            .unwrap(),
        1
    );
    let query = client
        .prepare("SELECT id,n,active,name,payload FROM t WHERE id=$1")
        .await
        .unwrap();
    let r = client.query_one(&query, &[&1_i64]).await.unwrap();
    assert_eq!(r.get::<_, i64>(0), 1);
    assert_eq!(r.get::<_, i32>(1), 42);
    assert!(r.get::<_, bool>(2));
    assert_eq!(r.get::<_, String>(3), "hello");
    assert_eq!(r.get::<_, Vec<u8>>(4), vec![0, 1, 255]);
    client.batch_execute("BEGIN").await.unwrap();
    client
        .execute("UPDATE t SET name=$1 WHERE id=$2", &[&"changed", &1_i64])
        .await
        .unwrap();
    assert!(
        client
            .execute(
                &insert,
                &[&1_i64, &42_i32, &true, &"duplicate", &vec![1_u8]]
            )
            .await
            .is_err()
    );
    assert_eq!(
        client
            .simple_query("SELECT * FROM t")
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "25P02"
    );
    client.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        client
            .query_one(&query, &[&1_i64])
            .await
            .unwrap()
            .get::<_, String>(3),
        "hello"
    );
    assert_eq!(
        client
            .execute("DELETE FROM t WHERE id=$1", &[&1_i64])
            .await
            .unwrap(),
        1
    );
    drop(client);
    conn.await.unwrap().unwrap();
    server.abort();
}
#[tokio::test]
async fn cursors_type_inference_and_schema_invalidation() {
    let d = tempfile::tempdir().unwrap();
    let c = Arc::new(Config::new(d.path().join("cursor.db")));
    engine::initialize(&c).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        pgwire::tokio::process_socket(
            socket,
            None,
            Handlers(Arc::new(Backend::new(Session::open(c).unwrap()))),
        )
        .await
        .unwrap();
    });
    let (mut client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test sslmode=disable",
            addr.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    let connection = tokio::spawn(connection);
    client
        .batch_execute("CREATE TABLE t (id INT PRIMARY KEY); INSERT INTO t VALUES(1),(2),(3)")
        .await
        .unwrap();
    let prepared = client
        .prepare("SELECT id FROM t ORDER BY id")
        .await
        .unwrap();
    let tx = client.transaction().await.unwrap();
    let portal = tx.bind(&prepared, &[]).await.unwrap();
    assert_eq!(
        tx.query_portal(&portal, 1).await.unwrap()[0].get::<_, i32>(0),
        1
    );
    assert_eq!(
        tx.query_portal(&portal, 1).await.unwrap()[0].get::<_, i32>(0),
        2
    );
    assert_eq!(
        tx.query_portal(&portal, 2).await.unwrap()[0].get::<_, i32>(0),
        3
    );
    tx.commit().await.unwrap();
    client
        .batch_execute("DROP TABLE t; CREATE TABLE t (id TEXT PRIMARY KEY)")
        .await
        .unwrap();
    assert_eq!(
        client
            .query(&prepared, &[])
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "0A000"
    );
    let typed = client
        .prepare_typed("SELECT $1", &[tokio_postgres::types::Type::INT4])
        .await
        .unwrap();
    let scalar = client.query_one(&typed, &[&7_i32]).await.unwrap();
    assert_eq!(scalar.get::<_, i32>(0), 7);
    drop(client);
    connection.await.unwrap().unwrap();
    server.await.unwrap();
}
#[tokio::test]
async fn fragmented_protocol_and_invalid_bind_recover_after_sync() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn read(stream: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            let tag = stream.read_u8().await.unwrap();
            let n = stream.read_u32().await.unwrap();
            assert!((4..100000).contains(&n));
            let mut data = vec![0; (n - 4) as usize];
            stream.read_exact(&mut data).await.unwrap();
            (tag, data)
        })
        .await
        .unwrap()
    }
    async fn send(stream: &mut tokio::net::TcpStream, tag: u8, body: &[u8]) {
        let mut msg = vec![tag];
        msg.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        msg.extend_from_slice(body);
        for chunk in msg.chunks(3) {
            stream.write_all(chunk).await.unwrap();
        }
    }
    let d = tempfile::tempdir().unwrap();
    let c = Arc::new(Config::new(d.path().join("raw.db")));
    engine::initialize(&c).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        pgwire::tokio::process_socket(
            socket,
            None,
            Handlers(Arc::new(Backend::new(Session::open(c).unwrap()))),
        )
        .await
        .unwrap();
    });
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    let body = b"\0\x03\0\0user\0test\0database\0test\0\0";
    stream
        .write_all(&((body.len() + 4) as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(body).await.unwrap();
    while read(&mut stream).await.0 != b'Z' {}
    send(&mut stream, b'P', b"stmt\0SELECT 1\0\0\0").await;
    assert_eq!(read(&mut stream).await.0, b'1');
    send(&mut stream, b'B', b"\0stmt\0\0\0\0\0\0\x02\0\0\0\0").await;
    let (tag, error) = read(&mut stream).await;
    assert_eq!(tag, b'E');
    assert!(String::from_utf8_lossy(&error).contains("08P01"));
    send(&mut stream, b'S', b"").await;
    assert_eq!(read(&mut stream).await.0, b'Z');
    send(&mut stream, b'Q', b"SELECT 1\0").await;
    let mut saw_row = false;
    loop {
        let (tag, _) = read(&mut stream).await;
        if tag == b'D' {
            saw_row = true;
        }
        if tag == b'Z' {
            break;
        }
        assert_ne!(tag, b'E');
    }
    assert!(saw_row);
    send(&mut stream, b'X', b"").await;
    drop(stream);
    server.await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_writers_do_not_starve_commit_and_stale_snapshots_still_fail() {
    let d = tempfile::tempdir().unwrap();
    let config = Arc::new(Config::new(d.path().join("writers.db")));
    engine::initialize(&config).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let backend = Backend::new(Session::open(config.clone()).unwrap());
            tokio::spawn(pgwire::tokio::process_socket(
                socket,
                None,
                Handlers(Arc::new(backend)),
            ));
        }
    });
    async fn client(addr: std::net::SocketAddr) -> tokio_postgres::Client {
        let (c, conn) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=test sslmode=disable",
                addr.port()
            ),
            NoTls,
        )
        .await
        .unwrap();
        tokio::spawn(conn);
        c
    }
    let a = client(addr).await;
    let b = client(addr).await;
    a.batch_execute(
        "CREATE TABLE t (id INT PRIMARY KEY, value TEXT); BEGIN; INSERT INTO t VALUES(1,'a')",
    )
    .await
    .unwrap();
    let pending = tokio::spawn(async move { b.execute("INSERT INTO t VALUES(2,'b')", &[]).await });
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    assert!(!pending.is_finished());
    a.batch_execute("COMMIT").await.unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        1
    );
    // Like JDBC before its prepare threshold: BEGIN, Parse UPDATE, then Execute.
    // Parse reads schema metadata and must not create a pre-admission snapshot.
    a.batch_execute("BEGIN").await.unwrap();
    let update = a.prepare("UPDATE t SET value=$1 WHERE id=1").await.unwrap();
    let other = client(addr).await;
    let pending = tokio::spawn(async move {
        other
            .execute("UPDATE t SET value='other' WHERE id=2", &[])
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    assert!(
        !pending.is_finished(),
        "transactional write planning must retain admission"
    );
    a.execute(&update, &[&"prepared"]).await.unwrap();
    a.batch_execute("COMMIT").await.unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        1
    );
    a.batch_execute("BEGIN; SELECT * FROM t").await.unwrap();
    let c = client(addr).await;
    c.execute("UPDATE t SET value='new' WHERE id=1", &[])
        .await
        .unwrap();
    assert_eq!(
        a.execute("UPDATE t SET value='stale' WHERE id=1", &[])
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "40001"
    );
    a.batch_execute("ROLLBACK").await.unwrap();
    a.batch_execute("BEGIN; UPDATE t SET value='holding' WHERE id=1")
        .await
        .unwrap();
    let waiting = client(addr).await;
    let cancel = waiting.cancel_token();
    let queued = tokio::spawn(async move {
        waiting
            .execute("INSERT INTO t VALUES(3,'cancelled')", &[])
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    cancel.cancel_query(NoTls).await.unwrap();
    let error = tokio::time::timeout(std::time::Duration::from_secs(2), queued)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "57014");
    a.batch_execute("ROLLBACK").await.unwrap();
    let mut writers = tokio::task::JoinSet::new();
    for n in 0..8 {
        writers.spawn(async move {
            let c = client(addr).await;
            let p = c.prepare("INSERT INTO t VALUES($1,$2)").await.unwrap();
            for i in 0..20 {
                c.execute(&p, &[&(100 + n * 20 + i), &"queued"])
                    .await
                    .unwrap();
            }
        });
    }
    while let Some(result) = writers.join_next().await {
        result.unwrap();
    }
    server.abort();
}

#[derive(Debug)]
struct TextFormat<'a>(&'a [u8]);
impl tokio_postgres::types::ToSql for TextFormat<'_> {
    fn to_sql(
        &self,
        _ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(self.0);
        Ok(tokio_postgres::types::IsNull::No)
    }
    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        matches!(
            *ty,
            tokio_postgres::types::Type::TEXT | tokio_postgres::types::Type::VARCHAR
        )
    }
    fn encode_format(&self, _ty: &tokio_postgres::types::Type) -> tokio_postgres::types::Format {
        tokio_postgres::types::Format::Text
    }
    tokio_postgres::types::to_sql_checked!();
}

#[tokio::test]
async fn bound_text_preserves_bytes_in_both_wire_formats() {
    let d = tempfile::tempdir().unwrap();
    let config = Arc::new(Config::new(d.path().join("text.db")));
    engine::initialize(&config).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        pgwire::tokio::process_socket(
            socket,
            None,
            Handlers(Arc::new(Backend::new(Session::open(config).unwrap()))),
        )
        .await
        .unwrap();
    });
    let (c, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test sslmode=disable",
            addr.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    let conn = tokio::spawn(conn);
    c.batch_execute("CREATE TABLE t(id INT PRIMARY KEY, body TEXT); INSERT INTO t VALUES(1,NULL)")
        .await
        .unwrap();
    let update = c.prepare("UPDATE t SET body=$1 WHERE id=1").await.unwrap();
    for value in [
        "a''b",
        "''''",
        "'quoted'",
        "a\\b\\nc",
        "",
        " \tline\n",
        "こんにちは🙂",
    ] {
        c.execute(&update, &[&TextFormat(value.as_bytes())])
            .await
            .unwrap();
        assert_eq!(
            c.query_one("SELECT body FROM t WHERE id=1", &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            value
        );
        c.execute(&update, &[&value]).await.unwrap();
        assert_eq!(
            c.query_one("SELECT body FROM t WHERE id=1", &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            value
        );
    }
    c.execute(&update, &[&None::<TextFormat<'_>>])
        .await
        .unwrap();
    assert_eq!(
        c.query_one("SELECT body FROM t WHERE id=1", &[])
            .await
            .unwrap()
            .get::<_, Option<String>>(0),
        None
    );
    assert_eq!(
        c.execute(&update, &[&TextFormat(&[0xff])])
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "22021"
    );
    // SQL literals still obey SQL quoting rules; the fix must be limited to Bind values.
    c.batch_execute("UPDATE t SET body='a''b' WHERE id=1")
        .await
        .unwrap();
    assert_eq!(
        c.query_one("SELECT body FROM t WHERE id=1", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "a'b"
    );
    drop(c);
    conn.await.unwrap().unwrap();
    server.await.unwrap();
}
