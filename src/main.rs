use pglite_server::{
    engine::{self, Config, Session},
    wire::{Backend, Handlers},
};
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::Semaphore};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::new("data/pglite.db");
    let mut addr: SocketAddr = "127.0.0.1:5433".parse()?;
    let mut max_clients = 32usize;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data" => config.path = PathBuf::from(args.next().ok_or("--data requires a path")?),
            "--listen" => addr = args.next().ok_or("--listen requires an address")?.parse()?,
            "--max-clients" => {
                max_clients = args
                    .next()
                    .ok_or("--max-clients requires a value")?
                    .parse()?
            }
            "--cache-kib" => {
                config.cache_kib = args.next().ok_or("--cache-kib requires a value")?.parse()?
            }
            "--max-result-bytes" => {
                config.max_result_bytes = args.next().ok_or("missing byte limit")?.parse()?
            }
            "--max-wal-bytes" => {
                config.max_wal_bytes = args.next().ok_or("missing WAL limit")?.parse()?
            }
            "--statement-timeout-ms" => {
                config.statement_timeout =
                    Duration::from_millis(args.next().ok_or("missing timeout")?.parse()?)
            }
            "--transaction-timeout-ms" => {
                config.transaction_timeout =
                    Duration::from_millis(args.next().ok_or("missing timeout")?.parse()?)
            }
            "--help" => {
                println!(
                    "pglite-server [--data data/pglite.db] [--listen 127.0.0.1:5433] [--max-clients 32] [--cache-kib 2048] [--statement-timeout-ms 10000] [--transaction-timeout-ms 30000]\nLocal development release: loopback only; no authentication or TLS."
                );
                return Ok(());
            }
            _ => return Err(format!("unknown option: {arg}").into()),
        }
    }
    if !addr.ip().is_loopback() {
        return Err("this release only accepts loopback bindings; remote authentication/TLS is not implemented".into());
    }
    if max_clients == 0
        || max_clients > 256
        || config.cache_kib == 0
        || config.max_result_bytes == 0
        || config.max_wal_bytes == 0
        || config.statement_timeout.is_zero()
        || config.transaction_timeout.is_zero()
    {
        return Err("max-clients must be 1..256 and cache-kib must be positive".into());
    }
    if let Some(parent) = config.path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let lock_path = PathBuf::from(format!("{}.lock", config.path.display()));
    let instance_lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(lock_path)?;
    instance_lock
        .try_lock()
        .map_err(|_| "another server owns this data path")?;
    engine::initialize(&config)?;
    let config = Arc::new(config);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(8)
        .enable_all()
        .build()?
        .block_on(serve(config, addr, max_clients))?;
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut term) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {_=tokio::signal::ctrl_c()=>{},_=term.recv()=>{}}
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

async fn serve(config: Arc<Config>, addr: SocketAddr, max_clients: usize) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let permits = Arc::new(Semaphore::new(max_clients));
    let mut tasks = tokio::task::JoinSet::new();
    let checkpoint_config = config.clone();
    let checkpoint = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            match engine::checkpoint_admitted(checkpoint_config.clone()).await {
                Ok(_) => {}
                result => eprintln!("checkpoint failed: {result:?}"),
            }
        }
    });
    println!(
        "ready addr={} data={} cache_kib_per_client={} max_clients={}",
        listener.local_addr()?,
        config.path.display(),
        config.cache_kib,
        max_clients
    );
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _=&mut shutdown=>break,
            Some(result)=tasks.join_next(),if !tasks.is_empty()=>{
                if let Err(e)=result{eprintln!("connection task failed: {e}");}
            },
            incoming=listener.accept()=>{
                let(socket,_)=incoming?;
                let Ok(permit)=permits.clone().try_acquire_owned()else{drop(socket);continue;};
                socket.set_nodelay(true)?;
                let config=config.clone();
                tasks.spawn(async move {
                    let _permit=permit;
                    if let Err(e)=serve_connection(socket,config).await{eprintln!("connection ended: {e}");}
                });
            }
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    checkpoint.abort();
    Ok(())
}

async fn serve_connection(
    socket: tokio::net::TcpStream,
    config: Arc<Config>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let session = tokio::task::spawn_blocking(move || Session::open(config)).await??;
    let backend = Arc::new(Backend::new(session));
    let expiry = backend.session.clone();
    let process = pgwire::tokio::process_socket(socket, None, Handlers(backend));
    tokio::pin!(process);
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    loop {
        tokio::select! {
            result=&mut process=>{result?;break;},
            _=interval.tick()=>{
                let session=expiry.clone();
                let expired=tokio::task::spawn_blocking(move||session.try_lock().map(|mut s|s.expire()).unwrap_or(false)).await?;
                // Closing also drops suspended portals; expired transactions must
                // not continue yielding materialized rows from old cursors.
                if expired {break;}
            }
        }
    }
    Ok(())
}
