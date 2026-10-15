use crate::sql;
use rusqlite::{Connection, params, types::Value};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
pub struct DbError {
    pub code: &'static str,
    pub message: String,
}
pub type Result<T> = std::result::Result<T, DbError>;
impl DbError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}
impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for DbError {}
impl From<rusqlite::Error> for DbError {
    fn from(e: rusqlite::Error) -> Self {
        let code = match &e {
            rusqlite::Error::SqliteFailure(err, _) => match err.extended_code {
                517 => "40001",
                5 | 6 => "55P03",
                2067 | 1555 => "23505",
                1299 => "23502",
                275 | 3091 => "23514",
                9 => "57014",
                13 => "53100",
                _ => "XX000",
            },
            _ => "XX000",
        };
        Self::new(code, e.to_string())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Kind {
    Int4,
    Int8,
    Bool,
    Text,
    Bytes,
}
impl Kind {
    pub fn storage(self) -> &'static str {
        match self {
            Self::Int4 | Self::Int8 | Self::Bool => "INTEGER",
            Self::Text => "TEXT",
            Self::Bytes => "BLOB",
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub kind: Kind,
    pub nullable: bool,
    pub primary: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}
#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub kind: Kind,
}
#[derive(Debug)]
pub struct Output {
    pub tag: String,
    pub fields: Vec<Field>,
    pub rows: Vec<Vec<Value>>,
    pub affected: usize,
}
impl Output {
    pub fn command(tag: &str, affected: usize) -> Self {
        Self {
            tag: tag.into(),
            fields: vec![],
            rows: vec![],
            affected,
        }
    }
}
#[derive(Clone, Debug)]
pub struct Config {
    pub schema_epoch: Arc<AtomicU64>,
    pub writer: Arc<tokio::sync::Semaphore>,
    pub path: PathBuf,
    pub cache_kib: usize,
    pub max_result_bytes: usize,
    pub statement_timeout: Duration,
    pub transaction_timeout: Duration,
    pub max_wal_bytes: u64,
}
impl Config {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            schema_epoch: Arc::new(AtomicU64::new(0)),
            writer: Arc::new(tokio::sync::Semaphore::new(1)),
            cache_kib: 2048,
            max_result_bytes: 8 * 1024 * 1024,
            statement_timeout: Duration::from_secs(10),
            transaction_timeout: Duration::from_secs(30),
            max_wal_bytes: 64 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tx {
    Idle,
    Active,
    Failed,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SchemaVersion {
    sqlite: i64,
    epoch: u64,
}
pub struct Session {
    pub conn: Connection,
    pub config: Arc<Config>,
    pub tx: Tx,
    started: Option<Instant>,
}

pub fn initialize(config: &Config) -> Result<()> {
    if let Some(parent) = config.path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).map_err(|e| DbError::new("58030", e.to_string()))?;
    }
    let conn = Connection::open(&config.path)?;
    let mode: String = conn.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
    if mode != "wal" {
        return Err(DbError::new("58030", "WAL mode is required"));
    }
    conn.execute_batch("PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS _pglite_catalog(name TEXT PRIMARY KEY, schema_json TEXT NOT NULL) STRICT;")?;
    let version: i32 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version > 1 {
        return Err(DbError::new("0A000", "unsupported storage format"));
    }
    conn.pragma_update(None, "user_version", 1)?;
    Ok(())
}
impl Session {
    pub fn open(config: Arc<Config>) -> Result<Self> {
        let conn = Connection::open(&config.path)?;
        conn.busy_timeout(Duration::ZERO)?;
        conn.execute_batch(
            "PRAGMA synchronous=FULL; PRAGMA trusted_schema=OFF; PRAGMA wal_autocheckpoint=0;",
        )?;
        conn.pragma_update(None, "cache_size", -(config.cache_kib as i64))?;
        conn.set_prepared_statement_cache_capacity(32);
        conn.set_limit(
            rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH,
            config
                .max_result_bytes
                .max(1024 * 1024)
                .min(i32::MAX as usize) as i32,
        )?;
        conn.set_limit(
            rusqlite::limits::Limit::SQLITE_LIMIT_SQL_LENGTH,
            1024 * 1024,
        )?;
        Ok(Self {
            conn,
            config,
            tx: Tx::Idle,
            started: None,
        })
    }
    pub fn table(&self, name: &str) -> Result<Table> {
        let json: String = self
            .conn
            .query_row(
                "SELECT schema_json FROM _pglite_catalog WHERE name=?",
                [name],
                |r| r.get(0),
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => {
                    DbError::new("42P01", format!("table {name} does not exist"))
                }
                _ => e.into(),
            })?;
        serde_json::from_str(&json).map_err(|e| DbError::new("XX000", e.to_string()))
    }
    pub fn expire(&mut self) -> bool {
        if self
            .started
            .is_some_and(|t| t.elapsed() >= self.config.transaction_timeout)
        {
            let _ = self.conn.execute_batch("ROLLBACK");
            // Retain the expired timestamp until ROLLBACK so the socket watchdog
            // can still close a transaction expired by statement execution.
            self.tx = Tx::Failed;
            return true;
        }
        false
    }
    pub fn execute(
        &mut self,
        query: &str,
        values: &[Value],
        hints: &[Kind],
    ) -> Result<Vec<Output>> {
        self.expire();
        let statements = sql::parse(query).inspect_err(|_| {
            if self.tx == Tx::Active {
                self.tx = Tx::Failed;
            }
        })?;
        // A simple-query batch is atomic unless it contains explicit transaction controls.
        if statements.len() > 64 {
            return Err(DbError::new("54000", "batch exceeds 64 statements"));
        }
        let implicit = !statements.is_empty()
            && self.tx == Tx::Idle
            && !statements.iter().any(sql::is_transaction);
        if implicit {
            self.conn.execute_batch("BEGIN")?;
        }
        let mut out = Vec::new();
        let mut result_bytes = 0usize;
        for statement in &statements {
            let deadline = (Instant::now() + self.config.statement_timeout).min(
                self.started
                    .map(|t| t + self.config.transaction_timeout)
                    .unwrap_or(Instant::now() + self.config.statement_timeout),
            );
            self.conn
                .progress_handler(1000, Some(move || Instant::now() >= deadline))?;
            let result = self.execute_statement(statement, values, hints);
            self.conn.progress_handler(0, None::<fn() -> bool>)?;
            match result {
                Ok(v) => {
                    result_bytes += v
                        .rows
                        .iter()
                        .flatten()
                        .map(|v| match v {
                            Value::Text(s) => s.len() + 24,
                            Value::Blob(b) => b.len() + 24,
                            _ => 32,
                        })
                        .sum::<usize>();
                    if result_bytes > self.config.max_result_bytes {
                        if implicit {
                            let _ = self.conn.execute_batch("ROLLBACK");
                        }
                        if self.tx == Tx::Active {
                            self.tx = Tx::Failed;
                        }
                        return Err(DbError::new("54000", "batch result exceeds byte limit"));
                    }
                    out.push(v)
                }
                Err(e) => {
                    if implicit {
                        let _ = self.conn.execute_batch("ROLLBACK");
                    }
                    if self.tx == Tx::Active {
                        self.tx = Tx::Failed;
                    }
                    return Err(e);
                }
            }
        }
        if implicit && let Err(e) = self.conn.execute_batch("COMMIT") {
            let _ = self.conn.execute_batch("ROLLBACK");
            return Err(e.into());
        }
        Ok(out)
    }
    fn execute_statement(
        &mut self,
        s: &sqlparser::ast::Statement,
        values: &[Value],
        hints: &[Kind],
    ) -> Result<Output> {
        use sqlparser::ast::Statement;
        if matches!(
            s,
            Statement::Commit { chain: true }
                | Statement::Rollback { chain: true, .. }
                | Statement::Rollback {
                    savepoint: Some(_),
                    ..
                }
                | Statement::StartTransaction {
                    modifier: Some(_),
                    ..
                }
        ) {
            return Err(sql::unsupported(
                "transaction chaining, savepoints, or modifiers",
            ));
        }
        match s {
            Statement::Rollback { .. } => {
                if !self.conn.is_autocommit() {
                    self.conn.execute_batch("ROLLBACK")?;
                }
                self.tx = Tx::Idle;
                self.started = None;
                return Ok(Output::command("ROLLBACK", 0));
            }
            Statement::Commit { .. } => {
                let tag = if self.tx == Tx::Failed {
                    "ROLLBACK"
                } else {
                    "COMMIT"
                };
                if !self.conn.is_autocommit() {
                    self.conn.execute_batch(tag)?;
                }
                self.tx = Tx::Idle;
                self.started = None;
                return Ok(Output::command(tag, 0));
            }
            _ => {}
        }
        if self.tx == Tx::Failed {
            return Err(DbError::new(
                "25P02",
                "transaction aborted; ROLLBACK required",
            ));
        }
        match s {
            Statement::StartTransaction { modes, .. } => {
                for mode in modes {
                    if !matches!(
                        mode.to_string().as_str(),
                        "ISOLATION LEVEL REPEATABLE READ" | "READ WRITE"
                    ) {
                        return Err(sql::unsupported("transaction mode"));
                    }
                }
                if self.tx == Tx::Idle {
                    self.conn.execute_batch("BEGIN DEFERRED")?;
                    self.started = Some(Instant::now());
                    self.tx = Tx::Active;
                }
                Ok(Output::command("BEGIN", 0))
            }
            _ => {
                let plan = sql::plan(self, s, hints)?;
                self.run_plan(&plan, values)
            }
        }
    }
    pub fn schema_version(&self) -> Result<SchemaVersion> {
        Ok(SchemaVersion {
            sqlite: self
                .conn
                .query_row("PRAGMA schema_version", [], |r| r.get(0))?,
            epoch: self.config.schema_epoch.load(Ordering::Acquire),
        })
    }
    pub fn execute_cached(
        &mut self,
        plan: &sql::Plan,
        version: SchemaVersion,
        values: &[Value],
    ) -> Result<Output> {
        self.expire();
        if self.tx == Tx::Failed {
            return Err(DbError::new(
                "25P02",
                "transaction aborted; ROLLBACK required",
            ));
        }
        let implicit = self.tx == Tx::Idle;
        if implicit {
            self.conn.execute_batch("BEGIN")?;
        }
        let deadline = (Instant::now() + self.config.statement_timeout).min(
            self.started
                .map(|t| t + self.config.transaction_timeout)
                .unwrap_or(Instant::now() + self.config.statement_timeout),
        );
        self.conn
            .progress_handler(1000, Some(move || Instant::now() >= deadline))?;
        let result = (|| {
            if self.schema_version()? != version {
                return Err(DbError::new(
                    "0A000",
                    "schema changed; prepare statement again",
                ));
            }
            self.run_plan(plan, values)
        })();
        self.conn.progress_handler(0, None::<fn() -> bool>)?;
        if implicit
            && !self.conn.is_autocommit()
            && let Err(e) =
                self.conn
                    .execute_batch(if result.is_ok() { "COMMIT" } else { "ROLLBACK" })
        {
            let _ = self.conn.execute_batch("ROLLBACK");
            return Err(e.into());
        }
        if result.is_err() && self.tx == Tx::Active {
            self.tx = Tx::Failed;
        }
        result
    }
    pub fn run_plan(&mut self, plan: &sql::Plan, values: &[Value]) -> Result<Output> {
        if plan.params.len() != values.len() {
            return Err(DbError::new("08P01", "parameter count mismatch"));
        }
        for (kind, value) in plan.params.iter().zip(values) {
            sql::validate_value(*kind, value)?;
        }
        for index in &plan.nonnegative {
            if !matches!(values.get(*index),Some(Value::Integer(n)) if *n>=0) {
                return Err(DbError::new(
                    "2201W",
                    "LIMIT/OFFSET require nonnegative integer parameters",
                ));
            }
        }
        if matches!(
            plan.tag.as_str(),
            "INSERT"
                | "UPDATE"
                | "DELETE"
                | "CREATE TABLE"
                | "DROP TABLE"
                | "CREATE INDEX"
                | "DROP INDEX"
        ) {
            let wal = PathBuf::from(format!("{}-wal", self.config.path.display()));
            if std::fs::metadata(wal).is_ok_and(|m| m.len() > self.config.max_wal_bytes) {
                return Err(DbError::new(
                    "53100",
                    "WAL exceeds soft limit; end long transactions and retry after checkpoint",
                ));
            }
        }
        // SQLite's schema cookie can be reused after DDL rollback. The process-local
        // epoch also invalidates plans bound to a rolled-back incarnation of a table.
        if plan.tag.starts_with("CREATE ") || plan.tag.starts_with("DROP ") {
            self.config.schema_epoch.fetch_add(1, Ordering::AcqRel);
        }
        let schema_change = plan.create.is_some() || plan.drop.is_some();
        if schema_change {
            self.conn.execute_batch("SAVEPOINT schema_change")?;
        }
        let result = (|| {
            let mut stmt = self.conn.prepare_cached(&plan.sql)?;
            if !plan.fields.is_empty() {
                let mut cursor = stmt.query(rusqlite::params_from_iter(values))?;
                let mut rows = Vec::new();
                let mut size = 0usize;
                while let Some(row) = cursor.next()? {
                    let mut cells = Vec::new();
                    for (i, field) in plan.fields.iter().enumerate() {
                        let v: Value = row.get(i)?;
                        sql::validate_value(field.kind, &v)?;
                        size += match &v {
                            Value::Text(t) => t.len(),
                            Value::Blob(b) => b.len(),
                            _ => 8,
                        } + 24;
                        if size > self.config.max_result_bytes {
                            return Err(DbError::new(
                                "54000",
                                "result exceeds byte limit; use LIMIT or keyset pagination",
                            ));
                        }
                        cells.push(v);
                    }
                    rows.push(cells);
                }
                Ok(Output {
                    tag: plan.tag.clone(),
                    fields: plan.fields.clone(),
                    affected: rows.len(),
                    rows,
                })
            } else {
                let n = stmt.execute(rusqlite::params_from_iter(values))?;
                if let Some(t) = &plan.create {
                    self.conn.execute(
                        "INSERT INTO _pglite_catalog VALUES (?, ?)",
                        params![t.name, serde_json::to_string(t).unwrap()],
                    )?;
                }
                if let Some(name) = &plan.drop {
                    self.conn
                        .execute("DELETE FROM _pglite_catalog WHERE name=?", [name])?;
                }
                Ok(Output::command(&plan.tag, n))
            }
        })();
        if schema_change {
            if result.is_err() {
                self.conn.execute_batch("ROLLBACK TO schema_change")?;
            }
            self.conn.execute_batch("RELEASE schema_change")?;
            self.conn.flush_prepared_statement_cache();
        }
        result
    }
}
pub fn checkpoint(path: &Path, max_wal_bytes: u64) -> Result<(i64, i64, i64)> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(Duration::ZERO)?;
    let stats: (i64, i64, i64) = conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
    })?;
    let wal = PathBuf::from(format!("{}-wal", path.display()));
    if stats.1 == stats.2 && std::fs::metadata(wal).is_ok_and(|m| m.len() > max_wal_bytes / 2) {
        let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)");
    }
    Ok(stats)
}

/// Serialize checkpoint truncation with server writes. Keep the permit inside the
/// blocking job so cancellation of its async caller cannot release it prematurely.
pub async fn checkpoint_admitted(config: Arc<Config>) -> Result<(i64, i64, i64)> {
    let permit = config
        .writer
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| DbError::new("57P01", "writer admission closed"))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        checkpoint(&config.path, config.max_wal_bytes)
    })
    .await
    .map_err(|e| DbError::new("XX000", e.to_string()))?
}
