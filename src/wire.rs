use crate::{
    engine::{DbError, Field, Kind, Output, Session, Tx},
    sql,
};
use async_trait::async_trait;
use futures::{Sink, SinkExt, stream};
use pgwire::api::{
    ClientPortalStore, DEFAULT_NAME,
    auth::{StartupHandler, noop::NoopStartupHandler},
    cancel::CancelHandler,
    stmt::StoredStatement,
    store::{Entry, PortalStore},
};
use pgwire::messages::{
    PgWireBackendMessage, PgWireFrontendMessage,
    extendedquery::{Bind, BindComplete, Close, CloseComplete, Parse, ParseComplete},
};
use pgwire::{
    api::{
        ClientInfo, ErrorHandler, PgWireServerHandlers, Type,
        portal::{Format, Portal},
        query::{ExtendedQueryHandler, SimpleQueryHandler},
        results::{DataRowEncoder, FieldInfo, QueryResponse, Response, Tag},
        stmt::QueryParser,
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::response::TransactionStatus,
};
use rusqlite::types::Value;
use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::sync::{LazyLock, Weak};

pub fn pg_error(e: DbError) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        e.code.into(),
        e.message,
    )))
}
pub fn pg_type(k: Kind) -> Type {
    match k {
        Kind::Int4 => Type::INT4,
        Kind::Int8 => Type::INT8,
        Kind::Bool => Type::BOOL,
        Kind::Text => Type::TEXT,
        Kind::Bytes => Type::BYTEA,
    }
}
fn kind(t: &Type) -> PgWireResult<Kind> {
    match *t {
        Type::INT4 => Ok(Kind::Int4),
        Type::INT8 => Ok(Kind::Int8),
        Type::BOOL => Ok(Kind::Bool),
        Type::TEXT | Type::VARCHAR => Ok(Kind::Text),
        Type::BYTEA => Ok(Kind::Bytes),
        _ => Err(pg_error(sql::unsupported("parameter type"))),
    }
}
fn fields(fs: &[Field], format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    if let Format::Individual(v) = format
        && (v.len() != fs.len() || v.iter().any(|v| *v != 0 && *v != 1))
    {
        return Err(pg_error(DbError::new(
            "08P01",
            "invalid result format count or code",
        )));
    }
    Ok(fs
        .iter()
        .enumerate()
        .map(|(i, f)| {
            FieldInfo::new(
                f.name.clone(),
                None,
                None,
                pg_type(f.kind),
                format.format_for(i),
            )
        })
        .collect())
}
fn response(out: Output, format: &Format) -> PgWireResult<Response> {
    let tag = Tag::new(&out.tag);
    if out.tag == "BEGIN" {
        return Ok(Response::TransactionStart(tag));
    }
    if out.tag == "COMMIT" || out.tag == "ROLLBACK" {
        return Ok(Response::TransactionEnd(tag));
    }
    if out.fields.is_empty() {
        return Ok(Response::Execution(match out.tag.as_str() {
            "INSERT" => tag.with_oid(0).with_rows(out.affected),
            "UPDATE" | "DELETE" => tag.with_rows(out.affected),
            _ => tag,
        }));
    }
    let schema = Arc::new(fields(&out.fields, format)?);
    let encode_schema = schema.clone();
    let types = out.fields;
    let rows = out.rows.into_iter().map(move |row| {
        let mut encoder = DataRowEncoder::new(encode_schema.clone());
        for (v, f) in row.iter().zip(&types) {
            match (v, f.kind) {
                (Value::Null, _) => encoder.encode_field(&None::<i32>)?,
                (Value::Integer(i), Kind::Int4) => encoder.encode_field(&(*i as i32))?,
                (Value::Integer(i), Kind::Int8) => encoder.encode_field(i)?,
                (Value::Integer(i), Kind::Bool) => encoder.encode_field(&(*i != 0))?,
                (Value::Text(s), Kind::Text) => encoder.encode_field(s)?,
                (Value::Blob(b), Kind::Bytes) => encoder.encode_field(b)?,
                _ => return Err(pg_error(DbError::new("XX000", "unexpected result type"))),
            }
        }
        Ok(encoder.take_row())
    });
    Ok(Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    )))
}
#[derive(Clone)]
pub struct Backend {
    pub session: Arc<Mutex<Session>>,
    failed: Arc<AtomicBool>,
    statements: Arc<Mutex<HashSet<String>>>,
    portals: Arc<Mutex<HashSet<String>>>,
    active: Arc<Mutex<bool>>,
    interrupt: Arc<rusqlite::InterruptHandle>,
    writer: Arc<tokio::sync::Semaphore>,
    writer_lease: Arc<Mutex<Option<tokio::sync::OwnedSemaphorePermit>>>,
    admission_timeout: std::time::Duration,
    waiting: Arc<AtomicBool>,
    cancel_wait: Arc<tokio::sync::Notify>,
}
impl Backend {
    pub fn new(session: Session) -> Self {
        Self {
            interrupt: Arc::new(session.conn.get_interrupt_handle()),
            writer: session.config.writer.clone(),
            writer_lease: Arc::new(Mutex::new(None)),
            admission_timeout: session.config.statement_timeout,
            waiting: Arc::new(AtomicBool::new(false)),
            cancel_wait: Arc::new(tokio::sync::Notify::new()),
            session: Arc::new(Mutex::new(session)),
            failed: Arc::new(AtomicBool::new(false)),
            statements: Arc::default(),
            portals: Arc::default(),
            active: Arc::new(Mutex::new(false)),
        }
    }
    async fn admit_write(&self) -> PgWireResult<()> {
        if self.writer_lease.lock().unwrap().is_some() {
            return Ok(());
        }
        let tx = self
            .blocking(|s| {
                s.expire();
                Ok(s.tx)
            })
            .await?;
        if tx == Tx::Failed {
            return Err(pg_error(DbError::new(
                "25P02",
                "transaction aborted; ROLLBACK required",
            )));
        }
        let cancelled = self.cancel_wait.notified();
        tokio::pin!(cancelled);
        cancelled.as_mut().enable();
        self.waiting.store(true, Ordering::Release);
        let permit = tokio::select! {
            result=tokio::time::timeout(self.admission_timeout,self.writer.clone().acquire_owned())=>{
                result.map_err(|_|pg_error(DbError::new("57014","writer admission timed out")))
                    .and_then(|p|p.map_err(|_|pg_error(DbError::new("57P01","server is shutting down"))))
            },
            _=&mut cancelled=>Err(pg_error(DbError::new("57014","writer admission cancelled")))
        };
        self.waiting.store(false, Ordering::Release);
        *self.writer_lease.lock().unwrap() = Some(permit?);
        Ok(())
    }
    fn release_writer_if_idle(&self, tx: Tx) {
        if tx == Tx::Idle {
            self.writer_lease.lock().unwrap().take();
        }
    }
    async fn blocking<T, F>(&self, f: F) -> PgWireResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Session) -> crate::engine::Result<T> + Send + 'static,
    {
        let session = self.session.clone();
        let failed = self.failed.clone();
        let active = self.active.clone();
        let writer_lease = self.writer_lease.clone();
        tokio::task::spawn_blocking(move || {
            let mut s = session
                .lock()
                .map_err(|_| DbError::new("XX000", "session lock poisoned"))?;
            if failed.swap(false, Ordering::AcqRel) && s.tx == Tx::Active {
                s.tx = Tx::Failed;
            }
            *active.lock().unwrap() = true;
            let result = f(&mut s);
            if s.conn.is_autocommit() {
                writer_lease.lock().unwrap().take();
            }
            *active.lock().unwrap() = false;
            result
        })
        .await
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?
        .map_err(pg_error)
    }
}
impl ErrorHandler for Backend {
    fn on_error<C: ClientInfo>(&self, _client: &C, _error: &mut PgWireError) {
        self.failed.store(true, Ordering::Release);
    }
}
#[derive(Debug, Clone)]
pub struct Prepared {
    query: String,
    params: Vec<Kind>,
    fields: Vec<Field>,
    plan: Option<sql::Plan>,
    version: crate::engine::SchemaVersion,
}
#[async_trait]
impl QueryParser for Backend {
    type Statement = Prepared;
    async fn parse_sql<C>(
        &self,
        _client: &C,
        query: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Option<Prepared>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query = query.to_owned();
        let hints = types
            .iter()
            .map(|t| t.as_ref().map(kind).transpose())
            .collect::<PgWireResult<Vec<_>>>()?;
        self.blocking(move |s| {
            let statements = sql::parse(&query)?;
            if statements.is_empty() {
                return Ok(None);
            }
            if statements.len() != 1 {
                return Err(DbError::new(
                    "42601",
                    "prepared statements require exactly one SQL statement",
                ));
            }
            let hints = if hints.iter().all(Option::is_none) {
                vec![]
            } else {
                hints
                    .into_iter()
                    .map(|h| {
                        h.ok_or_else(|| {
                            sql::unsupported("mixed inferred and explicit parameter types")
                        })
                    })
                    .collect::<crate::engine::Result<Vec<_>>>()?
            };
            if sql::is_transaction(&statements[0]) {
                return Ok(Some(Prepared {
                    query,
                    params: vec![],
                    fields: vec![],
                    plan: None,
                    version: Default::default(),
                }));
            }
            let implicit = s.conn.is_autocommit();
            if implicit {
                s.conn.execute_batch("BEGIN")?;
            }
            let result = (|| {
                let version = s.schema_version()?;
                let p = sql::plan(s, &statements[0], &hints)?;
                Ok(Some(Prepared {
                    query,
                    params: p.params.clone(),
                    fields: p.fields.clone(),
                    plan: Some(p),
                    version,
                }))
            })();
            if implicit {
                s.conn.execute_batch("ROLLBACK")?;
            }
            result
        })
        .await
    }
    fn get_parameter_types(&self, s: &Prepared) -> PgWireResult<Vec<Type>> {
        Ok(s.params.iter().map(|k| pg_type(*k)).collect())
    }
    fn get_result_schema(&self, s: &Prepared, f: Option<&Format>) -> PgWireResult<Vec<FieldInfo>> {
        fields(&s.fields, f.unwrap_or(&Format::UnifiedText))
    }
}
#[async_trait]
impl SimpleQueryHandler for Backend {
    async fn on_query<C>(
        &self,
        client: &mut C,
        query: pgwire::messages::simplequery::Query,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client.portal_store().rm_statement(DEFAULT_NAME);
        self.statements.lock().unwrap().remove(DEFAULT_NAME);
        let result = self._on_query(client, query).await;
        if client.transaction_status() == TransactionStatus::Idle {
            client.portal_store().clear_portals();
            self.portals.lock().unwrap().clear();
        }
        result
    }
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        if sql::parse(query).map_err(pg_error)?.iter().any(|s| {
            matches!(
                s,
                sqlparser::ast::Statement::Insert(_)
                    | sqlparser::ast::Statement::Update { .. }
                    | sqlparser::ast::Statement::Delete(_)
                    | sqlparser::ast::Statement::CreateTable(_)
                    | sqlparser::ast::Statement::CreateIndex(_)
                    | sqlparser::ast::Statement::Drop { .. }
            )
        }) {
            self.admit_write().await?;
        }
        let query = query.to_owned();
        let (out, tx) = self
            .blocking(move |s| {
                let out = s.execute(&query, &[], &[]);
                Ok((out, s.tx))
            })
            .await?;
        self.release_writer_if_idle(tx);
        client.set_transaction_status(match tx {
            Tx::Idle => TransactionStatus::Idle,
            Tx::Active => TransactionStatus::Transaction,
            Tx::Failed => TransactionStatus::Error,
        });
        let outputs = out.map_err(pg_error)?;
        if outputs.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }
        outputs
            .into_iter()
            .map(|o| response(o, &Format::UnifiedText))
            .collect()
    }
}
#[async_trait]
impl ExtendedQueryHandler for Backend {
    type Statement = Prepared;
    type QueryParser = Backend;
    fn query_parser(&self) -> Arc<Backend> {
        Arc::new(self.clone())
    }
    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name.as_deref().unwrap_or(DEFAULT_NAME).to_owned();
        {
            let names = self.statements.lock().unwrap();
            if name != DEFAULT_NAME && names.contains(&name) {
                return Err(pg_error(DbError::new(
                    "42P05",
                    "prepared statement already exists",
                )));
            }
            if !names.contains(&name) && names.len() >= 32 {
                return Err(pg_error(DbError::new(
                    "54000",
                    "prepared statement limit is 32",
                )));
            }
        }
        match StoredStatement::parse(client, &message, self.query_parser()).await? {
            Some(stmt) => client.portal_store().put_statement(Arc::new(stmt)),
            None => client.portal_store().put_empty_statement(&name),
        }
        self.statements.lock().unwrap().insert(name);
        client
            .send(PgWireBackendMessage::ParseComplete(ParseComplete::new()))
            .await?;
        Ok(())
    }
    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message
            .portal_name
            .as_deref()
            .unwrap_or(DEFAULT_NAME)
            .to_owned();
        let statement_name = message.statement_name.as_deref().unwrap_or(DEFAULT_NAME);
        {
            let names = self.portals.lock().unwrap();
            if name != DEFAULT_NAME && client.portal_store().get_portal(&name).is_some() {
                return Err(pg_error(DbError::new("42P03", "portal already exists")));
            }
            if !names.contains(&name) && names.len() >= 8 {
                return Err(pg_error(DbError::new("54000", "portal limit is 8")));
            }
        }
        let (np, nr) = match client.portal_store().get_statement(statement_name) {
            Some(Entry::Value(s)) => (s.statement.params.len(), s.statement.fields.len()),
            Some(Entry::Empty) => (0, 0),
            None => return Err(PgWireError::StatementNotFound(statement_name.into())),
        };
        if message.parameters.len() != np {
            return Err(pg_error(DbError::new("08P01", "parameter count mismatch")));
        }
        for (codes, n) in [
            (&message.parameter_format_codes, np),
            (&message.result_column_format_codes, nr),
        ] {
            if (codes.len() > 1 && codes.len() != n) || codes.iter().any(|c| *c != 0 && *c != 1) {
                return Err(pg_error(DbError::new("08P01", "invalid format codes")));
            }
        }
        if message
            .parameters
            .iter()
            .flatten()
            .map(|v| v.len())
            .sum::<usize>()
            > 1024 * 1024
        {
            return Err(pg_error(DbError::new(
                "54000",
                "bound parameters exceed 1 MiB",
            )));
        }
        match client.portal_store().get_statement(statement_name) {
            Some(Entry::Value(s)) => {
                let p = Portal::try_new(&message, s)?;
                client.portal_store().put_portal(Arc::new(p));
            }
            Some(Entry::Empty) => client.portal_store().put_empty_portal(&name),
            None => unreachable!(),
        }
        self.portals.lock().unwrap().insert(name);
        client
            .send(PgWireBackendMessage::BindComplete(BindComplete::new()))
            .await?;
        Ok(())
    }
    async fn on_close<C>(&self, client: &mut C, message: Close) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        match message.target_type {
            b'S' => {
                client.portal_store().rm_statement(name);
                self.statements.lock().unwrap().remove(name);
            }
            b'P' => {
                client.portal_store().rm_portal(name);
                self.portals.lock().unwrap().remove(name);
            }
            _ => return Err(pg_error(DbError::new("08P01", "invalid Close target"))),
        }
        client
            .send(PgWireBackendMessage::CloseComplete(CloseComplete::new()))
            .await?;
        Ok(())
    }
    async fn on_sync<C>(
        &self,
        client: &mut C,
        _message: pgwire::messages::extendedquery::Sync,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let tx = self
            .blocking(|s| {
                s.expire();
                Ok(s.tx)
            })
            .await?;
        let status = match tx {
            Tx::Idle => TransactionStatus::Idle,
            Tx::Active => TransactionStatus::Transaction,
            Tx::Failed => TransactionStatus::Error,
        };
        client.set_transaction_status(status);
        if tx == Tx::Idle {
            client.portal_store().clear_portals();
            self.portals.lock().unwrap().clear();
        } else {
            client.portal_store().rm_portal(DEFAULT_NAME);
            self.portals.lock().unwrap().remove(DEFAULT_NAME);
        }
        client
            .send(PgWireBackendMessage::ReadyForQuery(
                pgwire::messages::response::ReadyForQuery::new(status),
            ))
            .await?;
        client.flush().await?;
        Ok(())
    }
    async fn do_query<C>(
        &self,
        client: &mut C,
        p: &Portal<Prepared>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let stmt = &p.statement.statement;
        if p.parameter_len() != stmt.params.len() {
            return Err(pg_error(DbError::new("08P01", "parameter count mismatch")));
        }
        if let Format::Individual(v) = &p.parameter_format
            && (v.len() != p.parameter_len() || v.iter().any(|v| *v != 0 && *v != 1))
        {
            return Err(pg_error(DbError::new("08P01", "invalid parameter formats")));
        }
        fields(&stmt.fields, &p.result_column_format)?;
        let mut values = Vec::new();
        for (i, k) in stmt.params.iter().enumerate() {
            let ty = pg_type(*k);
            let v = match k {
                Kind::Int4 => p
                    .parameter::<i32>(i, &ty)?
                    .map(|i| Value::Integer(i as i64)),
                Kind::Int8 => p.parameter::<i64>(i, &ty)?.map(Value::Integer),
                Kind::Bool => p
                    .parameter::<bool>(i, &ty)?
                    .map(|v| Value::Integer(v as i64)),
                Kind::Text => p.parameter::<String>(i, &ty)?.map(Value::Text),
                Kind::Bytes => p.parameter::<Vec<u8>>(i, &ty)?.map(Value::Blob),
            };
            values.push(v.unwrap_or(Value::Null));
        }
        if stmt.plan.as_ref().is_some_and(|p| {
            matches!(
                p.tag.as_str(),
                "INSERT"
                    | "UPDATE"
                    | "DELETE"
                    | "CREATE TABLE"
                    | "CREATE INDEX"
                    | "DROP TABLE"
                    | "DROP INDEX"
            )
        }) {
            self.admit_write().await?;
        }
        let statement = stmt.clone();
        let (out, tx) = self
            .blocking(move |s| {
                let out = if let Some(plan) = &statement.plan {
                    s.execute_cached(plan, statement.version, &values)
                        .map(|o| vec![o])
                } else {
                    s.execute(&statement.query, &values, &statement.params)
                };
                Ok((out, s.tx))
            })
            .await?;
        self.release_writer_if_idle(tx);
        client.set_transaction_status(match tx {
            Tx::Idle => TransactionStatus::Idle,
            Tx::Active => TransactionStatus::Transaction,
            Tx::Failed => TransactionStatus::Error,
        });
        let out = out
            .map_err(pg_error)?
            .into_iter()
            .next()
            .ok_or_else(|| pg_error(DbError::new("42601", "empty query")))?;
        response(out, &p.result_column_format)
    }
}
pub struct Handlers(pub Arc<Backend>);
impl PgWireServerHandlers for Handlers {
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(Startup(self.0.clone()))
    }
    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        Arc::new(Cancel)
    }
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.0.clone()
    }
    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.0.clone()
    }
    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        self.0.clone()
    }
}

type CancelMap = HashMap<(i32, bytes::Bytes), Weak<Backend>>;
static CANCELS: LazyLock<Mutex<CancelMap>> = LazyLock::new(|| Mutex::new(HashMap::new()));
pub struct Startup(Arc<Backend>);
#[async_trait]
impl NoopStartupHandler for Startup {
    async fn post_startup<C>(
        &self,
        client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let (pid, key) = client.pid_and_secret_key();
        let mut registry = CANCELS.lock().unwrap();
        registry.retain(|_, b| b.strong_count() > 0);
        registry.insert((pid, key.to_bytes()), Arc::downgrade(&self.0));
        Ok(())
    }
}
pub struct Cancel;
#[async_trait]
impl CancelHandler for Cancel {
    async fn on_cancel_request(&self, r: pgwire::messages::cancel::CancelRequest) {
        let backend = CANCELS
            .lock()
            .unwrap()
            .get(&(r.pid, r.secret_key.to_bytes()))
            .and_then(Weak::upgrade);
        if let Some(b) = backend {
            let active = b.active.lock().unwrap();
            if *active {
                b.interrupt.interrupt();
            } else if b.waiting.load(Ordering::Acquire) {
                b.cancel_wait.notify_waiters();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{self, Config};
    use std::time::{Duration, Instant};
    #[tokio::test]
    async fn cancellation_targets_the_active_connection_and_secret() {
        let d = tempfile::tempdir().unwrap();
        let config = Arc::new(Config::new(d.path().join("cancel.db")));
        engine::initialize(&config).unwrap();
        let backend = Arc::new(Backend::new(Session::open(config).unwrap()));
        let secret = pgwire::messages::startup::SecretKey::I32(123456);
        CANCELS
            .lock()
            .unwrap()
            .insert((42, secret.to_bytes()), Arc::downgrade(&backend));
        let started = Arc::new(AtomicBool::new(false));
        let ready = started.clone();
        let worker = backend.clone();
        let query = tokio::spawn(async move {
            worker.blocking(move|s|{
            let deadline=Instant::now()+Duration::from_secs(2);
            s.conn.progress_handler(1000,Some(move||{ready.store(true,Ordering::Release);Instant::now()>=deadline}))?;
            let n:i64=s.conn.query_row("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT sum(x) FROM n",[],|r|r.get(0))?;
            Ok(n)
        }).await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        Cancel
            .on_cancel_request(pgwire::messages::cancel::CancelRequest::new(
                42,
                pgwire::messages::startup::SecretKey::I32(0),
            ))
            .await;
        assert!(!query.is_finished());
        Cancel
            .on_cancel_request(pgwire::messages::cancel::CancelRequest::new(
                42,
                secret.clone(),
            ))
            .await;
        let err = query.await.unwrap().unwrap_err();
        assert!(matches!(err,PgWireError::UserError(e) if e.code=="57014"));
        CANCELS.lock().unwrap().remove(&(42, secret.to_bytes()));
    }
}
