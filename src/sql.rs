use crate::engine::{Column, DbError, Field, Kind, Result, Session, Table};
use rusqlite::types::Value as Cell;
use sqlparser::{ast::*, dialect::PostgreSqlDialect, parser::Parser};

pub fn unsupported(what: &str) -> DbError {
    DbError::new("0A000", format!("unsupported: {what}"))
}
pub fn parse(sql: &str) -> Result<Vec<Statement>> {
    if sql.len() > 1024 * 1024 {
        return Err(DbError::new("54000", "SQL exceeds 1 MiB"));
    }
    Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|e| DbError::new("42601", e.to_string()))
}
pub fn is_transaction(s: &Statement) -> bool {
    matches!(
        s,
        Statement::StartTransaction { .. } | Statement::Commit { .. } | Statement::Rollback { .. }
    )
}
pub fn ident(i: &Ident) -> Result<String> {
    let s = if i.quote_style.is_some() {
        i.value.clone()
    } else {
        i.value.to_lowercase()
    };
    if s.starts_with('_') || s.starts_with("sqlite_") || s.is_empty() || s.contains('\0') {
        return Err(unsupported("reserved identifier"));
    }
    Ok(s)
}
pub fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
pub fn name(n: &ObjectName) -> Result<String> {
    match n.0.as_slice() {
        [i] => ident(i),
        [schema, i] if ident(schema)? == "public" => ident(i),
        _ => Err(unsupported("schema")),
    }
}
pub fn validate_value(kind: Kind, v: &Cell) -> Result<()> {
    let valid = match (kind, v) {
        (_, Cell::Null) => true,
        (Kind::Int4, Cell::Integer(i)) => i32::try_from(*i).is_ok(),
        (Kind::Int8, Cell::Integer(_)) => true,
        (Kind::Bool, Cell::Integer(i)) => *i == 0 || *i == 1,
        (Kind::Text, Cell::Text(_)) | (Kind::Bytes, Cell::Blob(_)) => true,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(DbError::new(
            "22003",
            "value outside supported type or range",
        ))
    }
}
fn data_kind(d: &DataType) -> Result<Kind> {
    match d.to_string().to_uppercase().as_str() {
        "INT" | "INTEGER" | "INT4" => Ok(Kind::Int4),
        "BIGINT" | "INT8" => Ok(Kind::Int8),
        "BOOLEAN" | "BOOL" => Ok(Kind::Bool),
        "TEXT" => Ok(Kind::Text),
        "BYTEA" => Ok(Kind::Bytes),
        _ => Err(unsupported("data type")),
    }
}
#[derive(Debug, Clone)]
pub struct Plan {
    pub sql: String,
    pub tag: String,
    pub fields: Vec<Field>,
    pub params: Vec<Kind>,
    pub create: Option<Table>,
    pub drop: Option<String>,
    pub nonnegative: Vec<usize>,
}
impl Plan {
    fn new(sql: String, tag: &str) -> Self {
        Self {
            sql,
            tag: tag.into(),
            fields: vec![],
            params: vec![],
            create: None,
            drop: None,
            nonnegative: vec![],
        }
    }
}
struct Binder<'a> {
    table: Option<&'a Table>,
    params: Vec<Option<Kind>>,
}
impl<'a> Binder<'a> {
    fn new(table: Option<&'a Table>, hints: &[Kind]) -> Self {
        Self {
            table,
            params: hints.iter().copied().map(Some).collect(),
        }
    }
    fn column(&self, i: &Ident) -> Result<&Column> {
        let n = ident(i)?;
        self.table
            .and_then(|t| t.columns.iter().find(|c| c.name == n))
            .ok_or_else(|| DbError::new("42703", format!("unknown column {n}")))
    }
    fn known(&self, e: &Expr) -> Option<Kind> {
        match e {
            Expr::Identifier(i) => self.column(i).ok().map(|c| c.kind),
            Expr::Value(Value::Boolean(_)) => Some(Kind::Bool),
            Expr::Value(Value::SingleQuotedString(_)) => Some(Kind::Text),
            Expr::Value(Value::Number(n, _)) => n.parse::<i64>().ok().map(|v| {
                if i32::try_from(v).is_ok() {
                    Kind::Int4
                } else {
                    Kind::Int8
                }
            }),
            Expr::Value(Value::Placeholder(p)) => p
                .strip_prefix('$')
                .and_then(|p| p.parse::<usize>().ok())
                .and_then(|n| n.checked_sub(1))
                .and_then(|n| self.params.get(n))
                .copied()
                .flatten(),
            Expr::Nested(e) => self.known(e),
            _ => None,
        }
    }
    fn expr(&mut self, e: &Expr, expected: Option<Kind>) -> Result<(String, Kind)> {
        let (sql, kind) = match e {
            Expr::Identifier(i) => {
                let c = self.column(i)?;
                (quote(&c.name), c.kind)
            }
            Expr::Nested(e) => {
                let (s, k) = self.expr(e, expected)?;
                (format!("({s})"), k)
            }
            Expr::Value(Value::Null) => ("NULL".into(), expected.unwrap_or(Kind::Text)),
            Expr::Value(Value::Boolean(v)) => (if *v { "1" } else { "0" }.into(), Kind::Bool),
            Expr::Value(Value::SingleQuotedString(v)) => {
                (format!("'{}'", v.replace('\'', "''")), Kind::Text)
            }
            Expr::Value(Value::Number(n, _)) => {
                let i = n.parse::<i64>().map_err(|_| {
                    DbError::new("22003", "only signed integer numbers are supported")
                })?;
                let k = expected
                    .filter(|k| matches!(k, Kind::Int4 | Kind::Int8))
                    .unwrap_or(if i32::try_from(i).is_ok() {
                        Kind::Int4
                    } else {
                        Kind::Int8
                    });
                validate_value(k, &Cell::Integer(i))?;
                (i.to_string(), k)
            }
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            } if matches!(expr.as_ref(), Expr::Value(Value::Number(..))) => {
                let n = format!("-{expr}");
                let i = n
                    .parse::<i64>()
                    .map_err(|_| DbError::new("22003", "integer out of range"))?;
                let k = expected.unwrap_or(Kind::Int8);
                validate_value(k, &Cell::Integer(i))?;
                (n, k)
            }
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => {
                let (s, _) = self.expr(expr, Some(Kind::Bool))?;
                (format!("NOT ({s})"), Kind::Bool)
            }
            Expr::Value(Value::Placeholder(p)) => {
                let n = p
                    .strip_prefix('$')
                    .and_then(|v| v.parse::<usize>().ok())
                    .filter(|n| *n > 0 && *n <= 1024)
                    .ok_or_else(|| unsupported("parameter"))?;
                if self.params.len() < n {
                    self.params.resize(n, None);
                }
                let k = expected
                    .or(self.params[n - 1])
                    .ok_or_else(|| DbError::new("42P18", "cannot infer parameter type"))?;
                if self.params[n - 1].is_some_and(|old| old != k) {
                    return Err(DbError::new("42804", "inconsistent parameter type"));
                }
                self.params[n - 1] = Some(k);
                (format!("?{n}"), k)
            }
            Expr::BinaryOp { left, op, right } => {
                let logical = matches!(op, BinaryOperator::And | BinaryOperator::Or);
                if !logical
                    && !matches!(
                        op,
                        BinaryOperator::Eq
                            | BinaryOperator::NotEq
                            | BinaryOperator::Lt
                            | BinaryOperator::LtEq
                            | BinaryOperator::Gt
                            | BinaryOperator::GtEq
                    )
                {
                    return Err(unsupported("operator (arithmetic is not yet supported)"));
                }
                let k = if logical {
                    Some(Kind::Bool)
                } else {
                    self.known(left).or(self.known(right))
                };
                let (l, lk) = self.expr(left, k)?;
                let (r, _) = self.expr(right, Some(lk))?;
                (format!("({l} {op} {r})"), Kind::Bool)
            }
            Expr::IsNull(inner) => {
                let (s, _) = self.expr(inner, None)?;
                (format!("({s} IS NULL)"), Kind::Bool)
            }
            Expr::IsNotNull(inner) => {
                let (s, _) = self.expr(inner, None)?;
                (format!("({s} IS NOT NULL)"), Kind::Bool)
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let (s, k) = self.expr(expr, None)?;
                let values = list
                    .iter()
                    .map(|e| self.expr(e, Some(k)).map(|v| v.0))
                    .collect::<Result<Vec<_>>>()?;
                if values.is_empty() {
                    return Err(unsupported("empty IN list"));
                }
                (
                    format!(
                        "({s} {}IN ({}))",
                        if *negated { "NOT " } else { "" },
                        values.join(",")
                    ),
                    Kind::Bool,
                )
            }
            _ => return Err(unsupported("expression")),
        };
        if let Some(want) = expected
            && kind != want
            && !matches!(
                (kind, want),
                (Kind::Int4, Kind::Int8) | (Kind::Int8, Kind::Int4)
            )
        {
            return Err(DbError::new("42804", "expression type mismatch"));
        }
        Ok((sql, kind))
    }
    fn finish(self, p: &mut Plan) -> Result<()> {
        p.params = self
            .params
            .into_iter()
            .map(|k| k.ok_or_else(|| DbError::new("42P18", "cannot infer parameter type")))
            .collect::<Result<_>>()?;
        Ok(())
    }
    fn filter(&mut self, e: &Option<Expr>) -> Result<String> {
        e.as_ref()
            .map(|e| {
                self.expr(e, Some(Kind::Bool))
                    .map(|(s, _)| format!(" WHERE {s}"))
            })
            .unwrap_or(Ok(String::new()))
    }
}
fn table_from(session: &Session, from: &TableWithJoins) -> Result<Table> {
    if !from.joins.is_empty() {
        return Err(unsupported("joins"));
    }
    if let TableFactor::Table {
        name,
        alias: None,
        args: None,
        with_hints,
        version: None,
        partitions,
        ..
    } = &from.relation
    {
        if !with_hints.is_empty() || !partitions.is_empty() {
            return Err(unsupported("table options"));
        }
        session.table(&name_fn(name)?)
    } else {
        Err(unsupported("table expression or alias"))
    }
}
fn name_fn(n: &ObjectName) -> Result<String> {
    name(n)
}

pub fn plan(session: &Session, s: &Statement, hints: &[Kind]) -> Result<Plan> {
    validate_shape(s)?;
    match s {
        Statement::CreateTable(c) => {
            if c.if_not_exists
                || c.temporary
                || c.query.is_some()
                || !c.constraints.is_empty()
                || c.like.is_some()
                || c.clone.is_some()
                || c.or_replace
                || c.external
            {
                return Err(unsupported("CREATE TABLE options; use inline primary key"));
            }
            let n = name(&c.name)?;
            let mut cols = Vec::new();
            let mut defs = Vec::new();
            for col in &c.columns {
                let cn = ident(&col.name)?;
                let k = data_kind(&col.data_type)?;
                let mut nullable = true;
                let mut primary = false;
                let mut unique = false;
                for opt in &col.options {
                    match &opt.option {
                        ColumnOption::Null => {}
                        ColumnOption::NotNull => nullable = false,
                        ColumnOption::Unique { is_primary, .. } => {
                            if *is_primary {
                                primary = true;
                                nullable = false;
                            } else {
                                unique = true;
                            }
                        }
                        _ => return Err(unsupported("column option")),
                    }
                }
                if col.collation.is_some() || cols.iter().any(|c: &Column| c.name == cn) {
                    return Err(unsupported("collation or duplicate column"));
                }
                let q = quote(&cn);
                let mut d = format!("{q} {}", k.storage());
                if !nullable {
                    d.push_str(" NOT NULL");
                }
                if primary {
                    d.push_str(" PRIMARY KEY");
                }
                if unique {
                    d.push_str(" UNIQUE");
                }
                if k == Kind::Int4 {
                    d.push_str(&format!(" CHECK ({q} BETWEEN -2147483648 AND 2147483647)"));
                }
                if k == Kind::Bool {
                    d.push_str(&format!(" CHECK ({q} IN (0,1))"));
                }
                defs.push(d);
                cols.push(Column {
                    name: cn,
                    kind: k,
                    nullable,
                    primary,
                });
            }
            if cols.iter().filter(|c| c.primary).count() != 1 {
                return Err(unsupported("exactly one inline primary key is required"));
            }
            // WITHOUT ROWID avoids SQLite's NULL-to-generated-rowid behavior.
            let mut p = Plan::new(
                format!(
                    "CREATE TABLE {} ({}) STRICT, WITHOUT ROWID",
                    quote(&n),
                    defs.join(",")
                ),
                "CREATE TABLE",
            );
            p.create = Some(Table {
                name: n,
                columns: cols,
            });
            Ok(p)
        }
        Statement::CreateIndex(c) => {
            if c.concurrently
                || c.if_not_exists
                || c.predicate.is_some()
                || !c.include.is_empty()
                || c.using
                    .as_ref()
                    .is_some_and(|i| i.value.to_lowercase() != "btree")
            {
                return Err(unsupported("index options"));
            }
            let t = session.table(&name(&c.table_name)?)?;
            let n = name(
                c.name
                    .as_ref()
                    .ok_or_else(|| unsupported("unnamed index"))?,
            )?;
            let b = Binder::new(Some(&t), hints);
            let mut columns = Vec::new();
            for item in &c.columns {
                let Expr::Identifier(i) = &item.expr else {
                    return Err(unsupported("expression index"));
                };
                let col = b.column(i)?;
                if item.nulls_first.is_some() {
                    return Err(unsupported("index NULL ordering"));
                }
                columns.push(format!(
                    "{} {}",
                    quote(&col.name),
                    if item.asc == Some(false) {
                        "DESC"
                    } else {
                        "ASC"
                    }
                ));
            }
            Ok(Plan::new(
                format!(
                    "CREATE {}INDEX {} ON {} ({})",
                    if c.unique { "UNIQUE " } else { "" },
                    quote(&n),
                    quote(&t.name),
                    columns.join(",")
                ),
                "CREATE INDEX",
            ))
        }
        Statement::Drop {
            object_type,
            names,
            if_exists,
            cascade,
            restrict,
            purge,
            temporary,
        } => {
            if names.len() != 1 || *cascade || *restrict || *purge || *temporary || *if_exists {
                return Err(unsupported("DROP options"));
            }
            let n = name(&names[0])?;
            let mut p = Plan::new(
                format!("DROP {object_type} {}", quote(&n)),
                &format!("DROP {object_type}"),
            );
            match object_type {
                ObjectType::Table => {
                    session.table(&n)?;
                    p.drop = Some(n);
                }
                ObjectType::Index => {}
                _ => return Err(unsupported("DROP object")),
            };
            Ok(p)
        }
        Statement::Insert(i) => {
            if i.or.is_some()
                || i.overwrite
                || i.on.is_some()
                || i.returning.is_some()
                || i.partitioned.is_some()
                || !i.after_columns.is_empty()
                || i.ignore
                || i.replace_into
            {
                return Err(unsupported("INSERT options"));
            }
            let t = session.table(&name(&i.table_name)?)?;
            let mut b = Binder::new(Some(&t), hints);
            let cs = if i.columns.is_empty() {
                t.columns.iter().collect::<Vec<_>>()
            } else {
                i.columns
                    .iter()
                    .map(|i| b.column(i))
                    .collect::<Result<Vec<_>>>()?
            };
            let cols = cs
                .iter()
                .map(|c| (c.name.clone(), c.kind))
                .collect::<Vec<_>>();
            let q = i
                .source
                .as_ref()
                .ok_or_else(|| unsupported("INSERT DEFAULT VALUES"))?;
            let SetExpr::Values(v) = q.body.as_ref() else {
                return Err(unsupported("INSERT source"));
            };
            let mut rows = Vec::new();
            for row in &v.rows {
                if row.len() != cols.len() {
                    return Err(DbError::new("42601", "INSERT column count mismatch"));
                }
                rows.push(format!(
                    "({})",
                    row.iter()
                        .zip(&cols)
                        .map(|(e, (_, k))| b.expr(e, Some(*k)).map(|v| v.0))
                        .collect::<Result<Vec<_>>>()?
                        .join(",")
                ));
            }
            let mut p = Plan::new(
                format!(
                    "INSERT INTO {} ({}) VALUES {}",
                    quote(&t.name),
                    cols.iter()
                        .map(|(n, _)| quote(n))
                        .collect::<Vec<_>>()
                        .join(","),
                    rows.join(",")
                ),
                "INSERT",
            );
            b.finish(&mut p)?;
            Ok(p)
        }
        Statement::Query(q) => {
            if q.with.is_some()
                || !q.locks.is_empty()
                || q.fetch.is_some()
                || !q.limit_by.is_empty()
            {
                return Err(unsupported("query options"));
            }
            let SetExpr::Select(sel) = q.body.as_ref() else {
                return Err(unsupported("set query"));
            };
            if sel.from.len() > 1
                || sel.distinct.is_some()
                || sel.having.is_some()
                || sel.into.is_some()
                || !sel.named_window.is_empty()
                || sel.qualify.is_some()
                || sel.top.is_some()
                || !matches!(&sel.group_by,GroupByExpr::Expressions(v,_) if v.is_empty())
            {
                return Err(unsupported("SELECT options"));
            }
            let t = sel
                .from
                .first()
                .map(|f| table_from(session, f))
                .transpose()?;
            let mut b = Binder::new(t.as_ref(), hints);
            let mut fields = Vec::new();
            let mut exprs = Vec::new();
            for item in &sel.projection {
                match item {
                    SelectItem::Wildcard(_) => {
                        for c in &t
                            .as_ref()
                            .ok_or_else(|| unsupported("wildcard without table"))?
                            .columns
                        {
                            fields.push(Field {
                                name: c.name.clone(),
                                kind: c.kind,
                            });
                            exprs.push(quote(&c.name));
                        }
                    }
                    SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                        let (x, k) = b.expr(e, None)?;
                        let n = match item {
                            SelectItem::ExprWithAlias { alias, .. } => ident(alias)?,
                            _ => match e {
                                Expr::Identifier(i) => ident(i)?,
                                _ => "?column?".into(),
                            },
                        };
                        exprs.push(x);
                        fields.push(Field { name: n, kind: k });
                    }
                    _ => return Err(unsupported("projection")),
                }
            }
            let mut sql = format!("SELECT {}", exprs.join(","));
            if let Some(t) = &t {
                sql.push_str(&format!(" FROM {}", quote(&t.name)));
            }
            sql.push_str(&b.filter(&sel.selection)?);
            if let Some(order) = &q.order_by {
                let mut items = Vec::new();
                for o in &order.exprs {
                    let (x, _) = b.expr(&o.expr, None)?;
                    let asc = o.asc.unwrap_or(true);
                    let first = o.nulls_first.unwrap_or(!asc);
                    items.push(format!(
                        "{x} {} NULLS {}",
                        if asc { "ASC" } else { "DESC" },
                        if first { "FIRST" } else { "LAST" }
                    ));
                }
                sql.push_str(&format!(" ORDER BY {}", items.join(",")));
            }
            let mut nonnegative = Vec::new();
            if let Some(l) = &q.limit {
                sql.push_str(&format!(
                    " LIMIT {}",
                    pagination(&mut b, l, &mut nonnegative)?
                ));
            } else if q.offset.is_some() {
                sql.push_str(" LIMIT -1");
            }
            if let Some(o) = &q.offset {
                sql.push_str(&format!(
                    " OFFSET {}",
                    pagination(&mut b, &o.value, &mut nonnegative)?
                ));
            }
            let mut p = Plan::new(sql, "SELECT");
            p.fields = fields;
            p.nonnegative = nonnegative;
            b.finish(&mut p)?;
            Ok(p)
        }
        Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
            ..
        } => {
            if from.is_some() || returning.is_some() {
                return Err(unsupported("UPDATE options"));
            }
            let t = table_from(session, table)?;
            let mut b = Binder::new(Some(&t), hints);
            let mut sets = Vec::new();
            for a in assignments {
                let AssignmentTarget::ColumnName(n) = &a.target else {
                    return Err(unsupported("assignment"));
                };
                if n.0.len() != 1 {
                    return Err(unsupported("qualified assignment"));
                }
                let c = b.column(&n.0[0])?;
                let (cn, k) = (c.name.clone(), c.kind);
                let (x, _) = b.expr(&a.value, Some(k))?;
                sets.push(format!("{}={x}", quote(&cn)));
            }
            let mut p = Plan::new(
                format!(
                    "UPDATE {} SET {}{}",
                    quote(&t.name),
                    sets.join(","),
                    b.filter(selection)?
                ),
                "UPDATE",
            );
            b.finish(&mut p)?;
            Ok(p)
        }
        Statement::Delete(d) => {
            if d.using.is_some()
                || d.returning.is_some()
                || d.limit.is_some()
                || !d.order_by.is_empty()
                || !d.tables.is_empty()
            {
                return Err(unsupported("DELETE options"));
            }
            let from = match &d.from {
                FromTable::WithFromKeyword(v) | FromTable::WithoutKeyword(v) => v,
            };
            if from.len() != 1 {
                return Err(unsupported("DELETE tables"));
            }
            let t = table_from(session, &from[0])?;
            let mut b = Binder::new(Some(&t), hints);
            let mut p = Plan::new(
                format!("DELETE FROM {}{}", quote(&t.name), b.filter(&d.selection)?),
                "DELETE",
            );
            b.finish(&mut p)?;
            Ok(p)
        }
        Statement::ShowVariable { variable } if variable.len() == 1 => {
            let n = ident(&variable[0])?;
            let value = match n.as_str() {
                "transaction_isolation" | "default_transaction_isolation" => "repeatable read",
                "server_version" => "14.0",
                "client_encoding" | "server_encoding" => "UTF8",
                "standard_conforming_strings" => "on",
                _ => return Err(unsupported("SHOW setting")),
            };
            let mut p = Plan::new(format!("SELECT '{value}'"), "SHOW");
            p.fields.push(Field {
                name: n,
                kind: Kind::Text,
            });
            Ok(p)
        }
        _ => Err(unsupported("statement")),
    }
}
// Compare complete ASTs against a canonical skeleton so unhandled syntax is rejected,
// including options added by the parser that are not in our SQL contract.
fn validate_shape(s: &Statement) -> Result<()> {
    match s {
        Statement::CreateTable(c) => {
            let Statement::CreateTable(mut base) =
                parse("CREATE TABLE x (id INT PRIMARY KEY)")?.remove(0)
            else {
                unreachable!()
            };
            base.name = c.name.clone();
            base.columns = c.columns.clone();
            if &base != c {
                return Err(unsupported("CREATE TABLE options"));
            }
            for col in &c.columns {
                for option in &col.options {
                    if option.name.is_some() {
                        return Err(unsupported("named column constraints"));
                    }
                    if matches!(
                        &option.option,
                        ColumnOption::Unique {
                            characteristics: Some(_),
                            ..
                        }
                    ) {
                        return Err(unsupported("constraint characteristics"));
                    }
                }
            }
        }
        Statement::Insert(i) => {
            let Statement::Insert(mut base) = parse("INSERT INTO x VALUES (1)")?.remove(0) else {
                unreachable!()
            };
            base.table_name = i.table_name.clone();
            base.columns = i.columns.clone();
            base.source = i.source.clone();
            base.into = i.into;
            if &base != i {
                return Err(unsupported("INSERT options"));
            }
            if let Some(q) = &i.source {
                validate_query_shape(q, false)?;
            }
        }
        Statement::Query(q) => {
            validate_query_shape(q, true)?;
        }
        Statement::CreateIndex(c) if c.nulls_distinct.is_some() || !c.with.is_empty() => {
            return Err(unsupported("index options"));
        }
        Statement::Update { or: Some(_), .. } => return Err(unsupported("UPDATE conflict mode")),
        _ => {}
    }
    Ok(())
}
fn validate_query_shape(q: &Query, select: bool) -> Result<()> {
    let Statement::Query(mut base) = parse("SELECT 1")?.remove(0) else {
        unreachable!()
    };
    base.body = q.body.clone();
    if select {
        base.order_by = q.order_by.clone();
        base.limit = q.limit.clone();
        base.offset = q.offset.clone();
    }
    if base.as_ref() != q {
        return Err(unsupported("query clause"));
    }
    if let Some(o) = &q.order_by
        && o.interpolate.is_some()
    {
        return Err(unsupported("ORDER BY interpolation"));
    }
    if let SetExpr::Select(sel) = q.body.as_ref() {
        let Statement::Query(bq) = parse("SELECT 1")?.remove(0) else {
            unreachable!()
        };
        let SetExpr::Select(mut bs) = *bq.body else {
            unreachable!()
        };
        bs.select_token = sel.select_token.clone();
        bs.projection = sel.projection.clone();
        bs.from = sel.from.clone();
        bs.selection = sel.selection.clone();
        if &bs != sel {
            return Err(unsupported("SELECT clause"));
        }
        for item in &sel.projection {
            if let SelectItem::Wildcard(options) = item
                && options != &WildcardAdditionalOptions::default()
            {
                return Err(unsupported("wildcard options"));
            }
        }
        for table in &sel.from {
            if let TableFactor::Table { name, .. } = &table.relation {
                let Statement::Query(bq) = parse(&format!("SELECT * FROM {name}"))?.remove(0)
                else {
                    unreachable!()
                };
                let SetExpr::Select(bs) = *bq.body else {
                    unreachable!()
                };
                if table != &bs.from[0] {
                    return Err(unsupported("table options or joins"));
                }
            }
        }
    }
    Ok(())
}

fn pagination(b: &mut Binder<'_>, e: &Expr, params: &mut Vec<usize>) -> Result<String> {
    match e {
        Expr::Value(Value::Number(n, _)) if n.parse::<i64>().is_ok_and(|v| v >= 0) => {}
        Expr::Value(Value::Placeholder(p)) => {
            let n = p
                .strip_prefix('$')
                .and_then(|p| p.parse::<usize>().ok())
                .filter(|n| *n > 0)
                .ok_or_else(|| unsupported("pagination parameter"))?;
            params.push(n - 1);
        }
        _ => {
            return Err(DbError::new(
                "2201W",
                "LIMIT/OFFSET require a nonnegative integer or parameter",
            ));
        }
    }
    Ok(b.expr(e, Some(Kind::Int8))?.0)
}
