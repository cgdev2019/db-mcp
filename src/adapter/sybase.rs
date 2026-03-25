use super::DatabaseAdapter;
use crate::model::*;
use anyhow::{Context, Result};
use async_trait::async_trait;
use odbc_api::{buffers::TextRowSet, ConnectionOptions, Cursor, Environment, IntoParameter, ResultSetMetadata};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct SybaseAdapter {
    env: Arc<Environment>,
    connection_string: String,
    conn_mutex: Mutex<()>,
}

impl SybaseAdapter {
    pub async fn new(
        host: &str,
        port: u16,
        database: &str,
        username: &str,
        password: &str,
        _connect_timeout_ms: u64,
    ) -> Result<Self> {
        let env = Environment::new().context("Failed to create ODBC environment")?;
        let env = Arc::new(env);

        let connection_string = format!(
            "DRIVER={{FreeTDS}};SERVER={};PORT={};DATABASE={};UID={};PWD={};TDS_Version=5.0;ClientCharset=UTF-8;",
            host, port, database, username, password
        );

        // Test connection
        {
            let _conn = env
                .connect_with_connection_string(&connection_string, ConnectionOptions::default())
                .context("Failed to connect to Sybase via ODBC/FreeTDS")?;
        }

        Ok(Self {
            env,
            connection_string,
            conn_mutex: Mutex::new(()),
        })
    }
}

fn get_column_names(cursor: &mut impl ResultSetMetadata) -> Result<Vec<String>> {
    let col_count = cursor.num_result_cols()? as u16;
    let mut columns = Vec::with_capacity(col_count as usize);
    for i in 1..=col_count {
        let name = cursor.col_name(i)?;
        columns.push(name);
    }
    Ok(columns)
}

fn extract_text_rows(
    mut cursor: impl Cursor,
    max_rows: usize,
) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>, bool)> {
    let columns = get_column_names(&mut cursor)?;
    let col_count = columns.len();

    let batch_size = max_rows.min(1000);
    let row_set = TextRowSet::for_cursor(batch_size, &mut cursor, Some(8192))?;
    let mut block_cursor = cursor.bind_buffer(row_set)?;

    let mut rows = Vec::new();
    let mut truncated = false;

    while let Some(batch) = block_cursor.fetch()? {
        for row_idx in 0..batch.num_rows() {
            if rows.len() >= max_rows {
                truncated = true;
                break;
            }
            let mut values = Vec::with_capacity(col_count);
            for col_idx in 0..col_count {
                let val = match batch.at(col_idx, row_idx) {
                    Some(bytes) => {
                        let s = String::from_utf8_lossy(bytes).to_string();
                        if let Ok(n) = s.parse::<i64>() {
                            serde_json::json!(n)
                        } else if let Ok(f) = s.parse::<f64>() {
                            serde_json::json!(f)
                        } else {
                            serde_json::Value::String(s)
                        }
                    }
                    None => serde_json::Value::Null,
                };
                values.push(val);
            }
            rows.push(values);
        }
        if truncated {
            break;
        }
    }

    Ok((columns, rows, truncated))
}

#[async_trait]
impl DatabaseAdapter for SybaseAdapter {
    fn db_type(&self) -> &str {
        "sybase"
    }
    fn default_schema(&self) -> &str {
        "dbo"
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        let _lock = self.conn_mutex.lock().await;
        let env = self.env.clone();
        let cs = self.connection_string.clone();
        tokio::task::spawn_blocking(move || {
            let conn = env
                .connect_with_connection_string(&cs, ConnectionOptions::default())?;
            let cursor = conn
                .execute("SELECT name FROM master..sysdatabases ORDER BY name", ())?
                .context("No result set")?;
            let (_, rows, _) = extract_text_rows(cursor, 1000)?;
            Ok(rows
                .into_iter()
                .map(|row| SchemaInfo {
                    name: row[0].as_str().unwrap_or("").to_string(),
                    catalog: None,
                })
                .collect())
        })
        .await?
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let _lock = self.conn_mutex.lock().await;
        let env = self.env.clone();
        let cs = self.connection_string.clone();
        let db = schema.unwrap_or("dbo").to_string();
        tokio::task::spawn_blocking(move || {
            let conn = env
                .connect_with_connection_string(&cs, ConnectionOptions::default())?;
            let cursor = conn
                .execute(
                    "SELECT user_name(uid) as table_schema, name as table_name, \
                     CASE type WHEN 'U' THEN 'TABLE' WHEN 'V' THEN 'VIEW' ELSE type END as table_type \
                     FROM sysobjects \
                     WHERE type IN ('U', 'V') \
                     ORDER BY name",
                    (),
                )?
                .context("No result set")?;
            let (_, rows, _) = extract_text_rows(cursor, 10000)?;
            Ok(rows
                .into_iter()
                .map(|row| TableInfo {
                    schema: row[0].as_str().unwrap_or(&db).to_string(),
                    name: row[1].as_str().unwrap_or("").to_string(),
                    table_type: row[2].as_str().unwrap_or("TABLE").to_string(),
                    remarks: None,
                })
                .collect())
        })
        .await?
    }

    async fn describe_table(&self, _schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        let _lock = self.conn_mutex.lock().await;
        let env = self.env.clone();
        let cs = self.connection_string.clone();
        let table = table.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = env
                .connect_with_connection_string(&cs, ConnectionOptions::default())?;

            // Get columns via sp_columns
            let param = table.as_str().into_parameter();
            let cursor = conn
                .execute("sp_columns @table_name = ?", &param)?
                .context("No result set from sp_columns")?;
            let (cols, rows, _) = extract_text_rows(cursor, 5000)?;

            let col_name_idx = cols.iter().position(|c| c.eq_ignore_ascii_case("COLUMN_NAME")).unwrap_or(3);
            let type_name_idx = cols.iter().position(|c| c.eq_ignore_ascii_case("TYPE_NAME")).unwrap_or(5);
            let length_idx = cols.iter().position(|c| c.eq_ignore_ascii_case("LENGTH")).unwrap_or(7);
            let nullable_idx = cols.iter().position(|c| c.eq_ignore_ascii_case("NULLABLE")).unwrap_or(10);

            // Get primary key columns via sp_pkeys
            let param2 = table.as_str().into_parameter();
            let pk_cols: Vec<String> = if let Some(pk_cursor) = conn.execute("sp_pkeys @table_name = ?", &param2)? {
                let (pk_hdrs, pk_rows, _) = extract_text_rows(pk_cursor, 100)?;
                let pk_col_idx = pk_hdrs.iter().position(|c| c.eq_ignore_ascii_case("COLUMN_NAME")).unwrap_or(3);
                pk_rows.into_iter()
                    .filter_map(|r| r.get(pk_col_idx).and_then(|v| v.as_str()).map(|s| s.to_string()))
                    .collect()
            } else {
                vec![]
            };

            Ok(rows
                .into_iter()
                .map(|row| {
                    let name = row.get(col_name_idx)
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    ColumnInfo {
                        primary_key: pk_cols.contains(&name),
                        name,
                        col_type: row.get(type_name_idx)
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        size: row.get(length_idx)
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0) as i32,
                        nullable: row.get(nullable_idx)
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0) == 1,
                        default_value: None,
                    }
                })
                .collect())
        })
        .await?
    }

    async fn query(&self, sql: &str, max_rows: u32, timeout_secs: u64) -> Result<QueryResult> {
        let _lock = self.conn_mutex.lock().await;
        let env = self.env.clone();
        let cs = self.connection_string.clone();
        let sql = sql.to_string();
        let limit = max_rows as usize;

        tokio::time::timeout(
            Duration::from_secs(timeout_secs),
            tokio::task::spawn_blocking(move || {
                let conn = env
                    .connect_with_connection_string(&cs, ConnectionOptions::default())?;
                let cursor = conn
                    .execute(&sql, ())?
                    .context("No result set")?;
                let (columns, rows, truncated) = extract_text_rows(cursor, limit)?;
                let row_count = rows.len();
                Ok::<_, anyhow::Error>(QueryResult {
                    columns,
                    rows,
                    row_count,
                    truncated,
                })
            }),
        )
        .await
        .map_err(|_| anyhow::anyhow!("Query timed out after {}s", timeout_secs))?
        .map_err(|e| anyhow::anyhow!("Query failed: {}", e))?
    }

    async fn execute_sql(&self, sql: &str, timeout_secs: u64) -> Result<u64> {
        let _lock = self.conn_mutex.lock().await;
        let env = self.env.clone();
        let cs = self.connection_string.clone();
        let sql = sql.to_string();

        tokio::time::timeout(
            Duration::from_secs(timeout_secs),
            tokio::task::spawn_blocking(move || -> Result<u64> {
                let conn = env
                    .connect_with_connection_string(&cs, ConnectionOptions::default())?;
                let result = match conn.execute(&sql, ())? {
                    Some(_cursor) => Ok(0),
                    None => Ok(0),
                };
                result
            }),
        )
        .await
        .map_err(|_| anyhow::anyhow!("Statement timed out after {}s", timeout_secs))?
        .map_err(|e| anyhow::anyhow!("Execute failed: {}", e))?
    }

    async fn close(&self) {
        // Connections are per-call, nothing to close
    }
}
