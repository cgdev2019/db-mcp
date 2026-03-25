use super::DatabaseAdapter;
use crate::model::*;
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct OracleAdapter {
    conn: Arc<Mutex<oracle::Connection>>,
}

impl OracleAdapter {
    pub async fn new(
        connect_string: &str,
        username: &str,
        password: &str,
        connect_timeout_ms: u64,
    ) -> Result<Self> {
        let connect_string = connect_string.to_string();
        let username = username.to_string();
        let password = password.to_string();

        let conn = tokio::time::timeout(
            Duration::from_millis(connect_timeout_ms),
            tokio::task::spawn_blocking(move || {
                oracle::Connection::connect(&username, &password, &connect_string)
                    .context("Failed to connect to Oracle")
            }),
        )
        .await
        .map_err(|_| anyhow::anyhow!("Oracle connection timed out"))??
        .context("Oracle connection failed")?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

fn oracle_value_to_json(row: &oracle::Row, idx: usize) -> serde_json::Value {
    // Try numeric types first, then string fallback
    if let Ok(v) = row.get::<usize, Option<i64>>(idx) {
        return v.map(|n| serde_json::json!(n)).unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.get::<usize, Option<f64>>(idx) {
        return v.map(|n| serde_json::json!(n)).unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.get::<usize, Option<String>>(idx) {
        return v.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null);
    }
    serde_json::Value::Null
}

#[async_trait]
impl DatabaseAdapter for OracleAdapter {
    fn db_type(&self) -> &str {
        "oracle"
    }
    fn default_schema(&self) -> &str {
        "" // resolved from connection user
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let rows = conn.query(
                "SELECT username FROM all_users ORDER BY username",
                &[],
            )?;

            let mut schemas = Vec::new();
            for row_result in rows {
                let row = row_result?;
                let name: String = row.get(0)?;
                schemas.push(SchemaInfo {
                    name,
                    catalog: None,
                });
            }
            Ok(schemas)
        })
        .await?
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let conn = self.conn.clone();
        let schema_owned = schema.map(|s| s.to_uppercase());
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let effective = match &schema_owned {
                Some(s) => s.clone(),
                None => {
                    let row = conn.query_row("SELECT USER FROM DUAL", &[])?;
                    row.get::<usize, String>(0)?
                }
            };

            let rows = conn.query(
                "SELECT owner, table_name, 'TABLE' as table_type, NULL as remarks \
                 FROM all_tables WHERE owner = :1 \
                 UNION ALL \
                 SELECT owner, view_name, 'VIEW', NULL \
                 FROM all_views WHERE owner = :1 \
                 ORDER BY 2",
                &[&effective],
            )?;

            let mut tables = Vec::new();
            for row_result in rows {
                let row = row_result?;
                tables.push(TableInfo {
                    schema: row.get(0)?,
                    name: row.get(1)?,
                    table_type: row.get(2)?,
                    remarks: row.get(3)?,
                });
            }
            Ok(tables)
        })
        .await?
    }

    async fn describe_table(&self, schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        let conn = self.conn.clone();
        let schema_owned = schema.map(|s| s.to_uppercase());
        let table_owned = table.to_uppercase();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let effective = match &schema_owned {
                Some(s) => s.clone(),
                None => {
                    let row = conn.query_row("SELECT USER FROM DUAL", &[])?;
                    row.get::<usize, String>(0)?
                }
            };

            // Get primary key columns
            let pk_rows = conn.query(
                "SELECT cols.column_name \
                 FROM all_constraints cons \
                 JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name \
                   AND cons.owner = cols.owner \
                 WHERE cons.constraint_type = 'P' \
                   AND cons.owner = :1 AND cons.table_name = :2",
                &[&effective, &table_owned],
            )?;
            let mut pk_cols: Vec<String> = Vec::new();
            for row_result in pk_rows {
                let row = row_result?;
                pk_cols.push(row.get(0)?);
            }

            let rows = conn.query(
                "SELECT column_name, data_type, data_length, nullable, data_default \
                 FROM all_tab_columns \
                 WHERE owner = :1 AND table_name = :2 \
                 ORDER BY column_id",
                &[&effective, &table_owned],
            )?;

            let mut columns = Vec::new();
            for row_result in rows {
                let row = row_result?;
                let name: String = row.get(0)?;
                columns.push(ColumnInfo {
                    primary_key: pk_cols.contains(&name),
                    name,
                    col_type: row.get(1)?,
                    size: row.get::<usize, Option<i32>>(2)?.unwrap_or(0),
                    nullable: row.get::<usize, String>(3)? == "Y",
                    default_value: row.get(4)?,
                });
            }
            Ok(columns)
        })
        .await?
    }

    async fn query(&self, sql: &str, max_rows: u32, timeout_secs: u64) -> Result<QueryResult> {
        let conn = self.conn.clone();
        let sql_owned = sql.to_string();
        let limit = max_rows as usize;

        tokio::time::timeout(
            Duration::from_secs(timeout_secs),
            tokio::task::spawn_blocking(move || {
                let conn = conn.blocking_lock();
                let mut stmt = conn.statement(&sql_owned).build()?;
                let result_set = stmt.query(&[])?;

                let columns: Vec<String> = result_set
                    .column_info()
                    .iter()
                    .map(|ci| ci.name().to_string())
                    .collect();

                let col_count = columns.len();
                let mut rows = Vec::new();
                let mut truncated = false;

                for row_result in result_set {
                    let row = row_result?;
                    if rows.len() < limit {
                        let values: Vec<serde_json::Value> = (0..col_count)
                            .map(|i| oracle_value_to_json(&row, i))
                            .collect();
                        rows.push(values);
                    } else {
                        truncated = true;
                        break;
                    }
                }

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
        .map_err(|e| anyhow::anyhow!("Query task failed: {}", e))?
    }

    async fn execute_sql(&self, sql: &str, timeout_secs: u64) -> Result<u64> {
        let conn = self.conn.clone();
        let sql_owned = sql.to_string();

        tokio::time::timeout(
            Duration::from_secs(timeout_secs),
            tokio::task::spawn_blocking(move || {
                let conn = conn.blocking_lock();
                let stmt = conn.execute(&sql_owned, &[])?;
                let rows_affected = stmt.row_count()?;
                conn.commit()?;
                Ok::<_, anyhow::Error>(rows_affected as u64)
            }),
        )
        .await
        .map_err(|_| anyhow::anyhow!("Statement timed out after {}s", timeout_secs))?
        .map_err(|e| anyhow::anyhow!("Execute task failed: {}", e))?
    }

    async fn close(&self) {
        let conn = self.conn.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let _ = conn.close();
        })
        .await;
    }
}
