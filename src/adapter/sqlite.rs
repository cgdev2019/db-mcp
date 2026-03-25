use super::DatabaseAdapter;
use crate::model::*;
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions, SqliteRow};
use sqlx::{Column, Row, TypeInfo, ValueRef};
use std::time::Duration;

pub struct SqliteAdapter {
    pool: SqlitePool,
}

impl SqliteAdapter {
    pub async fn new(url: &str, max_pool_size: u32, connect_timeout_ms: u64) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(max_pool_size)
            .acquire_timeout(Duration::from_millis(connect_timeout_ms))
            .idle_timeout(Duration::from_secs(300))
            .connect(url)
            .await?;
        Ok(Self { pool })
    }
}

fn sqlite_row_to_values(row: &SqliteRow) -> Vec<serde_json::Value> {
    row.columns()
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let raw = row.try_get_raw(i).unwrap();
            if raw.is_null() {
                return serde_json::Value::Null;
            }
            let type_name = col.type_info().name();
            match type_name {
                "INTEGER" | "BIGINT" | "INT" => row
                    .try_get::<i64, _>(i)
                    .map(|v| serde_json::json!(v))
                    .unwrap_or(serde_json::Value::Null),
                "REAL" | "FLOAT" | "DOUBLE" => row
                    .try_get::<f64, _>(i)
                    .map(|v| serde_json::json!(v))
                    .unwrap_or(serde_json::Value::Null),
                "BOOLEAN" => row
                    .try_get::<bool, _>(i)
                    .map(|v| serde_json::json!(v))
                    .unwrap_or(serde_json::Value::Null),
                _ => row
                    .try_get::<String, _>(i)
                    .map(serde_json::Value::String)
                    .unwrap_or_else(|_| serde_json::json!("<unsupported>")),
            }
        })
        .collect()
}

#[async_trait]
impl DatabaseAdapter for SqliteAdapter {
    fn db_type(&self) -> &str {
        "sqlite"
    }
    fn default_schema(&self) -> &str {
        "main"
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        Ok(vec![SchemaInfo {
            name: "main".into(),
            catalog: None,
        }])
    }

    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let rows = sqlx::query_as::<_, (String, String)>(
            "SELECT name, type FROM sqlite_master \
             WHERE type IN ('table','view') \
             ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(name, table_type)| TableInfo {
                schema: "main".into(),
                name,
                table_type: table_type.to_uppercase(),
                remarks: None,
            })
            .collect())
    }

    async fn describe_table(&self, _schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        // Escape single quotes in table name
        let safe_table = table.replace('\'', "''");
        let sql = format!("PRAGMA table_info('{}')", safe_table);

        let rows =
            sqlx::query_as::<_, (i32, String, String, i32, Option<String>, i32)>(&sql)
                .fetch_all(&self.pool)
                .await?;

        Ok(rows
            .into_iter()
            .map(|(_cid, name, col_type, notnull, dflt_value, pk)| ColumnInfo {
                name,
                col_type,
                size: 0,
                nullable: notnull == 0,
                default_value: dflt_value,
                primary_key: pk > 0,
            })
            .collect())
    }

    async fn query(&self, sql: &str, max_rows: u32, timeout_secs: u64) -> Result<QueryResult> {
        let limit = max_rows as usize;

        let result = tokio::time::timeout(Duration::from_secs(timeout_secs), async {
            let mut stream = sqlx::query(sql).fetch(&self.pool);
            let mut columns: Option<Vec<String>> = None;
            let mut rows = Vec::new();
            let mut truncated = false;

            while let Some(row) = stream.next().await {
                let row = row?;
                if columns.is_none() {
                    columns = Some(
                        row.columns().iter().map(|c| c.name().to_string()).collect(),
                    );
                }
                if rows.len() < limit {
                    rows.push(sqlite_row_to_values(&row));
                } else {
                    truncated = true;
                    break;
                }
            }

            let columns = columns.unwrap_or_default();
            let row_count = rows.len();
            Ok::<_, anyhow::Error>(QueryResult {
                columns,
                rows,
                row_count,
                truncated,
            })
        })
        .await
        .map_err(|_| anyhow::anyhow!("Query timed out after {}s", timeout_secs))??;

        Ok(result)
    }

    async fn execute_sql(&self, sql: &str, timeout_secs: u64) -> Result<u64> {
        let result = tokio::time::timeout(Duration::from_secs(timeout_secs), async {
            let res = sqlx::query(sql).execute(&self.pool).await?;
            Ok::<_, anyhow::Error>(res.rows_affected())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Statement timed out after {}s", timeout_secs))??;

        Ok(result)
    }

    async fn close(&self) {
        self.pool.close().await;
    }
}
