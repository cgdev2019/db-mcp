use super::DatabaseAdapter;
use crate::model::*;
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{Column, Row, TypeInfo, ValueRef};
use std::time::Duration;

pub struct PostgresAdapter {
    pool: PgPool,
}

impl PostgresAdapter {
    pub async fn new(url: &str, max_pool_size: u32, connect_timeout_ms: u64) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_pool_size)
            .acquire_timeout(Duration::from_millis(connect_timeout_ms))
            .idle_timeout(Duration::from_secs(300))
            .connect(url)
            .await?;
        Ok(Self { pool })
    }
}

fn pg_row_to_values(row: &PgRow) -> Vec<serde_json::Value> {
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
                "BOOL" => row
                    .try_get::<bool, _>(i)
                    .map(|v| serde_json::json!(v))
                    .unwrap_or(serde_json::Value::Null),
                "INT2" => row
                    .try_get::<i16, _>(i)
                    .map(|v| serde_json::json!(v))
                    .unwrap_or(serde_json::Value::Null),
                "INT4" => row
                    .try_get::<i32, _>(i)
                    .map(|v| serde_json::json!(v))
                    .unwrap_or(serde_json::Value::Null),
                "INT8" => row
                    .try_get::<i64, _>(i)
                    .map(|v| serde_json::json!(v))
                    .unwrap_or(serde_json::Value::Null),
                "FLOAT4" => row
                    .try_get::<f32, _>(i)
                    .map(|v| serde_json::json!(v))
                    .unwrap_or(serde_json::Value::Null),
                "FLOAT8" => row
                    .try_get::<f64, _>(i)
                    .map(|v| serde_json::json!(v))
                    .unwrap_or(serde_json::Value::Null),
                "JSON" | "JSONB" => row
                    .try_get::<serde_json::Value, _>(i)
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
impl DatabaseAdapter for PostgresAdapter {
    fn db_type(&self) -> &str {
        "postgresql"
    }
    fn default_schema(&self) -> &str {
        "public"
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        let rows = sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT schema_name, catalog_name FROM information_schema.schemata ORDER BY schema_name",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(name, catalog)| SchemaInfo { name, catalog })
            .collect())
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let effective = schema.unwrap_or("public");
        let rows = sqlx::query_as::<_, (String, String, String, Option<String>)>(
            "SELECT table_schema, table_name, table_type, NULL as remarks \
             FROM information_schema.tables \
             WHERE table_schema = $1 \
             ORDER BY table_name",
        )
        .bind(effective)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(schema, name, table_type, remarks)| TableInfo {
                schema,
                name,
                table_type,
                remarks,
            })
            .collect())
    }

    async fn describe_table(&self, schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        let effective = schema.unwrap_or("public");

        // Get primary key columns
        let pk_rows = sqlx::query_as::<_, (String,)>(
            "SELECT kcu.column_name \
             FROM information_schema.table_constraints tc \
             JOIN information_schema.key_column_usage kcu \
               ON tc.constraint_name = kcu.constraint_name \
              AND tc.table_schema = kcu.table_schema \
             WHERE tc.constraint_type = 'PRIMARY KEY' \
               AND tc.table_schema = $1 AND tc.table_name = $2",
        )
        .bind(effective)
        .bind(table)
        .fetch_all(&self.pool)
        .await?;

        let pk_cols: Vec<String> = pk_rows.into_iter().map(|(name,)| name).collect();

        let rows = sqlx::query_as::<_, (String, String, Option<i32>, String, Option<String>)>(
            "SELECT column_name, data_type, character_maximum_length, is_nullable, column_default \
             FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
             ORDER BY ordinal_position",
        )
        .bind(effective)
        .bind(table)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(name, col_type, size, nullable, default_value)| ColumnInfo {
                primary_key: pk_cols.contains(&name),
                name,
                col_type,
                size: size.unwrap_or(0),
                nullable: nullable == "YES",
                default_value,
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
                    rows.push(pg_row_to_values(&row));
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
