pub mod mysql;
#[cfg(feature = "oracle")]
pub mod oracle;
pub mod postgres;
pub mod sqlite;
pub mod sybase;

use crate::model::{ColumnInfo, QueryResult, SchemaInfo, TableInfo};
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
#[allow(dead_code)]
pub trait DatabaseAdapter: Send + Sync {
    fn db_type(&self) -> &str;
    fn default_schema(&self) -> &str;

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>>;
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>>;
    async fn describe_table(&self, schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>>;
    async fn query(&self, sql: &str, max_rows: u32, timeout_secs: u64) -> Result<QueryResult>;
    async fn execute_sql(&self, sql: &str, timeout_secs: u64) -> Result<u64>;
    async fn close(&self);
}
