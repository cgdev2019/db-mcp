use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct SchemaInfo {
    pub name: String,
    pub catalog: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableInfo {
    pub schema: String,
    pub name: String,
    #[serde(rename = "type")]
    pub table_type: String,
    pub remarks: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
    pub size: i32,
    pub nullable: bool,
    #[serde(rename = "defaultValue")]
    pub default_value: Option<String>,
    #[serde(rename = "primaryKey")]
    pub primary_key: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    #[serde(rename = "rowCount")]
    pub row_count: usize,
    pub truncated: bool,
}
