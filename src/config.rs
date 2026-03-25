use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub defaults: DefaultsConfig,
    #[serde(default)]
    pub databases: Vec<DatabaseEntry>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_name")]
    pub name: String,
    #[serde(default = "default_version")]
    pub version: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            name: default_name(),
            version: default_version(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct LoggingConfig {
    #[serde(default = "default_log_file")]
    pub file: String,
    #[serde(default = "default_log_level")]
    pub level: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            file: default_log_file(),
            level: default_log_level(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct DefaultsConfig {
    #[serde(default = "default_pool_size")]
    pub max_pool_size: u32,
    #[serde(default = "default_conn_timeout")]
    pub connection_timeout_ms: u64,
    #[serde(default = "default_stmt_timeout")]
    pub statement_timeout_seconds: u64,
    #[serde(default = "default_max_rows")]
    pub max_rows: u32,
}

impl Default for DefaultsConfig {
    fn default() -> Self {
        Self {
            max_pool_size: default_pool_size(),
            connection_timeout_ms: default_conn_timeout(),
            statement_timeout_seconds: default_stmt_timeout(),
            max_rows: default_max_rows(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseEntry {
    pub db_id: String,
    #[serde(rename = "type")]
    pub db_type: String,
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

fn default_name() -> String { "db-mcp".into() }
fn default_version() -> String { "1.0.0".into() }
fn default_log_file() -> String { "logs/db-mcp.log".into() }
fn default_log_level() -> String { "info".into() }
fn default_pool_size() -> u32 { 5 }
fn default_conn_timeout() -> u64 { 10000 }
fn default_stmt_timeout() -> u64 { 30 }
fn default_max_rows() -> u32 { 1000 }

/// Convert JDBC-style URLs to native URLs
pub fn normalize_url(url: &str, db_type: &str) -> String {
    let url = url.strip_prefix("jdbc:").unwrap_or(url);
    match db_type {
        "postgresql" | "postgres" => {
            if url.starts_with("postgresql://") {
                url.replacen("postgresql://", "postgres://", 1)
            } else {
                url.to_string()
            }
        }
        "oracle" => {
            // Strip jdbc:oracle:thin:@ prefix if present
            let url = url.strip_prefix("oracle:thin:@").unwrap_or(url);
            url.to_string()
        }
        "sybase" | "mssql" | "sqlserver" => {
            // Strip jdbc:sqlserver:// prefix if present
            let url = url.strip_prefix("sqlserver://").unwrap_or(url);
            url.to_string()
        }
        _ => url.to_string(),
    }
}
