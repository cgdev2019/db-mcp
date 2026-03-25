use crate::adapter::mysql::MysqlAdapter;
#[cfg(feature = "oracle")]
use crate::adapter::oracle::OracleAdapter;
use crate::adapter::postgres::PostgresAdapter;
use crate::adapter::sqlite::SqliteAdapter;
use crate::adapter::sybase::SybaseAdapter;
use crate::adapter::DatabaseAdapter;
use crate::config::{normalize_url, DefaultsConfig};
use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

#[derive(Clone, Serialize)]
pub struct DatabaseInfo {
    pub db_id: String,
    pub db_type: String,
    pub registered_at: DateTime<Utc>,
}

pub struct DatabaseConnection {
    pub info: DatabaseInfo,
    pub adapter: Arc<dyn DatabaseAdapter>,
}

pub struct DatabaseRegistry {
    connections: RwLock<HashMap<String, DatabaseConnection>>,
    defaults: DefaultsConfig,
}

impl DatabaseRegistry {
    pub fn new(defaults: DefaultsConfig) -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            defaults,
        }
    }

    pub async fn register(
        &self,
        db_id: &str,
        db_type: &str,
        url: &str,
        username: Option<&str>,
        password: Option<&str>,
    ) -> Result<DatabaseInfo> {
        let mut conns = self.connections.write().await;
        if conns.contains_key(db_id) {
            bail!("Database '{}' is already registered", db_id);
        }

        let db_type_lower = db_type.to_lowercase();
        let url = normalize_url(url, &db_type_lower);

        // Embed credentials in URL if provided
        let url = embed_credentials(&url, username, password);

        let adapter: Arc<dyn DatabaseAdapter> = match db_type_lower.as_str() {
            "postgresql" | "postgres" => Arc::new(
                PostgresAdapter::new(
                    &url,
                    self.defaults.max_pool_size,
                    self.defaults.connection_timeout_ms,
                )
                .await?,
            ),
            "mysql" => Arc::new(
                MysqlAdapter::new(
                    &url,
                    self.defaults.max_pool_size,
                    self.defaults.connection_timeout_ms,
                )
                .await?,
            ),
            "sqlite" => Arc::new(
                SqliteAdapter::new(
                    &url,
                    self.defaults.max_pool_size,
                    self.defaults.connection_timeout_ms,
                )
                .await?,
            ),
            "sybase" | "mssql" | "sqlserver" => {
                let (host, port, database) = parse_sybase_url(&url)?;
                let user = username.unwrap_or("");
                let pass = password.unwrap_or("");
                Arc::new(
                    SybaseAdapter::new(
                        &host,
                        port,
                        &database,
                        user,
                        pass,
                        self.defaults.connection_timeout_ms,
                    )
                    .await?,
                )
            }
            #[cfg(feature = "oracle")]
            "oracle" => {
                let user = username.unwrap_or("");
                let pass = password.unwrap_or("");
                Arc::new(
                    OracleAdapter::new(
                        &url,
                        user,
                        pass,
                        self.defaults.connection_timeout_ms,
                    )
                    .await?,
                )
            }
            _ => bail!(
                "Unsupported database type: '{}'. Supported: postgresql, mysql, sqlite, sybase, oracle",
                db_type
            ),
        };

        let info = DatabaseInfo {
            db_id: db_id.to_string(),
            db_type: db_type_lower,
            registered_at: Utc::now(),
        };

        let connection = DatabaseConnection {
            info: info.clone(),
            adapter,
        };

        info!("Registered database '{}' (type={})", db_id, info.db_type);
        conns.insert(db_id.to_string(), connection);
        Ok(info)
    }

    pub async fn unregister(&self, db_id: &str) -> Result<()> {
        let mut conns = self.connections.write().await;
        match conns.remove(db_id) {
            Some(conn) => {
                conn.adapter.close().await;
                info!("Unregistered database '{}'", db_id);
                Ok(())
            }
            None => bail!("Database '{}' is not registered", db_id),
        }
    }

    pub async fn get(&self, db_id: &str) -> Result<Arc<dyn DatabaseAdapter>> {
        let conns = self.connections.read().await;
        match conns.get(db_id) {
            Some(conn) => Ok(conn.adapter.clone()),
            None => bail!("Database '{}' is not registered", db_id),
        }
    }

    pub async fn get_info(&self, db_id: &str) -> Result<DatabaseInfo> {
        let conns = self.connections.read().await;
        match conns.get(db_id) {
            Some(conn) => Ok(conn.info.clone()),
            None => bail!("Database '{}' is not registered", db_id),
        }
    }

    pub async fn list_all(&self) -> Vec<DatabaseInfo> {
        let conns = self.connections.read().await;
        conns.values().map(|c| c.info.clone()).collect()
    }

    pub async fn has(&self, db_id: &str) -> bool {
        let conns = self.connections.read().await;
        conns.contains_key(db_id)
    }

    pub fn defaults(&self) -> &DefaultsConfig {
        &self.defaults
    }
}

/// Parse a Sybase/MSSQL URL into (host, port, database).
/// Accepts formats:
///   - host:port/database
///   - host:port
///   - host/database
///   - mssql://host:port/database
///   - jdbc:sqlserver://host:port;databaseName=db
fn parse_sybase_url(url: &str) -> Result<(String, u16, String)> {
    let url = url
        .strip_prefix("jdbc:sqlserver://")
        .or_else(|| url.strip_prefix("mssql://"))
        .or_else(|| url.strip_prefix("sybase://"))
        .unwrap_or(url);

    // Handle JDBC-style ;databaseName=xxx
    if url.contains(";databaseName=") {
        let parts: Vec<&str> = url.splitn(2, ';').collect();
        let host_port = parts[0];
        let db_name = parts
            .get(1)
            .and_then(|s| s.strip_prefix("databaseName="))
            .unwrap_or("master");

        let (host, port) = parse_host_port(host_port);
        return Ok((host, port, db_name.to_string()));
    }

    // Handle host:port/database
    let (host_port, database) = if let Some((hp, db)) = url.split_once('/') {
        (hp, db.to_string())
    } else {
        (url, "master".to_string())
    };

    let (host, port) = parse_host_port(host_port);
    Ok((host, port, database))
}

fn parse_host_port(s: &str) -> (String, u16) {
    if let Some((host, port_str)) = s.rsplit_once(':') {
        let port = port_str.parse::<u16>().unwrap_or(1433);
        (host.to_string(), port)
    } else {
        (s.to_string(), 1433)
    }
}

fn embed_credentials(url: &str, username: Option<&str>, password: Option<&str>) -> String {
    let user = username.filter(|u| !u.is_empty());
    let pass = password.filter(|p| !p.is_empty());

    if user.is_none() && pass.is_none() {
        return url.to_string();
    }

    // For URLs like postgres://host:port/db -> postgres://user:pass@host:port/db
    if let Some(rest) = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("mysql://"))
    {
        let scheme = if url.starts_with("postgres") {
            "postgres"
        } else {
            "mysql"
        };

        // Only embed if no credentials already in URL
        if !rest.contains('@') {
            let creds = match (user, pass) {
                (Some(u), Some(p)) => format!("{}:{}", u, p),
                (Some(u), None) => u.to_string(),
                _ => return url.to_string(),
            };
            return format!("{}://{}@{}", scheme, creds, rest);
        }
    }

    url.to_string()
}
