mod adapter;
mod config;
mod mcp;
mod model;
mod registry;
mod tools;
mod validator;

use config::AppConfig;
use registry::DatabaseRegistry;
use std::path::Path;
use std::sync::Arc;
use tracing::info;
use tracing_appender::rolling;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    // Load config
    let config = load_config();

    // Setup file-only logging (no stdout - it's used for MCP protocol)
    setup_logging(&config.logging);

    info!("db-mcp {} starting", config.server.version);

    // Create registry
    let registry = Arc::new(DatabaseRegistry::new(config.defaults.clone()));

    // Pre-register configured databases
    for db in &config.databases {
        match registry
            .register(
                &db.db_id,
                &db.db_type,
                &db.url,
                db.username.as_deref(),
                db.password.as_deref(),
            )
            .await
        {
            Ok(info) => info!(
                "Pre-registered database '{}' (type={})",
                info.db_id, info.db_type
            ),
            Err(e) => tracing::error!(
                "Failed to pre-register database '{}': {}",
                db.db_id, e
            ),
        }
    }

    // Run MCP server
    mcp::run_server(config.server, registry).await;
}

fn load_config() -> AppConfig {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.yml".into());

    if Path::new(&config_path).exists() {
        let content = std::fs::read_to_string(&config_path).expect("Failed to read config file");
        serde_yaml::from_str(&content).expect("Failed to parse config file")
    } else {
        // Use defaults
        AppConfig {
            server: Default::default(),
            logging: Default::default(),
            defaults: Default::default(),
            databases: vec![],
        }
    }
}

fn setup_logging(logging_config: &config::LoggingConfig) {
    let log_path = Path::new(&logging_config.file);
    let log_dir = log_path.parent().unwrap_or(Path::new("."));
    let log_filename = log_path
        .file_name()
        .unwrap_or_default()
        .to_str()
        .unwrap_or("db-mcp.log");

    // Create log directory
    let _ = std::fs::create_dir_all(log_dir);

    let file_appender = rolling::never(log_dir, log_filename);
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    // Keep the guard alive for the lifetime of the program
    std::mem::forget(_guard);

    let filter = EnvFilter::try_new(&logging_config.level)
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_target(true)
        .init();
}
