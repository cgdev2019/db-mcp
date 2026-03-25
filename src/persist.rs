use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::{info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedDatabase {
    pub db_id: String,
    #[serde(rename = "type")]
    pub db_type: String,
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

fn persist_path() -> PathBuf {
    let exe = std::env::current_exe().unwrap_or_default();
    exe.parent().unwrap_or(std::path::Path::new(".")).join("databases.json")
}

pub fn load() -> Vec<PersistedDatabase> {
    let path = persist_path();
    if !path.exists() {
        return vec![];
    }
    match std::fs::read_to_string(&path) {
        Ok(content) => match serde_json::from_str(&content) {
            Ok(dbs) => {
                info!("Loaded {} persisted databases from {}", Vec::<PersistedDatabase>::len(&dbs), path.display());
                dbs
            }
            Err(e) => {
                warn!("Failed to parse {}: {}", path.display(), e);
                vec![]
            }
        },
        Err(e) => {
            warn!("Failed to read {}: {}", path.display(), e);
            vec![]
        }
    }
}

pub fn save(databases: &[PersistedDatabase]) -> Result<()> {
    let path = persist_path();
    let content = serde_json::to_string_pretty(databases)?;
    std::fs::write(&path, content)?;
    info!("Saved {} databases to {}", databases.len(), path.display());
    Ok(())
}

pub fn add(entry: PersistedDatabase) -> Result<()> {
    let mut dbs = load();
    // Remove existing with same id
    dbs.retain(|d| d.db_id != entry.db_id);
    dbs.push(entry);
    save(&dbs)
}

pub fn remove(db_id: &str) -> Result<()> {
    let mut dbs = load();
    dbs.retain(|d| d.db_id != db_id);
    save(&dbs)
}
