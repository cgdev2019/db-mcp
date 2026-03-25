use crate::persist;
use crate::registry::DatabaseRegistry;
use crate::validator;
use serde_json::{json, Value};
use std::sync::Arc;

pub fn tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "name": "register_database",
            "description": "Register a new database connection. Supported types: postgresql, mysql, sqlite. Returns the db_id to use in subsequent calls.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "dbId": { "type": "string", "description": "Unique identifier for this database" },
                    "type": { "type": "string", "description": "Database type: postgresql, mysql, sqlite" },
                    "url": { "type": "string", "description": "Connection URL (e.g. postgres://host:5432/mydb or jdbc:postgresql://host:5432/mydb)" },
                    "username": { "type": "string", "description": "Username (optional for SQLite)" },
                    "password": { "type": "string", "description": "Password (optional for SQLite)" }
                },
                "required": ["dbId", "type", "url"]
            }
        }),
        json!({
            "name": "unregister_database",
            "description": "Unregister a database connection and close its connection pool.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "dbId": { "type": "string", "description": "Database identifier" }
                },
                "required": ["dbId"]
            }
        }),
        json!({
            "name": "list_databases",
            "description": "List all registered database connections.",
            "inputSchema": {
                "type": "object",
                "properties": {}
            }
        }),
        json!({
            "name": "list_schemas",
            "description": "List all schemas (or databases for MySQL) in the connection.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "dbId": { "type": "string", "description": "Database identifier" }
                },
                "required": ["dbId"]
            }
        }),
        json!({
            "name": "list_tables",
            "description": "List all tables and views in a schema.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "dbId": { "type": "string", "description": "Database identifier" },
                    "schema": { "type": "string", "description": "Schema name (uses default if omitted)" }
                },
                "required": ["dbId"]
            }
        }),
        json!({
            "name": "describe_table",
            "description": "Get column definitions for a table, including types, nullability, defaults and primary keys.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "dbId": { "type": "string", "description": "Database identifier" },
                    "table": { "type": "string", "description": "Table name" },
                    "schema": { "type": "string", "description": "Schema name (uses default if omitted)" }
                },
                "required": ["dbId", "table"]
            }
        }),
        json!({
            "name": "query",
            "description": "Execute a read-only SQL query (SELECT) and return results as JSON. Limited to 1000 rows by default.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "dbId": { "type": "string", "description": "Database identifier" },
                    "sql": { "type": "string", "description": "SQL SELECT query" },
                    "maxRows": { "type": "integer", "description": "Maximum rows to return (default 1000)" }
                },
                "required": ["dbId", "sql"]
            }
        }),
        json!({
            "name": "execute",
            "description": "Execute a SQL statement (INSERT, UPDATE, DELETE, DDL). Returns affected row count and danger level assessment.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "dbId": { "type": "string", "description": "Database identifier" },
                    "sql": { "type": "string", "description": "SQL statement to execute" }
                },
                "required": ["dbId", "sql"]
            }
        }),
        json!({
            "name": "get_table_schema",
            "description": "Get the full schema definition of a table as JSON, including all columns, types, constraints and primary keys.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "dbId": { "type": "string", "description": "Database identifier" },
                    "table": { "type": "string", "description": "Table name" },
                    "schema": { "type": "string", "description": "Schema name (uses default if omitted)" }
                },
                "required": ["dbId", "table"]
            }
        }),
        json!({
            "name": "get_database_overview",
            "description": "Get an overview of all schemas and tables in a database, useful for understanding the database structure.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "dbId": { "type": "string", "description": "Database identifier" }
                },
                "required": ["dbId"]
            }
        }),
    ]
}

pub async fn handle_tool_call(
    registry: &Arc<DatabaseRegistry>,
    name: &str,
    args: &Value,
) -> Value {
    let result = match name {
        "register_database" => tool_register_database(registry, args).await,
        "unregister_database" => tool_unregister_database(registry, args).await,
        "list_databases" => tool_list_databases(registry).await,
        "list_schemas" => tool_list_schemas(registry, args).await,
        "list_tables" => tool_list_tables(registry, args).await,
        "describe_table" => tool_describe_table(registry, args).await,
        "query" => tool_query(registry, args).await,
        "execute" => tool_execute(registry, args).await,
        "get_table_schema" => tool_get_table_schema(registry, args).await,
        "get_database_overview" => tool_get_database_overview(registry, args).await,
        _ => Err(anyhow::anyhow!("Unknown tool: {}", name)),
    };

    match result {
        Ok(value) => value,
        Err(e) => json!({ "error": true, "message": e.to_string() }),
    }
}

// --- Tool implementations ---

async fn tool_register_database(
    registry: &Arc<DatabaseRegistry>,
    args: &Value,
) -> anyhow::Result<Value> {
    let db_id = args["dbId"].as_str().unwrap_or_default();
    let db_type = args["type"].as_str().unwrap_or_default();
    let url = args["url"].as_str().unwrap_or_default();
    let username = args["username"].as_str();
    let password = args["password"].as_str();

    let info = registry.register(db_id, db_type, url, username, password).await?;

    // Persist registration
    let _ = persist::add(persist::PersistedDatabase {
        db_id: db_id.to_string(),
        db_type: info.db_type.clone(),
        url: url.to_string(),
        username: username.map(|s| s.to_string()),
        password: password.map(|s| s.to_string()),
    });

    Ok(json!({
        "success": true,
        "dbId": info.db_id,
        "type": info.db_type,
        "message": format!("Database '{}' registered successfully", db_id)
    }))
}

async fn tool_unregister_database(
    registry: &Arc<DatabaseRegistry>,
    args: &Value,
) -> anyhow::Result<Value> {
    let db_id = args["dbId"].as_str().unwrap_or_default();
    registry.unregister(db_id).await?;

    // Remove from persistence
    let _ = persist::remove(db_id);

    Ok(json!({
        "success": true,
        "message": format!("Database '{}' unregistered successfully", db_id)
    }))
}

async fn tool_list_databases(registry: &Arc<DatabaseRegistry>) -> anyhow::Result<Value> {
    let databases = registry.list_all().await;
    Ok(json!(databases))
}

async fn tool_list_schemas(
    registry: &Arc<DatabaseRegistry>,
    args: &Value,
) -> anyhow::Result<Value> {
    let db_id = args["dbId"].as_str().unwrap_or_default();
    let adapter = registry.get(db_id).await?;
    let schemas = adapter.list_schemas().await?;
    Ok(json!({ "schemas": schemas }))
}

async fn tool_list_tables(
    registry: &Arc<DatabaseRegistry>,
    args: &Value,
) -> anyhow::Result<Value> {
    let db_id = args["dbId"].as_str().unwrap_or_default();
    let schema = args["schema"].as_str();
    let adapter = registry.get(db_id).await?;
    let tables = adapter.list_tables(schema).await?;
    Ok(json!({ "tables": tables }))
}

async fn tool_describe_table(
    registry: &Arc<DatabaseRegistry>,
    args: &Value,
) -> anyhow::Result<Value> {
    let db_id = args["dbId"].as_str().unwrap_or_default();
    let table = args["table"].as_str().unwrap_or_default();
    let schema = args["schema"].as_str();
    let adapter = registry.get(db_id).await?;
    let columns = adapter.describe_table(schema, table).await?;
    Ok(json!({ "columns": columns }))
}

async fn tool_query(
    registry: &Arc<DatabaseRegistry>,
    args: &Value,
) -> anyhow::Result<Value> {
    let db_id = args["dbId"].as_str().unwrap_or_default();
    let sql = args["sql"].as_str().unwrap_or_default();
    let max_rows = args["maxRows"]
        .as_u64()
        .unwrap_or(registry.defaults().max_rows as u64) as u32;
    let timeout = registry.defaults().statement_timeout_seconds;

    validator::validate_read_only(sql)?;
    let adapter = registry.get(db_id).await?;
    let result = adapter.query(sql, max_rows, timeout).await?;

    Ok(json!({
        "columns": result.columns,
        "rows": result.rows,
        "rowCount": result.row_count,
        "truncated": result.truncated
    }))
}

async fn tool_execute(
    registry: &Arc<DatabaseRegistry>,
    args: &Value,
) -> anyhow::Result<Value> {
    let db_id = args["dbId"].as_str().unwrap_or_default();
    let sql = args["sql"].as_str().unwrap_or_default();
    let timeout = registry.defaults().statement_timeout_seconds;

    let danger = validator::assess_danger(sql);
    let adapter = registry.get(db_id).await?;
    let affected = adapter.execute_sql(sql, timeout).await?;

    let mut result = json!({
        "success": true,
        "affectedRows": affected,
        "dangerLevel": danger.as_str()
    });

    if danger != validator::DangerLevel::Safe {
        result["warning"] = json!(match danger {
            validator::DangerLevel::Warning => "Statement modifies data without WHERE clause",
            validator::DangerLevel::Dangerous => "Statement performs destructive DDL operation",
            _ => unreachable!(),
        });
    }

    Ok(result)
}

async fn tool_get_table_schema(
    registry: &Arc<DatabaseRegistry>,
    args: &Value,
) -> anyhow::Result<Value> {
    let db_id = args["dbId"].as_str().unwrap_or_default();
    let table = args["table"].as_str().unwrap_or_default();
    let schema = args["schema"].as_str();

    let adapter = registry.get(db_id).await?;
    let effective_schema = schema.unwrap_or(adapter.default_schema());
    let columns = adapter.describe_table(Some(effective_schema), table).await?;

    Ok(json!({
        "dbId": db_id,
        "schema": effective_schema,
        "table": table,
        "columns": columns
    }))
}

async fn tool_get_database_overview(
    registry: &Arc<DatabaseRegistry>,
    args: &Value,
) -> anyhow::Result<Value> {
    let db_id = args["dbId"].as_str().unwrap_or_default();
    let info = registry.get_info(db_id).await?;
    let adapter = registry.get(db_id).await?;

    let schemas = adapter.list_schemas().await?;
    let mut schema_details = Vec::new();

    for schema_info in &schemas {
        let tables = adapter.list_tables(Some(&schema_info.name)).await?;
        schema_details.push(json!({
            "schema": schema_info.name,
            "tableCount": tables.len(),
            "tables": tables.iter().map(|t| json!({
                "name": t.name,
                "type": t.table_type
            })).collect::<Vec<_>>()
        }));
    }

    Ok(json!({
        "dbId": db_id,
        "type": info.db_type,
        "schemas": schema_details
    }))
}
