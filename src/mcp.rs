use crate::config::ServerConfig;
use crate::registry::DatabaseRegistry;
use crate::tools;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{debug, error, info};

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    #[allow(dead_code)]
    jsonrpc: String,
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

impl JsonRpcResponse {
    fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: Some(result),
            error: None,
        }
    }

    fn error(id: Value, code: i32, message: String) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message,
                data: None,
            }),
        }
    }
}

pub async fn run_server(server_config: ServerConfig, registry: Arc<DatabaseRegistry>) {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut reader = BufReader::new(stdin);
    let mut writer = stdout;

    info!("MCP server starting (stdio transport)");

    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(0) => {
                info!("EOF received, shutting down");
                break;
            }
            Ok(_) => {}
            Err(e) => {
                error!("Error reading stdin: {}", e);
                break;
            }
        }

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        debug!("Received: {}", line);

        let request: JsonRpcRequest = match serde_json::from_str(line) {
            Ok(req) => req,
            Err(e) => {
                let resp = JsonRpcResponse::error(Value::Null, -32700, format!("Parse error: {}", e));
                let _ = write_response(&mut writer, &resp).await;
                continue;
            }
        };

        // Notifications (no id) don't get responses
        if request.id.is_none() {
            debug!("Notification: {}", request.method);
            continue;
        }

        let id = request.id.unwrap();
        let response = handle_request(&server_config, &registry, &request.method, &request.params, id.clone()).await;

        if let Err(e) = write_response(&mut writer, &response).await {
            error!("Error writing response: {}", e);
            break;
        }
    }
}

async fn write_response(
    writer: &mut io::Stdout,
    response: &JsonRpcResponse,
) -> io::Result<()> {
    let json = serde_json::to_string(response).unwrap();
    debug!("Sending: {}", json);
    writer.write_all(json.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

async fn handle_request(
    server_config: &ServerConfig,
    registry: &Arc<DatabaseRegistry>,
    method: &str,
    params: &Value,
    id: Value,
) -> JsonRpcResponse {
    match method {
        "initialize" => {
            JsonRpcResponse::success(
                id,
                json!({
                    "protocolVersion": "2024-11-05",
                    "capabilities": {
                        "tools": {}
                    },
                    "serverInfo": {
                        "name": server_config.name,
                        "version": server_config.version
                    }
                }),
            )
        }

        "ping" => JsonRpcResponse::success(id, json!({})),

        "tools/list" => {
            let tools = tools::tool_definitions();
            JsonRpcResponse::success(id, json!({ "tools": tools }))
        }

        "tools/call" => {
            let tool_name = params["name"].as_str().unwrap_or("");
            let arguments = &params["arguments"];

            info!("Tool call: {} with args: {}", tool_name, arguments);
            let result = tools::handle_tool_call(registry, tool_name, arguments).await;
            let text = serde_json::to_string_pretty(&result).unwrap_or_default();

            JsonRpcResponse::success(
                id,
                json!({
                    "content": [{
                        "type": "text",
                        "text": text
                    }]
                }),
            )
        }

        _ => JsonRpcResponse::error(id, -32601, format!("Method not found: {}", method)),
    }
}
