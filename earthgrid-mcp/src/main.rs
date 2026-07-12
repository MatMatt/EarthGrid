//! EarthGrid MCP Server
//!
//! Implements Model Context Protocol (MCP) over stdio (JSON-RPC).
//! Connects to a local EarthGrid API and exposes tools for AI agents
//! to trigger data fetches, query coverage, and search the catalog.
//!
//! Usage: earthgrid-mcp --api-key <key> [--api-url http://localhost:8400]

use clap::Parser;
use reqwest::Client;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{BufRead, BufReader, Write};

#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// EarthGrid API base URL
    #[arg(long, default_value = "http://localhost:8400")]
    api_url: String,
    /// EarthGrid API key (required — passed as x-api-key header)
    /// Can also be set via EARTHGRID_API_KEY environment variable.
    #[arg(long)]
    api_key: String,
}

// ── MCP JSON-RPC types ───────────────────────────────────────────

#[derive(Serialize)]
struct RpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
}

#[derive(Serialize)]
struct RpcError {
    code: i32,
    message: String,
}

#[derive(Deserialize)]
struct RpcRequest {
    #[serde(default)]
    id: Value,
    method: String,
    #[serde(default)]
    params: Value,
}

// ── Tool definitions ─────────────────────────────────────────────

fn tools_list() -> Value {
    serde_json::json!({
        "tools": [
            {
                "name": "earthgrid_fetch_enqueue",
                "description": "Enqueue a data fetch job on EarthGrid. Searches Element84 STAC and ingests matching Sentinel-2 scenes. Returns immediately with a job ID.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "bbox": {"type": "string", "description": "Bounding box as 'west,south,east,north' (e.g. '10.5,46.0,11.5,46.5')"},
                        "start_date": {"type": "string", "description": "Start date YYYY-MM-DD"},
                        "end_date": {"type": "string", "description": "End date YYYY-MM-DD"},
                        "collection": {"type": "string", "description": "STAC collection ID (default: sentinel-2-l2a)"},
                        "limit": {"type": "integer", "description": "Max items to fetch"},
                        "bands": {"type": "string", "description": "Comma-separated band list (e.g. 'B04,B08')"},
                        "cloud_cover": {"type": "integer", "description": "Max cloud cover percentage"}
                    },
                    "required": ["bbox", "start_date", "end_date"]
                }
            },
            {
                "name": "earthgrid_fetch_status",
                "description": "Get the status of a fetch job.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "integer", "description": "Job ID from earthgrid_fetch_enqueue"}
                    },
                    "required": ["job_id"]
                }
            },
            {
                "name": "earthgrid_fetch_list",
                "description": "List fetch jobs, optionally filtered by status.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string", "description": "Filter: pending, running, completed, failed"}
                    }
                }
            },
            {
                "name": "earthgrid_coverage",
                "description": "Get spatial coverage from the local node's catalog. Returns tiles with polygon geometry, date counts, and coverage percentages.",
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            },
            {
                "name": "earthgrid_catalog_search",
                "description": "Search STAC items in the local catalog.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "collection": {"type": "string", "description": "Collection ID"},
                        "bbox": {"type": "string", "description": "Bounding box 'west,south,east,north'"},
                        "limit": {"type": "integer", "description": "Max results (default: 10)"}
                    }
                }
            }
        ]
    })
}

// ── Tool handlers ────────────────────────────────────────────────

async fn handle_fetch_enqueue(client: &Client, api_url: &str, params: &Value) -> Result<Value, String> {
    let bbox = params["bbox"].as_str().ok_or("bbox required")?;
    let start = params["start_date"].as_str().ok_or("start_date required")?;
    let end = params["end_date"].as_str().ok_or("end_date required")?;
    let collection = params["collection"].as_str().unwrap_or("sentinel-2-l2a");
    let limit = params["limit"].as_u64().unwrap_or(3000);
    let bands = params["bands"].as_str().unwrap_or("");
    let cloud = params["cloud_cover"].as_u64().unwrap_or(100);

    let mut url = format!(
        "{}/api/fetch/queue?bbox={}&start_date={}&end_date={}&limit={}&collection={}",
        api_url.trim_end_matches('/'),
        bbox, start, end, limit, collection
    );
    if !bands.is_empty() {
        url.push_str(&format!("&bands={}", bands));
    }
    if cloud < 100 {
        url.push_str(&format!("&cloud_cover={}", cloud));
    }

    let resp = client.post(&url).send().await.map_err(|e| e.to_string())?;
    let body = resp.text().await.map_err(|e| e.to_string())?;
    serde_json::from_str(&body).map_err(|e| format!("parse: {} — body: {}", e, &body[..200.min(body.len())]))
}

async fn handle_fetch_status(client: &Client, api_url: &str, params: &Value) -> Result<Value, String> {
    let job_id = params["job_id"].as_u64().ok_or("job_id required")?;
    let url = format!("{}/api/fetch/queue/{}", api_url.trim_end_matches('/'), job_id);
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    resp.json().await.map_err(|e| e.to_string())
}

async fn handle_fetch_list(client: &Client, api_url: &str, params: &Value) -> Result<Value, String> {
    let mut url = format!("{}/api/fetch/queue", api_url.trim_end_matches('/'));
    if let Some(s) = params["status"].as_str() {
        url.push_str(&format!("?status={}", s));
    }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    resp.json().await.map_err(|e| e.to_string())
}

async fn handle_coverage(client: &Client, api_url: &str) -> Result<Value, String> {
    let url = format!("{}/api/coverage/spatial?source=local", api_url.trim_end_matches('/'));
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    resp.json().await.map_err(|e| e.to_string())
}

async fn handle_catalog_search(client: &Client, api_url: &str, params: &Value) -> Result<Value, String> {
    let collection = params["collection"].as_str().unwrap_or("sentinel-2-l2a");
    let limit = params["limit"].as_u64().unwrap_or(10);
    let mut url = format!(
        "{}/api/stac/collections/{}/items?limit={}",
        api_url.trim_end_matches('/'),
        collection, limit
    );
    if let Some(b) = params["bbox"].as_str() {
        url.push_str(&format!("&bbox={}", b));
    }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    resp.json().await.map_err(|e| e.to_string())
}

// ── Main ─────────────────────────────────────────────────────────

fn send_resp(id: Value, result: Option<Value>, error: Option<RpcError>) {
    let resp = RpcResponse {
        jsonrpc: "2.0",
        id,
        result,
        error,
    };
    let mut stdout = std::io::stdout().lock();
    let _ = serde_json::to_writer(&mut stdout, &resp);
    let _ = stdout.write_all(b"\n");
    let _ = stdout.flush();
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Build authenticated HTTP client
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-api-key",
        HeaderValue::from_str(&args.api_key).expect("invalid api-key characters"),
    );
    let client = Client::builder()
        .default_headers(headers)
        .build()
        .expect("failed to build HTTP client");

    let stdin = std::io::stdin().lock();

    // Notifications are never responded to.
    for line in BufReader::new(stdin).lines().map_while(Result::ok) {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }

        let req: RpcRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("MCP parse error: {} — input: {}", e, &line[..100.min(line.len())]);
                send_resp(
                    Value::Null,
                    None,
                    Some(RpcError { code: -32700, message: format!("Parse error: {}", e) }),
                );
                continue;
            }
        };

        match req.method.as_str() {
            // ── Lifecycle ──
            "initialize" => {
                send_resp(req.id, Some(serde_json::json!({
                    "protocolVersion": "2024-11-05",
                    "capabilities": { "tools": {} },
                    "serverInfo": {
                        "name": "earthgrid-mcp",
                        "version": "0.1.0"
                    }
                })), None);
            }
            "notifications/initialized" => {
                // No response for notifications
            }

            // ── Tools ──
            "tools/list" => {
                send_resp(req.id, Some(tools_list()), None);
            }
            "tools/call" => {
                let name = req.params["name"].as_str().unwrap_or("");
                let tool_params = req.params.get("arguments").cloned().unwrap_or(Value::Null);

                let result = match name {
                    "earthgrid_fetch_enqueue" => handle_fetch_enqueue(&client, &args.api_url, &tool_params).await,
                    "earthgrid_fetch_status" => handle_fetch_status(&client, &args.api_url, &tool_params).await,
                    "earthgrid_fetch_list" => handle_fetch_list(&client, &args.api_url, &tool_params).await,
                    "earthgrid_coverage" => handle_coverage(&client, &args.api_url).await,
                    "earthgrid_catalog_search" => handle_catalog_search(&client, &args.api_url, &tool_params).await,
                    _ => Err(format!("Unknown tool: {}", name)),
                };

                match result {
                    Ok(data) => {
                        send_resp(req.id, Some(serde_json::json!({
                            "content": [{ "type": "text", "text": serde_json::to_string_pretty(&data).unwrap_or_default() }]
                        })), None);
                    }
                    Err(e) => {
                        send_resp(req.id, Some(serde_json::json!({
                            "content": [{ "type": "text", "text": format!("Error: {}", e) }],
                            "isError": true
                        })), None);
                    }
                }
            }

            // ── Unknown ──
            _ => {
                send_resp(req.id, None, Some(RpcError {
                    code: -32601,
                    message: format!("Method not found: {}", req.method),
                }));
            }
        }
    }
}