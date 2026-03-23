//! openEO process graph and job types.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessGraph {
    pub process_graph: HashMap<String, ProcessNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessNode {
    pub process_id: String,
    pub arguments: serde_json::Value,
    #[serde(default)]
    pub result: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenEOBBox {
    pub west: f64,
    pub south: f64,
    pub east: f64,
    pub north: f64,
}

#[derive(Debug, Clone)]
pub struct DataRequirement {
    pub collection_id: String,
    pub spatial_extent: Option<OpenEOBBox>,
    pub temporal_extent: Option<Vec<String>>,
    pub bands: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct OperationInfo {
    pub operation: Option<String>,
    pub red: String,
    pub nir: String,
    pub green: String,
    pub blue: String,
    pub scl: String,
}

#[derive(Debug, Clone)]
pub struct RasterMeta {
    pub width: usize,
    pub height: usize,
    pub crs: String,
    pub transform: [f64; 6],
    pub dtype: String,
    /// Number of f32 bands stored BSQ in the pixel buffer (`width * height * 4` each).
    pub band_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResult {
    pub job_id: String,
    pub status: String,
    #[serde(skip_serializing)]
    pub data: Option<Vec<u8>>,
    pub errors: Vec<String>,
    pub created: f64,
    pub updated: f64,
}

pub type JobStore = Arc<Mutex<HashMap<String, JobResult>>>;
