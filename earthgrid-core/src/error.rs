//! EarthGrid error types.

use thiserror::Error;

/// All errors that can occur in earthgrid-core.
#[derive(Error, Debug)]
pub enum EarthGridError {
    #[error("Storage limit exceeded: {0}")]
    StorageLimitExceeded(String),

    #[error("Chunk not found: {0}")]
    ChunkNotFound(String),

    #[error("Item not found: {0}")]
    ItemNotFound(String),

    #[error("Collection not found: {0}")]
    CollectionNotFound(String),

    #[error("Integrity violation: expected {expected}, got {actual}")]
    IntegrityViolation { expected: String, actual: String },

    #[error("Authentication required")]
    AuthRequired,

    #[error("Insufficient permissions")]
    Forbidden,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, EarthGridError>;
