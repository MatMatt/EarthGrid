//! EarthGrid Core — distributed storage and processing for Earth observation data.
//!
//! This crate provides the core components:
//! - [`chunk_store`] — Content-addressed storage (SHA-256)
//! - [`catalog`] — SQLite-backed STAC catalog
//! - [`auth`] — API key authentication
//! - [`audit`] — Mutation audit log
//! - [`error`] — Error types

pub mod audit;
pub mod auth;
pub mod catalog;
pub mod chunk_store;
pub mod error;
