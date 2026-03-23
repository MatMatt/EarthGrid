//! openEO API v1.2.0 gateway for EarthGrid .
//!
//! Code is split under `graph`, `execute`, `output`, `geoprocess` (period labels, reducers, `gdalwarp`),
//! `catalogue`, and `api`. Items used outside this crate are re-exported below (`execute_sync`, types, …).

pub(crate) const API_VERSION: &str = "1.2.0";
pub(crate) const BACKEND_VERSION: &str = "0.3.0";

mod api;
mod catalogue;
mod execute;
pub mod geoprocess;
mod graph;
mod output;
mod types;

pub use api::{openeo_router, CreateJobRequest, OpenEOState};
pub use execute::execute_sync;
pub use graph::{
    extract_aggregate_temporal_period, extract_band_from_item_id, extract_operation,
    extract_output_format, extract_resample_spatial, extract_ymd_from_item_id, parse_requirements,
};
pub use output::{
    canonical_format, decode_geotiff_bsq_f32, decode_geotiff_first_band_f32, wrap_geotiff,
    wrap_netcdf, wrap_output,
};
pub use types::{
    DataRequirement, JobResult, JobStore, OpenEOBBox, OperationInfo, ProcessGraph, ProcessNode,
    RasterMeta,
};

#[cfg(test)]
mod tests;
