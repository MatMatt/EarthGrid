//! openEO process catalogue and capabilities.

pub(crate) fn process_catalogue() -> serde_json::Value {
    serde_json::json!({
        "processes": [
            {
                "id": "load_collection",
                "summary": "Load a collection from the current back-end by its id.",
                "description": "Loads a collection from the current back-end by its id and returns it as a processable data cube.",
                "parameters": [
                    {"name": "id", "description": "The collection id.", "schema": {"type": "string"}},
                    {"name": "spatial_extent", "description": "Bounding box.", "schema": {"type": "object"}},
                    {"name": "temporal_extent", "description": "Temporal filter.", "schema": {"type": "array"}},
                    {"name": "bands", "description": "Band names.", "schema": {"type": "array"}}
                ],
                "returns": {"description": "A data cube.", "schema": {"type": "object"}}
            },
            {
                "id": "save_result",
                "summary": "Save processed data.",
                "description": "Saves processed data to the given file format.",
                "parameters": [
                    {"name": "data", "description": "Data to save.", "schema": {"type": "object"}},
                    {"name": "format", "description": "Output format.", "schema": {"type": "string"}}
                ],
                "returns": {"description": "false", "schema": {"type": "boolean"}}
            },
            {
                "id": "ndvi",
                "summary": "Compute NDVI.",
                "description": "Normalized Difference Vegetation Index: (NIR - RED) / (NIR + RED)",
                "parameters": [
                    {"name": "data", "description": "Input data cube.", "schema": {"type": "object"}},
                    {"name": "nir", "description": "NIR band name.", "schema": {"type": "string"}, "default": "B08"},
                    {"name": "red", "description": "Red band name.", "schema": {"type": "string"}, "default": "B04"}
                ],
                "returns": {"description": "NDVI data cube.", "schema": {"type": "object"}}
            },
            {
                "id": "ndwi",
                "summary": "Compute NDWI.",
                "description": "Normalized Difference Water Index: (Green - NIR) / (Green + NIR)",
                "parameters": [
                    {"name": "data", "description": "Input data cube.", "schema": {"type": "object"}},
                    {"name": "green", "description": "Green band name.", "schema": {"type": "string"}, "default": "B03"},
                    {"name": "nir", "description": "NIR band name.", "schema": {"type": "string"}, "default": "B08"}
                ],
                "returns": {"description": "NDWI data cube.", "schema": {"type": "object"}}
            },
            {
                "id": "evi",
                "summary": "Compute EVI.",
                "description": "Enhanced Vegetation Index: 2.5 * (NIR - Red) / (NIR + 6*Red - 7.5*Blue + 1)",
                "parameters": [
                    {"name": "data", "description": "Input data cube.", "schema": {"type": "object"}},
                    {"name": "blue", "description": "Blue band name.", "schema": {"type": "string"}, "default": "B02"},
                    {"name": "red", "description": "Red band name.", "schema": {"type": "string"}, "default": "B04"},
                    {"name": "nir", "description": "NIR band name.", "schema": {"type": "string"}, "default": "B08"}
                ],
                "returns": {"description": "EVI data cube.", "schema": {"type": "object"}}
            },
            {
                "id": "cloud_mask",
                "summary": "Compute cloud mask from SCL.",
                "description": "Binary cloud mask from Scene Classification Layer. Cloud pixels (SCL 8/9/10) → 0, clear → 1.",
                "parameters": [
                    {"name": "data", "description": "Input data cube.", "schema": {"type": "object"}},
                    {"name": "scl", "description": "SCL band name.", "schema": {"type": "string"}, "default": "SCL"}
                ],
                "returns": {"description": "Cloud mask data cube.", "schema": {"type": "object"}}
            },
            {
                "id": "aggregate_temporal_period",
                "summary": "Temporal aggregations based on calendar hierarchies",
                "description": "Groups observations by calendar period (e.g. month) and applies a reducer. EarthGrid: NDVI graphs only; output is multiband GeoTIFF (one band per period label, sorted lexicographically).",
                "parameters": [
                    {"name": "data", "description": "Input data cube.", "schema": {"type": "object"}},
                    {"name": "period", "description": "Period: hour, day, week, dekad, month, season, tropical-season, year, decade, decade-ad.", "schema": {"type": "string"}},
                    {"name": "reducer", "description": "Reducer process graph (mean, min, max, median detected from JSON).", "schema": {"type": "object"}},
                    {"name": "dimension", "description": "Temporal dimension name (optional).", "schema": {"type": ["string", "null"]}}
                ],
                "returns": {"description": "Aggregated data cube.", "schema": {"type": "object"}}
            },
            {
                "id": "resample_spatial",
                "summary": "Resample and warp spatial dimensions",
                "description": "GDAL warp: target EPSG and/or resolution (-tr). Methods: near, bilinear, average, cubic, etc.",
                "parameters": [
                    {"name": "data", "description": "Raster data cube.", "schema": {"type": "object"}},
                    {"name": "resolution", "description": "Pixel size in target CRS units, or [x,y].", "schema": {"type": "object"}},
                    {"name": "projection", "description": "EPSG code (integer) or null.", "schema": {"type": "object"}},
                    {"name": "method", "description": "Resampling method (default near).", "schema": {"type": "string"}}
                ],
                "returns": {"description": "Resampled data cube.", "schema": {"type": "object"}}
            },
            {
                "id": "reduce_dimension",
                "summary": "Reduce a dimension.",
                "description": "Applies a reducer to a data cube dimension.",
                "parameters": [
                    {"name": "data", "description": "Data cube.", "schema": {"type": "object"}},
                    {"name": "reducer", "description": "Reducer process.", "schema": {"type": "object"}},
                    {"name": "dimension", "description": "Dimension name.", "schema": {"type": "string"}}
                ],
                "returns": {"description": "Reduced data cube.", "schema": {"type": "object"}}
            },
            {
                "id": "apply",
                "summary": "Apply a process to each value.",
                "description": "Applies a process to each value in the data cube.",
                "parameters": [
                    {"name": "data", "description": "Data cube.", "schema": {"type": "object"}},
                    {"name": "process", "description": "Process to apply.", "schema": {"type": "object"}}
                ],
                "returns": {"description": "Processed data cube.", "schema": {"type": "object"}}
            }
        ],
        "links": []
    })
}

pub(crate) fn capabilities(base_url: &str) -> serde_json::Value {
    serde_json::json!({
        "api_version": super::API_VERSION,
        "backend_version": super::BACKEND_VERSION,
        "stac_version": "1.0.0",
        "id": "earthgrid",
        "title": "EarthGrid openEO Backend",
        "description": "Distributed satellite data storage and openEO-compatible processing.",
        "production": false,
        "endpoints": [
            {"path": "/", "methods": ["GET"]},
            {"path": "/.well-known/openeo", "methods": ["GET"]},
            {"path": "/credentials/basic", "methods": ["GET"]},
            {"path": "/me", "methods": ["GET"]},
            {"path": "/collections", "methods": ["GET"]},
            {"path": "/collections/{collection_id}", "methods": ["GET"]},
            {"path": "/processes", "methods": ["GET"]},
            {"path": "/result", "methods": ["POST"]},
            {"path": "/jobs", "methods": ["GET", "POST"]},
            {"path": "/jobs/{job_id}", "methods": ["GET", "DELETE"]},
            {"path": "/jobs/{job_id}/results", "methods": ["GET"]},
            {"path": "/jobs/{job_id}/logs", "methods": ["GET"]}
        ],
        "links": [
            {"rel": "self", "href": format!("{}/", base_url)},
            {"rel": "conformance", "href": "https://openeo.net/openeo-api-spec/"},
            {"rel": "version-history", "href": format!("{}/.well-known/openeo", base_url)}
        ],
        "billing": null,
        "file_formats": {
            "output": [{
                "name": "GTiff",
                "title": "GeoTIFF",
                "gis_data_types": ["raster"],
                "parameters": {},
                "links": []
            }]
        }
    })
}
