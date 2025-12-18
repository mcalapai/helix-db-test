use crate::helix_engine::types::GraphError;

pub const EDGE_SECONDARY_INDEX_DB_PREFIX: &str = "eidx::";

pub fn edge_secondary_index_db_name(index: &str) -> String {
    format!("{EDGE_SECONDARY_INDEX_DB_PREFIX}{index}")
}

pub fn secondary_index_not_found(index: &str) -> GraphError {
    GraphError::New(format!("Secondary Index {index} not found"))
}

pub fn edge_secondary_index_not_found(index: &str) -> GraphError {
    GraphError::New(format!("Edge Secondary Index {index} not found"))
}
