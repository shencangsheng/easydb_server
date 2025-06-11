use serde::{Deserialize, Serialize};

pub struct TableCatalog {
    pub table_name: String,
    pub table_path: String,
}

#[derive(Deserialize, Serialize)]
pub struct TableFieldSchema {
    pub field: String,
    pub field_type: String,
    pub comment: Option<String>,
}