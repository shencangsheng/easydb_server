use serde::Deserialize;
use crate::utils::FileType;

#[derive(Deserialize)]
pub struct Fetch {
    pub sql: String,
}

#[derive(Deserialize)]
pub struct ExportFile {
    pub sql: String,
    pub file_type: FileType,
}