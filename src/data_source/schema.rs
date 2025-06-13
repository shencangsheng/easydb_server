use serde::Deserialize;

#[derive(Debug, PartialEq, Deserialize)]
pub enum DataSourceFormat {
    CSV,
    JSON,
    NdJson { file_extension: String },
    TSV,
    XLSX
}
