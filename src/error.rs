use std::{fmt, result};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::sqlparser::parser::ParserError;

#[derive(Debug)]
pub enum CoreError {
    UnsupportedSqlError(String),
    ParserError(ParserError),
    DataFusionError(DataFusionError)
}

pub type Result<T, E = CoreError> = result::Result<T, E>;
