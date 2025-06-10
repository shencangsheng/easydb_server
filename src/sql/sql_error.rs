use crate::sql::sql_error::SQLError::SQLSyntax;
use derive_more::{Display, Error};
use crate::response::http_error::Exception;
use crate::response::http_error::Exception::BadRequest;

#[derive(Debug, Display, Error, Clone)]
pub enum SQLError {
    #[display("SQL syntax error found: {message}, SQL: {sql}")]
    SQLSyntax { sql: String, message: String },
}

impl SQLError {
    pub fn sql_syntax_error(message: impl Into<String>, sql: &str) -> Self {
        SQLSyntax {
            message: message.into(),
            sql: sql.to_string(),
        }
    }
}

impl From<SQLError> for Exception {
    fn from(error: SQLError) -> Self {
        BadRequest {
            message: error.to_string(),
        }
    }
}
