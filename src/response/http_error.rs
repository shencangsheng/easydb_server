use crate::response::http_error::Exception::*;
use crate::response::schema::HttpResponseError;
use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use backtrace::Backtrace;
use calamine::XlsxError;
use datafusion::common::DataFusionError;
use datafusion::sql::sqlparser::parser::ParserError;
use derive_more::{Display, Error};
use glob::{GlobError, PatternError};

#[derive(Debug)]
struct ExceptionAttributes {
    status_code: StatusCode,
    resp_code: i32,
    resp_msg: String,
}

impl ExceptionAttributes {
    fn new(message: &String, status_code: StatusCode) -> Self {
        ExceptionAttributes {
            resp_code: 1,
            status_code,
            resp_msg: message.clone(),
        }
    }
}

#[derive(Debug, Display, Error, Clone)]
pub enum Exception {
    #[display("Internal server error {message}")]
    InternalServer { message: String },
    #[display("Bad Request: {message}")]
    BadRequest { message: String },
    #[display("File not found: {file_name}")]
    FileNotFound { file_name: String },
    #[display("The data is not as expected. Expected: {message}")]
    UnprocessableEntity { message: String },
}

impl ResponseError for Exception {
    fn error_response(&self) -> HttpResponse {
        self.log_error();
        let attributes = self.attributes();
        let error_response = HttpResponseError {
            resp_msg: attributes.resp_msg,
            resp_code: attributes.resp_code,
        };
        HttpResponse::build(attributes.status_code).json(error_response)
    }
}

impl Exception {
    fn attributes(&self) -> ExceptionAttributes {
        match self {
            BadRequest { message } => ExceptionAttributes::new(message, StatusCode::BAD_REQUEST),
            FileNotFound { file_name } => ExceptionAttributes::new(
                &format!("File not found: {}", file_name),
                StatusCode::NOT_FOUND,
            ),
            InternalServer { message } => {
                ExceptionAttributes::new(message, StatusCode::INTERNAL_SERVER_ERROR)
            }
            UnprocessableEntity { message } => {
                ExceptionAttributes::new(message, StatusCode::UNPROCESSABLE_ENTITY)
            }
        }
    }

    fn log_error(&self) {
        eprintln!("Error: {:?}", self)
    }

    pub fn internal_server_error(message: impl Into<String>) -> Self {
        InternalServer {
            message: message.into(),
        }
    }

    pub fn bad_request_error(message: impl Into<String>) -> Self {
        BadRequest {
            message: message.into(),
        }
    }

    pub fn file_not_found_error(file_name: impl Into<String>) -> Self {
        FileNotFound {
            file_name: file_name.into(),
        }
    }

    pub fn unprocessable_entity_error(message: impl Into<String>) -> Self {
        UnprocessableEntity {
            message: message.into(),
        }
    }
}

impl From<DataFusionError> for Exception {
    fn from(error: DataFusionError) -> Self {
        println!("Error: {:?}", Backtrace::new());
        BadRequest {
            message: error.to_string(),
        }
    }
}

impl From<ParserError> for Exception {
    fn from(error: ParserError) -> Self {
        println!("Error: {:?}", Backtrace::new());
        BadRequest {
            message: error.to_string(),
        }
    }
}

impl From<rusqlite::Error> for Exception {
    fn from(error: rusqlite::Error) -> Self {
        println!("Error: {:?}", Backtrace::new());
        BadRequest {
            message: error.to_string(),
        }
    }
}

impl From<arrow::error::ArrowError> for Exception {
    fn from(error: arrow::error::ArrowError) -> Self {
        println!("Error: {:?}", Backtrace::new());
        BadRequest {
            message: error.to_string(),
        }
    }
}

impl From<actix_web::Error> for Exception {
    fn from(error: actix_web::Error) -> Self {
        println!("Error: {:?}", Backtrace::new());
        BadRequest {
            message: error.to_string(),
        }
    }
}

impl From<serde_json::Error> for Exception {
    fn from(error: serde_json::Error) -> Self {
        println!("Error: {:?}", Backtrace::new());
        BadRequest {
            message: error.to_string(),
        }
    }
}

impl From<PatternError> for Exception {
    fn from(error: PatternError) -> Self {
        println!("Error: {:?}", Backtrace::new());
        BadRequest {
            message: error.to_string(),
        }
    }
}

impl From<GlobError> for Exception {
    fn from(error: GlobError) -> Self {
        println!("Error: {:?}", Backtrace::new());
        BadRequest {
            message: error.to_string(),
        }
    }
}

impl From<std::io::Error> for Exception {
    fn from(error: std::io::Error) -> Self {
        println!("Error: {:?}", Backtrace::new());
        BadRequest {
            message: error.to_string(),
        }
    }
}

impl From<calamine::Error> for Exception {
    fn from(error: calamine::Error) -> Self {
        println!("Error: {:?}", Backtrace::new());
        BadRequest {
            message: error.to_string(),
        }
    }
}

impl From<XlsxError> for Exception {
    fn from(error: XlsxError) -> Self {
        println!("Error: {:?}", Backtrace::new());
        BadRequest {
            message: error.to_string(),
        }
    }
}